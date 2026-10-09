"use client";

import { useMemo, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import MatchupGrid from "@/components/MatchupGrid";
import SortTable, { type Column } from "@/components/SortTable";
import IdentityCell from "@/components/IdentityCell";
import { Card, ErrorState, Loading, Signed } from "@/components/ui";
import { useTeamRatings } from "@/lib/api";
import { heat } from "@/lib/heat";
import { dateTimeET, longDate, pct, signed, signedPct } from "@/lib/format";
import type { Record3, TeamLineupPlayer, TeamRating, TeamsResponse } from "@/lib/types";
import { cn } from "@/lib/utils";

const SLOT_LABEL: Record<string, string> = { f1: "L1", f2: "L2", f3: "L3", f4: "L4", d1: "D1", d2: "D2", d3: "D3" };

const recordText = (r: Record3) => `${r.w}-${r.l}-${r.otl}`;

interface TeamScales {
  league: TeamsResponse["league"];
  gd: number; xgd: number; xgf: number; xga: number; fin: number; save: number; pp: number; pk: number;
}

/** Heat scales from all 32 teams (from the API); xGF/xGA are centred on the league average. */
function teamScales(data: TeamsResponse): TeamScales {
  const { gd, xgd, xgf, xga, fin, save, pp, pk } = data.scales;
  return { league: data.league, gd, xgd, xgf, xga, fin, save, pp, pk };
}

function columns(s: TeamScales, open: number | null): Column<TeamRating>[] {
  const { xg60_5v5: ev, xg60_pp: pp } = s.league;
  return [
    {
      key: "team", label: "Team", className: "h-px p-0", sort: (t) => t.abbr,
      render: (t) => (
        <IdentityCell abbr={t.abbr}>
          <span className="inline-flex items-center gap-1.5">
            <span className="font-medium">{t.place} {t.name}</span>
            {open === t.team_id ? <ChevronDown className="h-3.5 w-3.5 text-white/75" /> : <ChevronRight className="h-3.5 w-3.5 text-white/75" />}
          </span>
        </IdentityCell>
      ),
    },
    {
      key: "record", label: "Record", title: "This season (last season while no games played)", sort: (t) => t.record.pts_pct ?? t.prev_record?.pts_pct,
      render: (t) =>
        t.record.gp ? (
          <span>{recordText(t.record)}</span>
        ) : t.prev_record ? (
          <span className="text-muted-foreground">{recordText(t.prev_record)} (prev)</span>
        ) : (
          "–"
        ),
    },
    {
      key: "gd60", label: "5v5 GD/60", align: "right", className: "font-semibold",
      title: "5v5 goals for minus against per 60 vs. an average opponent, after finishing and goaltending",
      render: (t) => signed(t.gd60), sort: (t) => t.gd60, style: (t) => heat(t.gd60, s.gd),
    },
    {
      key: "xgd", label: "xGD/60", align: "right", title: "5v5 expected goals for minus against per 60 vs. an average opponent",
      render: (t) => signed(t.xgd60), sort: (t) => t.xgd60, style: (t) => heat(t.xgd60, s.xgd),
    },
    {
      key: "xgf", label: "xGF/60", align: "right", title: `5v5 expected goals for per 60 (league average ${ev.toFixed(2)})`,
      render: (t) => t.xgf60.toFixed(2), sort: (t) => t.xgf60, style: (t) => heat(t.xgf60, s.xgf, { center: ev }),
    },
    {
      key: "xga", label: "xGA/60", align: "right", title: `5v5 expected goals against per 60, lower is better (league average ${ev.toFixed(2)})`,
      render: (t) => t.xga60.toFixed(2), sort: (t) => -t.xga60, style: (t) => heat(t.xga60, s.xga, { center: ev, lowerIsBetter: true }),
    },
    {
      key: "fin", label: "Finishing", align: "right", title: "Shot-weighted shooter talent: goals per xG relative to average",
      render: (t) => signedPct(t.finishing), sort: (t) => t.finishing, style: (t) => heat(t.finishing, s.fin),
    },
    {
      key: "save", label: "Goaltending", align: "right", title: "Likely starters' save talent (weighted by recent starts): share of xG stopped beyond average",
      render: (t) => signedPct(t.save), sort: (t) => t.save, style: (t) => heat(t.save, s.save),
    },
    {
      key: "pp", label: "PP xGF/60", align: "right", title: `Power-play expected goals per 60 vs. an average PK (league average ${pp.toFixed(2)})`,
      render: (t) => t.pp_xgf60.toFixed(2), sort: (t) => t.pp_xgf60, style: (t) => heat(t.pp_xgf60, s.pp, { center: pp }),
    },
    {
      key: "pk", label: "PK xGA/60", align: "right", title: `Penalty-kill expected goals against per 60 vs. an average PP, lower is better (league average ${pp.toFixed(2)})`,
      render: (t) => t.pk_xga60.toFixed(2), sort: (t) => -t.pk_xga60, style: (t) => heat(t.pk_xga60, s.pk, { center: pp, lowerIsBetter: true }),
    },
    {
      key: "pen", label: "Pen Drawn/Taken", align: "right", title: "Penalty drawing and taking rates relative to league average (1.00 = average)",
      render: (t) => <span className="text-muted-foreground">{t.draw_f.toFixed(2)} / {t.take_f.toFixed(2)}</span>, sort: (t) => t.draw_f - t.take_f,
    },
  ];
}

function LineupDetail({ team }: { team: TeamRating }) {
  const groups = new Map<string, TeamLineupPlayer[]>();
  for (const p of team.lineup) {
    const key = p.slot && SLOT_LABEL[p.slot] ? p.slot : "other";
    groups.set(key, [...(groups.get(key) ?? []), p]);
  }
  return (
    <div className="grid gap-6 lg:grid-cols-[1fr_16rem]">
      <div className="space-y-1.5">
        <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Projected Lineup · 5v5 Net per Player</div>
        {[...groups.entries()].map(([slot, ps]) => (
          <div key={slot} className={cn("grid grid-cols-[2.25rem_1fr] items-start gap-2", slot === "d1" && "mt-3")}>
            <span className="pt-0.5 text-xs text-muted-foreground">{SLOT_LABEL[slot] ?? "Other"}</span>
            <div className={cn("grid gap-2", ps.length === 2 ? "grid-cols-2" : "grid-cols-3")}>
              {ps.map((p, i) => (
                <div key={p.player_id ?? `r${i}`} className="min-w-0">
                  <div className={cn("truncate text-sm", p.player_id == null && "italic text-muted-foreground")}>{p.player_name}</div>
                  <div className="tabular text-[11px] text-muted-foreground">
                    <Signed value={p.ev_net}>{signed(p.ev_net)}</Signed> · {pct(p.s5, 0)} 5v5
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
      <div>
        <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Goalies · share of recent starts</div>
        {team.goalies.length ? (
          <ul className="space-y-1 text-sm">
            {team.goalies.map((g) => (
              <li key={g.player_id} className="flex justify-between gap-2">
                <span className="truncate">{g.player_name ?? g.player_id}</span>
                <span className="tabular text-muted-foreground">{pct(g.weight, 0)}</span>
              </li>
            ))}
          </ul>
        ) : (
          <div className="text-xs text-muted-foreground">No recent starter on the roster; replacement level used.</div>
        )}
      </div>
    </div>
  );
}

export default function TeamsPage() {
  const { data, isLoading, error } = useTeamRatings();
  const [open, setOpen] = useState<number | null>(null);
  const scales = useMemo(() => (data ? teamScales(data) : null), [data]);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-xl font-semibold">Team Ratings</h1>
        {data && (
          <span className="text-xs text-muted-foreground">
            Snapshot {longDate(data.snapshot)} · lineups as of {dateTimeET(data.as_of)} ET
          </span>
        )}
      </div>

      {isLoading && <Loading />}
      {error && <ErrorState error={error} />}
      {data && (
        <>
          <Card>
            <SortTable
              rows={data.teams}
              columns={columns(scales!, open)}
              rowKey={(t) => t.team_id}
              initialSort={{ key: "gd60", desc: true }}
              onRowClick={(t) => setOpen(open === t.team_id ? null : t.team_id)}
              expanded={(t) =>
                open === t.team_id ? (
                  <div className="space-y-6">
                    <LineupDetail team={t} />
                    <div className="border-t border-border pt-4">
                      <MatchupGrid teamId={t.team_id} />
                    </div>
                  </div>
                ) : null
              }
            />
          </Card>
          <p className="text-xs text-muted-foreground">
            Each team is its projected lineup today (injuries, transactions and DailyFaceoff lines applied, as on the game
            pages) with every skater weighted by his ice-time shares, and its goalies weighted by recent starts — the same
            composition the simulator prices games with, against an average opponent on neutral ice. Rest, coaching and
            in-season team residuals are left out. Cell colors run from red (worse) through neutral (league average) to green (better). Click a team for its lineup and line matchups.
          </p>
        </>
      )}
    </div>
  );
}
