"use client";

import { useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import SortTable, { type Column } from "@/components/SortTable";
import TeamLogo from "@/components/TeamLogo";
import { Card, ErrorState, Loading, Signed } from "@/components/ui";
import { useTeamRatings } from "@/lib/api";
import { dateTimeET, longDate, pct, signed, signedPct } from "@/lib/format";
import type { Record3, TeamLineupPlayer, TeamRating } from "@/lib/types";
import { cn } from "@/lib/utils";

const SLOT_LABEL: Record<string, string> = { f1: "L1", f2: "L2", f3: "L3", f4: "L4", d1: "D1", d2: "D2", d3: "D3" };

const recordText = (r: Record3) => `${r.w}-${r.l}-${r.otl}`;

/** Diverging bar around zero, scaled to ``max``. */
function NetBar({ value, max }: { value: number; max: number }) {
  const w = Math.min(Math.abs(value) / max, 1) * 50;
  return (
    <div className="relative h-1.5 w-24 rounded-full bg-muted">
      <div className="absolute inset-y-0 left-1/2 w-px bg-border" />
      <div
        className={cn("absolute inset-y-0 rounded-full", value >= 0 ? "bg-positive" : "bg-negative")}
        style={value >= 0 ? { left: "50%", width: `${w}%` } : { right: "50%", width: `${w}%` }}
      />
    </div>
  );
}

function columns(maxGd: number, open: number | null): Column<TeamRating>[] {
  return [
    {
      key: "team", label: "Team", sort: (t) => t.abbr,
      render: (t) => (
        <span className="inline-flex items-center gap-2">
          {open === t.team_id ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" /> : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />}
          <TeamLogo abbr={t.abbr} className="h-6 w-6" />
          <span className="font-medium">{t.place} {t.name}</span>
        </span>
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
      key: "gd60", label: "5v5 GD/60", align: "right", className: "font-medium",
      title: "5v5 goals for minus against per 60 vs. an average opponent, after finishing and goaltending",
      sort: (t) => t.gd60,
      render: (t) => (
        <span className="inline-flex items-center justify-end gap-2">
          <NetBar value={t.gd60} max={maxGd} />
          <Signed value={t.gd60}>{signed(t.gd60)}</Signed>
        </span>
      ),
    },
    { key: "xgf", label: "xGF/60", align: "right", title: "5v5 expected goals for per 60", render: (t) => t.xgf60.toFixed(2), sort: (t) => t.xgf60 },
    { key: "xga", label: "xGA/60", align: "right", title: "5v5 expected goals against per 60 (lower is better)", render: (t) => t.xga60.toFixed(2), sort: (t) => -t.xga60 },
    {
      key: "fin", label: "Finishing", align: "right", title: "Shot-weighted shooter talent: goals per xG relative to average",
      render: (t) => <Signed value={t.finishing}>{signedPct(t.finishing)}</Signed>, sort: (t) => t.finishing,
    },
    {
      key: "save", label: "Goaltending", align: "right", title: "Likely starters' save talent (weighted by recent starts): share of xG stopped beyond average",
      render: (t) => <Signed value={t.save}>{signedPct(t.save)}</Signed>, sort: (t) => t.save,
    },
    { key: "pp", label: "PP xGF/60", align: "right", title: "Power-play expected goals per 60 vs. an average PK", render: (t) => t.pp_xgf60.toFixed(2), sort: (t) => t.pp_xgf60 },
    { key: "pk", label: "PK xGA/60", align: "right", title: "Penalty-kill expected goals against per 60 vs. an average PP (lower is better)", render: (t) => t.pk_xga60.toFixed(2), sort: (t) => -t.pk_xga60 },
    {
      key: "pen", label: "Pen drawn/taken", align: "right", title: "Penalty drawing and taking rates relative to league average (1.00 = average)",
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
        <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Projected lineup · 5v5 net per player</div>
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
  const maxGd = Math.max(0.1, ...(data?.teams ?? []).map((t) => Math.abs(t.gd60)));

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-xl font-semibold">Team ratings</h1>
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
              columns={columns(maxGd, open)}
              rowKey={(t) => t.team_id}
              initialSort={{ key: "gd60", desc: true }}
              onRowClick={(t) => setOpen(open === t.team_id ? null : t.team_id)}
              expanded={(t) => (open === t.team_id ? <LineupDetail team={t} /> : null)}
            />
          </Card>
          <p className="text-xs text-muted-foreground">
            Each team is its projected lineup today (injuries, transactions and DailyFaceoff lines applied, as on the game
            pages) with every skater weighted by his ice-time shares, and its goalies weighted by recent starts — the same
            composition the simulator prices games with, against an average opponent on neutral ice. Rest, coaching and
            in-season team residuals are left out. Click a team for its lineup.
          </p>
        </>
      )}
    </div>
  );
}
