"use client";

import { useMemo, useState } from "react";
import SortTable, { type Column } from "@/components/SortTable";
import IdentityCell from "@/components/IdentityCell";
import ArchetypeBadge from "@/components/ArchetypeBadge";
import PlayerContext from "@/components/PlayerContext";
import PlayerStyle from "@/components/PlayerStyle";
import { Card, Empty, ErrorState, Loading, Pills, Signed } from "@/components/ui";
import { usePlayerRatings } from "@/lib/api";
import { ARCHETYPES } from "@/lib/archetypes";
import { heat, heatScale } from "@/lib/heat";
import { dateTimeET, longDate, minutes, signed, signedPct } from "@/lib/format";
import type { Goalie, Skater } from "@/lib/types";

const VIEWS = [
  { key: "skaters", label: "Skaters" },
  { key: "goalies", label: "Goalies" },
] as const;

const POSITIONS = [
  { key: "all", label: "All" },
  { key: "F", label: "Forwards" },
  { key: "D", label: "Defense" },
] as const;

const DETAIL = [
  { key: "context", label: "On-Ice Context" },
  { key: "style", label: "Style" },
] as const;

type Scales = Record<string, number>;

/** The expanded panel under a skater: on-ice context or style, remembered across rows. */
function SkaterDetail({ playerId, tab, onTab }: {
  playerId: number; tab: (typeof DETAIL)[number]["key"]; onTab: (k: (typeof DETAIL)[number]["key"]) => void;
}) {
  return (
    <div className="space-y-4">
      <Pills options={DETAIL} value={tab} onChange={onTab} />
      {tab === "context" ? <PlayerContext playerId={playerId} /> : <PlayerStyle playerId={playerId} />}
    </div>
  );
}

const netPen = (p: Skater) => (p.pen_drawn60 == null || p.pen_taken60 == null ? null : p.pen_drawn60 - p.pen_taken60);
/** Relative xGA (lower is better); the ratings store prevention, so flip it back. */
const xga = (v: number | null) => (v == null ? null : -v);

/** League-wide scales for the heat columns, from every rated player (not the filtered rows). */
function skaterScales(all: Skater[]): Scales {
  return {
    xgd: heatScale(all.map((p) => p.ev_net)),
    xgf: heatScale(all.map((p) => p.ev_off)),
    xga: heatScale(all.map((p) => p.ev_def)),
    pp: heatScale(all.map((p) => p.pp_off)),
    pk: heatScale(all.map((p) => p.pk_def)),
    fin: heatScale(all.map((p) => p.finishing)),
  };
}

function goalieScales(all: Goalie[]): Scales {
  return { save: heatScale(all.map((g) => g.save)), gsax: heatScale(all.map((g) => g.gsax)) };
}

const nameColumn = <T extends { player_id: number; player_name: string | null; team_abbr: string | null }>(label: string): Column<T> => ({
  key: "name", label, className: "h-px p-0", sort: (p) => p.player_name,
  render: (p) => (
    <IdentityCell abbr={p.team_abbr}>
      <span className="font-medium">{p.player_name ?? p.player_id}</span>
    </IdentityCell>
  ),
});

function skaterColumns(s: Scales): Column<Skater>[] {
  return [
    nameColumn<Skater>("Player"),
    { key: "pos", label: "Pos", render: (p) => p.position ?? "–", sort: (p) => p.position },
    {
      key: "type", label: "Type",
      title: "Forward archetype from style alone (not quality): SW skill winger, BW balanced winger, PF power forward, OC offensive centre, 2C two-way centre. Defensemen have no archetype.",
      render: (p) => <ArchetypeBadge name={p.archetype} conf={p.archetype_conf} />, sort: (p) => p.archetype,
    },
    { key: "age", label: "Age", align: "right", render: (p) => (p.age == null ? "–" : Math.floor(p.age)), sort: (p) => p.age },
    {
      key: "xgd", label: "xGD/60", align: "right", className: "font-semibold",
      title: "5v5 on-ice xG differential per 60 this player adds, relative to an average skater (xGF − xGA)",
      render: (p) => signed(p.ev_net), sort: (p) => p.ev_net, style: (p) => heat(p.ev_net, s.xgd),
    },
    {
      key: "xgf", label: "xGF/60", align: "right", title: "5v5 on-ice xG for per 60 this player adds, relative to average",
      render: (p) => signed(p.ev_off), sort: (p) => p.ev_off, style: (p) => heat(p.ev_off, s.xgf),
    },
    {
      key: "xga", label: "xGA/60", align: "right", title: "5v5 on-ice xG against per 60 relative to average (lower is better)",
      render: (p) => signed(xga(p.ev_def)), sort: (p) => p.ev_def, style: (p) => heat(p.ev_def, s.xga),
    },
    {
      key: "move", label: "Δ Prior", align: "right", title: "xGD/60 now minus the preseason prior: what this season has changed",
      render: (p) => {
        const d = p.ev_net_prior == null ? null : p.ev_net - p.ev_net_prior;
        return <Signed value={d}>{signed(d, 3)}</Signed>;
      },
      sort: (p) => (p.ev_net_prior == null ? null : p.ev_net - p.ev_net_prior),
    },
    {
      key: "pp", label: "PP xGF/60", align: "right", title: "Power-play xG for per 60 relative to average",
      render: (p) => signed(p.pp_off), sort: (p) => p.pp_off, style: (p) => heat(p.pp_off, s.pp),
    },
    {
      key: "pk", label: "PK xGA/60", align: "right", title: "Penalty-kill xG against per 60 relative to average (lower is better)",
      render: (p) => signed(xga(p.pk_def)), sort: (p) => p.pk_def, style: (p) => heat(p.pk_def, s.pk),
    },
    {
      key: "fin", label: "Finishing", align: "right", title: "Goals per xG on his own shots, relative to average (shrunk)",
      render: (p) => signedPct(p.finishing), sort: (p) => p.finishing, style: (p) => heat(p.finishing, s.fin),
    },
    {
      key: "pen", label: "Pen ±/60", align: "right", title: "Penalties drawn minus taken per 60",
      render: (p) => <Signed value={netPen(p)}>{signed(netPen(p))}</Signed>, sort: netPen,
    },
    {
      key: "toi", label: "5v5 TOI", align: "right", title: "5v5 time on ice this season",
      render: (p) => <span className="text-muted-foreground">{minutes(p.ev_toi_s)}</span>, sort: (p) => p.ev_toi_s,
    },
  ];
}

function goalieColumns(s: Scales): Column<Goalie>[] {
  return [
    nameColumn<Goalie>("Goalie"),
    { key: "age", label: "Age", align: "right", render: (g) => (g.age == null ? "–" : Math.floor(g.age)), sort: (g) => g.age },
    {
      key: "save", label: "Save Talent", align: "right", className: "font-semibold",
      title: "Share of expected goals stopped beyond an average goalie (shrunk rating)",
      render: (g) => signedPct(g.save), sort: (g) => g.save, style: (g) => heat(g.save, s.save),
    },
    {
      key: "sd", label: "±", align: "right", title: "Rating uncertainty (one standard deviation, approx.)",
      render: (g) => <span className="text-muted-foreground">{(g.save_sd * 100).toFixed(1)}</span>, sort: (g) => g.save_sd,
    },
    {
      key: "move", label: "Δ Prior", align: "right", title: "Save talent now minus the preseason prior",
      render: (g) => {
        const d = g.save_prior == null ? null : g.save - g.save_prior;
        return <Signed value={d}>{signedPct(d)}</Signed>;
      },
      sort: (g) => (g.save_prior == null ? null : g.save - g.save_prior),
    },
    { key: "sa", label: "Shots", align: "right", title: "Unblocked shots faced this season", render: (g) => g.shots_against, sort: (g) => g.shots_against },
    { key: "ga", label: "GA", align: "right", render: (g) => g.goals_against, sort: (g) => g.goals_against },
    { key: "xga", label: "xGA", align: "right", render: (g) => g.xga.toFixed(1), sort: (g) => g.xga },
    {
      key: "gsax", label: "GSAx", align: "right", title: "Goals saved above expected this season (raw, not shrunk)",
      render: (g) => signed(g.gsax, 1), sort: (g) => g.gsax, style: (g) => heat(g.gsax, s.gsax),
    },
  ];
}

export default function PlayersPage() {
  const { data, isLoading, error } = usePlayerRatings();
  const [view, setView] = useState<(typeof VIEWS)[number]["key"]>("skaters");
  const [pos, setPos] = useState<(typeof POSITIONS)[number]["key"]>("all");
  const [team, setTeam] = useState("");
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState<number | null>(null);
  const [detail, setDetail] = useState<(typeof DETAIL)[number]["key"]>("context");
  const [type, setType] = useState("");

  const teams = useMemo(
    () => [...new Set((data?.skaters ?? []).map((p) => p.team_abbr).filter((t): t is string => !!t))].sort(),
    [data]
  );
  const q = query.trim().toLowerCase();
  const match = (p: { team_abbr: string | null; player_name: string | null }) =>
    !!p.team_abbr && (!team || p.team_abbr === team) && (!q || (p.player_name ?? "").toLowerCase().includes(q));
  const skaters = (data?.skaters ?? []).filter(
    (p) => match(p) && (pos === "all" || (pos === "D" ? p.position === "D" : p.position !== "D")) && (!type || p.archetype === type)
  );
  const goalies = (data?.goalies ?? []).filter(match);
  // Scales from the whole league (every player on a team), so filtering doesn't change the colors.
  const skaterCols = useMemo(() => skaterColumns(skaterScales((data?.skaters ?? []).filter((p) => p.team_abbr))), [data]);
  const goalieCols = useMemo(() => goalieColumns(goalieScales((data?.goalies ?? []).filter((g) => g.team_abbr))), [data]);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-xl font-semibold">Player Ratings</h1>
        {data && (
          <span className="text-xs text-muted-foreground">
            Snapshot {longDate(data.snapshot)} · built {dateTimeET(data.as_of)} ET
          </span>
        )}
      </div>

      {isLoading && <Loading />}
      {error && <ErrorState error={error} />}
      {data && (
        <>
          <div className="flex flex-wrap items-center gap-3">
            <Pills options={VIEWS} value={view} onChange={setView} />
            {view === "skaters" && <Pills options={POSITIONS} value={pos} onChange={setPos} />}
            <select
              value={team}
              onChange={(e) => setTeam(e.target.value)}
              className="rounded-md border border-border bg-card px-2 py-1 text-sm"
            >
              <option value="">All Teams</option>
              {teams.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
            {view === "skaters" && pos !== "D" && (
              <select
                value={type}
                onChange={(e) => setType(e.target.value)}
                className="rounded-md border border-border bg-card px-2 py-1 text-sm"
                title="Forward archetype"
              >
                <option value="">All Types</option>
                {Object.keys(ARCHETYPES).map((t) => (
                  <option key={t} value={t}>
                    {t[0].toUpperCase() + t.slice(1)}
                  </option>
                ))}
              </select>
            )}
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search player"
              className="w-44 rounded-md border border-border bg-card px-2 py-1 text-sm placeholder:text-muted-foreground"
            />
          </div>

          {view === "skaters" ? (
            skaters.length ? (
              <Card>
                <SortTable
                  rows={skaters}
                  columns={skaterCols}
                  rowKey={(p) => p.player_id}
                  initialSort={{ key: "xgd", desc: true }}
                  onRowClick={(p) => setOpen(open === p.player_id ? null : p.player_id)}
                  expanded={(p) => (open === p.player_id ? <SkaterDetail playerId={p.player_id} tab={detail} onTab={setDetail} /> : null)}
                />
              </Card>
            ) : (
              <Empty>No skaters match.</Empty>
            )
          ) : goalies.length ? (
            <Card>
              <SortTable rows={goalies} columns={goalieCols} rowKey={(g) => g.player_id} initialSort={{ key: "save", desc: true }} />
            </Card>
          ) : (
            <Empty>No goalies match.</Empty>
          )}

          <p className="text-xs text-muted-foreground">
            Ratings are Bayesian: each starts from a preseason prior (last season, aged) and moves with this season&apos;s games,
            so early in the year they are mostly prior. 5v5 and special-teams values are on-ice xG per 60 relative to an
            average player (xGA: lower is better). Cell colors are scaled across the whole league, so they
            don&apos;t change when you filter. Hover a header for its definition. Click a skater for his on-ice breakdown
            (own play vs teammates, competition and deployment), ice-time tiers and linemates, or his style: seven
            style axes, a forward archetype and the past players he plays most like. Style describes how a player
            plays, not how well; quality is the rating columns.
          </p>
        </>
      )}
    </div>
  );
}
