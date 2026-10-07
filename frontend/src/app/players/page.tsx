"use client";

import { useMemo, useState } from "react";
import SortTable, { type Column } from "@/components/SortTable";
import { TeamTag } from "@/components/TeamLogo";
import { Card, Empty, ErrorState, Loading, Signed } from "@/components/ui";
import { usePlayerRatings } from "@/lib/api";
import { dateTimeET, longDate, minutes, signed, signedPct } from "@/lib/format";
import type { Goalie, Skater } from "@/lib/types";
import { cn } from "@/lib/utils";

const VIEWS = [
  { key: "skaters", label: "Skaters" },
  { key: "goalies", label: "Goalies" },
] as const;

const POSITIONS = [
  { key: "all", label: "All" },
  { key: "F", label: "Forwards" },
  { key: "D", label: "Defense" },
] as const;

const SKATER_COLUMNS: Column<Skater>[] = [
  { key: "name", label: "Player", render: (p) => <span className="font-medium">{p.player_name ?? p.player_id}</span>, sort: (p) => p.player_name },
  { key: "team", label: "Team", render: (p) => (p.team_abbr ? <TeamTag abbr={p.team_abbr} /> : "–"), sort: (p) => p.team_abbr },
  { key: "pos", label: "Pos", render: (p) => p.position ?? "–", sort: (p) => p.position },
  { key: "age", label: "Age", align: "right", render: (p) => (p.age == null ? "–" : Math.floor(p.age)), sort: (p) => p.age },
  {
    key: "ev_net", label: "5v5 net", align: "right", className: "font-medium",
    title: "5v5 offence + defence: on-ice xG/60 impact relative to an average skater",
    render: (p) => <Signed value={p.ev_net}>{signed(p.ev_net)}</Signed>, sort: (p) => p.ev_net,
  },
  {
    key: "ev_off", label: "Off", align: "right", title: "5v5 xG for per 60, relative to average",
    render: (p) => <Signed value={p.ev_off}>{signed(p.ev_off)}</Signed>, sort: (p) => p.ev_off,
  },
  {
    key: "ev_def", label: "Def", align: "right", title: "5v5 xG against per 60 prevented, relative to average (higher is better)",
    render: (p) => <Signed value={p.ev_def}>{signed(p.ev_def)}</Signed>, sort: (p) => p.ev_def,
  },
  {
    key: "move", label: "Δ prior", align: "right", title: "5v5 net now minus the preseason prior: what this season has changed",
    render: (p) => {
      const d = p.ev_net_prior == null ? null : p.ev_net - p.ev_net_prior;
      return <Signed value={d}>{signed(d, 3)}</Signed>;
    },
    sort: (p) => (p.ev_net_prior == null ? null : p.ev_net - p.ev_net_prior),
  },
  {
    key: "pp", label: "PP", align: "right", title: "Power-play xG for per 60, relative to average",
    render: (p) => <Signed value={p.pp_off}>{signed(p.pp_off)}</Signed>, sort: (p) => p.pp_off,
  },
  {
    key: "pk", label: "PK", align: "right", title: "Penalty-kill xG against per 60 prevented, relative to average",
    render: (p) => <Signed value={p.pk_def}>{signed(p.pk_def)}</Signed>, sort: (p) => p.pk_def,
  },
  {
    key: "fin", label: "Finishing", align: "right", title: "Goals per xG on his own shots, relative to average (shrunk)",
    render: (p) => <Signed value={p.finishing}>{signedPct(p.finishing)}</Signed>, sort: (p) => p.finishing,
  },
  {
    key: "pen", label: "Pen ±/60", align: "right", title: "Penalties drawn minus taken per 60",
    render: (p) => {
      const d = p.pen_drawn60 == null || p.pen_taken60 == null ? null : p.pen_drawn60 - p.pen_taken60;
      return <Signed value={d}>{signed(d)}</Signed>;
    },
    sort: (p) => (p.pen_drawn60 == null || p.pen_taken60 == null ? null : p.pen_drawn60 - p.pen_taken60),
  },
  {
    key: "toi", label: "5v5 TOI", align: "right", title: "5v5 time on ice this season",
    render: (p) => <span className="text-muted-foreground">{minutes(p.ev_toi_s)}</span>, sort: (p) => p.ev_toi_s,
  },
];

const GOALIE_COLUMNS: Column<Goalie>[] = [
  { key: "name", label: "Goalie", render: (g) => <span className="font-medium">{g.player_name ?? g.player_id}</span>, sort: (g) => g.player_name },
  { key: "team", label: "Team", render: (g) => (g.team_abbr ? <TeamTag abbr={g.team_abbr} /> : "–"), sort: (g) => g.team_abbr },
  { key: "age", label: "Age", align: "right", render: (g) => (g.age == null ? "–" : Math.floor(g.age)), sort: (g) => g.age },
  {
    key: "save", label: "Save talent", align: "right", className: "font-medium",
    title: "Share of expected goals stopped beyond an average goalie (shrunk rating)",
    render: (g) => <Signed value={g.save}>{signedPct(g.save)}</Signed>, sort: (g) => g.save,
  },
  {
    key: "sd", label: "±", align: "right", title: "Rating uncertainty (one standard deviation, approx.)",
    render: (g) => <span className="text-muted-foreground">{(g.save_sd * 100).toFixed(1)}</span>, sort: (g) => g.save_sd,
  },
  {
    key: "move", label: "Δ prior", align: "right", title: "Save talent now minus the preseason prior",
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
    render: (g) => <Signed value={g.gsax}>{signed(g.gsax, 1)}</Signed>, sort: (g) => g.gsax,
  },
];

function Pills<K extends string>({ options, value, onChange }: { options: readonly { key: K; label: string }[]; value: K; onChange: (k: K) => void }) {
  return (
    <div className="flex gap-1">
      {options.map((o) => (
        <button
          key={o.key}
          onClick={() => onChange(o.key)}
          className={cn("rounded-md px-3 py-1 text-sm", value === o.key ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground")}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

export default function PlayersPage() {
  const { data, isLoading, error } = usePlayerRatings();
  const [view, setView] = useState<(typeof VIEWS)[number]["key"]>("skaters");
  const [pos, setPos] = useState<(typeof POSITIONS)[number]["key"]>("all");
  const [team, setTeam] = useState("");
  const [query, setQuery] = useState("");

  const teams = useMemo(
    () => [...new Set((data?.skaters ?? []).map((p) => p.team_abbr).filter((t): t is string => !!t))].sort(),
    [data]
  );
  const q = query.trim().toLowerCase();
  const match = (p: { team_abbr: string | null; player_name: string | null }) =>
    !!p.team_abbr && (!team || p.team_abbr === team) && (!q || (p.player_name ?? "").toLowerCase().includes(q));
  const skaters = (data?.skaters ?? []).filter(
    (p) => match(p) && (pos === "all" || (pos === "D" ? p.position === "D" : p.position !== "D"))
  );
  const goalies = (data?.goalies ?? []).filter(match);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-xl font-semibold">Player ratings</h1>
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
              <option value="">All teams</option>
              {teams.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
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
                <SortTable rows={skaters} columns={SKATER_COLUMNS} rowKey={(p) => p.player_id} initialSort={{ key: "ev_net", desc: true }} />
              </Card>
            ) : (
              <Empty>No skaters match.</Empty>
            )
          ) : goalies.length ? (
            <Card>
              <SortTable rows={goalies} columns={GOALIE_COLUMNS} rowKey={(g) => g.player_id} initialSort={{ key: "save", desc: true }} />
            </Card>
          ) : (
            <Empty>No goalies match.</Empty>
          )}

          <p className="text-xs text-muted-foreground">
            Ratings are Bayesian: each starts from a preseason prior (last season, aged) and moves with this season&apos;s games,
            so early in the year they are mostly prior. 5v5 and special-teams values are on-ice xG per 60 relative to an
            average player; defence is flipped so higher is better. Hover a header for its definition.
          </p>
        </>
      )}
    </div>
  );
}
