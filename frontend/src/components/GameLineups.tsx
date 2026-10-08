"use client";

import { useState } from "react";
import { ExternalLink } from "lucide-react";
import { useGameLineups } from "@/lib/api";
import type { GameLineupsResponse, GoalieStats, LineupPlayerStats, LineupSource, LineupUnit, TeamLineup, UnitRecord } from "@/lib/types";
import { dateTimeET, minutes, pct, seasonLabel, signed } from "@/lib/format";
import { team } from "@/lib/teams";
import { cn } from "@/lib/utils";
import TeamLogo from "./TeamLogo";
import { Badge, Card, ErrorState, Loading, Pills, SectionTitle } from "./ui";

type SeasonKey = "cur" | "prev";

const UNIT_LABEL: Record<string, string> = {
  f1: "Line 1", f2: "Line 2", f3: "Line 3", f4: "Line 4", d1: "Pair 1", d2: "Pair 2", d3: "Pair 3",
  pp1: "PP1", pp2: "PP2", pk1: "PK1", pk2: "PK2",
};

/** Signed value colored green/red, with ``invert`` for xGA (lower is better). */
function Rate({ v, invert, digits = 2 }: { v: number | null | undefined; invert?: boolean; digits?: number }) {
  const good = v == null || Math.abs(v) < 1e-9 ? null : invert ? v < 0 : v > 0;
  return <span className={cn("tabular", good === true && "text-positive", good === false && "text-negative")}>{signed(v, digits)}</span>;
}

const xgfPct = (r: { xgf: number; xga: number }) => (r.xgf + r.xga > 0 ? r.xgf / (r.xgf + r.xga) : null);

/** Time and results a unit has had together in the season. */
function Together({ r }: { r: UnitRecord | null }) {
  if (!r) return <span className="text-muted-foreground">Not together this season</span>;
  const share = xgfPct(r);
  return (
    <span className="tabular">
      {minutes(r.toi_s)} TOI · {r.games} GP · xG {r.xgf.toFixed(1)}–{r.xga.toFixed(1)}
      {share != null && (
        <span className={cn("ml-1 font-semibold", share >= 0.5 ? "text-positive" : "text-negative")}>({pct(share, 0)} xGF)</span>
      )}
      {" "}· Goals {r.gf}–{r.ga}
    </span>
  );
}

function SourceLink({ label, s }: { label: string; s: LineupSource | null }) {
  if (!s) return null;
  return (
    <div className="rounded-md border border-border bg-muted/40 p-2.5 text-xs">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <span className="font-semibold uppercase tracking-wide text-muted-foreground">{label}</span>
        {s.status && <Badge tone={s.status === "Confirmed" ? "pos" : "warn"}>{s.status}</Badge>}
        {s.goalie_name && <span className="font-medium">{s.goalie_name}</span>}
        <span className="text-muted-foreground">via {s.source_name ?? "DailyFaceoff"}</span>
        {s.updated_at && <span className="text-muted-foreground">· {dateTimeET(s.updated_at)} ET</span>}
        {s.url && (
          <a href={s.url} target="_blank" rel="noreferrer" className="ml-auto inline-flex items-center gap-1 font-medium text-accent hover:underline">
            {s.tweet || /x\.com|twitter\.com/.test(s.url) ? "View Tweet" : "View Source"} <ExternalLink className="h-3 w-3" />
          </a>
        )}
      </div>
      {(s.tweet?.text || s.details) && (
        <p className="mt-1.5 border-l-2 border-border pl-2 italic text-muted-foreground">
          {s.tweet?.text ?? s.details}
          {s.tweet?.author_handle && <span className="not-italic"> — @{s.tweet.author_handle}</span>}
        </p>
      )}
    </div>
  );
}

function Goalies({ rows, season }: { rows: GoalieStats[]; season: SeasonKey }) {
  if (rows.length === 0) return <div className="text-xs text-muted-foreground">No projection.</div>;
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm tabular">
        <thead className="text-xs text-muted-foreground">
          <tr className="border-b border-border">
            <th className="py-1.5 text-left font-medium">Goalie</th>
            <th className="py-1.5 text-right font-medium" title="Probability he starts (DailyFaceoff + model)">Start</th>
            <th className="py-1.5 text-right font-medium" title="Games started">GS</th>
            <th className="py-1.5 text-right font-medium" title="Shots against in his starts">SA</th>
            <th className="py-1.5 text-right font-medium">Sv%</th>
            <th className="py-1.5 text-right font-medium" title="Goals saved above expected (xGA − GA)">GSAx</th>
            <th className="py-1.5 text-right font-medium" title="Share of expected goals stopped beyond average (shrunk rating)">Save Talent</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((g) => {
            const s = g.season[season];
            return (
              <tr key={g.player_id ?? "other"} className="border-b border-border last:border-0">
                <td className="py-1.5">
                  {g.player_name} {g.dfo_status && <Badge tone={g.dfo_status === "Confirmed" ? "pos" : "muted"}>{g.dfo_status}</Badge>}
                </td>
                <td className="py-1.5 text-right font-semibold">{pct(g.p_start, 0)}</td>
                <td className="py-1.5 text-right">{s?.starts ?? "–"}</td>
                <td className="py-1.5 text-right">{s?.shots_against ?? "–"}</td>
                <td className="py-1.5 text-right">{s?.sv_pct != null ? s.sv_pct.toFixed(3).replace(/^0/, "") : "–"}</td>
                <td className="py-1.5 text-right"><Rate v={s?.gsax} digits={1} /></td>
                <td className="py-1.5 text-right" title={g.rating ? `± ${pct(g.rating.save_sd, 1)}` : undefined}>
                  {g.rating ? <Rate v={g.rating.save * 100} digits={1} /> : "–"}
                  {g.rating && <span className="text-muted-foreground">%</span>}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function PlayerRow({ p, season }: { p: LineupPlayerStats; season: SeasonKey }) {
  const o = p.onice[season];
  return (
    <tr className="border-b border-border/60 last:border-0">
      <td className="py-1 pl-2">
        <span className={cn(p.p_dressed < 1 && "text-amber-600 dark:text-amber-400")} title={p.issues || undefined}>{p.player_name}</span>
        <span className="ml-1 text-[11px] text-muted-foreground">{p.position}</span>
      </td>
      <td className="py-1 text-right text-muted-foreground">{pct(p.s5, 0)}</td>
      <td className="py-1 text-right"><Rate v={p.rating?.ev_off} /></td>
      <td className="py-1 text-right"><Rate v={p.rating ? -p.rating.ev_def : null} invert /></td>
      <td className="py-1 text-right font-semibold"><Rate v={p.rating?.ev_net} /></td>
      <td className="border-l border-border py-1 pl-2 text-right">{o ? o.xgf60.toFixed(2) : "–"}</td>
      <td className="py-1 text-right">{o ? o.xga60.toFixed(2) : "–"}</td>
      <td className="py-1 text-right"><Rate v={o ? o.xgf60 - o.xga60 : null} /></td>
      <td className="py-1 pr-2 text-right text-muted-foreground">{o ? Math.round(o.toi_s / 60) : "–"}</td>
    </tr>
  );
}

function UnitBlock({ unit, players, season }: { unit: LineupUnit; players: LineupPlayerStats[]; season: SeasonKey }) {
  return (
    <>
      <tr className="bg-muted/50 text-xs">
        <td colSpan={9} className="px-2 py-1.5">
          <span className="mr-2 font-semibold">{UNIT_LABEL[unit.slot] ?? unit.slot.toUpperCase()}</span>
          <span className="text-muted-foreground"><Together r={unit.record[season]} /></span>
        </td>
      </tr>
      {players.map((p) => <PlayerRow key={p.player_id} p={p} season={season} />)}
    </>
  );
}

function TeamCard({ abbr, t, season }: { abbr: string; t: TeamLineup; season: SeasonKey }) {
  const bySlot = (slot: string) => t.players.filter((p) => p.slot === slot);
  const fiveOnFive = t.units.filter((u) => u.kind === "F" || u.kind === "D");
  const special = t.units.filter((u) => u.kind === "PP" || u.kind === "PK").sort((a, b) => a.slot.localeCompare(b.slot));
  const names = Object.fromEntries(t.players.map((p) => [p.player_id, p.player_name]));
  const extras = t.players.filter((p) => !p.slot || !["f1", "f2", "f3", "f4", "d1", "d2", "d3"].includes(p.slot));

  return (
    <Card className="p-4">
      <div className="mb-3 flex items-center gap-2 text-base font-semibold">
        <TeamLogo abbr={abbr} className="h-7 w-7" /> {team(abbr).name || abbr}
      </div>
      <div className="space-y-2">
        <SourceLink label="Lines" s={t.sources.lines} />
        <SourceLink label="Goalie" s={t.sources.goalie} />
      </div>

      <h3 className="mb-1.5 mt-4 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Starting Goalie</h3>
      <Goalies rows={t.goalies} season={season} />

      <h3 className="mb-1.5 mt-5 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Lines and Pairs (5v5)</h3>
      {t.players.length === 0 ? (
        <div className="text-xs text-muted-foreground">No projection.</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[560px] text-sm tabular">
            <thead className="text-[11px] text-muted-foreground">
              <tr>
                <th />
                <th />
                <th colSpan={3} className="pb-0.5 text-center font-semibold uppercase tracking-wide" title="Talent ratings per 60 at 5v5, relative to average (shrunk)">Rating /60</th>
                <th colSpan={4} className="border-l border-border pb-0.5 text-center font-semibold uppercase tracking-wide" title="What happened with him on the ice at 5v5 this season">On-Ice /60</th>
              </tr>
              <tr className="border-b border-border">
                <th className="py-1 pl-2 text-left font-medium">Player</th>
                <th className="py-1 text-right font-medium" title="Projected share of 5v5 ice time">5v5</th>
                <th className="py-1 text-right font-medium">xGF</th>
                <th className="py-1 text-right font-medium" title="Lower is better">xGA</th>
                <th className="py-1 text-right font-medium">xGD</th>
                <th className="border-l border-border py-1 pl-2 text-right font-medium">xGF</th>
                <th className="py-1 text-right font-medium">xGA</th>
                <th className="py-1 text-right font-medium">xGD</th>
                <th className="py-1 pr-2 text-right font-medium" title="5v5 minutes">Min</th>
              </tr>
            </thead>
            <tbody>
              {fiveOnFive.map((u) => <UnitBlock key={u.slot} unit={u} players={bySlot(u.slot)} season={season} />)}
              {extras.length > 0 && (
                <>
                  <tr className="bg-muted/50 text-xs"><td colSpan={9} className="px-2 py-1.5 font-semibold">Other</td></tr>
                  {extras.map((p) => <PlayerRow key={p.player_id ?? p.player_name} p={p} season={season} />)}
                </>
              )}
            </tbody>
          </table>
        </div>
      )}

      {special.length > 0 && (
        <>
          <h3 className="mb-1.5 mt-5 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Special Teams Units</h3>
          <div className="space-y-1.5 text-xs">
            {special.map((u) => (
              <div key={u.slot} className="grid grid-cols-[2.5rem_1fr] gap-2">
                <span className="font-semibold">{UNIT_LABEL[u.slot] ?? u.slot.toUpperCase()}</span>
                <div>
                  <div>{u.player_ids.map((id) => names[id] ?? id).join(", ")}</div>
                  <div className="text-muted-foreground"><Together r={u.record[season]} /></div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </Card>
  );
}

/** Projected lineups and starters for both teams, with the stats and sources behind them. */
export default function GameLineups({ gameId, away, home }: { gameId: string; away: string; home: string }) {
  const { data, isLoading, error } = useGameLineups(gameId);
  const [season, setSeason] = useState<SeasonKey>("cur");
  if (isLoading) return <Loading />;
  if (error) return <ErrorState error={error} />;
  if (!data) return null;
  const d: GameLineupsResponse = data;
  const options = [
    { key: "cur" as const, label: `This Season (${seasonLabel(d.season)})` },
    { key: "prev" as const, label: `Last Season (${seasonLabel(d.season - 10001)})` },
  ];

  return (
    <div>
      <SectionTitle right={<Pills options={options} value={season} onChange={setSeason} />}>Projected Lineups</SectionTitle>
      <div className="grid gap-4 2xl:grid-cols-2">
        <TeamCard abbr={away} t={d.away} season={season} />
        <TeamCard abbr={home} t={d.home} season={season} />
      </div>
      <p className="mt-3 text-xs text-muted-foreground">
        Ratings are each skater&apos;s 5v5 talent per 60 relative to average (xGA: lower is better), as the model uses them today.
        On-Ice and time together are what happened in the selected season. Save Talent is the share of expected goals a goalie stops
        beyond average.
      </p>
    </div>
  );
}
