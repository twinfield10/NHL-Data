"use client";

import { useState, type CSSProperties, type ReactNode } from "react";
import { ExternalLink } from "lucide-react";
import { useGameLineups } from "@/lib/api";
import type {
  GameLineupsResponse, GoalieStats, HeatScale, LineupPlayerStats, LineupSource, LineupUnit, SeasonScales, TeamLineup, TeamSpecialRate,
  UnitRecord,
} from "@/lib/types";
import { dateTimeET, minutes, pct, seasonLabel, signed, signedPct } from "@/lib/format";
import { heat } from "@/lib/heat";
import { team } from "@/lib/teams";
import { cn } from "@/lib/utils";
import ArchetypeBadge from "./ArchetypeBadge";
import TeamLogo from "./TeamLogo";
import { Badge, Card, ErrorState, Loading, Pills, SectionTitle } from "./ui";

type SeasonKey = "cur" | "prev";
type Scales = GameLineupsResponse["scales"];

const UNIT_LABEL: Record<string, string> = {
  f1: "Line 1", f2: "Line 2", f3: "Line 3", f4: "Line 4", d1: "Pair 1", d2: "Pair 2", d3: "Pair 3",
  pp1: "PP1", pp2: "PP2", pk1: "PK1", pk2: "PK2",
};
const FIVE_ON_FIVE = ["f1", "f2", "f3", "f4", "d1", "d2", "d3"];
/** Every unit drawn for each team, with its size and the 5v5 positions to fill, so the two
 *  teams' tables line up row for row (a missing player shows as a replacement). */
const LAYOUT: Record<LineupUnit["kind"], { slots: string[]; size: number; roles?: string[] }> = {
  F: { slots: ["f1", "f2", "f3", "f4"], size: 3, roles: ["LW", "C", "RW"] },
  D: { slots: ["d1", "d2", "d3"], size: 2, roles: ["LD", "RD"] },
  PP: { slots: ["pp1", "pp2"], size: 5 },
  PK: { slots: ["pk1", "pk2"], size: 4 },
};
/** Goalies listed by name before the rest are folded into "Other". */
const TOP_GOALIES = 2;

/** Heat on a league scale with its own center (season results), or centered at 0 (ratings). */
const heatOn = (v: number | null | undefined, s: HeatScale | number | undefined, lowerIsBetter = false): CSSProperties | undefined =>
  s == null ? undefined : typeof s === "number" ? heat(v, s, { lowerIsBetter }) : heat(v, s.scale, { center: s.center, lowerIsBetter });

const xgfPct = (r: { xgf: number; xga: number }) => (r.xgf + r.xga > 0 ? r.xgf / (r.xgf + r.xga) : null);
const dash = "–";

/** Time and results a unit has had together in the season, for its header row. */
function Together({ r }: { r: UnitRecord | null }) {
  if (!r) return <span className="text-muted-foreground">Not together this season</span>;
  const share = xgfPct(r);
  return (
    <span className="tabular text-muted-foreground">
      {minutes(r.toi_s)} TOI · {r.games} GP · xG {r.xgf.toFixed(1)}–{r.xga.toFixed(1)}
      {share != null && (
        <span className={cn("ml-1 font-semibold", share >= 0.5 ? "text-positive" : "text-negative")}>({pct(share, 0)} xGF)</span>
      )}
      {" "}· Goals {r.gf}–{r.ga}
    </span>
  );
}

/** Where a source came from, for a link's tooltip: who, when, and the tweet text. */
const sourceHint = (s: LineupSource) =>
  [`via ${s.source_name ?? "DailyFaceoff"}${s.updated_at ? ` · ${dateTimeET(s.updated_at)} ET` : ""}`, s.tweet?.text ?? s.details]
    .filter(Boolean).join("\n\n");

/** One bordered panel per position group: title bar (a link to its source when given, with
 *  ``aside`` on the right), then its table. */
function Panel({ title, link, aside, children }: { title: string; link?: LineupSource | null; aside?: ReactNode; children: ReactNode }) {
  return (
    <section className="overflow-hidden rounded-md border border-border">
      <h3 className="flex h-8 items-center justify-between gap-2 border-b border-border bg-muted px-3 text-xs font-semibold uppercase tracking-wide">
        {link?.url ? (
          <a href={link.url} target="_blank" rel="noreferrer" title={sourceHint(link)}
            className="inline-flex items-center gap-1 hover:text-accent hover:underline">
            {title} <ExternalLink className="h-3 w-3" />
          </a>
        ) : title}
        {aside}
      </h3>
      {children}
    </section>
  );
}

// ---- Skater tables ----------------------------------------------------------------------------

interface Col {
  label: string;
  title?: string;
  /** Group header above (consecutive columns with the same group share one). */
  group?: string;
  render: (p: LineupPlayerStats) => ReactNode;
  style?: (p: LineupPlayerStats) => CSSProperties | undefined;
}

interface Group {
  key: string;
  label: string;
  record?: UnitRecord | null;
  /** ``null`` is an empty spot, shown as a replacement player. */
  players: (LineupPlayerStats | null)[];
  /** Position each player fills in this unit (defaults to his own). */
  roles?: string[];
}

/** Position, type (forward archetype) and name cells of a player row; amber when he may not
 *  dress. An empty spot shows as a replacement player. */
function PlayerCells({ p, role }: { p: LineupPlayerStats | null; role?: string }) {
  return (
    <>
      <td className="w-px whitespace-nowrap pl-3 pr-2 text-[11px] font-semibold text-muted-foreground"><span className="inline-block w-5">{p ? (role || p.role) : role}</span></td>
      <td className="w-px whitespace-nowrap px-1"><span className="inline-block w-7">{p?.archetype && <ArchetypeBadge name={p.archetype} />}</span></td>
      <td className="whitespace-nowrap px-2">
        {p ? (
          <span className={cn(p.p_dressed < 1 && "text-amber-600 dark:text-amber-400")} title={p.issues || undefined}>{p.player_name}</span>
        ) : (
          <span className="italic text-muted-foreground">Replacement Player</span>
        )}
      </td>
    </>
  );
}

/** Group header spans for a column list (``null`` for ungrouped columns). */
function groupSpans(cols: Col[]): { label: string | null; span: number; start: number }[] {
  const out: { label: string | null; span: number; start: number }[] = [];
  cols.forEach((c, i) => {
    const last = out[out.length - 1];
    if (last && last.label === (c.group ?? null)) last.span++;
    else out.push({ label: c.group ?? null, span: 1, start: i });
  });
  return out;
}

/** Players grouped by unit (a header row each), stats centered, heat on the rate columns. */
function UnitTable({ cols, groups }: { cols: Col[]; groups: Group[] }) {
  const spans = groupSpans(cols);
  const starts = new Set(spans.filter((s) => s.label && s.start > 0).map((s) => s.start));
  const edge = (i: number) => starts.has(i) && "border-l border-border";
  const width = cols.length + 3;
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[620px] text-sm tabular">
        <thead className="text-[11px] text-muted-foreground">
          {spans.some((s) => s.label) && (
            <tr>
              <th colSpan={3} />
              {spans.map((s) => (
                <th key={s.start} colSpan={s.span} className={cn("whitespace-nowrap px-1 pt-1.5 text-center font-semibold uppercase tracking-wide", edge(s.start))}>
                  {s.label}
                </th>
              ))}
            </tr>
          )}
          <tr className="border-b border-border">
            <th className="w-px whitespace-nowrap pl-3 pr-2 text-left font-medium">Pos</th>
            <th className="w-px whitespace-nowrap px-1 text-left font-medium" title="Forward archetype: SW skill winger, BW balanced winger, PF power forward, OC offensive centre, 2C two-way centre">Type</th>
            <th className="px-2 py-1.5 text-left font-medium">Player</th>
            {cols.map((c, i) => (
              <th key={c.label + i} title={c.title} className={cn("w-16 whitespace-nowrap px-1 py-0 text-center font-medium", edge(i))}>{c.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {groups.map((g) => (
            <GroupRows key={g.key} g={g} cols={cols} width={width} edge={edge} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function GroupRows({ g, cols, width, edge }: { g: Group; cols: Col[]; width: number; edge: (i: number) => string | false }) {
  return (
    <>
      <tr className="h-7 border-b border-border bg-muted/50 text-xs">
        <td colSpan={width} className="whitespace-nowrap px-3">
          <span className="mr-2 font-semibold">{g.label}</span>
          {g.record !== undefined && <Together r={g.record} />}
        </td>
      </tr>
      {g.players.map((p, j) => (
        <tr key={p ? (p.player_id ?? p.player_name) : `rep-${j}`} className="h-8 border-b border-border last:border-0">
          <PlayerCells p={p} role={g.roles?.[j]} />
          {cols.map((c, i) => (
            <td key={c.label + i} className={cn("px-1 py-0 text-center", edge(i), !p && "text-muted-foreground")} style={p ? c.style?.(p) : undefined}>
              {p ? c.render(p) : dash}
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

/** 5v5 columns: projected share, talent ratings, on-ice results this season. */
function evenStrengthCols(sc: Scales, season: SeasonKey): Col[] {
  const r = sc.rating, s = sc[season];
  const on = (p: LineupPlayerStats) => p.onice[season];
  const xgd = (p: LineupPlayerStats) => { const o = on(p); return o ? o.xgf60 - o.xga60 : null; };
  return [
    { label: "5v5", title: "Projected share of 5v5 ice time", render: (p) => pct(p.s5, 0) },
    { group: "Rating /60", label: "xGF", title: "5v5 offense vs average (shrunk)", render: (p) => signed(p.rating?.ev_off),
      style: (p) => heatOn(p.rating?.ev_off, r.ev_off) },
    { group: "Rating /60", label: "xGA", title: "5v5 expected goals against vs average: lower is better",
      render: (p) => signed(p.rating ? -p.rating.ev_def : null), style: (p) => heatOn(p.rating?.ev_def, r.ev_def) },
    { group: "Rating /60", label: "xGD", render: (p) => <span className="font-semibold">{signed(p.rating?.ev_net)}</span>,
      style: (p) => heatOn(p.rating?.ev_net, r.ev_net) },
    { group: "On-Ice /60", label: "xGF", title: "5v5 on-ice expected goals for per 60, this season",
      render: (p) => on(p)?.xgf60.toFixed(2) ?? dash, style: (p) => heatOn(on(p)?.xgf60, s.xgf) },
    { group: "On-Ice /60", label: "xGA", title: "5v5 on-ice expected goals against per 60: lower is better",
      render: (p) => on(p)?.xga60.toFixed(2) ?? dash, style: (p) => heatOn(on(p)?.xga60, s.xga, true) },
    { group: "On-Ice /60", label: "xGD", render: (p) => signed(xgd(p)), style: (p) => heatOn(xgd(p), s.xgd) },
    { label: "Fin", title: "Finishing: goals per xG on his own shots, relative to average (shrunk)",
      render: (p) => signedPct(p.rating?.finishing), style: (p) => heatOn(p.rating?.finishing, r.finishing) },
    { label: "Min", title: "5v5 minutes this season", render: (p) => { const o = on(p); return o ? Math.round(o.toi_s / 60) : dash; } },
  ];
}

/** Power-play or penalty-kill columns: projected share, season usage, on-ice rate, talent. */
function specialCols(kind: "pp" | "pk", sc: Scales, season: SeasonKey): Col[] {
  const s: SeasonScales = sc[season];
  const st = (p: LineupPlayerStats) => p.special[season]?.[kind] ?? null;
  const pp = kind === "pp";
  const label = pp ? "PP" : "PK";
  return [
    { label: "Proj", title: `Projected share of ${label} time tonight`, render: (p) => pct(pp ? p.spp : p.spk, 0) },
    { group: "Season", label: "TOI", title: `${pp ? "5v4" : "4v5"} minutes`, render: (p) => { const x = st(p); return x ? minutes(x.toi_s) : dash; } },
    { group: "Season", label: "Share", title: `Share of his team's ${pp ? "5v4" : "4v5"} time`, render: (p) => pct(st(p)?.share, 0) },
    pp
      ? { group: "Season", label: "xGF/60", title: "On-ice expected goals for per 60 at 5v4",
          render: (p) => st(p)?.xgf60.toFixed(2) ?? dash, style: (p) => heatOn(st(p)?.xgf60, s.pp) }
      : { group: "Season", label: "xGA/60", title: "On-ice expected goals against per 60 at 4v5: lower is better",
          render: (p) => st(p)?.xga60.toFixed(2) ?? dash, style: (p) => heatOn(st(p)?.xga60, s.pk, true) },
    pp
      ? { group: "Rating /60", label: "PP xGF", title: "Power-play offense vs average (shrunk)", render: (p) => signed(p.rating?.pp_off),
          style: (p) => heatOn(p.rating?.pp_off, sc.rating.pp_off) }
      : { group: "Rating /60", label: "PK xGA", title: "Penalty-kill expected goals against vs average: lower is better",
          render: (p) => signed(p.rating ? -p.rating.pk_def : null), style: (p) => heatOn(p.rating?.pk_def, sc.rating.pk_def) },
  ];
}

const ordinal = (n: number) => {
  const s = ["th", "st", "nd", "rd"], v = n % 100;
  return `${n}${s[(v - 20) % 10] || s[v] || s[0]}`;
};

/** The team's PP or PK rate for the season and its league rank, above that unit table. */
function TeamRate({ label, r }: { label: string; r: TeamSpecialRate | null | undefined }) {
  const mid = r ? (r.teams + 1) / 2 : 0;
  return (
    <div className="flex h-9 items-center gap-x-2 overflow-hidden whitespace-nowrap border-b border-border px-3 text-xs">
      <span className="font-semibold uppercase tracking-wide">{label}</span>
      {r?.pct != null && r.rank != null ? (
        <>
          <span className="rounded px-1.5 py-0.5 font-semibold tabular" style={heat(r.rank, mid - 1, { center: mid, lowerIsBetter: true })}>
            {pct(r.pct, 1)} · {ordinal(r.rank)} in NHL
          </span>
          <span className="text-muted-foreground tabular">{r.goals} goals {label === "Power Play" ? "on" : "allowed in"} {r.opps} times</span>
        </>
      ) : (
        <span className="text-muted-foreground">No games this season</span>
      )}
    </div>
  );
}

// ---- Goalies ----------------------------------------------------------------------------------

/** Exactly ``TOP_GOALIES`` named goalies (most likely starters first, padded when short) and one
 *  "Other" row holding everyone else's start probability. */
function goalieRows(rows: GoalieStats[]): GoalieStats[] {
  const blank = (name: string, p_start: number): GoalieStats => ({
    player_id: null, player_name: name, p_start, p_model: 0, dfo_status: null, source: "", rating: null, season: { cur: null, prev: null },
  });
  const named = rows.filter((g) => g.player_id != null).sort((a, b) => b.p_start - a.p_start);
  const top = named.slice(0, TOP_GOALIES);
  const rest = rows.filter((g) => !top.includes(g)).reduce((a, g) => a + g.p_start, 0);
  while (top.length < TOP_GOALIES) top.push(blank("Unknown", 0));
  return [...top, blank("Other", rest)];
}

function Goalies({ rows: all, season, sc }: { rows: GoalieStats[]; season: SeasonKey; sc: Scales }) {
  const rows = goalieRows(all);
  const s = sc[season];
  const th = "w-16 whitespace-nowrap px-1 py-0 text-center font-medium";
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[620px] text-sm tabular">
        <thead className="text-[11px] text-muted-foreground">
          <tr className="border-b border-border">
            <th className="px-3 py-1.5 text-left font-medium">Goalie</th>
            <th className={th} title="Probability he starts (DailyFaceoff + model)">Start</th>
            <th className={th} title="Games started">GS</th>
            <th className={th} title="Shots against in his starts">SA</th>
            <th className={th}>Sv%</th>
            <th className={th} title="Goals saved above expected (xGA − GA)">GSAx</th>
            <th className={th} title="Share of expected goals stopped beyond average (shrunk rating)">Save Talent</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((g, i) => {
            const x = g.season[season];
            return (
              <tr key={g.player_id ?? `${g.player_name}-${i}`} className="h-8 border-b border-border last:border-0">
                <td className="px-3 py-0">
                  <span className="inline-flex items-center gap-1.5">
                    {g.player_name}
                    {g.dfo_status && <Badge tone={g.dfo_status === "Confirmed" ? "pos" : "muted"}>{g.dfo_status}</Badge>}
                  </span>
                </td>
                <td className="px-1 py-0 text-center font-semibold">{pct(g.p_start, 0)}</td>
                <td className="px-1 py-0 text-center">{x?.starts ?? dash}</td>
                <td className="px-1 py-0 text-center">{x?.shots_against ?? dash}</td>
                <td className="px-1 py-0 text-center" style={heatOn(x?.sv_pct, s.sv_pct)}>
                  {x?.sv_pct != null ? x.sv_pct.toFixed(3).replace(/^0/, "") : dash}
                </td>
                <td className="px-1 py-0 text-center" style={heatOn(x?.gsax, s.gsax)}>{signed(x?.gsax, 1)}</td>
                <td className="px-1 py-0 text-center" style={heatOn(g.rating?.save, sc.rating.save)}
                  title={g.rating ? `± ${pct(g.rating.save_sd, 1)}` : undefined}>
                  {g.rating ? `${signed(g.rating.save * 100, 1)}%` : dash}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ---- Team card --------------------------------------------------------------------------------

function TeamCard({ abbr, t, season, sc }: { abbr: string; t: TeamLineup; season: SeasonKey; sc: Scales }) {
  const byId = new Map(t.players.map((p) => [p.player_id, p]));
  /** A unit's rows: projected players in their slots, empty spots as replacements. */
  const unitGroup = (kind: LineupUnit["kind"], slot: string): Group => {
    const { size, roles: want } = LAYOUT[kind];
    const u = t.units.find((x) => x.slot === slot);
    const present = (u?.player_ids ?? []).map((id, i) => ({ p: byId.get(id), role: u?.roles?.[i] ?? "" }))
      .filter((m): m is { p: LineupPlayerStats; role: string } => !!m.p).slice(0, size);
    const players: (LineupPlayerStats | null)[] = Array(size).fill(null);
    const roles: string[] = Array(size).fill("");
    if (want) {
      // 5v5: each player in the spot he fills (LW-C-RW, LD-RD), anyone left over in the first open one.
      const left = present.filter((m) => {
        const i = want.indexOf(m.role);
        if (i < 0 || players[i]) return true;
        [players[i], roles[i]] = [m.p, m.role];
        return false;
      });
      want.forEach((r, i) => { if (!players[i]) roles[i] = r; });
      left.forEach((m) => { const i = players.indexOf(null); [players[i], roles[i]] = [m.p, m.role]; });
    } else {
      present.forEach((m, i) => { [players[i], roles[i]] = [m.p, m.role]; });
    }
    return { key: slot, label: UNIT_LABEL[slot] ?? slot.toUpperCase(), record: u ? u.record[season] : null, players, roles };
  };
  const units = (kind: LineupUnit["kind"]) => LAYOUT[kind].slots.map((slot) => unitGroup(kind, slot));
  const extras = t.players.filter((p) => !p.slot || !FIVE_ON_FIVE.includes(p.slot));
  const ev = evenStrengthCols(sc, season);
  const goalie = t.sources.goalie;

  return (
    <Card className="space-y-4 p-4">
      <div className="flex items-center gap-2 text-base font-semibold">
        <TeamLogo abbr={abbr} className="h-7 w-7" /> {team(abbr).name || abbr}
      </div>

      <Panel
        title="Goalies"
        link={goalie}
        aside={goalie?.status && (
          <span className="inline-flex items-center gap-1.5 normal-case tracking-normal">
            <Badge tone={goalie.status === "Confirmed" ? "pos" : "warn"}>{goalie.status}</Badge>
            {goalie.goalie_name && <span className="font-medium">{goalie.goalie_name}</span>}
          </span>
        )}
      >
        <Goalies rows={t.goalies} season={season} sc={sc} />
      </Panel>

      <Panel title="Forwards · 5v5" link={t.sources.lines}>
        <UnitTable cols={ev} groups={units("F")} />
      </Panel>
      <Panel title="Defense · 5v5">
        <UnitTable cols={ev} groups={units("D")} />
      </Panel>

      <Panel title="Special Teams">
        <TeamRate label="Power Play" r={t.special_teams[season]?.pp} />
        <UnitTable cols={specialCols("pp", sc, season)} groups={units("PP")} />
        <div className="border-t-2 border-border">
          <TeamRate label="Penalty Kill" r={t.special_teams[season]?.pk} />
          <UnitTable cols={specialCols("pk", sc, season)} groups={units("PK")} />
        </div>
      </Panel>

      {extras.length > 0 && (
        <Panel title="Extras · 5v5">
          <UnitTable cols={ev} groups={[{ key: "extra", label: "Not in the projected lines", players: extras }]} />
        </Panel>
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
        <TeamCard abbr={away} t={d.away} season={season} sc={d.scales} />
        <TeamCard abbr={home} t={d.home} season={season} sc={d.scales} />
      </div>
      <p className="mt-3 text-xs text-muted-foreground">
        Ratings are each skater&apos;s talent per 60 relative to average (xGA: lower is better), as the model uses them today.
        On-Ice, Season and time together are what happened in the selected season. Save Talent is the share of expected goals a
        goalie stops beyond average. Team PP% and PK% are regular season only. Cell colors are scaled across the whole league (green better, red worse). Type is the
        forward archetype: SW skill winger, BW balanced winger, PF power forward, OC offensive centre, 2C two-way centre.
      </p>
    </div>
  );
}
