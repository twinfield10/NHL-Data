"use client";

import { useMemo, useState } from "react";
import ArchetypeBadge from "@/components/ArchetypeBadge";
import Breakdown, { type Parts } from "@/components/Breakdown";
import IdentityCell from "@/components/IdentityCell";
import SortTable, { type Column } from "@/components/SortTable";
import { Card, Empty, ErrorState, Loading, Pills, Signed } from "@/components/ui";
import { useLineRatings } from "@/lib/api";
import { dateTimeET, longDate, minutes, pct, seasonLabel, signed } from "@/lib/format";
import { heat, heatScale } from "@/lib/heat";
import type { LineRating } from "@/lib/types";

type Kind = LineRating["kind"];

/** Per kind: tab label, what one unit is called, the on-ice state it comes from, and its projected slots. */
const KIND: Record<Kind, { label: string; unit: string; plural: string; state: string; slots: string[] }> = {
  F: { label: "Forward Lines", unit: "Line", plural: "lines", state: "5v5", slots: ["1", "2", "3", "4"] },
  D: { label: "Defense Pairs", unit: "Pair", plural: "pairs", state: "5v5", slots: ["1", "2", "3"] },
  PP: { label: "Power Play", unit: "PP Unit", plural: "power-play units", state: "5v4", slots: ["1", "2"] },
  PK: { label: "Penalty Kill", unit: "PK Unit", plural: "penalty-kill units", state: "4v5", slots: ["1", "2"] },
};
const KINDS = (Object.keys(KIND) as Kind[]).map((k) => ({ key: k, label: KIND[k].label }));

/** "f2" -> "FWD 2", "d1" -> "PAIR 1", "pp1" -> "PP 1", "pk2" -> "PK 2". */
const SLOT_PREFIX: Record<string, string> = { f: "FWD", d: "PAIR", pp: "PP", pk: "PK" };
const slotLabel = (slot: string) => {
  const [, prefix, n] = slot.match(/^([a-z]+)(\d+)$/) ?? [];
  return prefix ? `${SLOT_PREFIX[prefix] ?? prefix.toUpperCase()} ${n}` : slot;
};
const slotNumber = (slot: string | null) => slot?.match(/\d+$/)?.[0] ?? null;
/** Slot filter: every unit, today's projected ones, or one projected slot. */
const slotOptions = (kind: Kind) => [
  { key: "all", label: "All" },
  { key: "current", label: "Current" },
  ...KIND[kind].slots.map((n) => ({ key: n, label: n })),
];

/** Minimum time together, as a share of the team's time in that state ("qualified" units). */
const MIN_TOI = [
  { key: "0", label: "Any" },
  { key: "0.01", label: "1%" },
  { key: "0.02", label: "2%" },
  { key: "0.03", label: "3%" },
  { key: "0.05", label: "5%" },
  { key: "0.1", label: "10%" },
  { key: "0.2", label: "20%" },
  { key: "0.3", label: "30%" },
] as const;

/** 20252026 -> "2025-26" */

const per60 = (v: number, toi: number) => (toi ? (v / toi) * 3600 : null);
const xgfPct = (l: LineRating) => (l.xgf + l.xga > 0 ? l.xgf / (l.xgf + l.xga) : null);
/** A unit's 5v5 decomposition part (xGF − xGA unless ``side`` is given), from the ctx_ fields. */
const ctx = (l: LineRating, part: string, side?: "f" | "a") => {
  const f = l[`ctx_${part}_f`], a = l[`ctx_${part}_a`];
  if (side) return (side === "f" ? f : a) ?? null;
  return f == null || a == null ? null : f - a;
};
const unitParts = (l: LineRating): Parts =>
  Object.fromEntries(
    ["own", "mates", "comp", "zone", "ctx", "resid", "actual", "league"].map((p) => [
      p, { f: ctx(l, p, "f"), a: ctx(l, p, "a"), d: ctx(l, p) },
    ])
  );

const unitKey = (l: LineRating) => `${l.team_id}-${l.kind}-${l.players.map((p) => p.player_id).join("-")}`;

/** Last names, with a first initial where two players on the unit share one ("A. Protas"). */
function shortNames(l: LineRating): string {
  const last = l.players.map((p) => p.player_name.split(" ").slice(-1)[0]);
  return l.players
    .map((p, i) => (last.filter((n) => n === last[i]).length > 1 ? `${p.player_name[0]}. ${last[i]}` : last[i]))
    .join(" – ");
}

type Scales = { xgd: number; xgf: number; xga: number };

/** Scales from every unit of one kind league-wide, so filtering doesn't change the colors. */
const scalesFor = (lines: LineRating[]): Scales => ({
  xgd: heatScale(lines.map((l) => l.xgd60)),
  xgf: heatScale(lines.map((l) => l.xgf60)),
  xga: heatScale(lines.map((l) => l.xga60)),
});

function columns(s: Scales, kind: Kind): Column<LineRating>[] {
  const { unit, state } = KIND[kind];
  const lead: Column<LineRating>[] = [
    {
      key: "line", label: unit, className: "h-px p-0",
      sort: (l) => shortNames(l),
      render: (l) => (
        <IdentityCell abbr={l.team_abbr}>
          <span className="font-medium">{shortNames(l)}</span>
        </IdentityCell>
      ),
    },
    ...(kind === "F"
      ? [{
          key: "mix", label: "Mix",
          title: "Each forward's archetype, in line order (style only): SW skill winger, BW balanced winger, PF power forward, OC offensive centre, 2C two-way centre. Line chemistry tested: archetype mixes don't beat the sum of the players.",
          sort: (l: LineRating) => l.players.map((p) => p.archetype ?? "").sort().join(","),
          render: (l: LineRating) => (
            <span className="inline-flex gap-1">
              {l.players.filter((p) => p.position !== "D").map((p) => <ArchetypeBadge key={p.player_id ?? p.player_name} name={p.archetype} />)}
            </span>
          ),
        } satisfies Column<LineRating>]
      : []),
    {
      key: "slot", label: "Slot", title: "Slot in today's projected lineup (– if the unit isn't in it)",
      render: (l) => <span className="text-muted-foreground">{l.slot ? slotLabel(l.slot) : "–"}</span>, sort: (l) => l.slot,
    },
    {
      key: "toi", label: "TOI Together", align: "right",
      title: `${state} time on ice together this season, and its share of the team's ${state} time`,
      render: (l) => (
        <span>
          {minutes(l.toi_s)} <span className="text-xs text-muted-foreground">{pct(l.toi_share, 1)}</span>
        </span>
      ),
      sort: (l) => l.toi_share,
    },
  ];
  const ratingF: Column<LineRating> = {
    key: "xgf", label: kind === "PP" ? "PP xGF/60" : "xGF/60", align: "right", className: kind === "PP" ? "font-semibold" : undefined,
    title: `${state} xG for per 60 this unit adds while on the ice, relative to average (sum of its players' current ratings)`,
    render: (l) => signed(l.xgf60), sort: (l) => l.xgf60, style: (l) => heat(l.xgf60, s.xgf),
  };
  const ratingA: Column<LineRating> = {
    key: "xga", label: kind === "PK" ? "PK xGA/60" : "xGA/60", align: "right", className: kind === "PK" ? "font-semibold" : undefined,
    title: `${state} xG against per 60 relative to average, lower is better (sum of its players' current ratings)`,
    render: (l) => signed(l.xga60), sort: (l) => -l.xga60, style: (l) => heat(l.xga60, s.xga, { lowerIsBetter: true }),
  };
  const actF: Column<LineRating> = {
    key: "oxgf", label: "Act. xGF/60", align: "right", title: `Actual ${state} xG for per 60 together (raw; small samples are noisy)`,
    render: (l) => per60(l.xgf, l.toi_s)?.toFixed(2) ?? "–", sort: (l) => per60(l.xgf, l.toi_s),
  };
  const actA: Column<LineRating> = {
    key: "oxga", label: "Act. xGA/60", align: "right", title: `Actual ${state} xG against per 60 together (raw)`,
    render: (l) => per60(l.xga, l.toi_s)?.toFixed(2) ?? "–", sort: (l) => -(per60(l.xga, l.toi_s) ?? Infinity),
  };
  const goals: Column<LineRating> = {
    key: "goals", label: "Act. GF–GA", align: "right", title: `${state} goals for and against together`,
    render: (l) => `${l.gf}–${l.ga}`, sort: (l) => l.gf - l.ga,
  };
  const gp: Column<LineRating> = {
    key: "gp", label: "GP", align: "right", title: "Games the unit played together",
    render: (l) => <span className="text-muted-foreground">{l.games}</span>, sort: (l) => l.games,
  };

  const tier: Column<LineRating> = {
    key: "tier", label: "Tier", title: "The members' most common ice-time tier (rank in the team's 5v5 TOI each game)",
    render: (l) => <span className="text-muted-foreground">{l.tier ?? "–"}</span>, sort: (l) => l.tier,
  };
  const comp: Column<LineRating> = {
    key: "comp", label: "Comp xGD/60", align: "right",
    title: "What the opponents they faced did to their 5v5 xGD per 60 (negative = tougher competition), while the whole unit was on the ice",
    render: (l) => <Signed value={ctx(l, "comp")}>{signed(ctx(l, "comp"))}</Signed>, sort: (l) => ctx(l, "comp"),
  };

  if (kind === "PP") return [...lead, ratingF, actF, actA, goals, gp];
  if (kind === "PK") return [...lead, ratingA, actA, actF, goals, gp];
  return [
    ...lead.slice(0, 2),
    tier,
    lead[2],
    {
      key: "xgd", label: "xGD/60", align: "right", className: "font-semibold",
      title: "5v5 xG differential per 60 this unit adds while on the ice, relative to average (sum of its players' current ratings)",
      render: (l) => signed(l.xgd60), sort: (l) => l.xgd60, style: (l) => heat(l.xgd60, s.xgd),
    },
    ratingF,
    ratingA,
    actF,
    actA,
    {
      key: "oxgfp", label: "Act. xGF%", align: "right", title: "Share of 5v5 xG that went their way together",
      render: (l) => pct(xgfPct(l)), sort: xgfPct,
    },
    comp,
    goals,
    gp,
  ];
}

/** Default sort per kind: net for lines and pairs, the side that matters for special teams. */
const DEFAULT_SORT: Record<Kind, string> = { F: "xgd", D: "xgd", PP: "xgf", PK: "xga" };

/** Each player's share of the unit's rating. */
function LineDetail({ line }: { line: LineRating }) {
  const st = line.kind === "PP" || line.kind === "PK";
  return (
    <div className="space-y-2">
      <div className="grid max-w-3xl gap-x-6 gap-y-1 text-sm sm:grid-cols-[1fr_auto_auto_auto]">
        <div className="text-xs text-muted-foreground">Player</div>
        <div className="text-right text-xs text-muted-foreground">5v5 xGD/60</div>
        <div className="text-right text-xs text-muted-foreground">5v5 xGF/60</div>
        <div className="text-right text-xs text-muted-foreground">5v5 xGA/60</div>
        {line.players.map((p, i) => {
          const xga = p.ev_def == null ? null : -p.ev_def;
          return [
            <div key={`n${i}`} className="flex items-center gap-1.5">
              {p.player_name} <span className="text-xs text-muted-foreground">{p.position ?? ""}</span>
              {p.archetype && <ArchetypeBadge name={p.archetype} />}
            </div>,
            <div key={`d${i}`} className="tabular text-right"><Signed value={p.ev_net}>{signed(p.ev_net)}</Signed></div>,
            <div key={`f${i}`} className="tabular text-right"><Signed value={p.ev_off}>{signed(p.ev_off)}</Signed></div>,
            <div key={`a${i}`} className="tabular text-right"><Signed value={xga == null ? null : -xga}>{signed(xga)}</Signed></div>,
          ];
        })}
      </div>
      {st && <div className="text-xs text-muted-foreground">Special-teams ratings per player are on the Players tab (PP / PK columns).</div>}
      {!st && line.ctx_toi_s ? (
        <div className="max-w-3xl border-t border-border pt-3">
          <Breakdown parts={unitParts(line)} toiS={line.ctx_toi_s} note="Own play is the unit's players; teammates are the other skaters with them." />
        </div>
      ) : null}
    </div>
  );
}

export default function LinesPage() {
  const [season, setSeason] = useState<number | null>(null);
  const { data, isLoading, error } = useLineRatings(season);
  const [kind, setKind] = useState<Kind>("F");
  const [slot, setSlot] = useState("all");
  const [minToi, setMinToi] = useState<(typeof MIN_TOI)[number]["key"]>("0.02");
  const [team, setTeam] = useState("");
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState<string | null>(null);

  const all = useMemo(() => data?.lines ?? [], [data]);
  const cols = useMemo(() => columns(scalesFor(all.filter((l) => l.kind === kind)), kind), [all, kind]);
  const teams = useMemo(() => [...new Set(all.map((l) => l.team_abbr))].sort(), [all]);
  const q = query.trim().toLowerCase();
  const shown = all.filter(
    (l) =>
      l.kind === kind &&
      (slot === "all" || (slot === "current" ? !!l.slot : slotNumber(l.slot) === slot)) &&
      l.toi_share >= Number(minToi) &&
      (!team || l.team_abbr === team) &&
      (!q || l.players.some((p) => p.player_name.toLowerCase().includes(q)))
  );
  const isCurrent = data ? data.line_season === data.season : true;
  const { plural, state } = KIND[kind];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-xl font-semibold">Line Ratings</h1>
        {data && (
          <span className="text-xs text-muted-foreground">
            Snapshot {longDate(data.snapshot)} · lineups as of {dateTimeET(data.as_of)} ET
          </span>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <Pills
          options={KINDS}
          value={kind}
          onChange={(k) => {
            setKind(k);
            if (slot !== "all" && slot !== "current" && !KIND[k].slots.includes(slot)) setSlot("all");
          }}
        />
        <select
          value={season ?? data?.line_season ?? ""}
          onChange={(e) => {
            setSeason(Number(e.target.value));
            setSlot("all");
          }}
          className="rounded-md border border-border bg-card px-2 py-1 text-sm"
        >
          {(data?.seasons ?? []).map((s) => (
            <option key={s} value={s}>
              {seasonLabel(s)}
            </option>
          ))}
        </select>
      </div>
      <div className="flex flex-wrap items-center gap-3">
        {isCurrent && (
          <span className="flex items-center gap-1 text-xs text-muted-foreground" title="Units in today's projected lineup, or one projected slot">
            Slot
            <Pills options={slotOptions(kind)} value={slot} onChange={setSlot} />
          </span>
        )}
        <label className="flex items-center gap-1.5 text-xs text-muted-foreground" title={`Minimum ${state} time together, as a share of the team's ${state} time this season`}>
          Min TOI
          <select
            value={minToi}
            onChange={(e) => setMinToi(e.target.value as (typeof MIN_TOI)[number]["key"])}
            className="rounded-md border border-border bg-card px-2 py-1 text-sm text-foreground"
          >
            {MIN_TOI.map((o) => (
              <option key={o.key} value={o.key}>
                {o.label}
              </option>
            ))}
          </select>
          of team {state}
        </label>
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
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search player"
          className="w-44 rounded-md border border-border bg-card px-2 py-1 text-sm placeholder:text-muted-foreground"
        />
      </div>

      {isLoading && <Loading />}
      {error && <ErrorState error={error} />}
      {data &&
        (shown.length ? (
          <Card>
            <SortTable
              key={kind}
              rows={shown}
              columns={cols}
              rowKey={unitKey}
              initialSort={{ key: DEFAULT_SORT[kind], desc: true }}
              onRowClick={(l) => setOpen(open === unitKey(l) ? null : unitKey(l))}
              expanded={(l) => (open === unitKey(l) ? <LineDetail line={l} /> : null)}
            />
          </Card>
        ) : (
          <Empty>No {plural} match{minToi !== "0" ? " (try a lower Min TOI)" : ""}.</Empty>
        ))}

      {data && (
        <p className="text-xs text-muted-foreground">
          Every unit a team has iced, from the play-by-play shifts: forward lines (three forwards) and pairs at 5v5,
          power-play units (all five skaters at 5v4) and penalty-kill units (all four at 4v5). A player appears in every
          combination he has played in; units under 0.5% of their team&apos;s time in that state are left out. TOI and the
          &quot;Act.&quot; columns are what the unit has done together, raw and noisy in small samples. The rating columns
          add up the players&apos; current ratings (5v5 for lines and pairs, special teams for PP and PK), which are
          additive: what the unit does relative to average with average teammates and opponents. Slot marks units in
          today&apos;s projected lineup. Min TOI keeps units that have played at least that share of their team&apos;s
          time in that state together (over a full season a top line runs about 10% of 5v5, a top PP unit 15–30% of 5v4).
          Colors are scaled across every unit of that kind in the league that season.
        </p>
      )}
    </div>
  );
}
