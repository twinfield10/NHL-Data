"use client";

import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { Consensus, ModelPoint, SideKey } from "@/lib/types";
import { dateTimeET, fairAmerican, pct, timeET } from "@/lib/format";

export interface ChartSide {
  key: SideKey;
  label: string;
  color: string;
}

interface Row {
  epoch: number;
  line?: number | null;
  [series: string]: number | null | undefined;
}

/** Merge consensus and model points into one row per moment (probabilities in percent). */
function toRows(history: Consensus[], model: ModelPoint[], sides: ChartSide[]): Row[] {
  const rows = new Map<number, Row>();
  const at = (t: string) => {
    const epoch = Date.parse(t);
    if (!rows.has(epoch)) rows.set(epoch, { epoch });
    return rows.get(epoch)!;
  };
  for (const h of history) {
    const r = at(h.t);
    r.line = h.line;
    for (const s of sides) r[`${s.key}_mkt`] = h.fair[s.key] != null ? h.fair[s.key]! * 100 : null;
  }
  for (const m of model) {
    const r = at(m.t);
    for (const s of sides) r[`${s.key}_model`] = m.p[s.key] != null ? m.p[s.key]! * 100 : null;
  }
  return [...rows.values()].sort((a, b) => a.epoch - b.epoch);
}

/**
 * Market consensus (solid, no vig) and the model (dashed) for every side of one market through
 * the day, in each side's color. The y axis is probability, labelled as fair American odds.
 */
export default function LineHistoryChart({ history, model, sides, start, lineLabel }: {
  history: Consensus[];
  model: ModelPoint[];
  sides: ChartSide[];
  start: string | null;
  /** Formats the line (puck line handicap, total) for the tooltip. */
  lineLabel?: (line: number) => string;
}) {
  const rows = toRows(history, model, sides);
  if (rows.length < 2) {
    return <div className="py-12 text-center text-sm text-muted-foreground">Not enough line movement yet</div>;
  }
  const startEpoch = start ? Date.parse(start) : null;
  const values = rows.flatMap((r) => sides.flatMap((s) => [r[`${s.key}_mkt`], r[`${s.key}_model`]])).filter((v): v is number => v != null);
  const lo = Math.max(0, Math.floor((Math.min(...values) - 2) / 5) * 5);
  const hi = Math.min(100, Math.ceil((Math.max(...values) + 2) / 5) * 5);
  // Leave room past puck drop for the "Game Start" label.
  const span = Math.max(rows[rows.length - 1].epoch, startEpoch ?? 0) - rows[0].epoch;
  const xMax = Math.max(rows[rows.length - 1].epoch, startEpoch != null ? startEpoch + Math.max(45 * 60_000, span * 0.08) : 0);
  const multiDay = xMax - rows[0].epoch > 20 * 3_600_000;
  const yTicks = Array.from({ length: (hi - lo) / 5 + 1 }, (_, i) => lo + 5 * i);

  return (
    <div>
      <ResponsiveContainer width="100%" height={380}>
        <LineChart data={rows} margin={{ top: 16, right: 12, bottom: 0, left: 0 }}>
          <CartesianGrid stroke="var(--border)" vertical={false} />
          <XAxis
            dataKey="epoch" type="number" scale="time" domain={[rows[0].epoch, xMax]}
            tickFormatter={(v: number) => (multiDay ? dateTimeET : timeET)(new Date(v).toISOString())}
            tick={{ fontSize: 11, fill: "var(--muted-foreground)" }} stroke="var(--border)" minTickGap={40}
          />
          <YAxis
            domain={[lo, hi]} ticks={yTicks} allowDataOverflow width={52}
            tickFormatter={(v: number) => fairAmerican(v / 100)}
            tick={{ fontSize: 11, fill: "var(--muted-foreground)" }} stroke="var(--border)"
          />
          <Tooltip
            contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 6, fontSize: 12 }}
            labelFormatter={(l, payload) => {
              const line = (payload?.[0]?.payload as Row | undefined)?.line;
              return `${timeET(new Date(Number(l)).toISOString())} ET${line != null && lineLabel ? ` · ${lineLabel(line)}` : ""}`;
            }}
            formatter={(v) => `${fairAmerican(Number(v) / 100)} (${pct(Number(v) / 100)})`}
          />
          {startEpoch != null && (
            <ReferenceLine x={startEpoch} stroke="#ef4444" strokeDasharray="4 4" label={{ value: "Game Start", position: "insideTopLeft", fontSize: 11, fill: "#ef4444" }} />
          )}
          {sides.map((s) => (
            <Line key={`${s.key}_mkt`} type="stepAfter" dataKey={`${s.key}_mkt`} name={`${s.label} Market`} stroke={s.color}
              strokeWidth={2} dot={false} connectNulls isAnimationActive={false} />
          ))}
          {sides.map((s) => (
            <Line key={`${s.key}_model`} type="stepAfter" dataKey={`${s.key}_model`} name={`${s.label} Model`} stroke={s.color}
              strokeWidth={2} strokeDasharray="5 4" dot={false} connectNulls isAnimationActive={false} />
          ))}
        </LineChart>
      </ResponsiveContainer>
      <div className="mt-2 flex flex-wrap items-center justify-center gap-x-5 gap-y-1 text-xs text-muted-foreground">
        {sides.map((s) => (
          <span key={s.key} className="inline-flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-sm" style={{ backgroundColor: s.color }} /> {s.label}
          </span>
        ))}
        <span className="inline-flex items-center gap-1.5"><span className="inline-block w-5 border-t-2 border-muted-foreground" /> Market (No Vig)</span>
        <span className="inline-flex items-center gap-1.5"><span className="inline-block w-5 border-t-2 border-dashed border-muted-foreground" /> Model</span>
      </div>
    </div>
  );
}
