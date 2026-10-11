"use client";

import { Bar, BarChart, CartesianGrid, Cell, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { GoalDistributions } from "@/lib/types";
import { pct } from "@/lib/format";

export interface DistSide {
  label: string;
  color: string;
}

const AXIS = { fontSize: 11, fill: "var(--muted-foreground)" };
const TOOLTIP = { background: "var(--card)", border: "1px solid var(--border)", borderRadius: 6, fontSize: 12 };
const RADIUS: [number, number, number, number] = [4, 4, 0, 0];

/** Fold everything at or above ``top`` into the last bucket ("7+"). */
function fold(p: number[], top: number): number[] {
  return [...p.slice(0, top), p.slice(top).reduce((a, b) => a + b, 0)];
}
const bucket = (i: number, top: number) => (i === top ? `${top}+` : String(i));
const mean = (p: number[]) => p.reduce((a, v, i) => a + v * i, 0);

function Legend({ items }: { items: { label: string; color: string }[] }) {
  return (
    <div className="mt-2 flex flex-wrap items-center justify-center gap-x-5 gap-y-1 text-xs text-muted-foreground">
      {items.map((s) => (
        <span key={s.label} className="inline-flex items-center gap-1.5">
          <span className="inline-block h-2.5 w-2.5 rounded-sm" style={{ backgroundColor: s.color }} /> {s.label}
        </span>
      ))}
    </div>
  );
}

/** Each team's goals, side by side (away first, home second). */
export function TeamGoalsChart({ goals, away, home }: { goals: GoalDistributions; away: DistSide; home: DistSide }) {
  const top = 7;
  const [a, h] = [fold(goals.away, top), fold(goals.home, top)];
  const rows = a.map((_, i) => ({ k: bucket(i, top), away: a[i] * 100, home: h[i] * 100 }));
  return (
    <div>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={rows} margin={{ top: 8, right: 8, bottom: 0, left: 0 }} barGap={2} barCategoryGap="22%">
          <CartesianGrid stroke="var(--border)" vertical={false} />
          <XAxis dataKey="k" tick={AXIS} stroke="var(--border)" tickLine={false} />
          <YAxis tick={AXIS} stroke="var(--border)" width={40} tickFormatter={(v: number) => `${v}%`} />
          <Tooltip cursor={{ fill: "var(--muted)", opacity: 0.4 }} contentStyle={TOOLTIP}
            labelFormatter={(l) => `${l} goal${l === "1" ? "" : "s"}`} formatter={(v) => pct(Number(v) / 100)} />
          <Bar dataKey="away" name={away.label} fill={away.color} radius={RADIUS} isAnimationActive={false} />
          <Bar dataKey="home" name={home.label} fill={home.color} radius={RADIUS} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
      <Legend items={[
        { label: `${away.label} · ${mean(goals.away).toFixed(2)} avg`, color: away.color },
        { label: `${home.label} · ${mean(goals.home).toFixed(2)} avg`, color: home.color },
      ]} />
    </div>
  );
}

/** One distribution split at a line: bars past the line in ``hi``'s color, below in ``lo``'s, a
 *  whole-number line's push in gray. ``values[i]`` is the outcome ``first + i``. */
function SplitChart({ probs, first, line, lo, hi, axisLabel, height = 220 }: {
  probs: number[]; first: number; line: number | null; lo: DistSide; hi: DistSide; axisLabel: (v: number) => string; height?: number;
}) {
  const rows = probs.map((p, i) => ({ v: first + i, k: axisLabel(first + i), p: p * 100 }));
  const color = (v: number) => (line == null ? "var(--muted-foreground)" : v > line ? hi.color : v < line ? lo.color : "var(--muted-foreground)");
  const above = line == null ? null : probs.reduce((s, p, i) => s + (first + i > line ? p : 0), 0);
  const below = line == null ? null : probs.reduce((s, p, i) => s + (first + i < line ? p : 0), 0);
  // The line sits between two bars on a half-point line, on a bar for a whole-number one.
  const refX = line == null ? null : Number.isInteger(line) ? axisLabel(line) : null;
  return (
    <div>
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={rows} margin={{ top: 8, right: 8, bottom: 0, left: 0 }} barCategoryGap={2}>
          <CartesianGrid stroke="var(--border)" vertical={false} />
          <XAxis dataKey="k" tick={AXIS} stroke="var(--border)" tickLine={false} interval={0} />
          <YAxis tick={AXIS} stroke="var(--border)" width={40} tickFormatter={(v: number) => `${v}%`} />
          <Tooltip cursor={{ fill: "var(--muted)", opacity: 0.4 }} contentStyle={TOOLTIP} formatter={(v) => [pct(Number(v) / 100), "Model"]} />
          {refX && <ReferenceLine x={refX} stroke="var(--foreground)" strokeDasharray="4 4" />}
          <Bar dataKey="p" radius={RADIUS} isAnimationActive={false}>
            {rows.map((r) => <Cell key={r.v} fill={color(r.v)} />)}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <Legend items={line == null ? [] : [
        { label: `${lo.label} ${pct(below)}`, color: lo.color },
        { label: `${hi.label} ${pct(above)}`, color: hi.color },
      ]} />
    </div>
  );
}

/** The game total (both teams' goals) against the total line. */
export function TotalGoalsChart({ goals, line, over, under }: { goals: GoalDistributions; line: number | null; over: DistSide; under: DistSide }) {
  const top = 12;
  return (
    <SplitChart probs={fold(goals.total, top)} first={0} line={line} lo={under} hi={over}
      axisLabel={(v) => bucket(v, top)} />
  );
}

/** The home margin against the puck line: the home side covers where margin + line > 0. */
export function MarginChart({ goals, line, away, home }: { goals: GoalDistributions; line: number | null; away: DistSide; home: DistSide }) {
  const span = 6;
  const zero = -goals.margin_min;
  const inner = goals.margin.slice(zero - span, zero + span + 1);
  const lowTail = goals.margin.slice(0, zero - span).reduce((a, b) => a + b, 0);
  const highTail = goals.margin.slice(zero + span + 1).reduce((a, b) => a + b, 0);
  const probs = [inner[0] + lowTail, ...inner.slice(1, -1), inner[inner.length - 1] + highTail];
  const label = (v: number) => (v === span ? `+${span}+` : v === -span ? `−${span}+` : v > 0 ? `+${v}` : v < 0 ? `−${-v}` : "0");
  // Covering the home handicap ``line`` means margin > −line.
  return (
    <SplitChart probs={probs} first={-span} line={line == null ? null : -line} lo={away} hi={home} axisLabel={label} />
  );
}
