"use client";

import { useState } from "react";
import { Pills } from "@/components/ui";
import { signed } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { PartKey, PartValue } from "@/lib/types";

const MODES = [
  { key: "d", label: "xGD" },
  { key: "f", label: "xGF" },
  { key: "a", label: "xGA" },
] as const;
type Mode = (typeof MODES)[number]["key"];

/** Parts in display order, with what each one means. */
export const PART_ROWS: { key: PartKey; label: string; title: string }[] = [
  { key: "own", label: "Own Play", title: "His own rating (for a unit, its members' ratings)" },
  { key: "mates", label: "Teammates", title: "The ratings of the teammates he was on the ice with" },
  { key: "comp", label: "Competition", title: "The ratings of the opponents he was on the ice against" },
  { key: "zone", label: "Zone Starts", title: "Offensive / defensive zone faceoff starts and line changes" },
  { key: "ctx", label: "Score, Venue, Rest, Coach", title: "Score state, home ice, rest days, coaching and post-penalty time" },
  { key: "resid", label: "Luck / Unexplained", title: "What actually happened minus everything above: finishing variance, model miss, noise" },
];

export type Parts = Partial<Record<PartKey, PartValue>>;

/** Is a part's value good (true), bad (false) or neutral (null) for the skater in this mode? */
const good = (v: number | null, mode: Mode) => (v == null || Math.abs(v) < 1e-9 ? null : mode === "a" ? v < 0 : v > 0);

/** One diverging bar from the centre line, scaled so ``scale`` fills half the width. */
function Bar({ v, mode, scale, strong }: { v: number | null; mode: Mode; scale: number; strong?: boolean }) {
  const g = good(v, mode);
  const w = v == null ? 0 : (Math.abs(v) / scale) * 50;
  return (
    <div className="relative h-4">
      <div className="absolute inset-y-0 left-1/2 w-px bg-border" />
      {v != null && w > 0 && (
        <div
          className={cn("absolute inset-y-0.5", strong ? "opacity-100" : "opacity-80", g ? "bg-positive" : g === false ? "bg-negative" : "bg-muted-foreground")}
          style={{
            width: `${w}%`,
            ...(v >= 0 ? { left: "50%", borderRadius: "0 4px 4px 0" } : { right: "50%", borderRadius: "4px 0 0 4px" }),
          }}
        />
      )}
    </div>
  );
}

/**
 * On-ice 5v5 rate relative to league average, split into parts that add up to it exactly.
 * Diverging bars from zero (green helps the skater, red hurts; for xGA, lower is better), with the
 * total on top and each part's xGF/xGA on hover.
 */
export default function Breakdown({ parts, toiS, note }: { parts: Parts; toiS?: number | null; note?: string }) {
  const [mode, setMode] = useState<Mode>("d");
  const val = (k: PartKey) => parts[k]?.[mode] ?? null;
  const league = mode === "d" ? 0 : (parts.league?.[mode] ?? 0);
  const actual = val("actual");
  const total = actual == null ? null : actual - league;
  const rows = PART_ROWS.map((r) => ({ ...r, v: val(r.key) }));
  const scale = Math.max(0.05, ...rows.map((r) => Math.abs(r.v ?? 0)), Math.abs(total ?? 0));
  const tip = (k: PartKey) => {
    const p = parts[k];
    return p ? `xGF ${signed(p.f)} · xGA ${signed(p.a)} · xGD ${signed(p.d)} per 60` : "";
  };

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          On-ice 5v5 {MODES.find((m) => m.key === mode)!.label}/60 vs average, broken down
        </div>
        <Pills options={MODES} value={mode} onChange={setMode} />
      </div>
      <div className="grid grid-cols-[11rem_1fr_3.5rem] items-center gap-x-3 gap-y-1 text-sm">
        <div className="font-medium" title={`On-ice ${mode === "d" ? "xGD" : mode === "f" ? "xGF" : "xGA"}/60 minus the league average`}>
          On-Ice vs. Average
        </div>
        <Bar v={total} mode={mode} scale={scale} strong />
        <div className="tabular text-right font-semibold">{signed(total)}</div>
        <div className="col-span-3 my-0.5 border-t border-border" />
        {rows.map((r) => (
          <div key={r.key} className="contents" title={`${r.title}. ${tip(r.key)}`}>
            <div className="truncate text-muted-foreground">{r.label}</div>
            <Bar v={r.v} mode={mode} scale={scale} />
            <div className="tabular text-right">{signed(r.v)}</div>
          </div>
        ))}
      </div>
      <div className="text-[11px] text-muted-foreground">
        {mode === "a" ? "xGA: lower is better, so green bars are below zero. " : ""}
        Parts add up to the total{toiS ? ` over ${Math.round(toiS / 60)} 5v5 minutes` : ""}; each uses the ratings in effect on the game&apos;s date.
        {note ? ` ${note}` : ""} Hover a row for its xGF / xGA split.
      </div>
    </div>
  );
}
