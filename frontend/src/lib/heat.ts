import type { CSSProperties } from "react";

/** Strongest fill (percent of the positive/negative color mixed into the cell). */
const MAX_MIX = 42;

// Column scales (the 95th percentile of |value − center| league-wide) come from the API
// (``nhl.api.lineupstats.spread``), so the colors don't change when a table is filtered.

/**
 * Cell background on a continuous diverging scale: neutral at ``center``, green for better and
 * red for worse, saturating at ``scale``. ``lowerIsBetter`` flips the direction (e.g. xGA).
 */
export function heat(
  value: number | null | undefined,
  scale: number,
  { center = 0, lowerIsBetter = false }: { center?: number; lowerIsBetter?: boolean } = {}
): CSSProperties | undefined {
  if (value == null || !Number.isFinite(value)) return undefined;
  const d = (value - center) * (lowerIsBetter ? -1 : 1);
  const mix = Math.round(Math.min(Math.abs(d) / scale, 1) * MAX_MIX);
  if (!mix) return undefined;
  return { background: `color-mix(in srgb, var(${d > 0 ? "--positive" : "--negative"}) ${mix}%, transparent)` };
}
