import type { CSSProperties } from "react";

/** Strongest fill (percent of the positive/negative color mixed into the cell). */
const MAX_MIX = 42;

/**
 * Scale for a heat column: the ``q`` quantile of |value − center| across the whole league,
 * so the colors don't change when the table is filtered and one outlier doesn't wash out the rest.
 */
export function heatScale(values: (number | null | undefined)[], center = 0, q = 0.95): number {
  const d = values.filter((v): v is number => v != null && Number.isFinite(v)).map((v) => Math.abs(v - center)).sort((a, b) => a - b);
  if (!d.length) return 1;
  return d[Math.min(d.length - 1, Math.floor(q * (d.length - 1)))] || 1;
}

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
