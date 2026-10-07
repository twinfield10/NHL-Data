const ET = "America/New_York";

/** 0.4123 -> "41.2%" */
export const pct = (v: number | null | undefined, digits = 1) =>
  v == null ? "–" : `${(v * 100).toFixed(digits)}%`;

/** 0.0213 -> "+2.1%" */
export const signedPct = (v: number | null | undefined, digits = 1) =>
  v == null ? "–" : `${v > 0 ? "+" : ""}${(v * 100).toFixed(digits)}%`;

/** -107 -> "-107", 110 -> "+110" */
export const american = (v: number | null | undefined) =>
  v == null ? "–" : `${v > 0 ? "+" : ""}${Math.round(v)}`;

/** Fair probability -> American odds, no vig. */
export const fairAmerican = (p: number | null | undefined) => {
  if (p == null || p <= 0 || p >= 1) return "–";
  return american(p >= 0.5 ? (-100 * p) / (1 - p) : (100 * (1 - p)) / p);
};

export const units = (v: number | null | undefined, digits = 2) =>
  v == null ? "–" : `${v > 0 ? "+" : ""}${v.toFixed(digits)}u`;

/** ISO timestamp -> "7:00 PM" Eastern */
export const timeET = (iso: string | null | undefined) =>
  iso ? new Date(iso).toLocaleTimeString("en-US", { timeZone: ET, hour: "numeric", minute: "2-digit" }) : "–";

/** ISO timestamp -> "Oct 6, 7:42 PM" Eastern */
export const dateTimeET = (iso: string | null | undefined) =>
  iso
    ? new Date(iso).toLocaleString("en-US", { timeZone: ET, month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })
    : "–";

/** "2026-10-06" -> "Tue, Oct 6" */
export const longDate = (d: string) =>
  new Date(`${d}T12:00:00`).toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });

/** Today's date (YYYY-MM-DD) in Eastern time. */
export const todayET = () => new Date().toLocaleDateString("en-CA", { timeZone: ET });

export const shiftDate = (d: string, days: number) => {
  const dt = new Date(`${d}T12:00:00`);
  dt.setDate(dt.getDate() + days);
  return dt.toLocaleDateString("en-CA");
};

/** Pipeline run stamp "20261006T234814Z" -> ISO timestamp. */
export const stampToIso = (s: string) =>
  `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}T${s.slice(9, 11)}:${s.slice(11, 13)}:${s.slice(13, 15)}Z`;

/** 0.123 -> "+0.12" (rates such as xG/60). */
export const signed = (v: number | null | undefined, digits = 2) =>
  v == null ? "–" : `${v > 0 ? "+" : ""}${v.toFixed(digits)}`;

/** Seconds -> "12:34" (minutes:seconds). */
export const minutes = (s: number | null | undefined) =>
  s == null ? "–" : `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, "0")}`;
