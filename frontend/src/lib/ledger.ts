import type { BetStatus, BetTotals, OpenTotals } from "./types";

/** The ledger fields the summaries read (game-market and prop bets both carry them). */
export interface LedgerBet {
  status: BetStatus;
  stake_units: number;
  pnl_units: number | null;
  clv: number | null;
  clv_now: number | null;
  price_clv: number | null;
}

/** Mean of the non-null values, or null when there are none (Polars' ``mean`` skips nulls). */
const mean = (xs: (number | null | undefined)[]): number | null => {
  const v = xs.filter((x): x is number => x != null);
  return v.length ? v.reduce((s, x) => s + x, 0) / v.length : null;
};

/** Share of the non-null values above zero. */
const sharePositive = (xs: (number | null | undefined)[]) => mean(xs.map((x) => (x == null ? null : x > 0 ? 1 : 0)));

/** Headline numbers for graded bets; mirrors ``_totals`` in nhl.api.routers.betting / props. */
export function gradedTotals(bets: LedgerBet[]): BetTotals {
  const staked = bets.reduce((s, b) => s + b.stake_units, 0);
  const pnl = bets.reduce((s, b) => s + (b.pnl_units ?? 0), 0);
  return {
    bets: bets.length, staked, pnl, roi: staked ? pnl / staked : null,
    mean_clv: mean(bets.map((b) => b.clv)), mean_price_clv: mean(bets.map((b) => b.price_clv)),
    beat_close: sharePositive(bets.map((b) => b.clv)),
  };
}

/** Ungraded bets against the market now; mirrors ``_open_totals`` in the API routers. */
export function openTotals(bets: LedgerBet[]): OpenTotals {
  const open = bets.filter((b) => b.status !== "graded");
  const count = (s: BetStatus) => open.filter((b) => b.status === s).length;
  return {
    bets: open.length, staked: open.reduce((s, b) => s + b.stake_units, 0),
    mean_clv: mean(open.map((b) => b.clv_now)), beating: sharePositive(open.map((b) => b.clv_now)),
    mean_price_clv: mean(open.map((b) => b.price_clv)),
    value: count("value"), faded: count("faded"), gone: count("gone"), closed: count("closed"),
  };
}

/** Graded totals per group of ``fields`` (bet columns such as ``prop_type`` and ``side``), in the
 *  order the groups appear in ``order`` (the unfiltered server rows), then first appearance. */
export function groupTotals<B extends LedgerBet, F extends keyof B>(
  bets: B[], fields: F[], order: Partial<Record<F, unknown>>[] = [],
): (Pick<B, F> & BetTotals)[] {
  const id = (r: Partial<Record<F, unknown>>) => JSON.stringify(fields.map((f) => r[f] ?? null));
  const groups = new Map<string, B[]>();
  for (const b of bets) groups.set(id(b), [...(groups.get(id(b)) ?? []), b]);
  const rank = new Map(order.map((r, i) => [id(r), i]));
  const at = (k: string) => rank.get(k) ?? order.length;
  return [...groups.entries()]
    .sort(([a], [b]) => at(a) - at(b))
    .map(([, g]) => ({ ...(Object.fromEntries(fields.map((f) => [f, g[0][f]])) as Pick<B, F>), ...gradedTotals(g) }));
}
