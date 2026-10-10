/** Short labels for the game markets. */
export const MARKET_LABEL: Record<string, string> = { moneyline: "ML", puckline: "PL", total: "Total" };

/** Game-market filter options: All, ML, PL, Total. */
export const MARKET_FILTERS = [
  { key: "all", label: "All Markets" },
  { key: "moneyline", label: "ML" },
  { key: "puckline", label: "PL" },
  { key: "total", label: "Total" },
] as const;
export type MarketFilter = (typeof MARKET_FILTERS)[number]["key"];
