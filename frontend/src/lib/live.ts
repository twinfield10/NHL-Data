import type { Bet, LiveGame, LivePlayer, PropBet } from "./types";

/** How a started bet stands right now. ``won`` / ``lost`` are settled by the score already (an over
 *  that has gone over, an under that hasn't survived); ``winning`` / ``losing`` / ``even`` can still
 *  change. The ledger's own grade (nightly) remains the record. */
export type Standing = "won" | "lost" | "winning" | "losing" | "even" | "pending";

export interface BetStanding {
  standing: Standing;
  /** "Leads by 2", "Needs 3 more", "1 to spare". */
  note: string;
}

/** A game-line bet (moneyline, puck line, total) against the live score. ``side`` 1 is home /
 *  over; ``line`` is the home handicap or the total. */
export function gameBetStanding(b: Pick<Bet, "market" | "side" | "line">, g: LiveGame): BetStanding {
  if (g.home_score == null || g.away_score == null || g.state === "pre") return { standing: "pending", note: "Not started" };
  const final = g.state === "final";
  const [h, a] = [g.home_score, g.away_score];
  if (b.market === "total" && b.line != null) {
    const total = h + a;
    const over = b.side === 1;
    if (total > b.line) return { standing: over ? "won" : "lost", note: `${total} goals` };
    if (final) return total === b.line ? { standing: "even", note: "Push" } : { standing: over ? "lost" : "won", note: `${total} goals` };
    const need = Math.floor(b.line) + 1 - total;
    return over
      ? { standing: "losing", note: `Needs ${need} more` }
      : { standing: "winning", note: need === 1 ? "Next goal loses" : `${need - 1} to spare` };
  }
  // Moneyline (line 0) or puck line: the side's margin with the handicap.
  const handicap = b.market === "puckline" ? (b.line ?? 0) : 0;
  const margin = (b.side === 1 ? h - a : a - h) + (b.side === 1 ? handicap : -handicap);
  if (margin === 0) {
    if (final) return { standing: "even", note: "Push" };
    return { standing: "even", note: b.market === "moneyline" ? "Tied" : "Even with the line" };
  }
  const by = Math.abs(h - a);
  const ahead = (h > a) === (b.side === 1);
  const note = h === a ? "Tied" : `${final ? (ahead ? "Won" : "Lost") : ahead ? "Leads" : "Trails"} by ${by}`;
  if (final) return { standing: margin > 0 ? "won" : "lost", note };
  return { standing: margin > 0 ? "winning" : "losing", note };
}

/** A prop bet against the player's box-score line so far (overs clinch, unders hold until final). */
export function propBetStanding(b: Pick<PropBet, "prop_type" | "line" | "side">, p: LivePlayer | undefined, g: LiveGame): BetStanding {
  if (g.state === "pre") return { standing: "pending", note: "Not started" };
  const stat = p?.[b.prop_type];
  if (stat == null) return { standing: "pending", note: p ? "No stat yet" : "Not in the box score" };
  const over = b.side === "over";
  if (stat > b.line) return { standing: over ? "won" : "lost", note: `${stat} so far` };
  if (g.state === "final") return { standing: over ? "lost" : "won", note: `${stat} final` };
  const need = Math.floor(b.line) + 1 - stat;
  return over
    ? { standing: "losing", note: `${stat} · needs ${need} more` }
    : { standing: "winning", note: need === 1 ? `${stat} · next one loses` : `${stat} · ${need - 1} to spare` };
}

/** Units won if the bet wins, at American ``price``. */
export const toWin = (stake: number, price: number) => (price < 0 ? stake * 100 / -price : stake * price / 100);
