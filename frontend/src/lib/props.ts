import type { PropType } from "./types";

export const PROP_LABEL: Record<PropType, string> = {
  goals: "Goals", assists: "Assists", points: "Points", shots: "Shots", blocks: "Blocks", saves: "Saves",
};

/** Projection column stem per prop type (the API says "ast" for assists). */
export const PROP_STEM: Record<PropType, string> = {
  goals: "goals", assists: "ast", points: "points", shots: "shots", blocks: "blocks", saves: "saves",
};

const RUNG_ABBR: Record<PropType, string> = { goals: "G", assists: "A", points: "Pts", shots: "SOG", blocks: "BLK", saves: "SV" };

/** ("points", 0.5, "over") -> "Points O0.5" */
export const propBet = (prop: PropType, line: number, side: "over" | "under") =>
  `${PROP_LABEL[prop]} ${side === "over" ? "O" : "U"}${line}`;

/** ("points", 0.5) -> "1+ Pts" (the ladder rung an over is the same bet as). */
export const rung = (prop: PropType, line: number) => `${Math.floor(line) + 1}+ ${RUNG_ABBR[prop]}`;

/** Model P(over line) from a projection row's p_{stem}_{k} columns. */
export const modelOver = (row: Record<string, unknown>, prop: PropType, line: number): number | null => {
  const v = row[`p_${PROP_STEM[prop]}_${Math.floor(line) + 1}`];
  return typeof v === "number" ? v : null;
};

/** Decimal-odds helpers for American prices. */
export const decimalOdds = (american: number) => (american < 0 ? 1 + 100 / -american : 1 + american / 100);
export const impliedProb = (american: number) => 1 / decimalOdds(american);

export const SLOT_LABEL: Record<string, string> = {
  f1: "L1", f2: "L2", f3: "L3", f4: "L4", d1: "D1", d2: "D2", d3: "D3", d4: "D4",
};

/** Longest price a play can be (nhl.props.live.MAX_PRICE); edge views hide longer rungs. */
export const MAX_PRICE = 400;

/** A side is a play when it's in the ledger, or flagged with a stake now. */
export const isPlay = (e: { bet_stake: number | null; flagged: boolean; stake_units: number }) =>
  e.bet_stake != null || (e.flagged && e.stake_units > 0);

/** Units at risk on a play: as placed in the ledger, else the current stake. */
export const playStake = (e: { bet_stake: number | null; stake_units: number }) => e.bet_stake ?? e.stake_units;

/** Short book labels for tight cells. */
export const BOOK_ABBR: Record<string, string> = { DraftKings: "DK", FanDuel: "FD", LowVig: "LV", "4Casters": "4C" };
export const bookAbbr = (book: string) => BOOK_ABBR[book] ?? book.slice(0, 2);
