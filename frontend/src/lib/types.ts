// Mirrors the JSON from src/nhl/api (rows are parquet columns, see nhl.pregame.slate).

export interface SlateGame {
  game_id: number;
  game_date: string;
  start_time: string;
  home_team_abbr: string;
  away_team_abbr: string;
  p_home_win: number;
  p_home_minus_1_5: number;
  p_away_minus_1_5: number;
  mean_home_goals: number;
  mean_away_goals: number;
  p_overtime: number;
  "p_over_5.5": number;
  "p_over_6.5": number;
  home_starter: string | null;
  home_starter_p: number | null;
  home_starter_dfo: string | null;
  away_starter: string | null;
  away_starter_p: number | null;
  away_starter_dfo: string | null;
  home_lineup_issues: string | null;
  away_lineup_issues: string | null;
  home_game_time_decisions: number | null;
  away_game_time_decisions: number | null;
  mkt_p_home_win: number | null;
  mkt_total_line: number | null;
  mkt_p_over: number | null;
  mkt_books: number | null;
  edge_home_win: number | null;
  stale_inputs: string | null;
  as_of: string;
  stamp: string;
  is_final: boolean | null;
  home_score: number | null;
  away_score: number | null;
  last_period: number | null;
  venue_name: string | null;
  venue_location: string | null;
}

export interface Record3 {
  gp: number;
  w: number;
  l: number;
  otl: number;
  pts: number;
  pts_pct: number | null;
}

export interface TeamInfo {
  abbr: string;
  place: string;
  name: string;
  record: Record3;
  l10: Record3;
  streak: string | null;
  /** Last season's record, only while the team hasn't played this season. */
  prev_record: Record3 | null;
}

export interface UnpricedGame {
  game_id: number;
  start_time_et: string | null;
  start_time: string | null;
  home_abbr: string;
  away_abbr: string;
  is_final: boolean;
  home_score: number | null;
  away_score: number | null;
  last_period: number | null;
  venue_name: string | null;
  venue_location: string | null;
}

export interface Freshness {
  source: string;
  last_update: string | null;
  age_hours: number | null;
  max_age_hours: number;
  stale: boolean;
  note: string | null;
}

export interface SlateResponse {
  date: string;
  prev_date: string;
  next_date: string;
  stamp: string | null;
  games: SlateGame[];
  unpriced: UnpricedGame[];
  freshness: Freshness[];
  teams: Record<string, TeamInfo>;
  edges: Edge[];
  /** Best market price per side for games the model hasn't priced yet (no model numbers). */
  lines: MarketLine[];
  bets: Bet[];
}

export interface MarketLine {
  game_id: number;
  market: "moneyline" | "puckline" | "total";
  side: number;
  line: number | null;
  cons_line: number | null;
  selection: string;
  book: string;
  price: number;
  /** Devigged market probability of this side. */
  p_market_side: number;
  captured_at: string;
}

export interface Edge {
  game_id: number;
  market: "moneyline" | "puckline" | "total";
  side: number;
  line: number | null;
  selection: string;
  book: string;
  price: number;
  p_model_side: number;
  p_market_side: number;
  p: number;
  edge: number;
  flagged: boolean;
  qualifies: boolean;
  outlier: boolean;
  stake_units: number;
  home_abbr: string;
  away_abbr: string;
  start_utc: string;
  captured_at: string;
  stamp: string;
  /** "close" = every book's last price before puck drop (game started); "live" = latest. */
  point?: "close" | "live";
}

export interface EdgesResponse {
  date: string;
  stamp: string | null;
  edges: Edge[];
}

export interface LineupPlayer {
  team_id: number;
  player_id: number;
  player_name: string;
  position: string;
  slot: string | null;
  s5: number;
  spp: number;
  spk: number;
  pp_unit: number | null;
  pk_unit: number | null;
  p_dressed: number;
  source: string;
  confidence: string;
  issues: string | null;
}

export interface GoalieProb {
  player_id: number | null;
  player_name: string;
  p_start: number;
  p_model: number;
  dfo_status: string | null;
  source: string;
}

export interface PricePoint {
  stamp: string;
  as_of: string;
  p_home_win: number;
  mkt_p_home_win: number | null;
  mean_home_goals: number;
  mean_away_goals: number;
  home_starter: string | null;
  away_starter: string | null;
}

export interface CatalogGame {
  game_id: number;
  game_date: string;
  start_time_et: string | null;
  home_abbr: string;
  away_abbr: string;
  home_team_id: number;
  away_team_id: number;
  is_final: boolean;
  home_score: number | null;
  away_score: number | null;
  last_period: number | null;
  venue_name: string | null;
  venue_location: string | null;
}

export interface GameResponse {
  game: CatalogGame;
  teams: Record<string, TeamInfo>;
  pregame: SlateGame | null;
  lineups: { home: LineupPlayer[]; away: LineupPlayer[] };
  goalies: { home: GoalieProb[]; away: GoalieProb[] };
  edges: Edge[];
  history: PricePoint[];
}

export interface Bet {
  bet_id: string;
  kind: "paper" | "real";
  side: number;
  line: number | null;
  placed_at: string;
  game_id: number;
  game_date: string;
  market: string;
  selection: string;
  home_abbr: string | null;
  away_abbr: string | null;
  book: string;
  price: number;
  stake_units: number;
  p_blend: number;
  edge: number;
  clv: number | null;
  result: string | null;
  pnl_units: number | null;
  graded_at: string | null;
  note: string | null;
}

export interface BetTotals {
  bets: number;
  staked: number;
  pnl: number;
  roi: number | null;
  mean_clv: number | null;
  beat_close: number | null;
}

export interface BetBreakdown extends BetTotals {
  kind: string;
  market: string;
}

export interface BetsResponse {
  bets: Bet[];
  totals: BetTotals;
  breakdown: BetBreakdown[];
}
