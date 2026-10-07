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

// /api/ratings (see nhl.ratings.rankings). EV/PP/PK terms are xG per 60 relative to average,
// defence sign-flipped so higher is better; finishing and save are fractions of xG.

export interface Skater {
  player_id: number;
  player_name: string | null;
  position: string | null;
  age: number | null;
  team_abbr: string | null;
  last_game_date: string | null;
  ev_off: number;
  ev_def: number;
  ev_net: number;
  ev_net_sd: number;
  ev_net_prior: number | null;
  ev_toi_s: number;
  pp_off: number | null;
  pk_def: number | null;
  finishing: number | null;
  shots: number | null;
  goals: number | null;
  ixg: number | null;
  pen_drawn60: number | null;
  pen_taken60: number | null;
}

export interface Goalie {
  player_id: number;
  player_name: string | null;
  age: number | null;
  team_abbr: string | null;
  last_game_date: string | null;
  save: number;
  save_prior: number | null;
  save_sd: number;
  shots_against: number;
  goals_against: number;
  xga: number;
  gsax: number;
}

export interface RatingsMeta {
  snapshot: string;
  as_of: string;
  season: number;
}

export interface PlayersResponse extends RatingsMeta {
  skaters: Skater[];
  goalies: Goalie[];
}

export interface TeamLineupPlayer {
  player_id: number | null;
  player_name: string;
  position: string;
  slot: string | null;
  s5: number;
  spp: number;
  spk: number;
  ev_off: number | null;
  ev_def: number | null;
  ev_net: number | null;
}

export interface TeamRating extends TeamInfo {
  team_id: number;
  xgf60: number;
  xga60: number;
  gf60: number;
  ga60: number;
  gd60: number;
  finishing: number;
  save: number;
  pp_xgf60: number;
  pk_xga60: number;
  take_f: number;
  draw_f: number;
  goalies: { player_id: number; player_name: string | null; weight: number }[];
  lineup: TeamLineupPlayer[];
}

export interface TeamsResponse extends RatingsMeta {
  teams: TeamRating[];
}
