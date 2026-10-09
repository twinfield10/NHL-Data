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
  /** Regulation three-way per game (model where priced, market where quoted). */
  three_way: ThreeWay[];
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
  edges: Edge[];
  markets: MarketView;
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
  /** Forward archetype from the current style view (null for defensemen and unplaced skaters). */
  archetype: string | null;
  archetype_conf: number | null;
  style_group: "F" | "D" | null;
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

/** Heat-color scale per column (the 95th percentile of |value − center| league-wide), from the API. */
export type HeatScales = Record<string, number>;

export interface PlayersResponse extends RatingsMeta {
  skaters: Skater[];
  goalies: Goalie[];
  /** Skaters: xgd, xgf, xga, pp, pk, fin; goalies: save, gsax (every rated player on a team). */
  scales: { skaters: HeatScales; goalies: HeatScales };
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
  xgd60: number;
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
  /** League-average xG/60 the team rates are measured against. */
  league: { xg60_5v5: number; xg60_pp: number };
  teams: TeamRating[];
  /** gd, xgd, xgf, xga, fin, save, pp, pk across all teams (xGF/xGA, PP/PK centred on the league). */
  scales: HeatScales;
}

export interface LinePlayer {
  player_id: number | null;
  player_name: string;
  position?: string | null;
  ev_off?: number | null;
  /** Prevention (higher is better), as on the player board. */
  ev_def?: number | null;
  ev_net?: number | null;
  /** Forward archetype (null for defensemen). */
  archetype?: string | null;
}

export interface LineRating {
  team_id: number;
  team_abbr: string;
  kind: "F" | "D" | "PP" | "PK";
  /** Slot in today's projected lineup (f1..f4, d1..d3, pp1, pk2..), null if the unit isn't in it. */
  slot: string | null;
  players: LinePlayer[];
  /** Summed current ratings (5v5 for lines and pairs, special teams for PP/PK), relative to
   *  average (xga60: lower is better). */
  xgf60: number;
  xga60: number;
  xgd60: number;
  /** On the ice together in the season (5v5, or 5v4 / 4v5 for PP / PK). */
  toi_s: number;
  xgf: number;
  xga: number;
  gf: number;
  ga: number;
  games: number;
  /** Team's total time in that state, and the unit's share of it together. */
  team_toi_s: number;
  toi_share: number;
  /** Lines and pairs: the members' most common ice-time tier, and the 5v5 decomposition while the
   *  whole unit was on the ice (regulation; per 60, ``_f`` = xGF, ``_a`` = xGA). Null for PP/PK. */
  tier?: string | null;
  ctx_toi_s?: number | null;
  [ctx: `ctx_${string}`]: number | null | undefined;
}

export interface LinesResponse extends RatingsMeta {
  /** Season the units come from, and the seasons available. */
  line_season: number;
  seasons: number[];
  lines: LineRating[];
  /** Per unit kind (F, D, PP, PK): xgd, xgf, xga from every unit of that kind. */
  scales: Record<string, HeatScales>;
}

/** One decomposition part per 60: xGF (``f``), xGA (``a``, lower is better) and xGD (``d``). */
export interface PartValue {
  f: number | null;
  a: number | null;
  d: number | null;
}

export type PartKey = "own" | "mates" | "comp" | "zone" | "ctx" | "resid" | "actual" | "league";

export interface Linemate {
  player_id: number;
  player_name: string;
  shared_s: number;
  /** Share of his 5v5 time with this teammate. */
  share: number | null;
  games: number;
  ev_net: number | null;
}

export interface PlayerContextRow {
  season: number;
  team_id: number;
  team_abbr: string | null;
  group: "F" | "D";
  games: number;
  toi_s: number;
  parts: Record<PartKey, PartValue>;
  /** Quality of teammates / competition: mean net rating (O − D) of the 4 teammates / 5 opponents,
   *  with percentiles within F or D (≥ 200 5v5 minutes, else null). */
  qot_net: number | null;
  qoc_net: number | null;
  qot_net_pct: number | null;
  qoc_net_pct: number | null;
  qot_toi: number | null;
  qoc_toi: number | null;
  usage: {
    tier_mode?: string | null;
    tier_avg?: number | null;
    toi_5v5_pg?: number | null;
    toi_pp_pg?: number | null;
    toi_pk_pg?: number | null;
    games_pp1?: number;
    games_pp2?: number;
    games_pk1?: number;
    games_pk2?: number;
    oz_start_share?: number | null;
    [games: `games_${string}`]: number | string | null | undefined;
  };
  linemates: Linemate[];
}

export interface PlayerContextResponse {
  player_id: number;
  player_name: string | null;
  season: number;
  rows: PlayerContextRow[];
}

export interface MatchupCell {
  venue: "all" | "home" | "away";
  own_tier: string;
  opp_tier: string;
  seconds: number;
  share: number;
  /** Share of time against that tier ÷ the tier's overall share: 1 = no matching. */
  ratio: number;
}

export interface TeamMatchupsResponse {
  team_id: number;
  season: number;
  seasons: number[];
  cells: MatchupCell[];
  coaches: string[];
  index: {
    mi_bits: number;
    pct: number;
    f1_vs_f1: number | null;
    d1_f1_home: number | null;
    d1_f1_away: number | null;
    teams: number;
  } | null;
}

export interface StyleAxis {
  key: string;
  name: string;
  /** What the positive end means. */
  label: string;
  /** Standardised score (league sd = 1). */
  value: number | null;
  /** Percentile within F or D (0-100) among 500+-minute skaters that season. */
  pct: number | null;
}

export interface StyleComp {
  player_id: number;
  player_name: string;
  season: number;
  distance: number;
}

export interface StyleView {
  season: number;
  window: "2yr" | "season";
  label: string;
  group: "F" | "D";
  toi_5v5_min: number;
  /** At least 500 5v5 minutes behind the view. */
  reliable: boolean;
  archetype: string | null;
  confidence: number | null;
  probs: { name: string; p: number }[];
  axes: StyleAxis[];
  comps: StyleComp[];
}

export interface PlayerStyleResponse {
  player_id: number;
  player_name: string | null;
  season: number;
  views: StyleView[];
  history: { season: number; archetype: string | null; confidence: number | null; toi_5v5_min: number }[];
}

// ---------------------------------------------------------------- markets (game page and cards)

export type MarketKey = "moneyline" | "moneyline_3way" | "puckline" | "total";
export type SideKey = "home" | "away" | "draw" | "over" | "under";

/** Regulation three-way for a card: model, blend and market and the best price per side (never flagged).
 *  The blend is the moneyline blend split by the calibrated overtime rate; the edge uses it. */
export interface ThreeWaySide {
  side: "home" | "draw" | "away";
  p_model: number | null;
  p_blend: number | null;
  p_market: number | null;
  price: number | null;
  book: string | null;
  edge: number | null;
}

export interface ThreeWay {
  game_id?: number;
  sides: ThreeWaySide[];
  books: number;
}

/** Market consensus at one moment: devigged, averaged over the books at the main line. */
export interface Consensus {
  t: string;
  market: MarketKey;
  /** Home handicap for puck lines, the total for totals. */
  line: number | null;
  books: number;
  fair: Partial<Record<SideKey, number>>;
  best: Partial<Record<SideKey, { price: number; book: string }>>;
}

export interface ModelPoint {
  t: string;
  market: MarketKey;
  line: number | null;
  p: Partial<Record<SideKey, number>>;
}

export interface BookQuote {
  book: string;
  line: number | null;
  prices: Partial<Record<SideKey, number>>;
  fair: Partial<Record<SideKey, number>>;
  captured_at: string;
}

export interface MarketView {
  start: string | null;
  history: Consensus[];
  model: ModelPoint[];
  books: Partial<Record<MarketKey, BookQuote[]>>;
  consensus: Partial<Record<MarketKey, Consensus>>;
  three_way: ThreeWay | null;
}

// ---------------------------------------------------------------- lineups tab

export interface SeasonPair<T> {
  cur: T | null;
  prev: T | null;
}

export interface OnIce {
  games: number;
  toi_s: number;
  xgf60: number;
  xga60: number;
}

/** A skater's power-play (5v4) or penalty-kill (4v5) season: time, share of his team's time in
 *  that state, and on-ice expected goals per 60. */
export interface SpecialTeamsSeason {
  toi_s: number;
  share: number;
  xgf60: number;
  xga60: number;
}

export interface LineupPlayerStats extends LineupPlayer {
  /** Ratings per 60 vs average (``ev_def`` / ``pk_def`` are prevention: higher is better). */
  rating: { ev_off: number; ev_def: number; ev_net: number; ev_toi_s: number; pp_off: number; pk_def: number;
    /** Goals per xG on his own shots vs average (shrunk). */
    finishing: number | null } | null;
  onice: SeasonPair<OnIce>;
  special: SeasonPair<{ pp: SpecialTeamsSeason | null; pk: SpecialTeamsSeason | null }>;
  /** Forward archetype (defensemen have none). */
  archetype: string | null;
  /** His own position: LW / C / RW, or LD / RD by shooting hand. */
  role: string;
}

export interface UnitRecord {
  toi_s: number;
  games: number;
  xgf: number;
  xga: number;
  gf: number;
  ga: number;
  /** Share of the team's time in that state (5v5, 5v4 or 4v5). */
  toi_share: number | null;
}

export interface LineupUnit {
  slot: string;
  kind: "F" | "D" | "PP" | "PK";
  player_ids: number[];
  /** Position each player fills, parallel to ``player_ids`` (the slot on a 5v5 line or pair). */
  roles: string[];
  record: SeasonPair<UnitRecord>;
}

export interface GoalieSeason {
  starts: number;
  shots_against: number;
  goals_against: number;
  xga: number;
  gsax: number;
  sv_pct: number | null;
}

export interface GoalieStats extends GoalieProb {
  rating: { save: number; save_sd: number; save_prior: number | null } | null;
  season: SeasonPair<GoalieSeason>;
}

export interface Tweet {
  text: string | null;
  author_name: string | null;
  author_handle: string | null;
  created_at: string | null;
}

export interface LineupSource {
  source_name: string | null;
  url: string | null;
  updated_at: string | null;
  tweet: Tweet | null;
  goalie_name?: string | null;
  status?: string | null;
  details?: string | null;
}

/** A team's regular-season power-play conversion or penalty-kill success and league rank (1 = best). */
export interface TeamSpecialRate {
  pct: number | null;
  rank: number | null;
  goals: number;
  opps: number;
  teams: number;
}

export interface TeamLineup {
  players: LineupPlayerStats[];
  special_teams: SeasonPair<{ pp: TeamSpecialRate; pk: TeamSpecialRate }>;
  units: LineupUnit[];
  goalies: GoalieStats[];
  sources: { lines: LineupSource | null; goalie: LineupSource | null };
}

/** A heat column's neutral point and saturation (``heat(v, scale, { center })``). */
export interface HeatScale {
  center: number;
  scale: number;
}

/** League color scales for one season's results. */
export interface SeasonScales {
  xgf?: HeatScale;
  xga?: HeatScale;
  xgd?: HeatScale;
  pp?: HeatScale;
  pk?: HeatScale;
  sv_pct?: HeatScale;
  gsax?: HeatScale;
}

export interface GameLineupsResponse {
  season: number;
  scales: {
    /** Talent ratings, centered at 0 (same scales as the ratings pages). */
    rating: Partial<Record<"ev_off" | "ev_def" | "ev_net" | "pp_off" | "pk_def" | "finishing" | "save", number>>;
    cur: SeasonScales;
    prev: SeasonScales;
  };
  home: TeamLineup;
  away: TeamLineup;
}

// /api/props, /api/games/{id}/props, /api/props/bets (see nhl.props.live and nhl.props.ledger).
// prop_type is goals | assists | points | shots | blocks | saves; side over | under; lines are N - 0.5 ("1+" = over 0.5).

export type PropType = "goals" | "assists" | "points" | "shots" | "blocks" | "saves";

export interface PropEdge {
  game_id: number;
  start_utc: string;
  away_abbr: string;
  home_abbr: string;
  player_id: number;
  player_name: string | null;
  team: string | null;
  position: string | null;
  slot: string | null;
  pp_unit: number | null;
  prop_type: PropType;
  line: number;
  side: "over" | "under";
  /** Best book for this side, and its American price. */
  book: string;
  price: number;
  /** Books quoting this player, stat and line. */
  books: number;
  p_model_side: number;
  p_market_side: number;
  /** Model-market blend for the side (what the edge uses). */
  p: number;
  edge: number;
  flagged: boolean;
  stake_units: number;
  stamp: string;
  open_price: number | null;
  opened_at: string | null;
  moves: number | null;
  /** The paper bet already in the props ledger for this side (price, book and stake as first placed). */
  bet_price: number | null;
  bet_book: string | null;
  bet_stake: number | null;
  placed_at: string | null;
  /** That bet's CLV against this row's consensus: p_market_side × decimal(bet_price) − 1. */
  bet_clv: number | null;
}

export interface PropsResponse {
  date: string;
  stamp: string | null;
  /** Longest price the model will flag (longer rungs are left out of the edge views). */
  max_price?: number;
  props: PropEdge[];
}

/** One projected skater: P(stat >= k) as p_{goals|ast|points}_{k}, and expected counts. */
export interface PropPlayer {
  player_id: number;
  player_name: string;
  team: string | null;
  position: string | null;
  slot: string | null;
  pp_unit: number | null;
  p_dressed: number | null;
  confidence: string | null;
  source: string | null;
  /** Skaters: goals / assists / points / shots / blocks; goalies (position "G"): saves, and
   * p_dressed is his chance to start (saves are conditional on starting). */
  exp_goals: number | null;
  exp_ast: number | null;
  exp_points: number | null;
  exp_shots?: number | null;
  exp_blocks?: number | null;
  exp_saves?: number | null;
  [prob: `p_${string}_${number}`]: number | null;
}

export interface PropQuote {
  player_id: number;
  prop_type: PropType;
  line: number;
  book: string;
  price_over: number | null;
  price_under: number | null;
  /** This book's devigged P(over). */
  p_book: number;
  /** Consensus P(over) across books. */
  p_market: number;
  books: number;
}

export interface GamePropsResponse {
  game_id: number;
  stamp: string | null;
  players: PropPlayer[];
  quotes: PropQuote[];
  edges: PropEdge[];
}

export interface PropBet {
  bet_id: string;
  kind: string;
  placed_at: string;
  game_id: number;
  game_date: string;
  player_id: number;
  player_name: string | null;
  team: string | null;
  prop_type: PropType;
  line: number;
  side: "over" | "under";
  book: string;
  price: number;
  stake_units: number;
  p_model: number;
  p_market: number;
  p_blend: number;
  edge: number;
  books: number;
  close_price: number | null;
  p_close: number | null;
  clv: number | null;
  stat: number | null;
  result: "win" | "loss" | "void" | null;
  pnl_units: number | null;
  graded_at: string | null;
  home_abbr: string | null;
  away_abbr: string | null;
  /** Puck drop, and how long before it the bet was placed. */
  start_utc: string | null;
  lead_minutes: number | null;
  lead_bucket: PropLeadBucket | null;
  /** The market now (the latest edges run; a started game's last pregame view). */
  now_price: number | null;
  now_book: string | null;
  now_edge: number | null;
  now_flagged: boolean | null;
  /** Devigged consensus for the side now. */
  p_now: number | null;
  now_stamp: string | null;
  /** Live CLV before grading: p_now × decimal(price) − 1. */
  clv_now: number | null;
  status: PropBetStatus;
}

/** graded; closed = started, awaiting grading; value = still a play now; faded = quoted, no longer
 * a play; gone = no quote at the bet's line now. */
export type PropBetStatus = "graded" | "closed" | "value" | "faded" | "gone";
export type PropLeadBucket = "<1h" | "1-3h" | "3-6h" | "6-12h" | "12h+";

export interface PropBetTiming {
  lead_bucket: PropLeadBucket;
  bets: number;
  staked: number;
  pnl: number;
  roi: number | null;
  mean_clv: number | null;
  beat_close: number | null;
}

export interface PropOpenTotals {
  bets: number;
  staked: number;
  mean_clv: number | null;
  /** Share of open bets with live CLV above zero. */
  beating: number | null;
  value: number;
  faded: number;
  gone: number;
  closed: number;
}

export interface PropBetBreakdown {
  prop_type: PropType;
  side: string;
  bets: number;
  staked: number;
  pnl: number;
  roi: number | null;
  mean_clv: number | null;
  beat_close: number | null;
}

export interface PropBetsResponse {
  bets: PropBet[];
  totals: BetTotals;
  open: PropOpenTotals;
  breakdown: PropBetBreakdown[];
  timing: PropBetTiming[];
}
