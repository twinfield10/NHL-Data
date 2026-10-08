"""Single source of truth for the S3 key layout.

::

    raw/catalog/games.json.gz             every game the stats API knows about
    raw/catalog/teams.json.gz             team id -> abbreviation/name
    raw/players/{kind}_bios_{season}.json.gz
    raw/pbp/{season}/{game_id}.json.gz    api-web gamecenter play-by-play (verbatim)
    raw/shifts/{season}/{game_id}.json.gz stats API shift charts (verbatim)
    processed/games.parquet
    processed/teams.parquet
    processed/players.parquet
    processed/events/{season}.parquet     one row per play-by-play event, on-ice players attached
    processed/shots/{season}.parquet      unblocked shot attempts with model features
    processed/stints/{season}.parquet     constant on-ice personnel intervals (M2)
    processed/lineups/{season}.parquet    inferred lines, pairs, PP/PK units per team-game
    processed/goalie_starts/{season}.parquet
    processed/coaches/{season}.parquet    head coach + scratches per team-game
    processed/rosters/{season}.parquet    dressed players and positions per game
    processed/game_logs/{team|player}/{season}.parquet
    models/xg/{version}/...               boosters, metadata, evaluation
    predictions/xg/{season}.parquet       per-shot expected goals
    legacy/v1/...                         archive of the pre-rewrite repo data
"""

from __future__ import annotations

CATALOG_GAMES = "raw/catalog/games.json.gz"
CATALOG_TEAMS = "raw/catalog/teams.json.gz"
GAMES = "processed/games.parquet"
TEAMS = "processed/teams.parquet"
PLAYERS = "processed/players.parquet"


def raw_player_bios(kind: str, season: int) -> str:
    """Key for a season's skater or goalie bios (``kind`` in {"skater", "goalie"})."""
    return f"raw/players/{kind}_bios_{season}.json.gz"


def raw_pbp(season: int, game_id: int) -> str:
    """Key for one game's raw play-by-play."""
    return f"raw/pbp/{season}/{game_id}.json.gz"


def raw_shifts(season: int, game_id: int) -> str:
    """Key for one game's raw shift chart."""
    return f"raw/shifts/{season}/{game_id}.json.gz"


def raw_pbp_prefix(season: int) -> str:
    """Prefix holding every raw play-by-play file for a season."""
    return f"raw/pbp/{season}/"


def raw_shifts_prefix(season: int) -> str:
    """Prefix holding every raw shift file for a season."""
    return f"raw/shifts/{season}/"


def events(season: int) -> str:
    """Key for a season's processed event table."""
    return f"processed/events/{season}.parquet"


def shots(season: int) -> str:
    """Key for a season's shot feature table."""
    return f"processed/shots/{season}.parquet"


def shots_rink_adjusted(season: int) -> str:
    """Shot features with arena scorer-bias-adjusted locations (see nhl.features.rink)."""
    return f"processed/shots_rink/{season}.parquet"


RINK_MAPS = "processed/rink_maps.parquet"


def xg_model_prefix(version: str) -> str:
    """Prefix for one trained xG model version."""
    return f"models/xg/{version}/"


XG_LATEST = "models/xg/LATEST"


def xg_predictions(season: int) -> str:
    """Key for a season's per-shot xG."""
    return f"predictions/xg/{season}.parquet"


def game_id_from_key(key: str) -> int:
    """Extract the game id from a raw per-game key."""
    return int(key.rsplit("/", 1)[-1].split(".", 1)[0])


# --- third-party sources ----------------------------------------------------------------
#: Raw payloads live under raw/external/{source}/{date}/ (see nhl.sources.common.archive_raw).


def odds(season: int, source: str) -> str:
    """Line transitions from one poller (``lowvig``, ``fourcasters``, ``espn``) for a season.

    One file per poller so concurrent polls never rewrite the same object; read them
    together with :func:`nhl.odds.store.load_live_odds`.
    """
    return f"external/odds/live/{season}/{source}.parquet"


def odds_live_prefix(season: int) -> str:
    """Prefix holding every poller's live odds file for a season."""
    return f"external/odds/live/{season}/"


def odds_history(season: int) -> str:
    """Backfilled historical prices (open/close/last) from sources without poll times."""
    return f"external/odds/history/{season}.parquet"


def dailyfaceoff_goalies(season: int) -> str:
    """Starting-goalie report transitions (status, news, source tweet)."""
    return f"external/dailyfaceoff/goalies/{season}.parquet"


def dailyfaceoff_lines(season: int) -> str:
    """Line-combination versions: one row per player per published update."""
    return f"external/dailyfaceoff/lines/{season}.parquet"


TWEETS = "external/tweets/tweets.parquet"


def tweet_raw(tweet_id: str) -> str:
    """Raw oEmbed payload for one tweet (immutable)."""
    return f"raw/external/tweets/{tweet_id}.json.gz"


def injuries(season: int) -> str:
    """ESPN injury report transitions."""
    return f"external/injuries/{season}.parquet"


def odds_history_sbr(season: int) -> str:
    """Historical open/close lines from the SBR odds archive (via the Wayback Machine)."""
    return f"external/odds/history_sbr/{season}.parquet"


def odds_props(season: int) -> str:
    """Player and goalie prop price transitions from live polls."""
    return f"external/odds/props/{season}.parquet"


def officials(season: int) -> str:
    """Referees and linesmen per game (from the NHL API)."""
    return f"processed/officials/{season}.parquet"


def ref_assignments(season: int) -> str:
    """Pregame referee assignment transitions (published before games)."""
    return f"external/ref_assignments/{season}.parquet"


VENUES = "reference/venues.parquet"
GAME_VENUES = "processed/game_venues.parquet"


def edge(kind: str, season: int) -> str:
    """NHL EDGE tracking aggregates, e.g. kind = "skater", "goalie", "team"."""
    return f"external/edge/{kind}/{season}.parquet"


def transactions(season: int) -> str:
    """Roster transactions (call-ups, waivers, trades, signings)."""
    return f"external/transactions/{season}.parquet"


def shifts(season: int) -> str:
    """Merged player shifts per game (start/end seconds into the period), from shift charts."""
    return f"processed/shifts/{season}.parquet"


# --- game state (M2) ----------------------------------------------------------------------


def stints(season: int) -> str:
    """Stints: maximal intervals of constant on-ice personnel (see nhl.gamestate.stints)."""
    return f"processed/stints/{season}.parquet"


def lineups(season: int) -> str:
    """Inferred forward lines, D pairs and PP/PK units per team-game."""
    return f"processed/lineups/{season}.parquet"


def goalie_starts(season: int) -> str:
    """Starting goalie per team-game, with workload and saves above expected."""
    return f"processed/goalie_starts/{season}.parquet"


def coaches(season: int) -> str:
    """Head coach and scratches per team-game."""
    return f"processed/coaches/{season}.parquet"


def rosters(season: int) -> str:
    """Dressed players and their position per game."""
    return f"processed/rosters/{season}.parquet"


def team_game_logs(season: int) -> str:
    """Team-game box and on-ice counts by strength."""
    return f"processed/game_logs/team/{season}.parquet"


def player_game_logs(season: int) -> str:
    """Player-game individual and on-ice counts by strength."""
    return f"processed/game_logs/player/{season}.parquet"


# --- usage and context (docs/plans/usage-context.md) -------------------------------------


def usage(season: int) -> str:
    """Deployment tier, TOI by strength, PP/PK unit and zone starts per skater-game."""
    return f"processed/usage/{season}.parquet"


def usage_summary(season: int) -> str:
    """Usage per (season, player, team): games by tier, TOI per game, special-teams roles."""
    return f"processed/usage_summary/{season}.parquet"


def onice_context(season: int) -> str:
    """5v5 on-ice xGF/xGA split into own, teammates, competition, zone, context, league, residual."""
    return f"processed/onice_context/{season}.parquet"


def onice_context_summary(season: int) -> str:
    """On-ice decomposition per (season, player, team) with QoT/QoC percentiles."""
    return f"processed/onice_context_summary/{season}.parquet"


def usage_matchups(season: int) -> str:
    """5v5 matching matrix per team, coach and venue: own tier x opponent tier seconds, share, ratio."""
    return f"processed/usage_matchups/{season}.parquet"


def unit_context(season: int) -> str:
    """5v5 decomposition per forward line and D pair while the whole unit is on the ice."""
    return f"processed/unit_context/{season}.parquet"


def linemates(season: int) -> str:
    """Season 5v5 time together per (team, player, teammate)."""
    return f"processed/linemates/{season}.parquet"


# --- archetypes (docs/plans/archetypes.md) ------------------------------------------------


def style_counts(season: int) -> str:
    """Regular-season style counts per skater-game (shots by location/type, physical, deployment)."""
    return f"processed/style_counts/{season}.parquet"


def style(season: int) -> str:
    """Style features per (player, season, window): raw and shrunk to the position mean."""
    return f"processed/style/{season}.parquet"


def style_priors(season: int) -> str:
    """Shrinkage prior per (window, position group, feature): mean, prior weight k, reliability."""
    return f"processed/style_priors/{season}.parquet"


def archetypes(season: int) -> str:
    """Style axes (all skaters), forward archetype probabilities and style comps per (player, window)."""
    return f"processed/archetypes/{season}.parquet"


# --- ratings (M3) -------------------------------------------------------------------------


def freeze_predictions(season: int) -> str:
    """Per saved shot on goal: P(goalie freezes the puck) (see nhl.ratings.freeze)."""
    return f"predictions/freeze/{season}.parquet"


def sim_backtest(season: int) -> str:
    """Per-game simulator prices and outcomes from the M4 backtest."""
    return f"predictions/sim_backtest/{season}.parquet"


# --- pregame (M5) -------------------------------------------------------------------------


def starter_model(season: int) -> str:
    """Starting-goalie model for a season, fitted on the seasons before it (JSON)."""
    return f"models/starters/{season}.json"


def pregame_backtest(season: int) -> str:
    """Per-game prices under actual / projected lineups and starters (M5 backtest)."""
    return f"predictions/pregame_backtest/{season}.parquet"


def pregame_lineups(day: date, stamp: str) -> str:
    """Projected lineups snapshot for a game date, one per pregame run."""
    return f"pregame/lineups/{day.isoformat()}/{stamp}.parquet"


def pregame_goalies(day: date, stamp: str) -> str:
    """Starting-goalie probabilities snapshot for a game date."""
    return f"pregame/goalies/{day.isoformat()}/{stamp}.parquet"


def pregame_prices(day: date, stamp: str) -> str:
    """Market prices (starter mixture) snapshot for a game date."""
    return f"pregame/prices/{day.isoformat()}/{stamp}.parquet"


def pregame_slate(day: date, stamp: str) -> str:
    """Slate summary for a game date: one row per game (grain date -> game_id)."""
    return f"pregame/slate/{day.isoformat()}/{stamp}.parquet"


def pregame_freshness(day: date, stamp: str) -> str:
    """Age of every pregame input at run time."""
    return f"pregame/freshness/{day.isoformat()}/{stamp}.parquet"


def pregame_latest(day: date) -> str:
    """Pointer to the newest pregame run for a date (JSON with its stamp)."""
    return f"pregame/latest/{day.isoformat()}.json"


def pregame_history(season: int) -> str:
    """Honest pregame prices with score matrices per game (M6 history; pregame variant)."""
    return f"predictions/pregame_history/{season}.parquet"


# --- betting (M6) -------------------------------------------------------------------------

BETS_LEDGER = "bets/ledger.parquet"


def blend_model() -> str:
    """Model/market blend coefficients per market and season segment (JSON)."""
    return "models/betting/blend.json"


def betting_edges(day: date, stamp: str) -> str:
    """Live edges snapshot for a game date, one per `nhl edges` run."""
    return f"pregame/edges/{day.isoformat()}/{stamp}.parquet"


def pregame_deployment(season: int) -> str:
    """Projected deployment per team-game as known the morning of each game (M5 lineups;
    cached for the props backtest)."""
    return f"predictions/pregame_deployment/{season}.parquet"


def props_backtest(season: int) -> str:
    """Player goals / assists / points projections and baselines per skater-game (M9 phase B)."""
    return f"predictions/props_backtest/{season}.parquet"


def props_projections(day: date, stamp: str) -> str:
    """Player goals / assists / points projections for a game date, keyed by the newest pregame run used."""
    return f"pregame/props/{day.isoformat()}/{stamp}.parquet"


def props_edges(day: date, stamp: str) -> str:
    """Live player-prop edges snapshot for a game date, one per props edges run."""
    return f"pregame/props_edges/{day.isoformat()}/{stamp}.parquet"


PROPS_LEDGER = "bets/props_ledger.parquet"
