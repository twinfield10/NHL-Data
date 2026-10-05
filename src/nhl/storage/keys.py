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
