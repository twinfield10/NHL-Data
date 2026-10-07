"""Venue reference table, game venue classification and point-in-time travel features."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from nhl.reference import venues as v
from nhl.teams import TEAMS, load_teams


@pytest.fixture(scope="module")
def venues() -> pl.DataFrame:
    """The checked-in curated venue table."""
    return v.load_venue_csv()


def gv_rows(rows: list[tuple]) -> pl.DataFrame:
    """Raw game-venue rows: (game_id, season, home_abbr, venue_name)."""
    return pl.DataFrame(
        [
            {"game_id": g, "season": s, "game_type": 2, "game_date": date(s // 10000, 12, 1), "start_time_utc": None,
             "home_abbr": h, "away_abbr": "XXX", "venue_name": name, "venue_location": None,
             "venue_utc_offset": None, "special_event": None}
            for g, s, h, name in rows
        ],
        schema=v.GAME_VENUE_SCHEMA,
    )


# --------------------------------------------------------------------------- venue table
def test_csv_is_valid_and_teams_are_known(venues):
    assert venues.schema == pl.Schema(v.VENUE_SCHEMA)
    known = {t.tricode for t in load_teams()}
    assert set(venues["home_team"].drop_nulls()) <= known
    assert venues["timezone"].null_count() == 0 and venues["elevation_m"].null_count() == 0


def test_every_current_team_has_a_home_arena(venues):
    current = sorted(set(TEAMS) - {"ARI", "ATL"})
    homes = v.default_home_arenas(venues, pl.DataFrame({"team": current, "season": [20262027] * len(current)}))
    assert sorted(homes["team"]) == current


@pytest.mark.parametrize(
    ("team", "season", "arena"),
    [
        ("ATL", 20102011, "philips_arena"),
        ("WPG", 20112012, "winnipeg_arena"),
        ("PHX", 20122013, "glendale_arena"),
        ("ARI", 20222023, "mullett_arena"),
        ("UTA", 20242025, "delta_center"),
        ("NYI", 20162017, "barclays_center"),
        ("NYI", 20212022, "ubs_arena"),
        ("SEA", 20212022, "climate_pledge_arena"),
        ("VGK", 20172018, "tmobile_arena"),
        ("DET", 20172018, "little_caesars_arena"),
        ("EDM", 20152016, "rexall_place"),
    ],
)
def test_relocations_and_arena_moves(venues, team, season, arena):
    homes = v.default_home_arenas(venues, pl.DataFrame({"team": [team], "season": [season]}))
    assert homes["arena_id"].to_list() == [arena]


def test_no_team_before_or_after_its_tenancy(venues):
    homes = v.default_home_arenas(venues, pl.DataFrame({"team": ["ATL", "SEA", "UTA"], "season": [20112012, 20202021, 20232024]}))
    assert homes.is_empty()


# --------------------------------------------------------------------------- game venues
def test_extract_game_venue_from_real_header():
    raw = {
        "id": 2010020001, "season": 20102011, "gameType": 2, "gameDate": "2010-10-07",
        "venue": {"default": "Air Canada Centre"}, "venueLocation": {"default": "Toronto"},
        "startTimeUTC": "2010-10-07T23:00:00Z", "venueUTCOffset": "-04:00",
        "specialEvent": {"parentId": 15, "name": {"default": "Face Off", "fr": "Confrontation"}},
        "awayTeam": {"id": 8, "abbrev": "MTL"}, "homeTeam": {"id": 10, "abbrev": "TOR"},
    }
    row = v.extract_game_venue(raw)
    assert row["venue_name"] == "Air Canada Centre" and row["venue_location"] == "Toronto"
    assert row["start_time_utc"] == datetime(2010, 10, 7, 23, tzinfo=timezone.utc)
    assert row["special_event"] == "Face Off" and row["home_abbr"] == "TOR"
    frame = pl.DataFrame([row], schema=v.GAME_VENUE_SCHEMA)
    assert v.timezone_mismatches(frame, v.load_venue_csv()).is_empty()


def test_timezone_mismatch_detected(venues):
    frame = gv_rows([(1, 20102011, "TOR", "Air Canada Centre")]).with_columns(
        pl.lit(datetime(2010, 10, 7, 23, tzinfo=timezone.utc)).alias("start_time_utc"),
        pl.lit("-05:00").alias("venue_utc_offset"),   # EDT in October is -04:00
    )
    assert v.timezone_mismatches(frame, venues)["expected_offset"].to_list() == ["-04:00"]


def test_neutral_site_and_outdoor_classification(venues):
    raw = gv_rows([
        (1, 20192020, "EDM", "Rogers Place"),             # 2020 hub, but EDM's own arena
        (2, 20192020, "COL", "Rogers Place"),             # 2020 hub: neutral
        (3, 20122013, "PHX", "Jobing.com Arena"),
        (4, 20152016, "ARI", "Gila River Arena"),
        (5, 20232024, "ARI", "Mullett Arena"),
        (6, 20242025, "UTA", "Delta Center"),
        (7, 20172018, "OTT", "Ericsson Globe"),           # Global Series
        (8, 20212022, "MIN", "Target Field"),             # Winter Classic
        (9, 20132014, "VAN", "BC Place"),                 # Heritage Classic under a closed roof
        (10, 20192020, "NYI", "NYCB Live/Nassau Coliseum"),
        (11, 20162017, "NYI", "Barclays Center"),
        (12, 20242025, "TOR", "Mystery Dome"),
    ])
    out = v.classify_game_venues(raw, venues).sort("game_id")
    assert out["is_neutral_site"].to_list() == [False, True, False, False, False, False, True, True, True, False, False, None]
    assert out["is_outdoor"].to_list() == [False, False, False, False, False, False, False, True, False, False, False, None]
    assert v.unmapped_venues(raw, venues)["venue_name"].to_list() == ["Mystery Dome"]


# --------------------------------------------------------------------------- travel
def schedule() -> tuple[pl.DataFrame, pl.DataFrame]:
    """A Boston road trip west, a home game, then a Global Series game in Prague."""
    rows = [
        (2024020001, date(2024, 10, 10), "BOS", "VAN", "TD Garden"),
        (2024020002, date(2024, 10, 12), "VAN", "BOS", "Rogers Arena"),
        (2024020003, date(2024, 10, 13), "CGY", "BOS", "Scotiabank Saddledome"),
        (2024020004, date(2024, 10, 15), "EDM", "BOS", None),           # no pbp yet -> home default
        (2024020005, date(2024, 10, 17), "BOS", "CGY", "TD Garden"),
        (2024020006, date(2024, 11, 1), "TBL", "BOS", "O2 Czech Republic"),
    ]
    games = pl.DataFrame(
        {
            "game_id": [r[0] for r in rows], "season": [20242025] * len(rows), "game_date": [r[1] for r in rows],
            "start_time_et": [f"{r[1]}T19:00:00" for r in rows], "home_abbr": [r[2] for r in rows],
            "away_abbr": [r[3] for r in rows], "is_final": [True] * len(rows),
        },
        schema_overrides={"season": pl.Int32},
    )
    raw = gv_rows([(r[0], 20242025, r[2], r[4]) for r in rows if r[4]])
    return games, v.classify_game_venues(raw, v.load_venue_csv())


def test_travel_features_for_a_road_trip(venues):
    games, gv = schedule()
    out = v.travel_features(games, gv, venues)
    assert out.height == 12
    bos = out.filter(pl.col("team") == "BOS").sort("game_date").to_dicts()

    assert bos[0]["days_rest"] is None and bos[0]["travel_km"] == pytest.approx(0, abs=1e-6)
    assert bos[0]["homestand_game"] == 1 and bos[0]["road_trip_game"] is None
    assert bos[0]["games_in_last_7_days"] == 0

    assert bos[1]["days_rest"] == 2 and not bos[1]["is_back_to_back"]
    assert 3950 < bos[1]["travel_km"] < 4100                     # Boston -> Vancouver
    assert bos[1]["tz_shift_hours"] == -3 and bos[1]["tz_shift_from_home"] == -3
    assert bos[1]["road_trip_game"] == 1

    assert bos[2]["is_back_to_back"] and bos[2]["road_trip_game"] == 2
    assert bos[2]["tz_shift_hours"] == 1 and bos[2]["tz_shift_from_home"] == -2
    assert 600 < bos[2]["travel_km"] < 750                        # Vancouver -> Calgary
    assert bos[2]["elevation_m"] == 1045

    assert bos[3]["venue_source"] == "home_default" and bos[3]["arena_id"] == "rogers_place"
    assert bos[3]["road_trip_game"] == 3 and 250 < bos[3]["travel_km"] < 300

    assert bos[4]["homestand_game"] == 1 and bos[4]["tz_shift_hours"] == 2
    assert bos[4]["games_in_last_7_days"] == 4

    # Prague on Nov 1 2024: Europe already on CET (+1), Boston still on EDT (-4).
    assert bos[5]["tz_shift_hours"] == 5 and bos[5]["road_trip_game"] == 1
    tbl = out.filter(pl.col("team") == "TBL").row(0, named=True)
    assert tbl["is_home"] and tbl["is_neutral_site"] and not tbl["at_home"]
    assert tbl["road_trip_game"] == 1 and tbl["days_rest"] is None
    assert 7000 < tbl["travel_km"] < 9000                         # first game: from Tampa


def test_travel_features_are_point_in_time(venues):
    games, gv = schedule()
    full = v.travel_features(games, gv, venues).filter(pl.col("game_id") < 2024020006)
    partial = v.travel_features(games.filter(pl.col("game_id") < 2024020006), gv, venues)
    assert full.sort("game_id", "team").equals(partial.sort("game_id", "team"))


def test_unplayed_games_are_dropped(venues):
    games, gv = schedule()
    games = games.with_columns(pl.when(pl.col("game_id") == 2024020003).then(False).otherwise(pl.col("is_final")).alias("is_final"))
    out = v.travel_features(games, gv, venues)
    assert 2024020003 not in out["game_id"].to_list()
    upcoming = v.travel_features(games, gv, venues, today=date(2024, 10, 13))
    assert 2024020003 in upcoming["game_id"].to_list()


def test_haversine_known_distance():
    frame = pl.DataFrame({"a": [40.7505], "b": [-73.9934], "c": [34.0430], "d": [-118.2673]})
    km = frame.select(v.haversine_km(pl.col("a"), pl.col("b"), pl.col("c"), pl.col("d"))).item()
    assert km == pytest.approx(3940, rel=0.01)                     # MSG -> Crypto.com Arena


def test_schedule_context_key():
    assert v.schedule_context_key(20242025) == "processed/schedule_context/20242025.parquet"
