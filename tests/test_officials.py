"""Game officials: NHL right-rail normalization and Scouting the Refs assignment parsing (real trimmed payloads)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from nhl.sources import officials as off
from nhl.sources.common import new_transitions

FIXTURES = Path(__file__).parent / "fixtures" / "officials"
CAPTURED = datetime(2026, 10, 4, 18, 0, tzinfo=timezone.utc)


def load_json(name: str) -> dict:
    """Read one JSON fixture."""
    return json.loads((FIXTURES / name).read_text())


def load_text(name: str) -> str:
    """Read one text fixture."""
    return (FIXTURES / name).read_text()


@pytest.fixture
def games() -> pl.DataFrame:
    """The real 2026-10-04 slate from processed/games.parquet."""
    rows = [
        (2026020035, "DET", "WPG"), (2026020036, "NYR", "UTA"), (2026020037, "ANA", "FLA"),
        (2026020038, "SEA", "CGY"), (2026020039, "VAN", "VGK"),
    ]
    return pl.DataFrame(
        {
            "game_id": [r[0] for r in rows],
            "season": [20262027] * len(rows),
            "game_date": [date(2026, 10, 4)] * len(rows),
            "home_abbr": [r[1] for r in rows],
            "away_abbr": [r[2] for r in rows],
        },
        schema_overrides={"season": pl.Int32},
    )


# --------------------------------------------------------------------------- identity
@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Frederick L’Ecuyer", "frederick-lecuyer"),
        ("Frederick L&#8217;Ecuyer", "frederick-lecuyer"),
        ("Francois St-Laurent", "francois-st-laurent"),
        ("François St-Laurent", "francois-st-laurent"),
        ("Kiel Murchison ", "kiel-murchison"),
        ("Jordan Samuels-Thomas", "jordan-samuels-thomas"),
        (None, None),
    ],
)
def test_official_id_is_accent_and_punctuation_insensitive(name, expected):
    assert off.official_id(name) == expected


def test_identity_report_flags_number_changes_and_shared_numbers():
    frame = pl.DataFrame({
        "official_id": ["a", "a", "b", "c"],
        "official_name": ["A", "A", "B", "C"],
        "sweater_number": [10, 12, 5, 5],
        "season": [20102011, 20152016, 20152016, 20152016],
        "role": ["referee"] * 4,
    })
    report = off.identity_report(frame)
    assert report["multi_number"]["official_id"].to_list() == ["a", "a"]
    assert report["shared_number"].row(0, named=True)["official_ids"] == ["b", "c"]


# --------------------------------------------------------------------------- right-rail
def test_right_rail_2025_has_two_referees_two_linesmen():
    frame = off.normalize_officials(load_json("right_rail_2025020001.json"), 2025020001, 20252026)
    assert frame.schema == pl.Schema(off.OFFICIAL_SCHEMA)
    assert frame.group_by("role").len().sort("role").rows() == [("linesman", 2), ("referee", 2)]
    refs = frame.filter(pl.col("role") == "referee").sort("slot")
    assert refs["official_name"].to_list() == ["Kelly Sutherland", "Eric Furlatt"]
    assert refs["sweater_number"].to_list() == [11, 27]
    assert refs["official_id"].to_list() == ["kelly-sutherland", "eric-furlatt"]


def test_right_rail_2012_historical_game():
    frame = off.normalize_officials(load_json("right_rail_2012020100.json"), 2012020100, 20122013)
    assert frame.height == 4
    assert ("Paul Devorski", 10) in frame.select("official_name", "sweater_number").rows()


def test_right_rail_trims_names_and_is_empty_before_puck_drop():
    final = off.normalize_officials(load_json("right_rail_2026020039.json"), 2026020039, 20262027)
    assert "Kiel Murchison" in final["official_name"].to_list()
    pregame = off.normalize_officials(load_json("right_rail_2026020040_pregame.json"), 2026020040, 20262027)
    assert pregame.is_empty() and pregame.schema == pl.Schema(off.OFFICIAL_SCHEMA)


def test_raw_cache_key_layout():
    assert off.raw_right_rail_key(20252026, 2025020001) == "raw/external/right_rail/20252026/2025020001.json.gz"
    assert off.raw_right_rail_key(20252026, 1).startswith(off.raw_right_rail_prefix(20252026))


# --------------------------------------------------------------------------- Scouting the Refs
def test_feed_items_have_slate_dates():
    items = off.parse_feed(load_text("str_feed.xml"))
    assert [i["slate_date"] for i in items] == [date(2026, 10, 4), date(2026, 10, 3), date(2026, 10, 2)]
    assert items[0]["url"].endswith("/tonights-nhl-referees-and-linespersons-10-4-26/")
    assert items[0]["published_at"] == datetime(2026, 10, 4, 15, 30, tzinfo=timezone.utc)


def test_slate_date_from_title_variants():
    assert off.slate_date("Today’s NHL Playoff Referees and Linespersons – 6/14/26") == date(2026, 6, 14)
    assert off.slate_date("Today&#8217;s NHL Officials – 10/1/26") == date(2026, 10, 1)
    assert off.slate_date("Ref roundup") is None


def test_post_parses_every_game_once_per_official():
    rows = off.parse_assignment_post(load_text("str_2026-10-04.html"))
    frame = pl.DataFrame(rows)
    assert frame.height == 20                              # 5 games x (2 refs + 2 linesmen)
    counts = frame.group_by("home_team", "role").len()
    assert counts["len"].unique().to_list() == [2]          # stat tables repeat names; deduped
    van = frame.filter(pl.col("home_team") == "VAN").sort("role", "slot")
    assert van["away_team"].unique().to_list() == ["VGK"]
    assert van.select("role", "official_name", "sweater_number").rows() == [
        ("linesman", "Andrew Smith", 51), ("linesman", "Jeremy Faucher", 97),
        ("referee", "Pierre Lambert", 25), ("referee", "Brandon Blandina", 39),
    ]


def test_playoff_post_header_with_round_and_game_number():
    rows = off.parse_assignment_post(load_text("str_2026-06-14_playoff.html"))
    assert {(r["away_team"], r["home_team"]) for r in rows} == {("CAR", "VGK")}
    assert sorted(r["official_name"] for r in rows if r["role"] == "referee") == ["Garrett Rank", "Jean Hebert"]


def test_post_times_from_meta():
    times = off.post_times(load_text("str_2026-10-04.html"))
    assert times["published"] == datetime(2026, 10, 4, 15, 30, tzinfo=timezone.utc)
    assert times["modified"] == datetime(2026, 10, 4, 16, 49, 28, tzinfo=timezone.utc)


def test_assignments_attach_game_ids(games):
    page = load_text("str_2026-10-04.html")
    frame = off.normalize_assignments(off.parse_assignment_post(page), games, date(2026, 10, 4), CAPTURED, "u", off.post_times(page))
    assert frame.schema == pl.Schema(off.ASSIGNMENT_SCHEMA)
    assert frame.height == 20 and frame["game_id"].n_unique() == 5
    assert frame.filter(pl.col("official_name") == "Pierre Lambert")["game_id"].to_list() == [2026020039]
    # A slate date with no matching games drops everything (and logs) instead of mis-assigning.
    assert off.normalize_assignments(off.parse_assignment_post(page), games, date(2026, 10, 5), CAPTURED).is_empty()


def test_pregame_assignment_vs_actual_right_rail(games):
    """The 10/4 post had Andrew Smith on the lines for VGK@VAN; Kiel Murchison actually worked it."""
    page = load_text("str_2026-10-04.html")
    pre = off.normalize_assignments(off.parse_assignment_post(page), games, date(2026, 10, 4), CAPTURED)
    actual = off.normalize_officials(load_json("right_rail_2026020039.json"), 2026020039, 20262027)
    pre_ids = set(pre.filter(pl.col("game_id") == 2026020039)["official_id"])
    assert pre_ids - set(actual["official_id"]) == {"andrew-smith"}
    assert set(actual["official_id"]) - pre_ids == {"kiel-murchison"}


def test_replacement_records_removal_then_poll_is_idempotent(games):
    page = load_text("str_2026-10-04.html")
    first = off.normalize_assignments(off.parse_assignment_post(page), games, date(2026, 10, 4), CAPTURED)
    stored = new_transitions(first.clear(), first, off.ASSIGNMENT_KEYS, off.ASSIGNMENT_VALUES)
    assert stored.height == 20

    later = CAPTURED.replace(hour=20)
    swapped = first.with_columns(
        pl.when(pl.col("official_name") == "Andrew Smith").then(pl.lit("Kiel Murchison")).otherwise(pl.col("official_name")).alias("official_name"),
        pl.when(pl.col("official_name") == "Andrew Smith").then(pl.lit(79)).otherwise(pl.col("sweater_number")).alias("sweater_number"),
        pl.lit(later).alias("captured_at"),
    )
    incoming = off.with_removals(swapped, stored)
    fresh = new_transitions(stored, incoming, off.ASSIGNMENT_KEYS, off.ASSIGNMENT_VALUES)
    assert sorted(fresh.select("official_name", "is_assigned").rows()) == [("Andrew Smith", False), ("Kiel Murchison", True)]

    stored = pl.concat([stored, fresh])
    again = off.with_removals(swapped.with_columns(pl.lit(later.replace(hour=21)).alias("captured_at")), stored)
    assert new_transitions(stored, again, off.ASSIGNMENT_KEYS, off.ASSIGNMENT_VALUES).is_empty()
