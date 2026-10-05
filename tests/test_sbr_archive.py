"""SBR archive workbook parsing and import on trimmed real workbooks (no network)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from nhl.sources import sbr_archive
from nhl.sources.sbr_archive import (
    _dates,
    download_workbook,
    import_season,
    list_archive_files,
    odds_rows,
    parse_workbook,
    resolve_sbr_team,
    season_start_year,
    workbook_filename,
)

FIXTURES = Path(__file__).parent / "fixtures" / "sbr"


def load(name: str) -> bytes:
    """Read one fixture workbook."""
    return (FIXTURES / name).read_bytes()


def game(df: pl.DataFrame, away: str, home: str) -> dict[str, Any]:
    """The single parsed game between two tricodes."""
    rows = df.filter((pl.col("away_team") == away) & (pl.col("home_team") == home))
    assert rows.height == 1, rows
    return rows.row(0, named=True)


def quote(odds: pl.DataFrame, point: str, market: str, side: str) -> tuple[float | None, float]:
    """The single (line, price) for one price point/market/side."""
    row = odds.filter((pl.col("price_point") == point) & (pl.col("market") == market) & (pl.col("side") == side))
    assert row.height == 1, (point, market, side, row)
    return row.item(0, "line"), row.item(0, "price")


class FakeStore:
    """In-memory stand-in for :class:`nhl.storage.s3.Store`."""

    def __init__(self) -> None:
        self.objects: dict[str, Any] = {}

    def get_bytes(self, key: str) -> bytes | None:
        return self.objects.get(key)

    def put_bytes(self, key: str, data: bytes, content_type: str | None = None) -> str:
        self.objects[key] = data
        return "etag"

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        self.objects[key] = df
        return "etag"

    def uri(self, key: str) -> str:
        return f"mem://{key}"


# ------------------------------------------------------------------------ helpers
def test_filenames_and_seasons():
    assert season_start_year("nhl%20odds%202010-11.xlsx") == 2010
    assert season_start_year("nhl odds 2021.xlsx") == 2020  # COVID 2020-21 season
    assert season_start_year("nhloddsarchives.htm") is None
    assert workbook_filename(2020) == "nhl odds 2021.xlsx"
    assert workbook_filename(2019) == "nhl odds 2019-20.xlsx"


@pytest.mark.parametrize("label, code", [
    ("NYRangers", "NYR"), ("NYIslanders", "NYI"), ("TampaBay", "TBL"), ("LosAngeles", "LAK"),
    ("SanJose", "SJS"), ("NewJersey", "NJD"), ("St.Louis", "STL"), ("Phoenix", "PHX"),
    ("Arizona", "ARI"), ("Vegas", "VGK"), ("Seattle", "SEA"), ("Atlanta", "ATL"),
    ("Winnipeg", "WPG"), ("Montreal", "MTL"), (" Boston ", "BOS"), ("Narnia", None),
])
def test_resolve_sbr_team(label, code):
    assert resolve_sbr_team(label) == code


def test_dates_roll_over_once_per_season():
    # Oct -> Jan rolls the year; the 2019-20 bubble (Mar -> Aug) does not.
    assert _dates(["1002", "1231", "101", "311", "801", "928"], 2019) == [
        date(2019, 10, 2), date(2019, 12, 31), date(2020, 1, 1), date(2020, 3, 11),
        date(2020, 8, 1), date(2020, 9, 28),
    ]
    # The 2020-21 file starts in January of the following year.
    assert _dates(["113", "707"], 2020) == [date(2021, 1, 13), date(2021, 7, 7)]
    assert _dates(["bad", "1010"], 2015) == [None, date(2015, 10, 10)]


# ------------------------------------------------------------------- 2012-13 era
@pytest.fixture(scope="module")
def parsed_2012() -> pl.DataFrame:
    """Header + 3 real games (opening night and the Cup-clinching game 6)."""
    return parse_workbook(load("sbr_2012-13_sample.xlsx"), 2012)


def test_parse_2012_moneyline_and_totals(parsed_2012):
    assert parsed_2012.height == 3
    g = game(parsed_2012, "PIT", "PHI")
    assert g["game_date"] == date(2013, 1, 19)
    assert (g["away_final"], g["home_final"]) == (3, 1)
    assert (g["ml_open_away"], g["ml_open_home"]) == (-115, -105)
    assert (g["ml_close_away"], g["ml_close_home"]) == (-110, -110)
    # Visitor row carries the over price, home row the under.
    assert (g["total_open"], g["over_open"], g["under_open"]) == (6.0, -125, 105)
    assert (g["total_close"], g["over_close"], g["under_close"]) == (6.0, -130, 110)
    assert g["pl_away_line"] is None and g["notes"] is None  # no puck line before 2014-15
    final = game(parsed_2012, "CHI", "BOS")
    assert final["game_date"] == date(2013, 6, 24) and final["total_close"] == 4.5


def test_odds_rows_2012(parsed_2012):
    odds = odds_rows(parsed_2012)
    assert set(odds["book"]) == {"SBR consensus"}
    assert set(odds["market"]) == {"moneyline", "total"}
    assert odds.height == 3 * 8  # ML + total, open + close, two sides
    pit = odds.filter(pl.col("away_team") == "PIT")
    assert quote(pit, "open", "moneyline", "away") == (None, -115)
    assert quote(pit, "close", "total", "under") == (6.0, 110)
    opened = pit.filter(pl.col("price_point") == "open")["captured_at"].unique().to_list()
    closed = pit.filter(pl.col("price_point") == "close")["captured_at"].unique().to_list()
    assert opened == [datetime(2013, 1, 19, tzinfo=timezone.utc)]
    assert closed == [datetime(2013, 1, 19, 23, 59, tzinfo=timezone.utc)]
    assert set(pit["source_event_id"]) == {"20130119-PIT-PHI"}


# ------------------------------------------------------------------- 2014-15 era
@pytest.fixture(scope="module")
def parsed_2014() -> pl.DataFrame:
    """Opening night (first puck-line season) and a game with a merged puck-line cell."""
    return parse_workbook(load("sbr_2014-15_sample.xlsx"), 2014)


def test_parse_2014_puckline(parsed_2014):
    g = game(parsed_2014, "MTL", "TOR")
    assert (g["pl_away_line"], g["pl_away"], g["pl_home_line"], g["pl_home"]) == (1.5, -305, -1.5, 250)
    odds = odds_rows(parsed_2014.filter(pl.col("away_team") == "MTL"))
    assert quote(odds, "close", "puckline", "home") == (-1.5, 250)
    assert quote(odds, "close", "puckline", "away") == (1.5, -305)
    assert odds.filter(pl.col("market") == "puckline")["price_point"].unique().to_list() == ["close"]


def test_parse_2014_repairs_merged_puckline_cell(parsed_2014):
    # Workbook cells: PuckLine "-278.5" / "238.5" with blank prices; CBJ is the ML favorite.
    g = game(parsed_2014, "PHI", "CBJ")
    assert g["notes"] == "puckline_merged_cell"
    assert (g["pl_away_line"], g["pl_away"], g["pl_home_line"], g["pl_home"]) == (1.5, -278, -1.5, 238)


# ------------------------------------------------------------------- 2021-22 era
@pytest.fixture(scope="module")
def parsed_2021() -> pl.DataFrame:
    """Opening night 2021-22 plus games across the new year."""
    return parse_workbook(load("sbr_2021-22_sample.xlsx"), 2021)


def test_parse_2021_seattle_and_new_year(parsed_2021):
    sea = game(parsed_2021, "SEA", "VGK")
    assert sea["game_date"] == date(2021, 10, 12)
    assert sea["pl_away_line"] == -sea["pl_home_line"]
    assert parsed_2021["game_date"].max().year == 2022
    assert sea["away_label"] == "SeattleKraken"  # this workbook spells some teams in full
    jan = game(parsed_2021, "BUF", "BOS")
    assert jan["game_date"] == date(2022, 1, 1)
    assert (jan["pl_away_line"], jan["pl_away"], jan["pl_home_line"], jan["pl_home"]) == (1.5, 105, -1.5, -125)
    odds = odds_rows(parsed_2021)
    assert set(odds["market"]) == {"moneyline", "puckline", "total"}
    # ARI-NSH: puck line prices are "NL" (no line), so no puckline rows for that game.
    ari = odds.filter(pl.col("away_team") == "ARI")
    assert set(ari["market"]) == {"moneyline", "total"}
    assert quote(ari, "close", "moneyline", "home") == (None, -270)


def test_odds_rows_requires_both_sides(parsed_2021):
    lone = parsed_2021.with_columns(pl.lit(None, pl.Float64).alias("under_close"))
    odds = odds_rows(lone)
    assert odds.filter((pl.col("market") == "total") & (pl.col("price_point") == "close")).is_empty()
    assert not odds.filter((pl.col("market") == "total") & (pl.col("price_point") == "open")).is_empty()


def test_swap_sides_for_neutral_games(parsed_2021):
    g = game(parsed_2021, "PIT", "TBL")
    flipped = sbr_archive.swap_sides(parsed_2021.filter(pl.col("away_team") == "PIT")).row(0, named=True)
    assert (flipped["away_team"], flipped["home_team"]) == ("TBL", "PIT")
    assert (flipped["ml_close_away"], flipped["ml_close_home"]) == (g["ml_close_home"], g["ml_close_away"])
    assert (flipped["away_final"], flipped["pl_home_line"]) == (g["home_final"], g["pl_away_line"])
    assert (flipped["over_close"], flipped["total_close"]) == (g["over_close"], g["total_close"])
    assert flipped["source_event_id"] == "20211012-TBL-PIT" and flipped["notes"] == "sides_swapped"


# ------------------------------------------------------------------------- import
def games_table() -> pl.DataFrame:
    """Two games in the games-table shape: one exact, one with a score that disagrees."""
    return pl.DataFrame({
        "game_id": [2012020001, 2012020002, 2012030417],
        "season": [20122013] * 3,
        "season_type": ["R", "R", "P"],
        "game_date": [date(2013, 1, 19), date(2013, 1, 19), date(2013, 6, 24)],
        "start_time_et": ["2013-01-19T15:00:00", "2013-01-19T20:00:00", "2013-06-24T20:00:00"],
        "home_score": [1, 2, 2],
        "away_score": [3, 4, 3],
        "is_final": [True, True, True],
        "home_abbr": ["PHI", "WPG", "BOS"],
        "away_abbr": ["PIT", "OTT", "CHI"],
    }).with_columns(pl.col("home_score", "away_score").cast(pl.Int16), pl.col("season").cast(pl.Int32))


def test_import_season_matches_validates_and_writes():
    store = FakeStore()
    games = games_table().filter(pl.col("game_id") != 2012030417)  # Cup final missing -> dropped
    stats = import_season(store, games, 2012, load("sbr_2012-13_sample.xlsx"))
    assert stats["games_in_file"] == 3 and stats["games_matched"] == 2 and stats["unmatched"] == 1
    assert stats["score_mismatches"] == 1  # OTT-WPG final 4-1 in the workbook, 4-2 in games
    assert stats["markets"] == {"moneyline/close": 2, "moneyline/open": 2, "total/close": 2, "total/open": 2}
    written = store.objects["external/odds/history_sbr/20122013.parquet"]
    assert written.height == stats["rows"] == 16
    assert set(written["game_id"]) == {2012020001, 2012020002}
    pit = written.filter(pl.col("game_id") == 2012020001)
    start = datetime(2013, 1, 19, 20, 0, tzinfo=timezone.utc)  # 15:00 ET
    assert set(pit["start_time"]) == {start}
    assert set(pit.filter(pl.col("price_point") == "close")["captured_at"]) == {start}
    assert set(pit.filter(pl.col("price_point") == "open")["captured_at"]) == {
        datetime(2013, 1, 19, tzinfo=timezone.utc)}


def test_match_games_phoenix_is_era_correct(parsed_2012):
    """Phoenix in 2012-13 resolves to PHX, the code the games table uses that season."""
    games = games_table().with_columns(pl.col("home_abbr").replace({"WPG": "PHX"}))
    coyotes = parsed_2012.with_columns(pl.col("home_team").replace({"WPG": "Phoenix"}))
    matched = sbr_archive.match_games(odds_rows(coyotes), sbr_archive._normalized_games(games, 20122013))
    assert set(matched.filter(pl.col("home_team") == "PHX")["game_id"]) == {2012020002}


# -------------------------------------------------------------------------- fetch
def test_list_archive_files_keeps_latest_snapshot(monkeypatch):
    cdx = [
        ["timestamp", "original", "length"],
        ["20171026095853", "http://www.sportsbookreviewsonline.com:80/scoresoddsarchives/nhl/nhl%20odds%202010-11.xlsx", "132105"],
        ["20221203065009", "https://www.sportsbookreviewsonline.com/scoresoddsarchives/nhl/nhl%20odds%202010-11.xlsx", "180865"],
        ["20221202175427", "https://www.sportsbookreviewsonline.com/scoresoddsarchives/nhl/nhl%20odds%202021.xlsx", "147857"],
        ["20101128074152", "http://sportsbookreviewsonline.com/scoresoddsarchives/nhl/nhloddsarchives.htm", "2379"],
    ]

    class Resp:
        def json(self) -> Any:
            return cdx

    monkeypatch.setattr(sbr_archive, "_get", lambda url, params=None: Resp())
    entries = list_archive_files()
    assert [(e["filename"], e["start_year"], e["timestamp"]) for e in entries] == [
        ("nhl odds 2010-11.xlsx", 2010, "20221203065009"),
        ("nhl odds 2021.xlsx", 2020, "20221202175427"),
    ]


def test_download_workbook_uses_cache(monkeypatch):
    store = FakeStore()
    entry = {"filename": "nhl odds 2012-13.xlsx", "timestamp": "20221204033548",
             "original": "https://www.sportsbookreviewsonline.com/scoresoddsarchives/nhl/nhl%20odds%202012-13.xlsx"}
    calls: list[str] = []

    class Resp:
        content = load("sbr_2012-13_sample.xlsx")

    def fake_get(url: str, params: Any = None) -> Resp:
        calls.append(url)
        return Resp()

    monkeypatch.setattr(sbr_archive, "_get", fake_get)
    first = download_workbook(store, entry)
    second = download_workbook(store, entry)
    assert first == second == Resp.content
    assert calls == ["https://web.archive.org/web/20221204033548id_/" + entry["original"]]
    assert "raw/external/sbr/nhl odds 2012-13.xlsx" in store.objects


def test_import_season_swaps_reversed_game(monkeypatch, parsed_2021):
    """A game listed TBL-home by SBR but PIT-home by the NHL matches once swapped (scores agree)."""
    neutral = parsed_2021.filter(pl.col("away_team") == "PIT").with_columns(pl.lit(True).alias("neutral"))
    monkeypatch.setattr(sbr_archive, "parse_workbook", lambda data, year: neutral)
    games = pl.DataFrame({
        "game_id": [2021020001], "season": [20212022], "season_type": ["R"],
        "game_date": [date(2021, 10, 12)], "start_time_et": ["2021-10-12T19:30:00"],
        "home_score": [6], "away_score": [2], "is_final": [True],
        "home_abbr": ["PIT"], "away_abbr": ["TBL"],
    })
    store = FakeStore()
    stats = import_season(store, games, 2021, b"")
    assert stats["games_matched"] == 1 and stats["sides_swapped"] == 1
    assert stats["unmatched"] == 0 and stats["score_mismatches"] == 0
    written = store.objects["external/odds/history_sbr/20212022.parquet"]
    assert set(written["home_team"]) == {"PIT"}
    assert quote(written, "close", "moneyline", "home") == (None, 220)  # PIT's price follows PIT
    # A swap whose score disagrees is not trusted.
    store = FakeStore()
    stats = import_season(store, games.with_columns(pl.lit(5, pl.Int64).alias("home_score")), 2021, b"")
    assert stats["games_matched"] == 0 and stats["unmatched"] == 1
