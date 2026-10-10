from datetime import date, datetime, timezone

import polars as pl
import pytest

from nhl.pregame import horizon
from nhl.storage import keys

NOW = datetime(2026, 10, 10, 20, 0, tzinfo=timezone.utc)  # 16:00 Eastern


class FakeStore:
    def __init__(self, games: pl.DataFrame, latest: set[date] | None = None) -> None:
        self.games, self.latest = games, latest or set()

    def read_parquet_required(self, key: str) -> pl.DataFrame:
        assert key == keys.GAMES
        return self.games

    def get_bytes(self, key: str) -> bytes | None:
        return b"{}" if any(key == keys.pregame_latest(d) for d in self.latest) else None


def games(*rows: tuple[int, str, str, bool]) -> pl.DataFrame:
    return pl.DataFrame([{"game_id": g, "game_date": date.fromisoformat(d), "start_time_et": t, "is_final": f,
                          "season": 20262027} for g, d, t, f in rows])


@pytest.fixture
def quoted(monkeypatch: pytest.MonkeyPatch):
    ids: set[int] = set()

    def load(store, season):  # noqa: ARG001
        return pl.DataFrame({"game_id": sorted(ids)}, schema={"game_id": pl.Int64})

    monkeypatch.setattr("nhl.odds.store.load_live_odds", load)
    return ids


def test_today_plus_quoted_later_dates(quoted: set[int]) -> None:
    quoted |= {3, 5}
    store = FakeStore(games(
        (1, "2026-10-10", "2026-10-10T13:00:00", False),  # today, started: doesn't count
        (2, "2026-10-10", "2026-10-10T19:00:00", False),  # today, not started
        (3, "2026-10-11", "2026-10-11T19:00:00", False),  # tomorrow, quoted
        (4, "2026-10-12", "2026-10-12T19:00:00", False),  # not quoted yet
        (5, "2026-10-13", "2026-10-13T19:00:00", False),  # quoted three days out
    ))
    assert horizon.dates(store, NOW) == [date(2026, 10, 10), date(2026, 10, 11), date(2026, 10, 13)]  # type: ignore[arg-type]


def test_today_drops_out_once_its_games_start(quoted: set[int]) -> None:
    quoted |= {3}
    store = FakeStore(games((1, "2026-10-10", "2026-10-10T13:00:00", False), (3, "2026-10-11", "2026-10-11T19:00:00", False)))
    assert horizon.dates(store, NOW) == [date(2026, 10, 11)]  # type: ignore[arg-type]


def test_unpriced_dates() -> None:
    store = FakeStore(games(), latest={date(2026, 10, 10)})
    assert horizon.unpriced(store, [date(2026, 10, 10), date(2026, 10, 11)]) == [date(2026, 10, 11)]  # type: ignore[arg-type]


def test_odds_poll_prices_newly_listed_dates_then_edges_every_date(monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse

    import polars as pl

    from nhl import cli

    today, tomorrow = date(2026, 10, 10), date(2026, 10, 11)
    calls: dict[str, list] = {"price": [], "edges": [], "props": [], "publish": []}

    class Store:
        def read_parquet_required(self, key: str) -> pl.DataFrame:
            return pl.DataFrame()

    class Out:
        prices = pl.DataFrame({"game_id": [1]})

    monkeypatch.setattr("nhl.storage.s3.Store", Store)
    for src in ("lowvig", "fourcasters", "espn_odds"):
        monkeypatch.setattr(f"nhl.sources.{src}.poll", lambda store, games: 0)
    monkeypatch.setattr("nhl.pregame.horizon.dates", lambda store: [today, tomorrow])
    monkeypatch.setattr("nhl.pregame.horizon.unpriced", lambda store, days: [tomorrow])
    monkeypatch.setattr("nhl.pregame.price.run", lambda store, day: calls["price"].append(day) or Out())
    monkeypatch.setattr("nhl.betting.edges.run", lambda store, day: calls["edges"].append(day) or pl.DataFrame())
    monkeypatch.setattr("nhl.props.live.run", lambda store, day: calls["props"].append(day) or pl.DataFrame())
    monkeypatch.setattr("nhl.site.publish.publish_quietly",
                        lambda store, day=None, parts=(): calls["publish"].append((day, tuple(parts))))

    cli.cmd_poll(argparse.Namespace(what="odds", window=None, reprice=False, edges=True))
    assert calls["price"] == [tomorrow]  # only the date the books just listed; no odds moved
    assert calls["edges"] == calls["props"] == [today, tomorrow]
    assert dict(calls["publish"])[tomorrow] == ("markets", "lineups", "props")
