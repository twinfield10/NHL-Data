from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from nhl.odds.core import MARKET_KEY, TRANSITION_VALUES, attach_game_ids, odds_frame
from nhl.sources.common import keep_transitions, new_transitions
from nhl.teams import resolve_team

T0 = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)


def row(minutes: int, price: float, market: str = "moneyline", side: str = "home", line: float | None = None) -> dict:
    return dict(book="LowVig", game_id=2026020040, captured_at=T0 + timedelta(minutes=minutes),
                start_time="2026-10-05T23:07:00Z", away_team="PHI", home_team="TBL",
                market=market, side=side, line=line, price=price)


@pytest.mark.parametrize("label,code", [
    ("Tampa Bay Lightning", "TBL"), ("TAMPA BAY LIGHTNING", "TBL"), ("tampa-bay-lightning", "TBL"),
    ("TB", "TBL"), ("NJ", "NJD"), ("UTAH", "UTA"), ("Utah Hockey Club", "UTA"),
    ("Montréal Canadiens", "MTL"), ("St. Louis Blues", "STL"), ("Phoenix Coyotes", "PHX"), ("Nope", None),
])
def test_resolve_team(label, code):
    assert resolve_team(label) == code


def test_odds_frame_types_and_market_uid():
    f = odds_frame([row(0, -218), row(0, 150, "puckline", "home", -1.5)])
    assert f.schema["captured_at"] == pl.Datetime("us", "UTC")
    assert f["market_uid"].to_list() == ["game|moneyline|game|main", "game|puckline|game|main"]
    assert f["price_point"].to_list() == ["live", "live"]


def test_keep_transitions_drops_unchanged_polls():
    f = odds_frame([row(0, -218), row(30, -218), row(60, -225), row(90, -225), row(120, -218)])
    assert keep_transitions(f, MARKET_KEY, TRANSITION_VALUES)["price"].to_list() == [-218, -225, -218]


def test_new_transitions_is_additive():
    stored = odds_frame([row(0, -218)])
    incoming = odds_frame([row(30, -218), row(30, 150, "puckline", "home", -1.5)])
    fresh = new_transitions(stored, incoming, MARKET_KEY, TRANSITION_VALUES)
    assert fresh["market"].to_list() == ["puckline"]


def test_attach_game_ids_handles_late_utc_start():
    games = pl.DataFrame({"game_id": [2026020040, 2026020099], "game_date": [date(2026, 10, 5), date(2026, 12, 1)],
                          "away_abbr": ["PHI", "PHI"], "home_abbr": ["TBL", "TBL"]})
    late = row(0, -218) | {"game_id": None, "start_time": "2026-10-06T02:30:00Z"}
    out = attach_game_ids(odds_frame([late]), games)
    assert out["game_id"].to_list() == [2026020040]
