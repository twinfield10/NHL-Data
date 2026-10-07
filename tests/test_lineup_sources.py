"""Pregame-context sources (DailyFaceoff, oEmbed tweets, ESPN injuries) on real trimmed payloads."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from nhl.sources import dailyfaceoff as df
from nhl.sources import espn_injuries as espn
from nhl.sources import tweets

FIXTURES = Path(__file__).parent / "fixtures" / "lineups"
CAPTURED = datetime(2026, 10, 5, 18, 0, tzinfo=timezone.utc)


def load(name: str) -> dict:
    """Read one JSON fixture."""
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def resolver() -> df.PlayerResolver:
    """Resolver with the real TBL roster and a tiny league table for cross-team fallback."""
    roster = load("roster_TBL.json")
    players = pl.DataFrame({
        "player_id": [8478406, 8480045, 8999999],
        "player_name": ["Dan Vladar", "Linus Ullmark", "Linus Ullmark"],
        "last_season": [20252026, 20172018, 20252026],
    })
    return df.PlayerResolver(players=players, roster_loader=lambda team: roster if team == "TBL" else {})


# --------------------------------------------------------------------------- goalies
def test_goalies_one_row_per_side_with_status(resolver):
    g = df.normalize_goalies(load("starting_goalies_2026-10-05.json"), CAPTURED, resolver)
    assert g.height == 8
    tbl = g.filter(pl.col("team") == "TBL").row(0, named=True)
    assert tbl["opponent"] == "PHI" and tbl["is_home"]
    assert tbl["goalie_name"] == "Andrei Vasilevskiy" and tbl["df_goalie_id"] == 2413
    assert tbl["player_id"] == 8476883                       # TBL roster, name match
    assert (tbl["status"], tbl["status_id"]) == ("Likely", 3)
    assert tbl["start_time"] == datetime(2026, 10, 5, 23, 0, tzinfo=timezone.utc)
    assert tbl["game_date"] == date(2026, 10, 5)
    assert tbl["news_source_url"] == "https://x.com/Gabby_Shirley_/status/2107115869549527516"
    phi = g.filter(pl.col("team") == "PHI").row(0, named=True)
    assert (phi["status"], phi["status_id"]) == ("Confirmed", 2)
    assert phi["player_id"] == 8478406                       # league fallback
    # Tracking query string removed so "?s=20" vs "?s=46" never looks like a change.
    assert phi["news_source_url"] == "https://x.com/NHLFlyers/status/2107127644206530795"
    assert phi["news_created_at"] == datetime(2026, 10, 5, 15, 16, 49, 641000, tzinfo=timezone.utc)


def test_league_fallback_prefers_most_recent_namesake(resolver):
    g = df.normalize_goalies(load("starting_goalies_2026-10-05.json"), CAPTURED, resolver)
    assert g.filter(pl.col("goalie_name") == "Linus Ullmark")["player_id"].to_list() == [8999999]


def test_projected_goalie_has_null_status():
    g = df.normalize_goalies(load("starting_goalies_2026-10-06.json"), CAPTURED)
    assert g.height == 4
    assert g["status"].null_count() == 4 and g["player_id"].null_count() == 4
    assert g.filter(pl.col("team") == "BUF")["goalie_name"].item() == "Ukko-Pekka Luukkonen"


def test_attach_game_ids_by_date_and_teams():
    g = df.normalize_goalies(load("starting_goalies_2026-10-05.json"), CAPTURED)
    games = pl.DataFrame({
        "game_id": [2026020040], "season": pl.Series([20262027], dtype=pl.Int32),
        "game_date": [date(2026, 10, 5)], "home_abbr": ["TBL"], "away_abbr": ["PHI"],
    })
    out = df.attach_game_ids(g, games)
    assert out.filter(pl.col("team").is_in(["TBL", "PHI"]))["game_id"].to_list() == [2026020040] * 2
    assert out["game_id"].null_count() == 6
    assert out["season"].unique().to_list() == [20262027]     # unmatched rows: season from date


def test_goalie_transitions_ignore_repeat_polls():
    from nhl.sources.common import new_transitions

    first = df.normalize_goalies(load("starting_goalies_2026-10-05.json"), CAPTURED)
    again = first.with_columns(captured_at=pl.lit(datetime(2026, 10, 5, 18, 5, tzinfo=timezone.utc)))
    assert new_transitions(first, again, df.GOALIE_KEYS, df.GOALIE_VALUES).is_empty()
    flipped = again.with_columns(status=pl.when(pl.col("team") == "TBL").then(pl.lit("Confirmed")).otherwise("status"))
    assert new_transitions(first, flipped, df.GOALIE_KEYS, df.GOALIE_VALUES)["team"].to_list() == ["TBL"]


# --------------------------------------------------------------------------- lines
@pytest.fixture
def lines(resolver) -> pl.DataFrame:
    return df.normalize_lines(load("line_combinations_tbl.json")["combinations"], CAPTURED, resolver)


def test_tbl_first_line_in_order(lines):
    f1 = lines.filter(pl.col("group") == "f1").sort("slot")
    assert f1["player_name"].to_list() == ["Gage Goncalves", "Brayden Point", "Nikita Kucherov"]
    assert f1["position"].to_list() == ["lw", "c", "rw"]
    assert f1["player_id"].to_list() == [8482201, 8478010, 8476453]
    assert set(lines["team"]) == {"TBL"}
    assert lines["updated_at"][0] == datetime(2026, 10, 4, 17, 10, 23, 716000, tzinfo=timezone.utc)
    assert lines["source_url"][0] == "https://x.com/Gabby_Shirley_/status/2106770978550473208"


def test_special_teams_and_ir(lines):
    pp1 = lines.filter((pl.col("category") == "pp") & (pl.col("group") == "pp1"))
    assert pp1.height == 5 and pp1["slot"].to_list() == [1, 2, 3, 4, 5]
    assert lines.filter(pl.col("group") == "pk1").height == 4
    ir = lines.filter(pl.col("group") == "ir")
    assert dict(zip(ir["player_name"], ir["injury_status"])) == {
        "Yanni Gourde": "out", "Dominic James": "out", "Emil Lilleberg": "dtd"}
    lilleberg = lines.filter((pl.col("player_name") == "Emil Lilleberg") & (pl.col("group") == "d2")).row(0, named=True)
    assert lilleberg["game_time_decision"] and lilleberg["jersey"] is None
    assert lilleberg["player_id"] == 8482929                 # no jersey -> name match


def test_jersey_mismatch_falls_back_to_name(lines):
    # DailyFaceoff lists Viel as #28, which on the NHL roster is Girgensons: the name check
    # must reject the jersey hit and the name match must find Viel.
    ids = dict(zip(lines["player_name"], lines["player_id"]))
    assert ids["Jeffrey Viel"] == 8479705
    assert ids["Zemgus Girgensons"] == 8476878
    assert ids["Charle-Edouard D'Astous"] == 8480426
    assert ids["J.J. Moser"] == 8482655
    assert lines["player_id"].null_count() == 0


def test_jersey_hit_on_a_brother_is_rejected():
    # Real case: DailyFaceoff listed Nick Foligno as #17, which is Marcus Foligno's number.
    roster = {"forwards": [
        {"id": 8475220, "firstName": {"default": "Marcus"}, "lastName": {"default": "Foligno"}, "sweaterNumber": 17},
        {"id": 8473422, "firstName": {"default": "Nick"}, "lastName": {"default": "Foligno"}, "sweaterNumber": 71},
    ]}
    r = df.PlayerResolver(roster_loader=lambda team: roster)
    assert r.resolve("MIN", "Nick Foligno", 17) == 8473422
    assert r.resolve("MIN", "Marcus Foligno", 17) == 8475220
    assert r.resolve("MIN", "Nobody Here", 99) is None and r.unresolved == {("MIN", "Nobody Here")}


def test_next_data_extraction():
    page = '<html><script id="__NEXT_DATA__" type="application/json">{"props": {"pageProps": {"x": 1}}}</script></html>'
    assert df.next_data(page)["props"]["pageProps"] == {"x": 1}
    with pytest.raises(ValueError):
        df.next_data("<html>Attention Required! | Cloudflare</html>")


def test_season_for_date():
    assert df.season_for_date(date(2026, 10, 5)) == 20262027
    assert df.season_for_date(date(2027, 4, 1)) == 20262027
    assert df.season_for_date(date(2027, 7, 1)) == 20272028


# --------------------------------------------------------------------------- tweets
def test_snowflake_time():
    assert tweets.snowflake_time("2106770978550473208").replace(microsecond=0) == datetime(
        2026, 10, 4, 15, 38, 37, tzinfo=timezone.utc)


def test_tweet_urls():
    url = "https://x.com/NHLFlyers/status/2107127644206530795?s=20"
    assert tweets.tweet_id_from_url(url) == "2107127644206530795"
    assert tweets.canonical_tweet_url(url) == "https://x.com/NHLFlyers/status/2107127644206530795"
    assert tweets.tweet_id_from_url("https://twitter.com/a/statuses/123") == "123"
    assert tweets.tweet_id_from_url("https://www.nhl.com/news/something") is None


def test_oembed_text_keeps_line_breaks():
    raw = {"status": 200, "fetched_at": "2026-10-05T18:00:00+00:00", "payload": load("oembed_2106770978550473208.json")}
    url = "https://x.com/Gabby_Shirley_/status/2106770978550473208"
    row = tweets.parse_oembed("2106770978550473208", url, raw, CAPTURED)
    assert row["available"] and row["author_name"] == "Gabby Shirley" and row["author_handle"] == "Gabby_Shirley_"
    lines = row["text"].split("\n")
    assert lines[0] == "a look at the #Bolts lines and D pairings from practice:"
    assert lines[1] == ""
    assert lines[2] == "Goncalves - Point - Kucherov"
    assert lines[-1] == "D’Astous"
    assert "Gabby Shirley (@" not in row["text"]                # byline dropped
    assert row["created_at"].replace(microsecond=0) == datetime(2026, 10, 4, 15, 38, 37, tzinfo=timezone.utc)


def test_unavailable_tweet_row():
    raw = {"status": 404, "fetched_at": "2026-10-05T18:00:00+00:00", "payload": None}
    row = tweets.parse_oembed("1234567890123456789", "https://x.com/someone/status/1234567890123456789", raw, CAPTURED)
    assert row["available"] is False and row["text"] is None and row["author_handle"] == "someone"


class FakeStore:
    """In-memory stand-in for :class:`nhl.storage.s3.Store`."""

    def __init__(self) -> None:
        self.objects: dict[str, object] = {}

    def get_parquet(self, key):
        return self.objects.get(key)

    def put_parquet(self, key, frame):
        self.objects[key] = frame

    def get_json_gz(self, key):
        return self.objects.get(key)

    def put_json_gz(self, key, obj):
        self.objects[key] = obj


def test_fetch_tweets_uses_cache_and_skips_known(monkeypatch):
    store = FakeStore()
    calls: list[str] = []

    def fake_fetch(client, url):
        calls.append(url)
        return {"status": 404, "fetched_at": "2026-10-05T18:00:00+00:00", "payload": None}

    monkeypatch.setattr(tweets, "_fetch_raw", fake_fetch)
    cached = "2106770978550473208"
    store.put_json_gz(tweets.keys.tweet_raw(cached), {
        "status": 200, "fetched_at": "2026-10-05T18:00:00+00:00", "payload": load("oembed_2106770978550473208.json")})
    urls = [f"https://x.com/Gabby_Shirley_/status/{cached}?s=20", "https://x.com/a/status/42", "https://x.com/a/status/42",
            "https://example.com/not-a-tweet", None]
    assert tweets.fetch_tweets(store, urls, client=object()) == 2
    assert calls == ["https://x.com/a/status/42"]              # cached raw reused, dupes collapsed
    table = store.get_parquet(tweets.keys.TWEETS)
    assert dict(zip(table["tweet_id"], table["available"])) == {cached: True, "42": False}
    assert tweets.fetch_tweets(store, urls, client=object()) == 0  # never refetched
    assert len(calls) == 1


# --------------------------------------------------------------------------- injuries
def test_normalize_injuries():
    inj = espn.normalize_injuries(load("espn_injuries.json"), CAPTURED)
    assert inj.height == 7
    lil = inj.filter(pl.col("player_name") == "Emil Lilleberg").row(0, named=True)
    assert lil["team"] == "TBL" and lil["espn_athlete_id"] == 5146599 and lil["status"] == "Out"
    assert lil["reported_at"] == datetime(2026, 10, 5, 14, 48, tzinfo=timezone.utc)
    mcavoy = inj.filter(pl.col("player_name") == "Charlie McAvoy").row(0, named=True)
    assert mcavoy["status"] == "Suspension" and mcavoy["return_date"] == date(2026, 10, 13)
    assert set(inj["status"]) == {"Injured Reserve", "Suspension", "Out"}


def test_injury_player_ids(resolver):
    inj = espn.normalize_injuries(load("espn_injuries.json"), CAPTURED, resolver)
    tbl = inj.filter(pl.col("team") == "TBL")
    assert dict(zip(tbl["player_name"], tbl["player_id"])) == {
        "Emil Lilleberg": 8482929, "Yanni Gourde": 8476826, "Dominic James": 8483752}


def test_removed_when_athlete_drops_off():
    from nhl.sources.common import new_transitions

    first = espn.normalize_injuries(load("espn_injuries.json"), CAPTURED)
    later = datetime(2026, 10, 6, 18, 0, tzinfo=timezone.utc)
    second = espn.normalize_injuries(load("espn_injuries.json"), later).filter(pl.col("player_name") != "Charlie McAvoy")
    gone = espn.removed_rows(first, second, later)
    assert gone["player_name"].to_list() == ["Charlie McAvoy"]
    row = gone.row(0, named=True)
    assert row["status"] == "Removed" and row["team"] == "BOS" and row["captured_at"] == later
    assert row["short_comment"] is None and row["reported_at"] is None
    written = new_transitions(first, pl.concat([second, gone]), espn.KEYS, espn.VALUES)
    assert written["player_name"].to_list() == ["Charlie McAvoy"]  # unchanged players write nothing
    # Already removed: a later poll without him emits nothing new.
    stored = pl.concat([first, written])
    third = datetime(2026, 10, 7, 18, 0, tzinfo=timezone.utc)
    assert espn.removed_rows(stored, second.with_columns(captured_at=pl.lit(third)), third).is_empty()
    # Re-listed after removal: a fresh transition back to his new status.
    back = espn.normalize_injuries(load("espn_injuries.json"), third)
    again = new_transitions(stored, back, espn.KEYS, espn.VALUES)
    assert again["player_name"].to_list() == ["Charlie McAvoy"] and again["status"].item() == "Suspension"


def test_no_removals_without_history():
    inj = espn.normalize_injuries(load("espn_injuries.json"), CAPTURED)
    assert espn.removed_rows(None, inj, CAPTURED).is_empty()


# --------------------------------------------------------------------------- line validation
def test_tbl_version_is_11f7d_with_lilleberg_questionable(lines):
    """f4 of two plus a d4 defenseman is an 11F/7D lineup (TBL dressed 11F/7D on 10/3);
    Lilleberg is in d2 and on the injury list (day-to-day): questionable, not a conflict."""
    f4 = lines.filter(pl.col("group") == "f4")
    assert f4["group_size"].to_list() == [2, 2] and f4["group_expected"].to_list() == [2, 2]
    assert f4["group_complete"].all()
    assert lines.filter(pl.col("group") == "f1")["group_complete"].all()
    lil = lines.filter(pl.col("player_name") == "Emil Lilleberg")
    assert lil["questionable"].all() and not lil["conflict"].any() and lil.height == 2  # d2 row and ir row
    assert not lines.filter(pl.col("player_name") == "Brayden Point")["questionable"].any()
    issues = lines["issues"][0]
    assert "Lilleberg active+injury list" in issues and "f4" not in issues
    assert lines["lineup_shape"][0] == "11F7D"
    assert lines["is_valid"].all()
    assert lines.filter(pl.col("group") == "ir")["group_expected"].null_count() == 3


def _clean_lineup() -> dict:
    """Synthetic 12F/6D version with full special teams and two goalies."""
    players, n = [], 0

    def add(cat, group, pos):
        nonlocal n
        n += 1
        players.append({"playerId": n, "name": f"Player {n}", "jerseyNumber": n, "positionIdentifier": pos,
                        "groupIdentifier": group, "categoryIdentifier": cat, "injuryStatus": None,
                        "gameTimeDecision": False, "latestNews": None})
        return n

    for line in range(1, 5):
        for pos in ("lw", "c", "rw"):
            add("ev", f"f{line}", pos)
    for pair in range(1, 4):
        for pos in ("ld", "rd"):
            add("ev", f"d{pair}", pos)
    add("ev", "g", "g1")
    add("ev", "g", "g2")
    by_id = {p["playerId"]: p for p in players}
    for unit, ids in (("pp1", [1, 2, 3, 4, 13]), ("pp2", [5, 6, 7, 8, 15]), ("pk1", [2, 5, 13, 14]), ("pk2", [8, 11, 15, 16])):
        for k, pid in enumerate(ids, 1):
            players.append({**by_id[pid], "groupIdentifier": unit, "categoryIdentifier": unit[:2], "positionIdentifier": f"sk{k}"})
    return {"teamAbbreviation": "TBL", "sourceName": "x", "source": "https://x.com/a/status/1",
            "updatedAt": "2026-10-05T12:00:00Z", "players": players}


def test_clean_lineup_is_valid():
    out = df.normalize_lines(_clean_lineup(), CAPTURED)
    assert out["is_valid"].all() and not out["conflict"].any()
    assert out["lineup_shape"][0] == "12F6D" and out["issues"][0] is None
    assert out["group_complete"].all()


def test_validation_flags_pp4_irregular_and_double_ev():
    combos = _clean_lineup()
    combos["players"] = [p for p in combos["players"] if not (p["groupIdentifier"] == "pp2" and p["playerId"] == 15)]
    out = df.normalize_lines(combos, CAPTURED)
    assert "pp2 has 4/5" in out["issues"][0] and out["is_valid"].all()   # pp4 flagged, still valid
    combos = _clean_lineup()
    combos["players"][3] = {**combos["players"][3], "playerId": 1, "name": "Player 1"}  # P1 on f1 and f2
    out = df.normalize_lines(combos, CAPTURED)
    assert "1 in f1+f2" in out["issues"][0] and not out["is_valid"].any()
    combos = _clean_lineup()
    combos["players"][0] = {**combos["players"][0], "injuryStatus": "dtd"}
    out = df.normalize_lines(combos, CAPTURED)
    assert "1 active+injury list" in out["issues"][0] and out["is_valid"].all()   # questionable, still valid
    assert out.filter(pl.col("df_player_id") == 1)["questionable"].all()
    combos = _clean_lineup()
    combos["players"] = [p for p in combos["players"] if not (p["groupIdentifier"] == "f4" and p["playerId"] == 12)]
    out = df.normalize_lines(combos, CAPTURED)   # f4 of two without a d4: incomplete, irregular
    assert "f4 has 2/3" in out["issues"][0] and out["lineup_shape"][0] == "irregular" and not out["is_valid"].any()


def test_validate_lines_is_idempotent(lines):
    again = df.validate_lines(lines)
    assert again.equals(lines)
