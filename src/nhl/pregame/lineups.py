"""Projected lineups (M5 phase C): who dresses and how each skater will be deployed.

For a team-game the default is the team's **last game**: its dressed skaters, with each
player's deployment (5v5, PP, PK and shot shares) from a recency-weighted average of his own
recent games. Absences and returns then come from **status events** known before ``as_of``:

* **ESPN injuries** (captured from 2026-10-05): ``Injured Reserve``, ``Out`` and
  ``Suspension`` rule a player out *until his expected return date*. A player whose return
  date is on or before the game, or who drops off the report (``Removed``), is available
  again. ``Day-To-Day`` changes nothing.
* **Transactions** (2010 on; dense from 2021): placed on IR, assigned, suspended, retired and
  traded away rule a player out; activated from IR, recalled, claimed and traded in make
  him available. A backfilled transaction counts as known the day after its date.

A ruled-out player's slot is filled (owner's decision, 2026-10-06) by the best available
player at the same position: a player back from injury first, then the most recently
dressed or added player (a healthy scratch, a recall, a trade). With nobody available, a
placeholder (``player_id`` null, the departed player's deployment, league-average ratings)
keeps the lineup at full strength. Every fill is flagged ``confidence="low"``.

A regular back from injury also displaces the lowest-usage player at his position when his
own 5v5 share is clearly higher, so a star's return shows up on the date ESPN expects it.

Output follows :func:`nhl.sim.inputs.actual_deployment` (shares sum to 5 / 5 / 4 / 1 per
team-game) plus ``source``, ``confidence`` and ``issues``.
"""

from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone

import polars as pl

from nhl import teams
from nhl.sim.inputs import _shares
from nhl.sources.dailyfaceoff import norm_name
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

#: Half-life (in the player's games) of the deployment averages.
HALF_LIFE = 5.0
HALF_LIFE_SHOT = 15.0
#: Healthy scratches older than this many team games are not used as fills.
FILL_LOOKBACK_GAMES = 30
#: A returning player displaces the lowest-usage regular when his 5v5 share is this much higher.
RETURN_MARGIN = 0.01
#: Backtest snapshot: 12:00 UTC (08:00 ET) on game day; transactions dated that day aren't known yet.
MORNING_UTC = time(12, 0)

OUT_STATUSES = {"Injured Reserve", "Out", "Suspension"}
OUT_TX = {"placed_ir", "assigned", "suspended", "retired"}
IN_TX = {"activated_ir", "recalled", "claimed"}
SHARES = ("s5", "spp", "spk", "sshot")
SCALE = {"s5": 5.0, "spp": 5.0, "spk": 4.0, "sshot": 1.0}


def group(position: str | None) -> str:
    """``F`` or ``D``."""
    return "D" if position == "D" else "F"


# --------------------------------------------------------------------------- history

def deployment_history(store: Store, seasons: list[int]) -> pl.DataFrame:
    """Per skater-game: ``game_id, game_date, team_id, player_id, position`` and the per-game
    shares ``s5, spp, spk, sshot`` (each sums to 1 per team-game; null when the team had no
    time in that state)."""
    frames = []
    for s in seasons:
        logs = store.get_parquet(keys.player_game_logs(s))
        if logs is None:
            continue
        logs = logs.filter(pl.col("position") != "G")
        dressed = logs.filter(pl.col("strength") == "all").select("game_id", "game_date", "team_id", "player_id", "position")
        part = dressed
        for strength, value, alias in (("5v5", "toi_s", "s5"), ("PP", "toi_s", "spp"), ("SH", "toi_s", "spk"), ("all", "ixg", "sshot")):
            sh = _shares(logs, strength, value, alias, cumulative=False).drop("game_date")
            part = part.join(sh, on=["game_id", "team_id", "player_id"], how="left")
        frames.append(part.with_columns(pl.col("s5").fill_null(0.0)))
    return pl.concat(frames).sort("game_date", "game_id")


def player_form(hist: pl.DataFrame) -> pl.DataFrame:
    """Per skater-game, the recency-weighted share averages *including* that game."""
    return hist.sort("player_id", "game_date").with_columns(
        *[pl.col(c).ewm_mean(half_life=HALF_LIFE_SHOT if c == "sshot" else HALF_LIFE, ignore_nulls=True).over("player_id")
          for c in SHARES]
    ).select("player_id", "game_date", *SHARES)


# --------------------------------------------------------------------------- status events

@dataclass(frozen=True)
class Event:
    known_at: datetime
    player_id: int
    kind: str  # "out" | "in"
    until: date | None  # expected return (ESPN), out events only
    cause: str  # e.g. "espn:Injured Reserve", "tx:recalled"
    injury: bool  # an injury return (vs a recall or trade) when kind == "in"


def status_events(store: Store, seasons: list[int], rosters: pl.DataFrame) -> pl.DataFrame:
    """Transactions and ESPN injury rows as ``team_id, player_id, known_at, kind, until, cause,
    injury``. Missing ``player_id``s are matched by name to the team's dressed players."""
    frames = []
    for s in seasons:
        tx = store.get_parquet(keys.transactions(s))
        if tx is not None and tx.height:
            frames.append(_tx_events(tx, s))
        inj = store.get_parquet(keys.injuries(s))
        if inj is not None and inj.height:
            frames.append(_injury_events(inj, s))
    if not frames:
        return pl.DataFrame(schema=_EVENT_SCHEMA)
    ev = pl.concat(frames, how="diagonal_relaxed")
    names = rosters.select("team_id", "player_id", pl.col("player_name").map_elements(norm_name, return_dtype=pl.String).alias("nm")).unique(
        subset=["team_id", "nm"], keep="last"
    )
    ev = ev.with_columns(pl.col("player_name").map_elements(norm_name, return_dtype=pl.String).alias("nm")).join(
        names.rename({"player_id": "pid_name"}), on=["team_id", "nm"], how="left"
    ).with_columns(pl.coalesce("player_id", "pid_name").alias("player_id"))
    unresolved = ev.filter(pl.col("player_id").is_null()).height
    if unresolved:
        logger.debug("status events: %d rows without a player_id (mostly non-NHL players)", unresolved)
    return ev.filter(pl.col("player_id").is_not_null() & pl.col("team_id").is_not_null()).select(list(_EVENT_SCHEMA)).sort("known_at")


_EVENT_SCHEMA = {
    "team_id": pl.Int64, "player_id": pl.Int64, "known_at": pl.Datetime("us", "UTC"), "kind": pl.String,
    "until": pl.Date, "cause": pl.String, "injury": pl.Boolean,
}


def _team_ids(frame: pl.DataFrame, season: int) -> pl.Expr:
    lookup = {t: teams.resolve_team_id(t, season) for t in frame["team"].unique().to_list()}
    return pl.col("team").replace_strict(lookup, default=None, return_dtype=pl.Int64)


def _tx_events(tx: pl.DataFrame, season: int) -> pl.DataFrame:
    kind = (
        pl.when(pl.col("type").is_in(list(OUT_TX)) | ((pl.col("type") == "traded") & (pl.col("direction") == "out"))).then(pl.lit("out"))
        .when(pl.col("type").is_in(list(IN_TX)) | ((pl.col("type") == "traded") & (pl.col("direction") == "in"))).then(pl.lit("in"))
    )
    # Known the day after its date (backfill), or when we captured it live, whichever is first.
    next_day = (pl.col("date").cast(pl.Datetime("us")) + pl.duration(days=1)).dt.replace_time_zone("UTC")
    return tx.with_columns(kind.alias("kind")).filter(pl.col("kind").is_not_null()).select(
        _team_ids(tx, season).alias("team_id"), pl.col("player_id").cast(pl.Int64), "player_name",
        pl.min_horizontal(next_day, pl.col("captured_at").dt.cast_time_unit("us")).alias("known_at"), "kind",
        pl.lit(None, dtype=pl.Date).alias("until"), ("tx:" + pl.col("type")).alias("cause"),
        (pl.col("type") == "activated_ir").alias("injury"),
    )


def _injury_events(inj: pl.DataFrame, season: int) -> pl.DataFrame:
    kind = (
        pl.when(pl.col("status").is_in(list(OUT_STATUSES))).then(pl.lit("out"))
        .when(pl.col("status") == "Removed").then(pl.lit("in"))
    )
    return inj.with_columns(kind.alias("kind")).filter(pl.col("kind").is_not_null()).select(
        _team_ids(inj, season).alias("team_id"), pl.col("player_id").cast(pl.Int64), "player_name",
        pl.col("captured_at").dt.cast_time_unit("us").alias("known_at"), "kind",
        pl.when(pl.col("kind") == "out").then(pl.col("return_date")).alias("until"),
        ("espn:" + pl.col("status")).alias("cause"), pl.lit(True).alias("injury"),
    )


# --------------------------------------------------------------------------- projection

@dataclass
class _Index:
    """Lookups for the per-team-game projection loop."""

    team_games: dict[int, list[tuple[date, int]]] = field(default_factory=dict)
    dressed: dict[tuple[int, int], list[tuple[int, str]]] = field(default_factory=dict)
    form: dict[int, tuple[list[date], list[tuple]]] = field(default_factory=dict)
    position: dict[int, str] = field(default_factory=dict)
    events: dict[int, tuple[list[datetime], list[Event]]] = field(default_factory=dict)


def _build_index(hist: pl.DataFrame, events: pl.DataFrame) -> _Index:
    ix = _Index()
    tg = hist.select("team_id", "game_date", "game_id").unique().sort("game_date", "game_id")
    for (team,), part in tg.partition_by("team_id", as_dict=True).items():
        ix.team_games[team] = list(zip(part["game_date"].to_list(), part["game_id"].to_list()))
    for (gid, team), part in hist.partition_by("game_id", "team_id", as_dict=True).items():
        ix.dressed[(gid, team)] = list(zip(part["player_id"].to_list(), part["position"].to_list()))
    for (pid,), part in player_form(hist).partition_by("player_id", as_dict=True).items():
        ix.form[pid] = (part["game_date"].to_list(), list(part.select(*SHARES).iter_rows()))
    ix.position = dict(hist.sort("game_date").select("player_id", "position").unique(subset="player_id", keep="last").iter_rows())
    for (team,), part in events.partition_by("team_id", as_dict=True).items():
        evs = [Event(r["known_at"], r["player_id"], r["kind"], r["until"], r["cause"], r["injury"]) for r in part.sort("known_at").iter_rows(named=True)]
        ix.events[team] = ([e.known_at for e in evs], evs)
    return ix


def _form_before(ix: _Index, pid: int | None, day: date) -> dict[str, float | None] | None:
    if pid is None or pid not in ix.form:
        return None
    dates, rows = ix.form[pid]
    i = bisect.bisect_left(dates, day) - 1
    return dict(zip(SHARES, rows[i])) if i >= 0 else None


def _latest_status(ix: _Index, team: int, as_of: datetime, day: date, last_date: date) -> tuple[dict[int, Event], dict[int, Event]]:
    """``(out, available)`` for ``day``, from each player's latest event known before ``as_of``.

    The team's last game (on ``last_date``) already reflects everything known that morning,
    so only newer events count; the exception is an injury whose expected return falls
    after the last game and on or before ``day`` (he's due back). ``out``: ruled out for
    ``day``. ``available``: an ``in`` event, or an injury whose expected return has come.
    """
    known, evs = ix.events.get(team, ([], []))
    latest: dict[int, Event] = {}
    for e in evs[: bisect.bisect_left(known, as_of)]:
        latest[e.player_id] = e
    since = morning(last_date)
    out, avail = {}, {}
    for pid, e in latest.items():
        if e.kind == "out" and e.until is not None and last_date < e.until <= day:
            avail[pid] = e
        elif e.known_at <= since:
            continue
        elif e.kind == "out" and (e.until is None or e.until > day):
            out[pid] = e
        elif e.kind == "in" or e.until <= day:
            avail[pid] = e
    return out, avail


def project_team_game(ix: _Index, team: int, game_id: int, day: date, as_of: datetime) -> list[dict]:
    """Projected dressed skaters for one team-game (see the module docstring)."""
    games = ix.team_games.get(team, [])
    k = bisect.bisect_left(games, (day, -1)) - 1
    if k < 0:
        return []
    last_date, last_gid = games[k]
    base = ix.dressed[(last_gid, team)]
    out, avail = _latest_status(ix, team, as_of, day, last_date)
    lineup = {pid: {"player_id": pid, "position": pos, "source": "last_game", "confidence": "high", "issues": ""} for pid, pos in base}
    in_lineup = set(lineup)

    # Recently dressed for this team, most recent first (fill candidates).
    recent: dict[int, date] = {}
    for d, gid in reversed(games[max(0, k - FILL_LOOKBACK_GAMES + 1): k + 1]):
        for pid, _pos in ix.dressed[(gid, team)]:
            recent.setdefault(pid, d)

    def usage(pid: int | None) -> float:
        f = _form_before(ix, pid, day)
        return (f or {}).get("s5") or 0.0

    def candidates(grp: str) -> list[int]:
        pool = [p for p in set(recent) | set(avail) if p not in in_lineup and p not in out and group(ix.position.get(p)) == grp]
        pool = [p for p in pool if p in ix.form or p in avail]

        def rank(p: int) -> tuple:
            e = avail.get(p)
            injury_return = e is not None and e.injury and (p not in recent or recent[p] < last_date)
            added = e.known_at.date() if e is not None else date.min
            return (not injury_return, -max(recent.get(p, date.min), added).toordinal(), -usage(p))
        return sorted(pool, key=rank)

    # Remove ruled-out players and fill their slots.
    for pid in [p for p in list(lineup) if p in out]:
        gone = lineup.pop(pid)
        e = out[pid]
        grp = group(gone["position"])
        cands = candidates(grp)
        note = f"{pid} out ({e.cause}{f' until {e.until}' if e.until else ''})"
        if cands:
            new = cands[0]
            in_lineup.add(new)
            lineup[new] = {"player_id": new, "position": ix.position.get(new, grp if grp == "D" else "C"),
                           "source": "fill", "confidence": "low", "issues": note}
        else:
            lineup[f"placeholder:{pid}"] = {"player_id": None, "position": gone["position"], "source": "fill",
                                            "confidence": "low", "issues": note + "; placeholder", "_like": pid}

    # Regulars back from injury displace the lowest-usage player at their position.
    for pid, e in avail.items():
        if not e.injury or pid in in_lineup or pid not in ix.form or (pid in recent and recent[pid] >= last_date):
            continue
        grp = group(ix.position.get(pid))
        same = [p for p, r in lineup.items() if group(r["position"]) == grp]
        if not same:
            continue
        weakest = min(same, key=lambda p: (lineup[p]["source"] != "fill", usage(lineup[p]["player_id"] if lineup[p]["player_id"] else lineup[p].get("_like"))))
        w_usage = usage(lineup[weakest]["player_id"] if lineup[weakest]["player_id"] else lineup[weakest].get("_like"))
        if usage(pid) > w_usage + RETURN_MARGIN:
            dropped = lineup.pop(weakest)
            in_lineup.discard(weakest)
            in_lineup.add(pid)
            lineup[pid] = {"player_id": pid, "position": ix.position.get(pid, "C"), "source": "return", "confidence": "medium",
                           "issues": f"back ({e.cause}); replaces {dropped['player_id']}"}

    rows = []
    for key, r in lineup.items():
        f = _form_before(ix, r["player_id"], day) or _form_before(ix, r.get("_like"), day) or {}
        rows.append({
            "game_id": game_id, "team_id": team, "player_id": r["player_id"], "position": r["position"],
            **{c: f.get(c) for c in SHARES}, "source": r["source"], "confidence": r["confidence"], "issues": r["issues"],
        })
    return rows


def project(store: Store, targets: pl.DataFrame, seasons: list[int], as_of: datetime | None = None,
            hist: pl.DataFrame | None = None, events: pl.DataFrame | None = None) -> pl.DataFrame:
    """Projected deployment for ``targets`` (``game_id, team_id, game_date``).

    Args:
        store: S3 store.
        targets: team-games to project.
        seasons: seasons of history to load (include the one before the targets').
        as_of: what is known; default per target is 12:00 UTC on its game day (backtest).
        hist, events: preloaded :func:`deployment_history` / :func:`status_events`.

    Returns:
        ``game_id, team_id, player_id, position, s5, spp, spk, sshot, is_d, source,
        confidence, issues``, shares scaled like :func:`nhl.sim.inputs.actual_deployment`.
    """
    hist = hist if hist is not None else deployment_history(store, seasons)
    if events is None:
        rosters = pl.concat([r for s in seasons if (r := store.get_parquet(keys.rosters(s))) is not None])
        events = status_events(store, seasons, rosters)
    ix = _build_index(hist, events)
    rows = []
    for gid, team, day in targets.select("game_id", "team_id", "game_date").iter_rows():
        moment = as_of or morning(day)
        rows.extend(project_team_game(ix, team, gid, day, moment))
    if not rows:
        return pl.DataFrame()
    dep = pl.DataFrame(rows, schema_overrides={"player_id": pl.Int64, **{c: pl.Float64 for c in SHARES}})
    return normalise(dep)


def normalise(dep: pl.DataFrame) -> pl.DataFrame:
    """Scale shares to the simulator's units per team-game (Σ s5 = 5, Σ spp = 5, Σ spk = 4,
    Σ sshot = 1); a share missing for everyone falls back to 5v5."""
    out = dep.with_columns(pl.col("s5").fill_null(0.0))
    for c in ("spp", "spk", "sshot"):
        out = out.with_columns(
            pl.when(pl.col(c).is_null().all().over("game_id", "team_id")).then(pl.col("s5")).otherwise(pl.col(c).fill_null(0.0)).alias(c)
        )
    out = out.with_columns(
        *[(pl.col(c) / pl.col(c).sum().over("game_id", "team_id") * SCALE[c]).fill_nan(0.0).alias(c) for c in SHARES],
        (pl.col("position") == "D").alias("is_d"),
    )
    return out


def lineup_shape(dep: pl.DataFrame) -> pl.DataFrame:
    """Per team-game: forwards, defensemen, shape and whether it's regular (12F6D or 11F7D)."""
    return dep.group_by("game_id", "team_id").agg(
        (pl.col("position") != "D").sum().alias("n_f"), (pl.col("position") == "D").sum().alias("n_d"),
    ).with_columns(
        (pl.col("n_f").cast(pl.String) + "F" + pl.col("n_d").cast(pl.String) + "D").alias("shape"),
    ).with_columns(pl.col("shape").is_in(["12F6D", "11F7D"]).alias("regular"))


def accuracy(projected: pl.DataFrame, hist: pl.DataFrame) -> pl.DataFrame:
    """Per team-game: how many of the actually dressed skaters were projected, and the
    5v5-share-weighted overlap (players who matter more count more)."""
    actual = hist.select("game_id", "team_id", "player_id", "s5")
    proj = projected.filter(pl.col("player_id").is_not_null()).select("game_id", "team_id", "player_id", pl.lit(True).alias("hit"))
    return actual.join(proj, on=["game_id", "team_id", "player_id"], how="left").group_by("game_id", "team_id").agg(
        pl.len().alias("dressed"), pl.col("hit").fill_null(False).sum().alias("found"),
        ((pl.col("s5") * pl.col("hit").fill_null(False)).sum() / pl.col("s5").sum()).alias("weighted"),
    )


def morning(day: date) -> datetime:
    """The backtest snapshot time for a game day."""
    return datetime.combine(day, MORNING_UTC, tzinfo=timezone.utc)
