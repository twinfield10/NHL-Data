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

**DailyFaceoff lines** (phase D) come last: a version updated after the team's last game
and captured before ``as_of`` sets the 18 skaters when it is valid; otherwise only its
complete, conflict-free groups are placed and the rest of the projection stays. Listed
players' shares are pulled halfway toward their slot's typical share (:data:`SLOT_S5`,
:data:`UNIT_SHARE`), so a 4th-liner promoted to the top line gets top-line ice time.

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

#: DailyFaceoff reconciliation: typical shares by slot (each player's share of team 5v5 / PP /
#: PK skater time), from M2 inferred lines 2024-26, and how far a listed player's own share
#: is pulled toward his slot's.
SLOT_S5 = {"f1": 0.0596, "f2": 0.0539, "f3": 0.0481, "f4": 0.0388, "d1": 0.0759, "d2": 0.0685, "d3": 0.0553,
           "d4": 0.0553}  # d4: DailyFaceoff's seventh defenseman (11F/7D), priced like a third pair
UNIT_SHARE = {"spp": {1: 0.1404, 2: 0.0672, None: 0.005}, "spk": {1: 0.1406, 2: 0.1096, None: 0.005}}
SLOT_WEIGHT = 0.5
EV_GROUPS = ("f1", "f2", "f3", "f4", "d1", "d2", "d3", "d4")
#: Game-time decision: P(the questionable player dresses). His deployment is split 75/25 with
#: the likeliest replacement. A starting value (owner, 2026-10-06), to be measured in phase E.
GTD_P_DRESSED = 0.75
DFO_POSITION = {"lw": "L", "c": "C", "rw": "R", "ld": "D", "rd": "D"}
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
    kind: str  # "out" | "in" | "dtd" (ESPN day-to-day: questionable, neither out nor in)
    until: date | None  # expected return / next evaluation (ESPN)
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
        .when(pl.col("status") == "Day-To-Day").then(pl.lit("dtd"))
    )
    return inj.with_columns(kind.alias("kind")).filter(pl.col("kind").is_not_null()).select(
        _team_ids(inj, season).alias("team_id"), pl.col("player_id").cast(pl.Int64), "player_name",
        pl.col("captured_at").dt.cast_time_unit("us").alias("known_at"), "kind",
        pl.when(pl.col("kind") != "in").then(pl.col("return_date")).alias("until"),
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
    dfo: dict[int, tuple[list[datetime], list[DfoVersion]]] = field(default_factory=dict)


def _build_index(hist: pl.DataFrame, events: pl.DataFrame, dfo: pl.DataFrame | None = None) -> _Index:
    ix = _Index()
    if dfo is not None and not dfo.is_empty():
        ix.dfo = _dfo_index(dfo)
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


def _latest_status(ix: _Index, team: int, as_of: datetime, day: date, last_date: date
                   ) -> tuple[dict[int, Event], dict[int, Event], dict[int, Event]]:
    """``(out, available, day_to_day)`` for ``day``, from each player's latest event known
    before ``as_of``. ``day_to_day``: ESPN lists him day-to-day with no evaluation date before
    ``day`` (questionable; only used to confirm a DailyFaceoff injury tag).

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
    out, avail, dtd = {}, {}, {}
    for pid, e in latest.items():
        if e.kind == "dtd":
            if e.until is None or e.until >= day:
                dtd[pid] = e
        elif e.kind == "out" and e.until is not None and last_date < e.until <= day:
            avail[pid] = e
        elif e.known_at <= since:
            continue
        elif e.kind == "out" and (e.until is None or e.until > day):
            out[pid] = e
        elif e.kind == "in" or e.until <= day:
            avail[pid] = e
    return out, avail, dtd


@dataclass
class DfoVersion:
    """One DailyFaceoff line-combinations version for a team."""

    updated_at: datetime
    valid: bool
    issues: str | None
    groups: dict[str, list[tuple[int | None, str]]]  # group -> [(player_id, NHL position)]
    complete: set[str]  # groups with the expected size, no conflict and every player resolved
    out: set[int]  # on the injury list and not in an active slot
    questionable: set[int] = field(default_factory=set)  # in an active slot and on the injury list
    gtd: set[int] = field(default_factory=set)  # DailyFaceoff's own game-time-decision flag


def load_dfo(store: Store, seasons: list[int]) -> pl.DataFrame:
    """Every captured DailyFaceoff lines version (validated) with ``team_id``."""
    from nhl.sources.dailyfaceoff import validate_lines

    frames = [f for s in seasons if (f := store.get_parquet(keys.dailyfaceoff_lines(s))) is not None and f.height]
    if not frames:
        return pl.DataFrame()
    lines = validate_lines(pl.concat(frames, how="diagonal_relaxed"))
    season = seasons[-1]
    return lines.with_columns(_team_ids(lines, season).alias("team_id")).filter(pl.col("team_id").is_not_null())


def _dfo_index(dfo: pl.DataFrame) -> dict[int, tuple[list[datetime], list[DfoVersion]]]:
    """Per team: versions ordered by when we first captured them."""
    out: dict[int, tuple[list[datetime], list[DfoVersion]]] = {}
    for (team, _updated), part in sorted(dfo.partition_by("team_id", "updated_at", as_dict=True).items(), key=lambda kv: kv[0][1]):
        rows = part.to_dicts()
        groups: dict[str, list[tuple[int | None, str]]] = {}
        listed_injured: set[int] = set()
        active: set[int] = set()
        questionable: set[int] = set()
        gtd: set[int] = set()
        bad: set[str] = set()
        seven_d = rows[0]["lineup_shape"] == "11F7D"
        for r in rows:
            g, pid = r["group"], r["player_id"]
            if (r["category"] == "oi" or g == "ir") and pid is not None:
                listed_injured.add(pid)
            if g in EV_GROUPS or g in ("pp1", "pp2", "pk1", "pk2"):
                groups.setdefault(g, []).append((pid, DFO_POSITION.get(r["position"], "C")))
                complete = r["group_complete"] if g != "d4" else seven_d
                if r["conflict"] or pid is None or not complete:
                    bad.add(g)
            if g in EV_GROUPS and pid is not None:
                active.add(pid)
                if r.get("questionable"):
                    questionable.add(pid)
                if r["game_time_decision"]:
                    gtd.add(pid)
        version = DfoVersion(rows[0]["updated_at"], bool(rows[0]["is_valid"]), rows[0]["issues"], groups,
                             {g for g in groups if g not in bad}, listed_injured - active, questionable, gtd)
        captured = min(r["captured_at"] for r in rows)
        times, versions = out.setdefault(team, ([], []))
        i = bisect.bisect_right(times, captured)
        times.insert(i, captured)
        versions.insert(i, version)
    return out


def _dfo_version(ix: _Index, team: int, as_of: datetime, last_date: date) -> DfoVersion | None:
    """The latest DailyFaceoff version captured before ``as_of`` and updated after the morning
    of the team's last game (older versions are already reflected in that game)."""
    times, versions = ix.dfo.get(team, ([], []))
    known = versions[: bisect.bisect_right(times, as_of)]
    if not known:
        return None
    latest = max(known, key=lambda v: v.updated_at)
    return latest if latest.updated_at > morning(last_date) else None


def _apply_dfo(lineup: dict, version: DfoVersion, usage, candidates, ruled_out: dict[int, Event]
               ) -> tuple[dict, dict[str, dict[int, int]]]:
    """Reconcile the projected lineup with a DailyFaceoff version.

    A valid version sets all 18 skaters. Otherwise only its complete, conflict-free EV groups
    are placed; the remaining slots keep the projection, trimmed to the same F/D counts by
    dropping the lowest-usage non-DailyFaceoff players, or filled (players DailyFaceoff lists
    in an incomplete group first, then ``candidates(position group)``, then a placeholder).
    Players DailyFaceoff lists only on its injury list are removed first. A listed player whom
    ESPN or a transaction rules out for the game (``ruled_out``) is not placed: ESPN's dated
    report beats a stale line slot (Lilleberg, 2026-10-05), and his group becomes partial.
    Complete PP / PK units set ``pp_unit`` / ``pk_unit``.
    """
    note = f"dfo {version.updated_at:%Y-%m-%d %H:%M}" + (f" ({version.issues})" if version.issues else "")
    shape = {g: sum(1 for r in lineup.values() if group(r["position"]) == g) for g in ("F", "D")}
    complete = {g for g in version.complete
                if not any(pid in ruled_out for pid, _ in version.groups.get(g, []))}
    listed = {pid: (g, pos) for g in EV_GROUPS if g in complete for pid, pos in version.groups.get(g, [])}
    if version.valid and complete == version.complete and len(listed) == sum(len(version.groups.get(g, [])) for g in EV_GROUPS):
        shape = {"F": sum(len(version.groups.get(g, [])) for g in EV_GROUPS if g[0] == "f"),
                 "D": sum(len(version.groups.get(g, [])) for g in EV_GROUPS if g[0] == "d")}
        new = {}
    else:
        new = {k: r for k, r in lineup.items()
               if r["player_id"] not in version.out and r["player_id"] not in listed and r["player_id"] not in ruled_out}
    for pid, (g, pos) in listed.items():
        new[pid] = {"player_id": pid, "position": pos, "slot": g, "source": "dfo", "confidence": "high",
                    "issues": "" if version.valid else note}
    for grp in ("F", "D"):
        members = [k for k, r in new.items() if group(r["position"]) == grp]
        extra = len(members) - shape[grp]
        if extra > 0:
            drop = sorted((k for k in members if new[k]["source"] != "dfo"),
                          key=lambda k: (new[k]["source"] != "fill", usage(new[k]["player_id"] or new[k].get("_like"))))
            for k in drop[:extra]:
                new.pop(k)
        elif extra < 0:
            taken = {r["player_id"] for r in new.values()}
            partial = [(pid, pos, g) for g in EV_GROUPS if g not in complete
                       for pid, pos in version.groups.get(g, []) if pid is not None and group(pos) == grp]
            blocked = version.out | set(ruled_out)
            pool = [(pid, pos, g, "dfo") for pid, pos, g in partial if pid not in taken and pid not in blocked]
            pool += [(pid, None, None, "fill") for pid in candidates(grp) if pid not in taken and pid not in blocked
                     and pid not in {p for p, *_ in pool}]
            for pid, pos, g, source in pool[:-extra]:
                new[pid] = {"player_id": pid, "position": pos or ("D" if grp == "D" else "C"), "slot": g, "source": source,
                            "confidence": "medium" if source == "dfo" else "low", "issues": f"{note}; fills a {grp} slot"}
            for i in range(-extra - len(pool[:-extra])):
                new[f"placeholder:dfo:{grp}:{i}"] = {"player_id": None, "position": "D" if grp == "D" else "C",
                                                     "slot": "d3" if grp == "D" else "f4",
                                                     "source": "fill", "confidence": "low", "issues": f"{note}; placeholder"}
    units: dict[str, dict[int, int]] = {}
    for col, prefix in (("spp", "pp"), ("spk", "pk")):
        if f"{prefix}1" in version.complete:
            # Unit 2 first so a player listed on both keeps unit 1.
            units[col] = {pid: n for n in (2, 1) if f"{prefix}{n}" in version.complete
                          for pid, _ in version.groups[f"{prefix}{n}"] if pid is not None}
    return new, units


def project_team_game(ix: _Index, team: int, game_id: int, day: date, as_of: datetime) -> list[dict]:
    """Projected dressed skaters for one team-game (see the module docstring)."""
    games = ix.team_games.get(team, [])
    k = bisect.bisect_left(games, (day, -1)) - 1
    if k < 0:
        return []
    last_date, last_gid = games[k]
    base = ix.dressed[(last_gid, team)]
    out, avail, dtd = _latest_status(ix, team, as_of, day, last_date)
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

    version = _dfo_version(ix, team, as_of, last_date)
    units: dict[str, dict[int, int]] = {}
    if version is not None:
        lineup, units = _apply_dfo(lineup, version, usage, candidates, out)

    rows = []
    for key, r in lineup.items():
        f = dict(_form_before(ix, r["player_id"], day) or _form_before(ix, r.get("_like"), day) or {})
        slot = r.get("slot")
        if slot in SLOT_S5:
            f["s5"] = SLOT_S5[slot] if f.get("s5") is None else (1 - SLOT_WEIGHT) * f["s5"] + SLOT_WEIGHT * SLOT_S5[slot]
        unit = {}
        for c, groups in units.items():
            unit[c] = groups.get(r["player_id"])
            typical = UNIT_SHARE[c][unit[c]]
            f[c] = typical if f.get(c) is None else (1 - SLOT_WEIGHT) * f[c] + SLOT_WEIGHT * typical
        rows.append({
            "game_id": game_id, "team_id": team, "player_id": r["player_id"], "position": r["position"],
            **{c: f.get(c) for c in SHARES}, "slot": slot, "pp_unit": unit.get("spp"), "pk_unit": unit.get("spk"),
            "p_dressed": 1.0, "source": r["source"], "confidence": r["confidence"], "issues": r["issues"],
        })
    if version is not None:
        rows = _game_time_blend(rows, version, dtd, candidates)
    return rows


def _game_time_blend(rows: list[dict], version: DfoVersion, dtd: dict[int, Event], candidates) -> list[dict]:
    """Split a game-time decision's deployment :data:`GTD_P_DRESSED` / rest with the
    likeliest replacement at his position.

    A game-time decision is a player DailyFaceoff has in a line slot and on its injury list
    whom ESPN also lists day-to-day, or one DailyFaceoff flags as a game-time decision. Both
    rows carry ``p_dressed``; shares keep the team's totals, so the simulator prices the
    expected lineup.
    """
    taken = {r["player_id"] for r in rows}
    out = []
    for r in rows:
        pid = r["player_id"]
        is_gtd = pid is not None and ((pid in version.questionable and pid in dtd) or pid in version.gtd)
        if not is_gtd:
            out.append(r)
            continue
        cause = "DailyFaceoff game-time decision" if pid in version.gtd else f"DailyFaceoff injury list + ESPN day-to-day ({dtd[pid].until})"
        backup = next((c for c in candidates(group(r["position"])) if c not in taken), None)
        if backup is None:
            out.append({**r, "confidence": "medium", "issues": _join(r["issues"], f"game-time decision ({cause}); no backup")})
            continue
        taken.add(backup)
        p = GTD_P_DRESSED
        out.append({**r, **{c: None if r[c] is None else r[c] * p for c in SHARES}, "p_dressed": p, "confidence": "medium",
                    "issues": _join(r["issues"], f"game-time decision ({cause}); {p:.0%} with backup {backup}")})
        out.append({**r, "player_id": backup, **{c: None if r[c] is None else r[c] * (1 - p) for c in SHARES},
                    "pp_unit": None, "pk_unit": None, "p_dressed": 1 - p, "source": "gtd_backup", "confidence": "low",
                    "issues": f"backup for game-time decision {pid}"})
    return out


def _join(a: str | None, b: str) -> str:
    return f"{a}; {b}" if a else b


def project(store: Store, targets: pl.DataFrame, seasons: list[int], as_of: datetime | None = None,
            hist: pl.DataFrame | None = None, events: pl.DataFrame | None = None,
            dfo: pl.DataFrame | None = None) -> pl.DataFrame:
    """Projected deployment for ``targets`` (``game_id, team_id, game_date``).

    Args:
        store: S3 store.
        targets: team-games to project.
        seasons: seasons of history to load (include the one before the targets').
        as_of: what is known; default per target is 12:00 UTC on its game day (backtest).
        hist, events, dfo: preloaded :func:`deployment_history` / :func:`status_events` /
            :func:`load_dfo` (DailyFaceoff lines; pass an empty frame to ignore them).

    Returns:
        ``game_id, team_id, player_id, position, s5, spp, spk, sshot, is_d, source,
        confidence, issues``, shares scaled like :func:`nhl.sim.inputs.actual_deployment`.
    """
    hist = hist if hist is not None else deployment_history(store, seasons)
    if events is None:
        rosters = pl.concat([r for s in seasons if (r := store.get_parquet(keys.rosters(s))) is not None])
        events = status_events(store, seasons, rosters)
    if dfo is None:
        dfo = load_dfo(store, seasons)
    ix = _build_index(hist, events, dfo)
    rows = []
    for gid, team, day in targets.select("game_id", "team_id", "game_date").iter_rows():
        moment = as_of or morning(day)
        rows.extend(project_team_game(ix, team, gid, day, moment))
    if not rows:
        return pl.DataFrame()
    dep = pl.DataFrame(rows, schema_overrides={"player_id": pl.Int64, "slot": pl.String, "pp_unit": pl.Int8, "pk_unit": pl.Int8,
                                               "p_dressed": pl.Float64,
                                               **{c: pl.Float64 for c in SHARES}})
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
    """Per team-game: forwards, defensemen, shape and whether it's regular (12F6D or 11F7D).
    A game-time decision's backup (``p_dressed`` < 0.5) isn't counted."""
    if "p_dressed" in dep.columns:
        dep = dep.filter(pl.col("p_dressed").fill_null(1.0) >= 0.5)
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
