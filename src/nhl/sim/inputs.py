"""Per-game simulator inputs: goal and penalty rates for both teams in every state.

Rates are composed from the latest rating snapshot strictly before the game date
(``ratings/{date}/``), league constants (:mod:`nhl.sim.constants`), deployment and the
starting goalies. Team A versus team B:

* ``xg60_5v5[A]`` = league 5v5 xG/60 + Σ share·offence(A) + Σ share·defence(B) + A's
  home term (centred: ±home/2) + rest terms (A attacking, B defending) + coach terms;
* ``goals60_5v5[A]`` = that × league goals/xG × finishing(A) × goalie(B), where
  finishing(A) = exp(Σ shot share·shooter term + defenseman effect × (A's defenseman shot
  share − league)) and goalie(B) = exp(B's starter's term);
* PP and PK the same way with the ST ratings and PP/PK shares;
* short-handed, 4v4 and 3v3: league constants, scaled by the teams' relative 5v5 rates
  (and by finishing/goalie);
* penalties: league PP-opportunity rate × (A's taking ÷ league) × (B's drawing ÷ league)
  × the referee crew factor (:func:`referee_factors`); home and game-state factors are
  applied in the engine.

Deployment for the backtest (``actual`` lineups, as the roadmap's M4 bar specifies):
* 5v5 shares: from the game itself;
* PP/PK shares and shot shares: from the team's earlier games that season, among the
  players dressed for this one.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import numpy as np
import polars as pl

from nhl.reference.venues import schedule_context_key
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

STATES = ("5v5", "pp", "sh", "4v4", "3v3")


@dataclass
class SeasonInputs:
    """Rates for every game of a season, aligned arrays (index = game)."""

    games: pl.DataFrame  # game_id, game_date, season_type, home_team_id, away_team_id, home_score, away_score, last_period
    goals60: dict[str, np.ndarray]  # "{state}_{home|away}" -> rate per 60
    penalties60: dict[str, np.ndarray]  # "home" / "away" -> rate of taking a PP-creating penalty
    score_terms: np.ndarray  # (7 leads, 3 periods) EV score effect on goals/60, centred
    xg60_5v5: dict[str, np.ndarray]  # for diagnostics
    level: np.ndarray | None = None  # per-game league scoring level (already in goals60)
    # Team residual evidence to date (before each game): Σ(actual − rating-predicted) 5v5 xG
    # and 5v5 hours, for each side's offence and defence. See :func:`team_residuals`.
    team_res: dict[str, np.ndarray] | None = None


def snapshot_dates(store: Store) -> list[date]:
    """Dates with a stored rating snapshot."""
    days = {k.split("/")[1] for k in store.list_keys("ratings/") if k.count("/") == 2 and k[8:9].isdigit()}
    return sorted(date.fromisoformat(d) for d in days)


def _latest_before(days: list[date], day: date) -> date | None:
    earlier = [d for d in days if d < day]
    return earlier[-1] if earlier else None


def _shares(logs: pl.DataFrame, strength: str, value: str, alias: str, cumulative: bool) -> pl.DataFrame:
    """Per game-team-player share of ``value`` in ``strength``; ``cumulative`` = from earlier games."""
    part = logs.filter(pl.col("strength") == strength).select("game_id", "game_date", "team_id", "player_id", pl.col(value).cast(pl.Float64).alias("v"))
    if cumulative:
        part = part.sort("game_date", "game_id").with_columns(
            (pl.col("v").cum_sum().over("team_id", "player_id") - pl.col("v")).alias("v")
        )
    return part.with_columns((pl.col("v") / pl.col("v").sum().over("game_id", "team_id")).fill_nan(None).alias(alias)).drop("v")


def actual_deployment(store: Store, season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Deployment and starters as actually played (the M4 backtest's lineups).

    Returns:
        ``(deployment, starters)``. ``deployment``: ``game_id, team_id, player_id, position,
        s5, spp, spk, sshot, is_d`` with shares scaled to skaters on the ice (Σ s5 = 5,
        Σ spp = 5, Σ spk = 4 per team-game). ``starters``: ``game_id, team_id, starter``.
    """
    logs = store.read_parquet_required(keys.player_game_logs(season)).filter(pl.col("position") != "G")
    starts = store.read_parquet_required(keys.goalie_starts(season)).select("game_id", "team_id", "starter")

    # Deployment: 5v5 from the game; PP / PK / shots from earlier games, among those dressed.
    dressed = logs.filter(pl.col("strength") == "all").select("game_id", "team_id", "player_id", "position")
    s5 = _shares(logs, "5v5", "toi_s", "s5", cumulative=False)
    pp = _shares(logs, "PP", "toi_s", "spp", cumulative=True)
    pk = _shares(logs, "SH", "toi_s", "spk", cumulative=True)
    shot = _shares(logs, "all", "ixg", "sshot", cumulative=True)
    dep = (
        dressed.join(s5.drop("game_date"), on=["game_id", "team_id", "player_id"], how="left")
        .join(pp.drop("game_date"), on=["game_id", "team_id", "player_id"], how="left")
        .join(pk.drop("game_date"), on=["game_id", "team_id", "player_id"], how="left")
        .join(shot.drop("game_date"), on=["game_id", "team_id", "player_id"], how="left")
        .with_columns(pl.col("s5").fill_null(0.0))
        # No history yet (first games): fall back to 5v5 deployment.
        .with_columns(
            pl.when(pl.col("spp").is_null().all().over("game_id", "team_id")).then(pl.col("s5")).otherwise(pl.col("spp").fill_null(0.0)).alias("spp"),
            pl.when(pl.col("spk").is_null().all().over("game_id", "team_id")).then(pl.col("s5")).otherwise(pl.col("spk").fill_null(0.0)).alias("spk"),
            pl.when(pl.col("sshot").is_null().all().over("game_id", "team_id")).then(pl.col("s5")).otherwise(pl.col("sshot").fill_null(0.0)).alias("sshot"),
        )
        .with_columns(
            (pl.col("s5") * 5).alias("s5"), (pl.col("spp") * 5).alias("spp"), (pl.col("spk") * 4).alias("spk"),
            (pl.col("position") == "D").alias("is_d"),
        )
    )
    return dep, starts


def build_season(store: Store, season: int, constants: dict, snapshots: list[date] | None = None) -> SeasonInputs:
    """Inputs for every final game of ``season`` that has a prior rating snapshot (actual lineups)."""
    games = store.read_parquet_required(keys.GAMES).filter((pl.col("season") == season) & pl.col("is_final"))
    dep, starts = actual_deployment(store, season)
    return build_inputs(store, season, games, dep, starts, constants, snapshots)


def build_inputs(store: Store, season: int, games: pl.DataFrame, dep: pl.DataFrame, starts: pl.DataFrame,
                 constants: dict, snapshots: list[date] | None = None,
                 coaches: pl.DataFrame | None = None, history: pl.DataFrame | None = None) -> SeasonInputs:
    """Inputs for ``games`` of ``season`` from any deployment and starters.

    Shared by the backtest (actual lineups) and the pregame path (projected lineups, one
    call per starter pair). ``games`` need ``game_id, game_date, season_type, home_team_id,
    away_team_id`` (scores and ``last_period`` may be null for future games). ``dep`` and
    ``starts`` follow :func:`actual_deployment`. ``coaches`` (``game_id, is_home, coach_id``)
    defaults to the season's coach table. The scoring level and team residuals use only
    the season's games completed before each game's date. ``history`` is the
    :func:`rate_table` of the season's completed games with actual lineups, which the team
    residuals compare against; it defaults to the rate table of ``games`` themselves (the
    backtest case, where ``games`` are the completed games).
    """
    snapshots = snapshots if snapshots is not None else snapshot_dates(store)
    table = rate_table(store, season, games, dep, starts, constants, snapshots, coaches)
    hist = table if history is None else history
    completed = store.read_parquet_required(keys.GAMES).filter((pl.col("season") == season) & pl.col("is_final"))
    level = scoring_level(completed, table, constants)
    # Playoffs: lower scoring and a smaller home edge (point-in-time factors from earlier
    # playoffs); folded into the level so the simulator's calibration keeps it.
    playoff = (table["season_type"] == "P").to_numpy()
    level = level * np.where(playoff, constants.get("playoff_goal_factor", 1.0), 1.0)
    home_f = np.where(playoff, constants.get("playoff_home_factor", 1.0), 1.0)
    goals60 = {}
    for c in table.columns:
        if not c.startswith("g60_"):
            continue
        v = table[c].to_numpy()
        if not c.startswith("g60_en_"):
            v = v * level * (home_f if c.endswith("_home") else 1 / home_f)
        goals60[c] = v
    return SeasonInputs(
        games=table.select("game_id", "game_date", "season_type", "home_team_id", "away_team_id", "home_score", "away_score", "last_period"),
        goals60={k[4:]: v for k, v in goals60.items()},
        penalties60={s: table[f"pen60_{s}"].to_numpy() * table["ref_factor"].to_numpy() for s in ("home", "away")},
        score_terms=_score_terms(constants),
        xg60_5v5={s: table[f"xg60_5v5_{s}"].to_numpy() for s in ("home", "away")},
        level=level,
        team_res={**team_residuals(store, season, table, hist), **team_finishing_residuals(store, season, table, snapshots)},
    )


def rate_table(store: Store, season: int, games: pl.DataFrame, dep: pl.DataFrame, starts: pl.DataFrame,
               constants: dict, snapshots: list[date], coaches: pl.DataFrame | None = None) -> pl.DataFrame:
    """Per-game rates (before the league level and calibration) with the referee factor."""
    rest = store.read_parquet_required(schedule_context_key(season)).select(
        "game_id", "is_home",
        pl.when(pl.col("days_rest") == 1).then(pl.lit("b2b")).when(pl.col("days_rest") == 2).then(pl.lit("normal"))
        .otherwise(pl.lit("rested")).alias("rest"),
    )
    if coaches is None:
        coaches = store.read_parquet_required(keys.coaches(season)).select("game_id", "is_home", "coach_id")
    rows = []
    for snap, part in games.with_columns(
        pl.col("game_date").map_elements(lambda d: _latest_before(snapshots, d), return_dtype=pl.Date).alias("snapshot")
    ).filter(pl.col("snapshot").is_not_null()).partition_by("snapshot", as_dict=True).items():
        rows.append(_rates_for(store, snap[0], part, dep, starts, rest, coaches, constants))
    table = pl.concat(rows).sort("game_date", "game_id")
    refs = referee_factors(store, season)
    return table.join(refs, on="game_id", how="left").with_columns(pl.col("ref_factor").fill_null(1.0))


LEVEL_PRIOR_GAMES = 150
#: Games of league-average evidence each referee's penalty tendency is shrunk toward:
#: game-to-game noise variance (sd 1.93 PP/game) ÷ true referee variance (sd ~0.21), 2021-2026.
REF_PRIOR_GAMES = 86
REF_LOOKBACK = 3


def referee_factors(store: Store, season: int) -> pl.DataFrame:
    """Per game: the referee crew's multiplier on power-play opportunities (point-in-time).

    Each referee's deviation = (sum over his games of PP opportunities − that game's expected)
    ÷ (games + ``REF_PRIOR_GAMES``), over games strictly before the date (this and
    ``REF_LOOKBACK`` earlier seasons). The expectation is the game's season mean (season to
    date for the target season), so league-wide trends don't leak into referees. The crew factor is 1 + (sum of the two deviations) ÷ league mean.
    Linesmen show no effect beyond noise and are ignored.
    """
    start = int(str(season)[:4])
    seasons = [int(f"{y}{y + 1}") for y in range(start - REF_LOOKBACK, start + 1) if y >= 2010]
    officials = pl.concat([o for s_ in seasons if (o := store.get_parquet(keys.officials(s_))) is not None]).filter(
        pl.col("role") == "referee"
    )
    pp = pl.concat([store.read_parquet_required(keys.team_game_logs(s_)) for s_ in seasons], how="diagonal_relaxed").filter(
        pl.col("strength") == "all"
    ).group_by("game_id").agg(pl.col("pp_opportunities").sum().cast(pl.Float64).alias("pp"))
    games = store.read_parquet_required(keys.GAMES).select("game_id", "game_date", "season")
    # Each game's expectation: its season's mean PP opportunities per game; for the target
    # season, the mean of games before that date (point-in-time).
    gpp = pp.join(games, on="game_id")
    season_mean = gpp.group_by("season").agg(pl.col("pp").mean().alias("expected"))
    by_day = gpp.filter(pl.col("season") == season).group_by("game_date").agg(pl.col("pp").sum(), pl.len().alias("n")).sort("game_date")
    prior_mean = float(gpp.filter(pl.col("season") < season)["pp"].mean()) if gpp.filter(pl.col("season") < season).height else float(gpp["pp"].mean())
    to_date = by_day.with_columns(
        ((pl.col("pp").cum_sum() - pl.col("pp") + prior_mean * 100) / (pl.col("n").cum_sum() - pl.col("n") + 100)).alias("expected")
    ).select("game_date", "expected")
    expected = pl.concat([
        gpp.filter(pl.col("season") < season).join(season_mean, on="season").select("game_id", "expected"),
        gpp.filter(pl.col("season") == season).join(to_date, on="game_date").select("game_id", "expected"),
    ])
    ref_games = officials.select("game_id", "official_id").join(gpp, on="game_id").join(expected, on="game_id").with_columns(
        (pl.col("pp") - pl.col("expected")).alias("resid")
    )
    daily = ref_games.group_by("official_id", "game_date").agg(pl.col("resid").sum(), pl.len().alias("n")).sort("game_date")
    daily = daily.with_columns(
        (pl.col("resid").cum_sum().over("official_id") - pl.col("resid")).alias("resid_before"),
        (pl.col("n").cum_sum().over("official_id") - pl.col("n")).alias("n_before"),
    ).with_columns((pl.col("resid_before") / (pl.col("n_before") + REF_PRIOR_GAMES)).alias("dev"))
    league = prior_mean
    target = officials.join(games.filter(pl.col("season") == season), on="game_id").join(
        daily.select("official_id", "game_date", "dev"), on=["official_id", "game_date"], how="left"
    )
    return target.group_by("game_id").agg((1 + pl.col("dev").fill_null(0.0).sum() / league).alias("ref_factor"))


def sum_before(left: pl.DataFrame, daily: pl.DataFrame, cols: list[str], by: str | None = None) -> pl.DataFrame:
    """``left`` (row order kept) with ``cols`` summed over ``daily`` rows strictly before each
    ``game_date`` (per ``by`` if given). Works for dates with no row of their own (future games)."""
    group = [by] if by else []
    cum = daily.sort("game_date").with_columns(
        *[(pl.col(c).cum_sum().over(group) if by else pl.col(c).cum_sum()).alias(f"{c}_td") for c in cols]
    ).select(*group, "game_date", *[f"{c}_td" for c in cols])
    out = left.with_row_index("_row").sort("game_date").join_asof(
        cum, on="game_date", by=by, strategy="backward", allow_exact_matches=False, check_sortedness=False
    )
    return out.sort("_row").drop("_row").with_columns(pl.col(f"{c}_td").fill_null(0.0) for c in cols)


def scoring_level(games: pl.DataFrame, table: pl.DataFrame, constants: dict) -> np.ndarray:
    """Per game: league scoring level relative to the lookback the constants came from.

    level = (season-to-date goals/game, shrunk toward last season's with
    ``LEVEL_PRIOR_GAMES`` games) ÷ lookback goals/game. Only games completed before each
    game's date count, so it's point-in-time; it removes the lag of multi-season constants
    when league scoring trends (2016-2019 rose ~7%).
    """
    done = games.with_columns(
        (pl.col("home_score") + pl.col("away_score")
         - ((pl.col("season_type") == "R") & (pl.col("last_period") == 5)).cast(pl.Int16)).alias("g")
    ).group_by("game_date").agg(pl.col("g").sum().cast(pl.Float64).alias("goals"), pl.len().cast(pl.Float64).alias("n"))
    prior = constants["goals_per_game_last"]
    lvl = sum_before(table.select("game_date"), done, ["goals", "n"]).with_columns(
        ((pl.col("goals_td") + prior * LEVEL_PRIOR_GAMES) / (pl.col("n_td") + LEVEL_PRIOR_GAMES)
         / constants["goals_per_game_lookback"]).alias("level")
    )
    return lvl["level"].to_numpy()


def team_residuals(store: Store, season: int, table: pl.DataFrame, history: pl.DataFrame | None = None) -> dict[str, np.ndarray]:
    """Running team-level residuals: what the team did at 5v5 beyond its player ratings.

    For each earlier game of the season, offence residual = actual 5v5 xGF − the rating-based
    prediction for that game (``xg60_5v5`` × 5v5 hours); defence residual = actual 5v5 xGA −
    the opponent's prediction. Summed over the team's games strictly before each game's
    date, with the 5v5 hours. The engine turns them into a shrunk team term. Captures
    system, chemistry and form not in the player ratings.

    Predictions come from ``history`` (the rate table of completed games; default ``table``).

    Returns:
        ``{"off_home", "def_home", "off_away", "def_away", "hours_home", "hours_away"}``
        aligned with ``table``.
    """
    history = table if history is None else history
    logs = store.read_parquet_required(keys.team_game_logs(season)).filter(pl.col("strength") == "5v5").select(
        "game_id", "team_id", "xgf", "xga", (pl.col("toi_s") / 3600).alias("h")
    )
    pred = pl.concat([
        history.select("game_id", "game_date", pl.col("home_team_id").alias("team_id"),
                       pl.col("xg60_5v5_home").alias("pf"), pl.col("xg60_5v5_away").alias("pa")),
        history.select("game_id", "game_date", pl.col("away_team_id").alias("team_id"),
                       pl.col("xg60_5v5_away").alias("pf"), pl.col("xg60_5v5_home").alias("pa")),
    ])
    r = pred.join(logs, on=["game_id", "team_id"], how="inner").with_columns(
        (pl.col("xgf") - pl.col("pf") * pl.col("h")).alias("ro"), (pl.col("xga") - pl.col("pa") * pl.col("h")).alias("rd")
    )
    daily = r.group_by("team_id", "game_date").agg(pl.col("ro", "rd", "h").sum())
    out = {}
    for side in ("home", "away"):
        j = sum_before(table.select("game_date", pl.col(f"{side}_team_id").alias("team_id")), daily, ["ro", "rd", "h"], by="team_id")
        out[f"off_{side}"], out[f"def_{side}"], out[f"hours_{side}"] = (
            j["ro_td"].to_numpy(), j["rd_td"].to_numpy(), j["h_td"].to_numpy()
        )
    return out


def team_finishing_residuals(store: Store, season: int, table: pl.DataFrame, snapshots: list[date]) -> dict[str, np.ndarray]:
    """Running team residuals of 5v5 **goals** against talent-adjusted expected goals.

    Each past shot's expected value is the finishing model's probability (xG + intercept +
    defenseman effect + shooter + goalie terms) from the latest rating snapshot before
    that game, so shooter and goalie form already known then isn't counted again. Summed
    per team over games strictly before each game's date:
    * ``fin_off_{side}``: goals for − expected (team finishing beyond its shooters);
    * ``fin_def_{side}``: goals against − expected (team defence beyond xG and its goalie);
    * ``fin_n_{side}``: expected goals (for + against) / 2, the evidence weight.
    Split-half persistence (2012-2025, after full-season player talent): offence about 0,
    defence 0.13 (0.12 road-only).
    """
    from nhl.ratings import finishing as fin_model

    shots = fin_model.load_shots(store, season).join(
        store.read_parquet_required(keys.xg_predictions(season)).select("game_id", "event_idx", "strength_state"),
        on=["game_id", "event_idx"],
    ).filter(pl.col("strength_state") == "5v5")
    ev = store.read_parquet_required(keys.events(season)).select("game_id", "event_idx", "event_team_id", "home_team_id", "away_team_id")
    shots = shots.join(ev, on=["game_id", "event_idx"]).with_columns(
        pl.col("game_date").map_elements(lambda d: _latest_before(snapshots, d), return_dtype=pl.Date).alias("snap")
    )
    parts = []
    for (snap,), part in shots.filter(pl.col("snap").is_not_null()).partition_by("snap", as_dict=True).items():
        terms = store.read_parquet_required(f"ratings/{snap.isoformat()}/finishing.parquet")
        if "intercept" not in terms.columns:
            continue
        fit = fin_model.Fit(
            intercept=float(terms["intercept"][0]),
            terms=terms.select("role", "player_id", "mean", (1 / pl.col("sd") ** 2).alias("precision")),
            defense=float(terms["defense"][0]),
        )
        parts.append(part.with_columns(pl.Series("p", fin_model.predict(fit, part))))
    if not parts:
        return {}
    d = pl.concat(parts).with_columns(
        pl.when(pl.col("event_team_id") == pl.col("home_team_id")).then(pl.col("away_team_id")).otherwise(pl.col("home_team_id")).alias("opp"),
        (pl.col("is_goal") - pl.col("p")).alias("r"),
    )
    off = d.group_by(pl.col("event_team_id").alias("team_id"), "game_date").agg(pl.col("r").sum().alias("ro"), pl.col("p").sum().alias("po"))
    dfn = d.group_by(pl.col("opp").alias("team_id"), "game_date").agg(pl.col("r").sum().alias("rd"), pl.col("p").sum().alias("pd"))
    daily = off.join(dfn, on=["team_id", "game_date"], how="full", coalesce=True).with_columns(
        pl.col("ro", "po", "rd", "pd").fill_null(0.0)
    )
    out = {}
    for side in ("home", "away"):
        j = sum_before(table.select("game_date", pl.col(f"{side}_team_id").alias("team_id")), daily, ["ro", "rd", "po", "pd"], by="team_id")
        out[f"fin_off_{side}"] = j["ro_td"].to_numpy()
        out[f"fin_def_{side}"] = j["rd_td"].to_numpy()
        out[f"fin_xoff_{side}"] = j["po_td"].to_numpy()
        out[f"fin_xdef_{side}"] = j["pd_td"].to_numpy()
    return out


def _score_terms(constants: dict) -> np.ndarray:
    """EV score effects (xG/60 → goals/60) by lead −3..+3 and period, centred on time shares."""
    terms = constants["ev_context"]
    out = np.zeros((7, 3))
    for i, lead in enumerate(range(-3, 4)):
        for p in range(3):
            out[i, p] = terms.get(f"score:{lead}:p{p + 1}", 0.0)
    shares = np.array(constants.get("score_time_share", np.full((7, 3), 1 / 21).tolist()))
    return (out - (out * shares).sum()) * constants["goals_per_xg_5v5"]


def _rates_for(store: Store, snap: date, games: pl.DataFrame, dep: pl.DataFrame, starts: pl.DataFrame,
               rest: pl.DataFrame, coaches: pl.DataFrame, c: dict) -> pl.DataFrame:
    """Rates for the games that use snapshot ``snap``."""
    base = f"ratings/{snap.isoformat()}/"
    ev = store.read_parquet_required(base + "ev.parquet").select("player_id", "side", "mean")
    st = store.read_parquet_required(base + "st.parquet").select("player_id", "side", "mean")
    ctx = c["ev_context"]  # previous season's full-season context terms (point-in-time)
    fin = store.read_parquet_required(base + "finishing.parquet")
    pen = store.read_parquet_required(base + "penalties.parquet")
    defense = float(fin["defense"][0]) if "defense" in fin.columns and fin.height else 0.0
    shooters = fin.filter(pl.col("role") == "shooter").select("player_id", pl.col("mean").alias("shoot"))
    goalies = fin.filter(pl.col("role") == "goalie").select(pl.col("player_id").alias("starter"), pl.col("mean").alias("goalie"))
    pen_w = pen.pivot(on="kind", index="player_id", values="rate")
    league_taken = float((pen.filter(pl.col("kind") == "taken")["rate"]).mean())
    league_drawn = float((pen.filter(pl.col("kind") == "drawn")["rate"]).mean())

    gids = games["game_id"].implode()
    d = (
        dep.filter(pl.col("game_id").is_in(gids))
        .join(ev.filter(pl.col("side") == "O").select("player_id", pl.col("mean").alias("o_ev")), on="player_id", how="left")
        .join(ev.filter(pl.col("side") == "D").select("player_id", pl.col("mean").alias("d_ev")), on="player_id", how="left")
        .join(st.filter(pl.col("side") == "O").select("player_id", pl.col("mean").alias("o_st")), on="player_id", how="left")
        .join(st.filter(pl.col("side") == "D").select("player_id", pl.col("mean").alias("d_st")), on="player_id", how="left")
        .join(shooters, on="player_id", how="left")
        .join(pen_w, on="player_id", how="left")
        .with_columns(pl.col("o_ev", "d_ev", "o_st", "d_st", "shoot").fill_null(0.0),
                      pl.col("taken").fill_null(league_taken), pl.col("drawn").fill_null(league_drawn))
    )
    team = d.group_by("game_id", "team_id").agg(
        (pl.col("s5") * pl.col("o_ev")).sum().alias("off5"), (pl.col("s5") * pl.col("d_ev")).sum().alias("def5"),
        (pl.col("spp") * pl.col("o_st")).sum().alias("offpp"), (pl.col("spk") * pl.col("d_st")).sum().alias("defpk"),
        (pl.col("sshot") * pl.col("shoot")).sum().alias("shoot"), (pl.col("sshot") * pl.col("is_d")).sum().alias("dshare"),
        ((pl.col("s5") * pl.col("taken")).sum() / (pl.col("s5").sum() * league_taken)).alias("take_f"),
        ((pl.col("s5") * pl.col("drawn")).sum() / (pl.col("s5").sum() * league_drawn)).alias("draw_f"),
    ).join(starts, on=["game_id", "team_id"], how="left").join(goalies, on="starter", how="left").with_columns(
        pl.col("goalie").fill_null(0.0)
    )
    league_dshare = float(team["dshare"].mean())
    g = games
    for side, opp in (("home", "away"), ("away", "home")):
        t = team.rename({c_: f"{c_}_{side}" for c_ in team.columns if c_ not in ("game_id",)})
        g = g.join(t, left_on=["game_id", f"{side}_team_id"], right_on=["game_id", f"team_id_{side}"], how="left")
    g = g.join(rest.filter(pl.col("is_home")).select("game_id", pl.col("rest").alias("rest_home")), on="game_id", how="left").join(
        rest.filter(~pl.col("is_home")).select("game_id", pl.col("rest").alias("rest_away")), on="game_id", how="left"
    ).join(coaches.filter(pl.col("is_home")).select("game_id", pl.col("coach_id").alias("coach_home")), on="game_id", how="left").join(
        coaches.filter(~pl.col("is_home")).select("game_id", pl.col("coach_id").alias("coach_away")), on="game_id", how="left"
    )

    home_term = ctx.get("home", 0.0)
    out = {}
    for side, opp in (("home", "away"), ("away", "home")):
        att_rest = g[f"rest_{side}"].to_list()
        def_rest = g[f"rest_{opp}"].to_list()
        rest_term = np.array([
            ctx.get("rest:att_b2b", 0.0) * (a == "b2b") + ctx.get("rest:att_rested", 0.0) * (a in ("rested", None))
            + ctx.get("rest:def_b2b", 0.0) * (b == "b2b") + ctx.get("rest:def_rested", 0.0) * (b in ("rested", None))
            for a, b in zip(att_rest, def_rest)
        ])
        coach_term = np.array([
            ctx.get(f"coach:O:{a}", 0.0) + ctx.get(f"coach:D:{b}", 0.0)
            for a, b in zip(g[f"coach_{side}"].to_list(), g[f"coach_{opp}"].to_list())
        ])
        xg5 = (c["xg60_5v5"] + g[f"off5_{side}"].fill_null(0).to_numpy() + g[f"def5_{opp}"].fill_null(0).to_numpy()
               + (home_term / 2) * (1 if side == "home" else -1) + rest_term + coach_term)
        xg5 = np.clip(xg5, 0.5, None)
        finish = np.exp(g[f"shoot_{side}"].fill_null(0).to_numpy() + defense * (g[f"dshare_{side}"].fill_null(league_dshare).to_numpy() - league_dshare))
        goalie = np.exp(g[f"goalie_{opp}"].fill_null(0).to_numpy())
        talent = finish * goalie
        xgpp = np.clip(c["xg60_pp"] + g[f"offpp_{side}"].fill_null(0).to_numpy() + g[f"defpk_{opp}"].fill_null(0).to_numpy(), 1.0, None)
        strength = xg5 / c["xg60_5v5"]
        out[f"xg60_5v5_{side}"] = xg5
        out[f"g60_5v5_{side}"] = xg5 * c["goals_per_xg_5v5"] * talent
        out[f"g60_pp_{side}"] = xgpp * c["goals_per_xg_pp"] * talent
        out[f"g60_sh_{side}"] = c["goals60_sh"] * strength * talent
        out[f"g60_4v4_{side}"] = c["goals60_4v4"] * strength * talent
        out[f"g60_3v3_{side}"] = c["goals60_3v3"] * strength * talent
        out[f"g60_en_own_{side}"] = np.full(g.height, c["goals60_en_own"])
        out[f"g60_en_opp_{side}"] = np.full(g.height, c["goals60_en_opp"])
        out[f"pen60_{side}"] = c["penalty_rate60"] * g[f"take_f_{side}"].fill_null(1.0).to_numpy() * g[f"draw_f_{opp}"].fill_null(1.0).to_numpy()
    return g.select("game_id", "game_date", "season_type", "home_team_id", "away_team_id", "home_score", "away_score", "last_period").with_columns(
        pl.lit(snap).alias("snapshot"), *[pl.Series(k, v) for k, v in out.items()]
    )
