"""League constants for the game simulator, estimated point-in-time.

Constants for season *s* use only the ``LOOKBACK`` seasons before it (regular season and
playoffs), so a backtest of *s* never sees *s*. Rates are per team per 60 minutes of the
state.

* ``xg60`` / ``goals_per_xg``: league xG rate and goals per xG in 5v5, PP (5v4), and for
  short-handed teams; 4v4 and 3v3 goal rates (from regular-season overtime); goals for
  and against with a net empty.
* ``pull_seconds``: per trailing deficit (1, 2, 3+), quantiles of the time remaining in
  regulation when a trailing team first pulled its goalie (games where it never did are
  ``never_share``).
* ``penalty_rate60``: PP-creating penalties per team per 60; ``penalty_mix``: shares of
  2, 4 and 5 minutes.
* ``shootout_home_win``: home team's shootout win rate.
"""

from __future__ import annotations

import json
import logging

import numpy as np
import polars as pl

from nhl import config
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

LOOKBACK = 3
PULL_QUANTILES = np.linspace(0.0, 1.0, 21)
REGULATION_S = 3600


def constants_key(season: int) -> str:
    """Stored constants for simulating ``season``."""
    return f"models/sim/constants/{season}.json"


def _rates(stints: pl.DataFrame, regular_ot: pl.DataFrame) -> dict[str, float]:
    """xG and goal rates by state from stints (both sides).

    4v4 and 3v3 come from regular-season overtime (``regular_ot``), where nearly all of
    that hockey is played; regulation 3v3 is a few odd seconds, partly shift-chart noise.
    """
    hg, ag = pl.col("home_goalie").is_not_null(), pl.col("away_goalie").is_not_null()
    hn, an = pl.col("home_n"), pl.col("away_n")
    regulation = stints.filter(pl.col("valid_personnel") & (pl.col("period") <= 3))
    overtime = regular_ot.filter(pl.col("valid_personnel"))

    def per_team(cond_home: pl.Expr, cond_away: pl.Expr, what: str, s: pl.DataFrame) -> float:
        h = s.filter(cond_home).select(pl.col(f"home_{what}").sum(), pl.col("duration_s").sum())
        a = s.filter(cond_away).select(pl.col(f"away_{what}").sum(), pl.col("duration_s").sum())
        num = (h.item(0, 0) or 0) + (a.item(0, 0) or 0)
        den = (h.item(0, 1) or 0) + (a.item(0, 1) or 0)
        return float(num * 3600 / den) if den else float("nan")

    both = hg & ag
    out = {}
    for name, ch, ca, src in (
        ("5v5", both & (hn == 5) & (an == 5), both & (hn == 5) & (an == 5), regulation),
        ("pp", both & (hn == 5) & (an == 4), both & (an == 5) & (hn == 4), regulation),
        ("sh", both & (hn == 4) & (an == 5), both & (an == 4) & (hn == 5), regulation),
        ("4v4", both & (hn == 4) & (an == 4), both & (hn == 4) & (an == 4), overtime),
        ("3v3", both & (hn == 3) & (an == 3), both & (hn == 3) & (an == 3), overtime),
        # Scoring with own net empty (extra attacker) and into the opponent's empty net.
        ("en_own", ~hg & ag, ~ag & hg, regulation),
        ("en_opp", hg & ~ag, ag & ~hg, regulation),
    ):
        out[f"goals60_{name}"] = per_team(ch, ca, "gf", src)
        out[f"xg60_{name}"] = per_team(ch, ca, "xgf", src)
    for name in ("5v5", "pp", "sh"):
        out[f"goals_per_xg_{name}"] = out[f"goals60_{name}"] / out[f"xg60_{name}"]
    return out


def _score_time_share(stints: pl.DataFrame) -> list[list[float]]:
    """Share of 5v5 team-time by the team's lead (−3..+3) and period (1-3)."""
    s = stints.filter(
        pl.col("valid_personnel") & (pl.col("period") <= 3) & (pl.col("home_n") == 5) & (pl.col("away_n") == 5)
        & pl.col("home_goalie").is_not_null() & pl.col("away_goalie").is_not_null()
    )
    lead = (pl.col("home_score").cast(pl.Int16) - pl.col("away_score").cast(pl.Int16)).clip(-3, 3)
    both = pl.concat([
        s.select(lead.alias("lead"), "period", "duration_s"),
        s.select((-lead).alias("lead"), "period", "duration_s"),
    ])
    t = both.group_by("lead", "period").agg(pl.col("duration_s").sum())
    total = t["duration_s"].sum()
    grid = [[0.0] * 3 for _ in range(7)]
    for lead_v, period, dur in t.iter_rows():
        grid[int(lead_v) + 3][int(period) - 1] = float(dur / total)
    return grid


#: 5v5 game-state buckets for score effects: periods 1 and 2, then the 3rd by seconds remaining.
STATE_BUCKETS = ("p1", "p2", "p3_20_10", "p3_10_5", "p3_5_2", "p3_2_0")
STATE_PRIOR_GOALS = 30.0


def _state_bucket(period: pl.Expr, remaining: pl.Expr) -> pl.Expr:
    return (
        pl.when(period == 1).then(0).when(period == 2).then(1)
        .when(remaining > 600).then(2).when(remaining > 300).then(3).when(remaining > 120).then(4).otherwise(5)
    )


def _score_state_multipliers(stints: pl.DataFrame, team_logs: pl.DataFrame) -> list[list[float]]:
    """5v5 goal-rate multipliers by the team's lead (−2..+2) and game-state bucket.

    Exposure controls for team quality: team's season 5v5 goals/60 × opponent's season 5v5
    goals-against/60 ÷ league, times the time in the state. Each cell is shrunk toward 1
    with ``STATE_PRIOR_GOALS`` expected goals. Late in tied games scoring drops to ~0.7×
    (teams play for the point), which is what produces overtime.
    """
    five = team_logs.filter(pl.col("strength") == "5v5").join(
        stints.select("game_id", "season").unique(), on="game_id", how="semi"
    )
    games_season = stints.select("game_id", "season").unique()
    rates = five.join(games_season, on="game_id").group_by("season", "team_id").agg(
        (pl.col("gf").sum() / (pl.col("toi_s").sum() / 3600)).alias("gf60"),
        (pl.col("ga").sum() / (pl.col("toi_s").sum() / 3600)).alias("ga60"),
    )
    league = float(five["gf"].sum() / (five["toi_s"].sum() / 3600))
    s = stints.filter(
        pl.col("valid_personnel") & (pl.col("period") <= 3) & (pl.col("home_n") == 5) & (pl.col("away_n") == 5)
        & pl.col("home_goalie").is_not_null() & pl.col("away_goalie").is_not_null()
    ).with_columns(_state_bucket(pl.col("period"), REGULATION_S - pl.col("start_s")).alias("bucket"))
    lead = (pl.col("home_score").cast(pl.Int16) - pl.col("away_score").cast(pl.Int16)).clip(-2, 2)
    parts = []
    for own, opp, sign in (("home", "away", 1), ("away", "home", -1)):
        part = s.select(
            "season", "bucket", (lead * sign).alias("lead"), pl.col(f"{own}_gf").alias("g"), "duration_s",
            pl.col(f"{own}_team_id").alias("team_id"), pl.col(f"{opp}_team_id").alias("opp_id"),
        ).join(rates.select("season", "team_id", "gf60"), on=["season", "team_id"]).join(
            rates.select("season", pl.col("team_id").alias("opp_id"), "ga60"), on=["season", "opp_id"]
        )
        parts.append(part.with_columns((pl.col("gf60") * pl.col("ga60") / league * pl.col("duration_s") / 3600).alias("e")))
    t = pl.concat(parts).group_by("lead", "bucket").agg(pl.col("g").sum(), pl.col("e").sum())
    grid = [[1.0] * len(STATE_BUCKETS) for _ in range(5)]
    weight = [[0.0] * len(STATE_BUCKETS) for _ in range(5)]
    for lead_v, bucket, g, e in t.iter_rows():
        grid[int(lead_v) + 2][int(bucket)] = float((g + STATE_PRIOR_GOALS) / (e + STATE_PRIOR_GOALS))
        weight[int(lead_v) + 2][int(bucket)] = float(e)
    # Normalise to an exposure-weighted mean of 1: the grid reshapes *when* goals come
    # (by score and time), not how many; the scoring level is set elsewhere.
    total = sum(sum(row) for row in weight)
    mean = sum(grid[i][j] * weight[i][j] for i in range(5) for j in range(len(STATE_BUCKETS))) / total
    return [[v / mean for v in row] for row in grid]


def _previous_context(store: Store, season: int) -> dict[str, float]:
    """EV context terms (home, rest, score, coaches, …) from the last rating snapshot of the
    previous season: fully estimated, and known before ``season`` starts. In-season
    snapshots fit context from that season's games only, so early ones are near zero."""
    start = int(str(season)[:4])
    days = sorted({k.split("/")[1] for k in store.list_keys("ratings/") if k.count("/") == 2 and k[8:9].isdigit()})
    prev = [d for d in days if f"{start - 1}-09-01" <= d < f"{start}-09-01"]
    if not prev:
        return {}
    ctx = store.read_parquet_required(f"ratings/{prev[-1]}/context_ev.parquet")
    return {t: float(m) for t, m in ctx.select("term", "mean").iter_rows()}


PULL_WINDOW_S = 600
PULL_BUCKET_S = 15


def _pull_hazard(stints: pl.DataFrame, step_s: int = 5) -> dict[str, list[float]]:
    """Per-step probability that a trailing team with its goalie in pulls him, by deficit
    (1, 2, 3+) and time remaining in regulation (``PULL_BUCKET_S`` buckets over the last
    ``PULL_WINDOW_S`` seconds, latest first)."""
    games = stints.filter(pl.col("period") == 3).select("game_id").unique()
    times = pl.DataFrame({"t": list(range(REGULATION_S - PULL_WINDOW_S, REGULATION_S, step_s))}, schema={"t": pl.Int32})
    grid = games.join(times, how="cross").sort("t")
    state = grid.join_asof(  # noqa: sortedness is guaranteed by the sorts above
        stints.filter(pl.col("period") == 3).select("game_id", "start_s", "home_goalie", "away_goalie", "home_score", "away_score").sort("start_s"),
        left_on="t", right_on="start_s", by="game_id", strategy="backward",
    ).sort("game_id", "t")
    rows = []
    for own, opp in (("home", "away"), ("away", "home")):
        part = state.select(
            "game_id", "t",
            (pl.col(f"{opp}_score") - pl.col(f"{own}_score")).alias("deficit"),
            pl.col(f"{own}_goalie").is_null().alias("out"),
        ).with_columns(
            # A pull must last: out at both of the next two steps (shift-chart gaps of a
            # second or two at a goalie's shift boundary otherwise look like pulls).
            (pl.col("out").shift(-1) & pl.col("out").shift(-2)).over("game_id").alias("out_next"),
            pl.col("deficit").shift(-1).over("game_id").alias("deficit_next"),
            # Only the first pull at each deficit: after a pull the goalie often returns for
            # a defensive-zone faceoff and goes out again; counting re-pulls inflates it.
            (pl.col("out") & (pl.col("deficit") >= 1)).cum_max().over("game_id", "deficit").alias("_pulled_before"),
        )
        rows.append(part.filter(~pl.col("_pulled_before")))
    risk = pl.concat(rows).filter(
        (pl.col("deficit") >= 1) & ~pl.col("out") & pl.col("out_next").is_not_null() & (pl.col("deficit_next") == pl.col("deficit"))
    ).with_columns(
        pl.col("deficit").clip(1, 3),
        ((REGULATION_S - pl.col("t") - 1) // PULL_BUCKET_S).alias("bucket"),
    )
    hz = risk.group_by("deficit", "bucket").agg(pl.col("out_next").mean().alias("h"), pl.len().alias("n"))
    n_buckets = PULL_WINDOW_S // PULL_BUCKET_S
    out = {}
    for d in (1, 2, 3):
        lookup = {b: h for _, b, h, _n in hz.filter(pl.col("deficit") == d).iter_rows()}
        out[str(d)] = [float(lookup.get(b, 0.0)) for b in range(n_buckets)]
    return out


def _pulls(stints: pl.DataFrame) -> dict[str, dict]:
    """When trailing teams pull the goalie late in regulation, by deficit."""
    late = stints.filter((pl.col("period") == 3) & (pl.col("end_s") > REGULATION_S - 600))
    sides = []
    for own, opp in (("home", "away"), ("away", "home")):
        sides.append(late.select(
            "game_id", "start_s",
            (pl.col(f"{opp}_score") - pl.col(f"{own}_score")).alias("deficit"),
            pl.col(f"{own}_goalie").is_null().alias("pulled"),
            pl.lit(own).alias("side"),
        ))
    rows = pl.concat(sides).filter(pl.col("deficit") >= 1).with_columns(pl.col("deficit").clip(1, 3))
    out = {}
    for d in (1, 2, 3):
        part = rows.filter(pl.col("deficit") == d)
        first = part.filter(pl.col("pulled") & (pl.col("start_s") >= REGULATION_S - 600)).group_by("game_id", "side").agg(
            pl.col("start_s").min()
        )
        trailing = part.select("game_id", "side").unique().height
        remaining = (REGULATION_S - first["start_s"]).to_numpy()
        out[str(d)] = {
            "quantiles": np.quantile(remaining, PULL_QUANTILES).tolist() if len(remaining) else [0.0] * len(PULL_QUANTILES),
            "never_share": float(1 - first.height / trailing) if trailing else 1.0,
        }
    return out


def _penalties(events: pl.DataFrame, team_logs: pl.DataFrame) -> dict[str, object]:
    """Power-play opportunities per team per 60, and the length mix of PP-creating calls.

    Fighting majors offset each other and create no power play, so they're excluded from
    the mix; the rate comes from actual power-play opportunities (M2 team logs).
    """
    pen = events.filter(
        (pl.col("event_type") == "PENALTY") & pl.col("penalty_minutes").is_in([2, 4, 5])
        & (pl.col("penalty_type") != "fighting")
    )
    minutes = pen["penalty_minutes"].value_counts()
    total = minutes["count"].sum()
    mix = {str(m): float(c / total) for m, c in minutes.iter_rows()}
    all_rows = team_logs.filter(pl.col("strength") == "all")
    rate = float(all_rows["pp_opportunities"].sum() / (all_rows["toi_s"].sum() / 3600))
    return {"penalty_rate60": rate, "penalty_mix": mix}


PLAYOFF_LOOKBACK = 5
#: Hours of the regular-season pattern each playoff penalty cell is shrunk toward.
PLAYOFF_PENALTY_PRIOR_H = 150.0


def _penalty_states(stints: pl.DataFrame, catalog: pl.DataFrame) -> dict[str, object]:
    """When power plays start, as multipliers on a team's base rate of taking one.

    A **power-play start** is a stint beginning at a faceoff where a team is short-handed
    (both goalies in) after a stint where it wasn't. It is credited to the short-handed team,
    in the state of the previous stint: period, that team's lead (−2..+2), home/away and
    season type.

    Returns:
        * ``penalty_home_factor``: the home team's multiplier (the away team's is its inverse);
        * ``penalty_state_factor``: ``{"R"|"P": [[factor per lead −2..+2] per period 1-3]}``.

        Factors are relative to the overall time-weighted rate, so they average to 1. Playoff
        cells are shrunk toward the regular-season cell × the overall playoff ratio with
        ``PLAYOFF_PENALTY_PRIOR_H`` hours. (2010-2026: playoff calls run 1.16× regular overall
        but 0.82× in tied 3rd periods.)
    """
    s = stints.filter(pl.col("period") <= 3).join(catalog.select("game_id", "season_type"), on="game_id").sort("game_id", "stint_id")
    goalies = pl.col("home_goalie").is_not_null() & pl.col("away_goalie").is_not_null()
    rows = []
    for own, opp in (("home", "away"), ("away", "home")):
        short = goalies & (pl.col(f"{own}_n") < pl.col(f"{opp}_n"))
        part = s.with_columns(short.alias("_short")).with_columns(
            pl.col("_short").shift(1).over("game_id").fill_null(False).alias("_was_short"),
            (pl.col(f"{own}_score").cast(pl.Int16) - pl.col(f"{opp}_score").cast(pl.Int16)).clip(-2, 2).alias("lead"),
        )
        starts = part.filter(pl.col("_short") & ~pl.col("_was_short") & (pl.col("start_type") != "on_the_fly"))
        rows.append((
            starts.group_by("season_type", "period", "lead").len().with_columns(pl.lit(own == "home").alias("home")),
            part.group_by("season_type", "period", "lead").agg((pl.col("duration_s").sum() / 3600).alias("hours")).with_columns(pl.lit(own == "home").alias("home")),
        ))
    n = pl.concat([a for a, _ in rows]).group_by("season_type", "period", "lead", "home").agg(pl.col("len").sum())
    h = pl.concat([b for _, b in rows]).group_by("season_type", "period", "lead", "home").agg(pl.col("hours").sum())
    t = h.join(n, on=["season_type", "period", "lead", "home"], how="left").with_columns(pl.col("len").fill_null(0))

    home = t.group_by("home").agg(pl.col("len").sum() / pl.col("hours").sum())
    rate_home = float(home.filter(pl.col("home"))["len"].item())
    rate_away = float(home.filter(~pl.col("home"))["len"].item())
    home_factor = float(np.sqrt(rate_home / rate_away))

    cells = t.group_by("season_type", "period", "lead").agg(pl.col("len").sum(), pl.col("hours").sum())
    overall = {st: float(cells.filter(pl.col("season_type") == st)["len"].sum() / cells.filter(pl.col("season_type") == st)["hours"].sum())
               for st in ("R", "P") if cells.filter(pl.col("season_type") == st).height}
    factor: dict[str, list[list[float]]] = {}
    for st in ("R", "P"):
        grid = []
        for period in (1, 2, 3):
            row = []
            for lead in range(-2, 3):
                c = cells.filter((pl.col("season_type") == st) & (pl.col("period") == period) & (pl.col("lead") == lead))
                r_cell = cells.filter((pl.col("season_type") == "R") & (pl.col("period") == period) & (pl.col("lead") == lead))
                r_rate = float(r_cell["len"].sum() / max(r_cell["hours"].sum(), 1e-9)) / overall["R"]
                if st == "R" or "P" not in overall:
                    row.append(r_rate)
                    continue
                prior = r_rate * overall["P"] / overall["R"]
                n_p, h_p = float(c["len"].sum()), float(c["hours"].sum())
                row.append((n_p / overall["R"] + prior * PLAYOFF_PENALTY_PRIOR_H) / (h_p + PLAYOFF_PENALTY_PRIOR_H))
            grid.append(row)
        factor[st] = grid
    # Normalise the regular-season grid to average 1 over regular-season time.
    rt = cells.filter(pl.col("season_type") == "R")
    weights = {(p, l): hrs for _, p, l, _n, hrs in rt.iter_rows()}
    total = sum(weights.values())
    mean_r = sum(factor["R"][p - 1][l + 2] * w for (p, l), w in weights.items()) / total
    for st in factor:
        factor[st] = [[v / mean_r for v in row] for row in factor[st]]
    return {"penalty_home_factor": home_factor, "penalty_state_factor": factor}


def _playoff_factors(store: Store, start: int) -> dict[str, float]:
    """Playoff adjustments from the ``PLAYOFF_LOOKBACK`` seasons before ``start``.

    * ``playoff_goal_factor``: playoff ÷ regular-season goals per game (shootout winners
      excluded);
    * ``playoff_home_factor``: the playoff home/away goal ratio ÷ the regular-season one,
      as a per-side multiplier (home × f, away ÷ f).

    Power-play opportunities per 60 are about the same in the playoffs (2015-2026: 2.69
    vs 2.62; the extra calls are offsetting or misconducts), so penalties aren't adjusted.
    """
    years = [y for y in range(start - PLAYOFF_LOOKBACK, start) if y >= config.FIRST_SEASON]
    g = store.read_parquet_required(keys.GAMES).filter(
        pl.col("season").is_in([config.season_id(y) for y in years]) & pl.col("is_final")
    ).with_columns(
        (pl.col("home_score") - ((pl.col("season_type") == "R") & (pl.col("last_period") == 5) & (pl.col("home_score") > pl.col("away_score"))).cast(pl.Int16)).alias("h"),
        (pl.col("away_score") - ((pl.col("season_type") == "R") & (pl.col("last_period") == 5) & (pl.col("away_score") > pl.col("home_score"))).cast(pl.Int16)).alias("a"),
    )
    r, p = g.filter(pl.col("season_type") == "R"), g.filter(pl.col("season_type") == "P")
    if p.height < 100:
        return {"playoff_goal_factor": 1.0, "playoff_home_factor": 1.0, "playoff_games": float(p.height)}
    goal = float((p["h"] + p["a"]).mean() / (r["h"] + r["a"]).mean())
    home = float(np.sqrt((p["h"].sum() / p["a"].sum()) / (r["h"].sum() / r["a"].sum())))
    return {"playoff_goal_factor": goal, "playoff_home_factor": home, "playoff_games": float(p.height)}


def _playoff_stints(store: Store, start: int) -> pl.DataFrame:
    """Playoff stints from the ``PLAYOFF_LOOKBACK`` seasons before ``start`` (more playoff data)."""
    games = store.read_parquet_required(keys.GAMES).filter(pl.col("season_type") == "P").select("game_id")
    years = [y for y in range(start - PLAYOFF_LOOKBACK, start) if y >= config.FIRST_SEASON]
    return pl.concat(
        [store.read_parquet_required(keys.stints(config.season_id(y))).join(games, on="game_id", how="semi") for y in years],
        how="diagonal_relaxed",
    )


def estimate(store: Store, season: int) -> dict:
    """Constants for simulating ``season`` from the ``LOOKBACK`` seasons before it."""
    start = int(str(season)[:4])
    years = [y for y in range(start - LOOKBACK, start) if y >= config.FIRST_SEASON]
    stints = pl.concat([store.read_parquet_required(keys.stints(config.season_id(y))) for y in years], how="diagonal_relaxed")
    events = pl.concat(
        [store.read_parquet_required(keys.events(config.season_id(y))).select("event_type", "penalty_minutes", "penalty_type", "period") for y in years]
    )
    team_logs = pl.concat([store.read_parquet_required(keys.team_game_logs(config.season_id(y))) for y in years], how="diagonal_relaxed")
    catalog = store.read_parquet_required(keys.GAMES).filter(pl.col("season").is_in([config.season_id(y) for y in years]) & pl.col("is_final"))
    regular_ot = stints.filter(pl.col("period") == 4).join(
        catalog.filter(pl.col("season_type") == "R").select("game_id"), on="game_id", how="semi"
    )
    games = catalog.filter((pl.col("season_type") == "R") & (pl.col("last_period") == 5))
    gpg = catalog.with_columns(
        (pl.col("home_score") + pl.col("away_score")
         - ((pl.col("season_type") == "R") & (pl.col("last_period") == 5)).cast(pl.Int16)).alias("g")
    )
    out = {
        "season": season, "from_seasons": [config.season_id(y) for y in years],
        **_rates(stints, regular_ot), "pull": _pulls(stints), "pull_hazard": _pull_hazard(stints),
        "pull_bucket_s": PULL_BUCKET_S, **_penalties(events, team_logs),
        "score_time_share": _score_time_share(stints), "ev_context": _previous_context(store, season),
        "score_state_mult": _score_state_multipliers(stints, team_logs),
        **_playoff_factors(store, start),
        **_penalty_states(
            pl.concat([stints, _playoff_stints(store, start)], how="diagonal_relaxed").unique(["game_id", "stint_id"]),
            store.read_parquet_required(keys.GAMES),
        ),
        "hours_3v3": float(regular_ot.filter((pl.col("home_n") == 3) & (pl.col("away_n") == 3))["duration_s"].sum() / 3600),
        "shootout_home_win": float((games["home_score"] > games["away_score"]).mean()) if games.height else 0.5,
        # Goals per game excluding shootout winners: over the lookback, and last season alone.
        "goals_per_game_lookback": float(gpg["g"].mean()),
        "goals_per_game_last": float(gpg.filter(pl.col("season") == config.season_id(years[-1]))["g"].mean()),
        "pull_quantile_levels": PULL_QUANTILES.tolist(),
    }
    return out


def build(store: Store, seasons: list[int]) -> None:
    """Estimate and store constants for each season."""
    for season in seasons:
        c = estimate(store, season)
        store.put_bytes(constants_key(season), json.dumps(c, indent=2).encode())
        logger.info("sim constants for %s from %s", season, c["from_seasons"])


def load(store: Store, season: int) -> dict:
    """Stored constants for ``season``."""
    raw = store.get_bytes(constants_key(season))
    if raw is None:
        raise FileNotFoundError(f"no sim constants for {season}; run `nhl sim-constants`")
    return json.loads(raw)
