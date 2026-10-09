"""Point-in-time player scoring rates by strength (M9 phase B).

For every skater-game of a season, the player's goal and assist talent from **games before
that one** only: this season's earlier games plus the two previous seasons, down-weighted by
age (:data:`SEASON_WEIGHTS`), each shrunk toward the league rate of his position group (F/D)
in the previous season.

Per strength bucket (:data:`BUCKETS`: even strength, power play, shorthanded) and per 60:

* ``xg60`` = individual xG per 60, shrunk with :attr:`Shrink.xg` minutes of league-average play;
* ``fin`` = goals / individual xG over all strengths, shrunk with :attr:`Shrink.fin` xG of
  league-average finishing (finishing is noisy: it takes a large sample to move);
* ``g60 = xg60 × fin``, the goal rate the projection uses;
* ``a60`` = assists (primary + secondary) per 60, shrunk with :attr:`Shrink.ast` minutes.

**Empty net** (``EN_opp``: the other team's goalie pulled; ``EN_own``: ours pulled, an extra
attacker) is its own bucket. Its minutes go mostly to stars (the top decile of projected scorers
gets ~16x the bottom's extra-attacker time but only 1.2x the even-strength time), so a player's
share there comes from his own history of empty-net minutes per game played (``en_pg``, shrunk
with :attr:`Shrink.en_games` games of his position group's average), and his even-strength
talent is used for the rate (``g60_en = g60_ev``, ``a60_en = a60_ev``). Finishing and the other
rates leave empty-net play out.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl

from nhl.storage import keys
from nhl.storage.s3 import Store

#: Game-log strengths -> bucket.
BUCKETS: dict[str, str] = {"EV": "ev", "EN_opp": "en", "EN_own": "en", "PP": "pp", "SH": "sh"}
BUCKET_NAMES = ("ev", "pp", "sh", "en")
#: Buckets with their own talent rates (empty net borrows even strength's).
RATE_BUCKETS = ("ev", "pp", "sh")
#: Weight of (this season, last season, two back) in the rate sums.
SEASON_WEIGHTS = (1.0, 0.6, 0.35)
STATS = ("toi", "goals", "ast", "ixg")


@dataclass(frozen=True)
class Shrink:
    """Prior strength: minutes of league-average play (xg, ast) and xG of average finishing (fin).

    Tuned on 2016-17 to 2018-19 (log loss of goals/assists/points >= 1); the optimum is flat
    (±0.0001 across 4x either way) because two-plus seasons of history already steady the rates.
    """

    xg: float = 50.0
    ast: float = 250.0
    fin: float = 20.0
    en_games: float = 5.0
    team_goals: float = 150.0


def group(position: pl.Expr) -> pl.Expr:
    """Position group: ``D`` or ``F``."""
    return pl.when(position == "D").then(pl.lit("D")).otherwise(pl.lit("F"))


def bucket_logs(store: Store, season: int) -> pl.DataFrame:
    """One row per skater-game with ``{stat}_{bucket}`` columns (toi in minutes).

    Returns:
        ``game_id, game_date, season, player_id, team_id, grp``, ``gp`` (1 per game) and, for
        each bucket, ``toi_*``, ``goals_*``, ``ast_*``, ``ixg_*``.
    """
    logs = store.get_parquet(keys.player_game_logs(season))
    if logs is None:
        return pl.DataFrame()
    logs = logs.filter((pl.col("position") != "G") & pl.col("strength").is_in(list(BUCKETS)))
    long = logs.select(
        "game_id", "game_date", "player_id", "team_id", group(pl.col("position")).alias("grp"),
        pl.col("strength").replace_strict(BUCKETS).alias("b"),
        (pl.col("toi_s") / 60).alias("toi"), pl.col("goals").cast(pl.Float64),
        (pl.col("a1") + pl.col("a2")).cast(pl.Float64).alias("ast"), pl.col("ixg").cast(pl.Float64),
    ).group_by("game_id", "game_date", "player_id", "team_id", "grp", "b").agg(pl.col(list(STATS)).sum())
    wide = long.pivot(on="b", index=["game_id", "game_date", "player_id", "team_id", "grp"], values=list(STATS))
    cols = [f"{s}_{b}" for s in STATS for b in BUCKET_NAMES]
    for c in cols:
        if c not in wide.columns:
            wide = wide.with_columns(pl.lit(0.0).alias(c))
    return wide.with_columns(pl.col(cols).fill_null(0.0), pl.lit(season).alias("season"), pl.lit(1.0).alias("gp")).select(
        "game_id", "game_date", "season", "player_id", "team_id", "grp", "gp", *cols)


def _sum_cols(frame: pl.DataFrame) -> list[str]:
    """The additive columns of bucket logs: every ``{stat}_{bucket}`` and ``gp``."""
    return [c for c in frame.columns if c == "gp" or c.split("_")[0] in STATS]


def league_rates(logs: pl.DataFrame) -> pl.DataFrame:
    """Per position group: league ``xg60_*``, ``a60_*``, ``fin`` from one season of bucket logs."""
    sums = logs.group_by("grp").agg(pl.col(_sum_cols(logs)).sum())
    return sums.select(
        "grp",
        *[(pl.col(f"ixg_{b}") / pl.col(f"toi_{b}") * 60).alias(f"lg_xg60_{b}") for b in RATE_BUCKETS],
        *[(pl.col(f"ast_{b}") / pl.col(f"toi_{b}") * 60).alias(f"lg_a60_{b}") for b in RATE_BUCKETS],
        (pl.sum_horizontal(*[f"goals_{b}" for b in RATE_BUCKETS])
         / pl.sum_horizontal(*[f"ixg_{b}" for b in RATE_BUCKETS])).alias("lg_fin"),
        (pl.col("toi_en") / pl.col("gp")).alias("lg_en_pg"),
    )


def strength_mix(logs: pl.DataFrame) -> dict[str, dict[str, float]]:
    """League share of goals by bucket (``f``) and assists per goal by bucket (``apg``)."""
    tot = {c: float(logs[c].sum()) for c in logs.columns if c.split("_")[0] in ("goals", "ast")}
    goals = sum(tot[f"goals_{b}"] for b in BUCKET_NAMES)
    return {"f": {b: tot[f"goals_{b}"] / goals for b in BUCKET_NAMES},
            "apg": {b: tot[f"ast_{b}"] / max(tot[f"goals_{b}"], 1.0) for b in BUCKET_NAMES}}


def _prior_totals(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Season-weighted per-player totals of earlier seasons (``frames[0]`` = last season)."""
    parts = []
    for weight, logs in zip(SEASON_WEIGHTS[1:], frames, strict=False):
        if logs.is_empty():
            continue
        cols = _sum_cols(logs)
        parts.append(logs.group_by("player_id").agg([(pl.col(c).sum() * weight).alias(c) for c in cols]))
    if not parts:
        return pl.DataFrame(schema={"player_id": pl.Int64})
    stacked = pl.concat(parts, how="diagonal_relaxed")
    return stacked.group_by("player_id").agg(pl.all().exclude("player_id").sum())


def season_rates(store: Store, season: int, shrink: Shrink = Shrink(),
                 logs: dict[int, pl.DataFrame] | None = None) -> tuple[pl.DataFrame, dict[str, dict[str, float]]]:
    """Point-in-time rates for every skater-game of ``season``.

    Args:
        store: S3 store.
        season: Target season.
        shrink: Prior strengths.
        logs: Preloaded :func:`bucket_logs` by season (loaded when missing).

    Returns:
        ``(rates, mix)``: rates has ``game_id, player_id, team_id, grp`` plus ``g60_*``,
        ``a60_*`` per bucket and ``fin``; the realized ``goals``, ``ast``, ``points`` and
        ``toi`` of that game (for scoring). ``mix`` is :func:`strength_mix` of last season.
    """
    logs = dict(logs or {})
    for s in (season, season - 10001, season - 20002):
        if s not in logs:
            logs[s] = bucket_logs(store, s)
    cur, last = logs[season], logs[season - 10001]
    lg = league_rates(last)
    mix = strength_mix(last)
    stat_cols = _sum_cols(cur)

    # This season's games before each game (cumulative minus the game itself).
    cur = cur.sort("game_date", "game_id")
    before = cur.with_columns([(pl.col(c).cum_sum().over("player_id") - pl.col(c)).alias(f"pre_{c}") for c in stat_cols])
    prior = _prior_totals([logs[season - 10001], logs[season - 20002]])
    prior = prior.rename({c: f"old_{c}" for c in prior.columns if c != "player_id"})
    df = before.join(prior, on="player_id", how="left").join(lg, on="grp", how="left")
    total = {c: pl.col(f"pre_{c}") + pl.col(f"old_{c}").fill_null(0.0) if f"old_{c}" in df.columns else pl.col(f"pre_{c}")
             for c in stat_cols}

    ixg_all = pl.sum_horizontal(*[total[f"ixg_{b}"] for b in RATE_BUCKETS])
    goals_all = pl.sum_horizontal(*[total[f"goals_{b}"] for b in RATE_BUCKETS])
    fin = ((goals_all + shrink.fin * pl.col("lg_fin")) / (ixg_all + shrink.fin)).alias("fin")
    en_pg = ((total["toi_en"] + shrink.en_games * pl.col("lg_en_pg")) / (total["gp"] + shrink.en_games)).alias("en_pg")
    exprs = [fin, en_pg]
    for b in RATE_BUCKETS:
        toi = total[f"toi_{b}"]
        xg60 = (total[f"ixg_{b}"] + shrink.xg / 60 * pl.col(f"lg_xg60_{b}")) / (toi + shrink.xg) * 60
        exprs += [xg60.alias(f"xg60_{b}"),
                  ((total[f"ast_{b}"] + shrink.ast / 60 * pl.col(f"lg_a60_{b}")) / (toi + shrink.ast) * 60).alias(f"a60_{b}")]
    out = df.with_columns(exprs).with_columns(
        *[(pl.col(f"xg60_{b}") * pl.col("fin")).alias(f"g60_{b}") for b in RATE_BUCKETS],
        pl.col("a60_ev").alias("a60_en"),
    ).with_columns(
        pl.col("g60_ev").alias("g60_en"),
        pl.sum_horizontal(*[f"goals_{b}" for b in BUCKET_NAMES]).alias("goals"),
        pl.sum_horizontal(*[f"ast_{b}" for b in BUCKET_NAMES]).alias("ast"),
        pl.sum_horizontal(*[f"toi_{b}" for b in BUCKET_NAMES]).alias("toi"),
    ).with_columns((pl.col("goals") + pl.col("ast")).alias("points"))
    keep = ["game_id", "game_date", "player_id", "team_id", "grp", "fin", "en_pg", "goals", "ast", "points", "toi",
            *[f"{k}_{b}" for k in ("g60", "a60") for b in BUCKET_NAMES]]
    return out.select(keep), mix


def team_mix(logs: dict[int, pl.DataFrame], season: int, mix: dict[str, dict[str, float]],
             shrink: Shrink = Shrink()) -> pl.DataFrame:
    """Each team's share of its goals by bucket before each of its games (``f_ev`` ... ``f_en``).

    This season's earlier games plus last season's (weighted :data:`SEASON_WEIGHTS`), shrunk
    toward the league mix with :attr:`Shrink.team_goals` goals. A team with a strong power play
    scores more of its goals there, and its power-play players get them.
    """
    cols = [f"goals_{b}" for b in BUCKET_NAMES]
    cur = logs[season].group_by("game_id", "game_date", "team_id").agg(pl.col(cols).sum()).sort("game_date", "game_id")
    cur = cur.with_columns([(pl.col(c).cum_sum().over("team_id") - pl.col(c)).alias(c) for c in cols])
    last = logs.get(season - 10001)
    if last is not None and not last.is_empty():
        # A team's last season, by the team it was (a relocated franchise keeps its id).
        old = last.group_by("team_id").agg([(pl.col(c).sum() * SEASON_WEIGHTS[1]).alias(f"old_{c}") for c in cols])
        cur = cur.join(old, on="team_id", how="left").with_columns(
            [(pl.col(c) + pl.col(f"old_{c}").fill_null(0.0)).alias(c) for c in cols]).drop([f"old_{c}" for c in cols])
    total = pl.sum_horizontal(*cols)
    return cur.select("game_id", "team_id", *[
        ((pl.col(f"goals_{b}") + shrink.team_goals * mix["f"][b]) / (total + shrink.team_goals)).alias(f"f_{b}")
        for b in BUCKET_NAMES])


def rates_for_day(store: Store, season: int, day: date, players: pl.DataFrame, shrink: Shrink = Shrink(),
                  logs: dict[int, pl.DataFrame] | None = None
                  ) -> tuple[pl.DataFrame, dict[str, dict[str, float]], pl.DataFrame]:
    """Rates for upcoming games on ``day`` from every game logged before it.

    Each target (``game_id, player_id, team_id, position``) is added as an empty game on
    ``day`` and run through :func:`season_rates` and :func:`team_mix`, so live and backtest use
    the same math.

    Returns:
        ``(rates, mix, team_f)`` for the target games.
    """
    logs = dict(logs or {})
    for s in (season, season - 10001, season - 20002):
        if s not in logs:
            logs[s] = bucket_logs(store, s)
    cur = logs[season]
    stat_cols = ["gp", *[f"{s}_{b}" for s in STATS for b in BUCKET_NAMES]]
    targets = players.filter(pl.col("player_id").is_not_null()).select(
        pl.col("game_id").cast(pl.Int64), pl.lit(day).alias("game_date"), pl.lit(season).alias("season"),
        pl.col("player_id").cast(pl.Int64), pl.col("team_id").cast(pl.Int64), group(pl.col("position")).alias("grp"),
        *[pl.lit(0.0).alias(c) for c in stat_cols],
    ).unique(["game_id", "player_id"])
    if not cur.is_empty():
        cur = cur.filter(pl.col("game_date") < day)
        targets = targets.select(cur.columns).cast(cur.schema)
    logs[season] = pl.concat([cur, targets], how="diagonal_relaxed") if not cur.is_empty() else targets
    out, mix = season_rates(store, season, shrink, logs)
    team_f = team_mix(logs, season, mix, shrink).join(targets.select("game_id", "team_id").unique(),
                                                      on=["game_id", "team_id"], how="inner")
    return out.join(targets.select("game_id", "player_id"), on=["game_id", "player_id"], how="inner").drop(
        "goals", "ast", "points", "toi"), mix, team_f


__all__ = ["BUCKETS", "BUCKET_NAMES", "RATE_BUCKETS", "SEASON_WEIGHTS", "Shrink", "bucket_logs", "league_rates",
           "rates_for_day", "season_rates", "strength_mix", "team_mix"]
