"""Starting-goalie probabilities (M5 phase B): a conditional logit over each team-game's candidates.

**Candidates** for a team-game: the goalies who dressed for the team in its previous
``WINDOW`` games (across season boundaries), plus one ``other`` option that stands for any
goalie outside that set (a recall, a trade, a return from a long injury). The ``other``
option has its own intercept, so its probability is learned, and a starter outside the set
is a normal outcome rather than a dropped row.

**Features** (all known before the game; per candidate, relative to the others because the
logit only sees differences within a team-game):

* starts in the team's last 5 / 10 / 30 games (shares of those games);
* started the team's previous game, and that streak's length before this game;
* team on the 2nd night of a back-to-back, or playing again tomorrow (the schedule is
  known), each interacted with "started the previous game";
* days since the goalie last appeared anywhere (capped), and a long-absence flag;
* the goalie's starts in the 7 days before the game;
* shrunk career GSAx per expected goal against, before the game;
* playoff game interacted with the previous-start and share terms;
* home game and opponent strength (goal differential per game to date, shrunk), each
  times the candidate's share of the last 10 starts: teams lean on the starter at home
  and against strong opponents.

Recent seasons predict better than the full history (repeat starts fell from 59% in
2015-16 to 43% in 2025-26 as teams moved to tandems), so each season's model is fitted on
the ``TRAIN_SEASONS`` seasons before it.

**Baselines** (fitted the same way, so their log loss is comparable):
``last_starter`` uses only "started the previous game"; ``last_starter_b2b`` adds the
back-to-back interaction.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date

import numpy as np
import polars as pl
from scipy.optimize import minimize

from nhl.reference.venues import schedule_context_key
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

WINDOW = 10
LONG_WINDOW = 30
REST_CAP = 10
LONG_ABSENCE_DAYS = 21
#: GSAx shrinkage: expected goals against of league-average evidence added to each goalie.
QUALITY_PRIOR_XGA = 150.0
#: Opponent strength: goal differential per game to date, with this many games of 0 added.
STRENGTH_PRIOR_GAMES = 10
TRAIN_SEASONS = 4
L2 = 1e-3

FEATURES = [
    "is_other",
    "share_5", "share_10", "share_30",
    "started_last", "streak",
    "b2b_2nd", "b2b_2nd_x_last", "b2b_1st_x_last",
    "log_rest", "long_absence", "starts_7d",
    "quality",
    "playoff_x_last", "playoff_x_share",
    "home_x_share", "opp_x_share",
]
BASELINES = {
    "last_starter": ["is_other", "started_last"],
    "last_starter_b2b": ["is_other", "started_last", "b2b_2nd_x_last"],
}


# --------------------------------------------------------------------------- data

def team_games(store: Store, seasons: list[int], today: date | None = None) -> pl.DataFrame:
    """One row per team-game from the schedule (completed games, plus games on or after
    ``today`` when given): ``game_id, season, game_date, team_id, home, playoff, starter``
    (null if not played), ``consecutive_starts``, ``n`` (team game number across seasons),
    ``b2b_2nd``, ``b2b_1st``, ``opp_str``."""
    games = store.read_parquet_required(keys.GAMES).filter(
        pl.col("season").is_in(seasons)
        & (pl.col("is_final") | (pl.col("game_date") >= today if today else pl.lit(False)))
    )
    sides = pl.concat([
        games.select("game_id", "season", "game_date", "season_type", "is_final", pl.lit(True).alias("home"),
                     pl.col("home_team_id").alias("team_id"), pl.col("home_abbr").alias("team"),
                     pl.col("away_team_id").alias("opp"), (pl.col("home_score") - pl.col("away_score")).alias("gd")),
        games.select("game_id", "season", "game_date", "season_type", "is_final", pl.lit(False).alias("home"),
                     pl.col("away_team_id").alias("team_id"), pl.col("away_abbr").alias("team"),
                     pl.col("home_team_id").alias("opp"), (pl.col("away_score") - pl.col("home_score")).alias("gd")),
    ])
    starts = pl.concat([
        store.read_parquet_required(keys.goalie_starts(s)).select("game_id", "team_id", "starter", "consecutive_starts")
        for s in seasons
    ])
    rest = pl.concat([
        store.read_parquet_required(schedule_context_key(s)).select("game_id", "team", "days_rest") for s in seasons
    ])
    t = (
        sides.join(starts, on=["game_id", "team_id"], how="left")
        .join(rest, on=["game_id", "team"], how="left")
        .with_columns((pl.col("season_type") == "P").alias("playoff"))
        .sort("team_id", "game_date", "game_id")
        .with_columns(pl.int_range(pl.len()).over("team_id").alias("n"))
        .with_columns(
            (pl.col("days_rest") == 1).fill_null(False).alias("b2b_2nd"),
            (pl.col("days_rest").shift(-1).over("team_id") == 1).fill_null(False).alias("b2b_1st"),
        )
    )
    return t.join(_strength(sides), left_on=["game_id", "opp"], right_on=["game_id", "team_id"], how="left").select(
        "game_id", "season", "game_date", "team_id", "home", "playoff", "starter", "consecutive_starts", "n",
        "b2b_2nd", "b2b_1st", pl.col("str_td").fill_null(0.0).alias("opp_str"),
    )


def _strength(sides: pl.DataFrame) -> pl.DataFrame:
    """``game_id, team_id, str_td``: the team's goal differential per game in the season,
    from completed games strictly before the date, shrunk toward 0."""
    from nhl.sim.inputs import sum_before

    done = sides.filter(pl.col("is_final")).group_by("team_id", "season", "game_date").agg(
        pl.col("gd").sum().cast(pl.Float64), pl.len().cast(pl.Float64).alias("gp")
    ).with_columns((pl.col("team_id") * 100_000_000 + pl.col("season")).alias("key"))
    left = sides.select("game_id", "game_date", "team_id", (pl.col("team_id") * 100_000_000 + pl.col("season")).alias("key"))
    j = sum_before(left, done.drop("team_id", "season"), ["gd", "gp"], by="key")
    return j.select("game_id", "team_id", (pl.col("gd_td") / (pl.col("gp_td") + STRENGTH_PRIOR_GAMES)).alias("str_td"))


def dressed_goalies(store: Store, seasons: list[int]) -> pl.DataFrame:
    """``game_id, team_id, player_id`` for every dressed goalie."""
    return pl.concat([
        store.read_parquet_required(keys.rosters(s)).filter(pl.col("is_goalie")).select("game_id", "team_id", "player_id")
        for s in seasons
    ]).unique()


def appearances(store: Store, seasons: list[int]) -> pl.DataFrame:
    """Every goalie appearance (start or relief): ``player_id, game_date, started, gsax, xga``."""
    gs = pl.concat([store.read_parquet_required(keys.goalie_starts(s)) for s in seasons], how="diagonal_relaxed")
    starts = gs.select(pl.col("starter").alias("player_id"), "game_date", pl.lit(True).alias("started"), "gsax", "xga")
    relief = gs.filter(pl.col("relief_goalie").is_not_null()).select(
        pl.col("relief_goalie").alias("player_id"), "game_date", pl.lit(False).alias("started"),
        pl.lit(0.0).alias("gsax"), pl.lit(0.0).alias("xga"),
    )
    return pl.concat([starts, relief]).with_columns(pl.col("gsax", "xga").fill_null(0.0))


# --------------------------------------------------------------------------- features

def candidate_features(tg: pl.DataFrame, dressed: pl.DataFrame, apps: pl.DataFrame) -> pl.DataFrame:
    """One row per (team-game, candidate) plus one ``other`` row per team-game, with
    :data:`FEATURES` and ``chosen`` (the actual starter, or ``other`` when he isn't a candidate).

    ``tg`` may contain future team-games (``starter`` null); their features use only the
    team's earlier games.
    """
    hist = (
        dressed.join(tg.select("game_id", "team_id", "n", "starter", "consecutive_starts"), on=["game_id", "team_id"])
        .with_columns((pl.col("player_id") == pl.col("starter")).alias("started"))
        .select("team_id", pl.col("n").alias("n_src"), "player_id", "started", "consecutive_starts")
    )
    # Each past appearance contributes to the next LONG_WINDOW team-games.
    lagged = hist.with_columns(pl.int_ranges(1, LONG_WINDOW + 1).alias("lag")).explode("lag", empty_as_null=True).with_columns(
        (pl.col("n_src") + pl.col("lag")).alias("n")
    )
    agg = lagged.group_by("team_id", "n", "player_id").agg(
        (pl.col("lag") <= WINDOW).any().alias("candidate"),
        (pl.col("started") & (pl.col("lag") <= 5)).sum().alias("starts_5"),
        (pl.col("started") & (pl.col("lag") <= WINDOW)).sum().alias("starts_10"),
        pl.col("started").sum().alias("starts_30"),
        (pl.col("started") & (pl.col("lag") == 1)).any().alias("started_last"),
        pl.when(pl.col("started") & (pl.col("lag") == 1)).then(pl.col("consecutive_starts")).max().fill_null(0).alias("streak"),
    ).filter(pl.col("candidate"))
    # Games available in each window (shorter at the start of the history).
    cand = (
        tg.select("game_id", "season", "game_date", "team_id", "n", "starter", "playoff", "b2b_2nd", "b2b_1st", "home", "opp_str")
        .join(agg, on=["team_id", "n"], how="inner")
        .with_columns(
            *[(pl.col(f"starts_{w}") / pl.min_horizontal(pl.col("n"), pl.lit(w)).clip(1, None)).alias(f"share_{w}")
              for w in (5, 10, 30)],
            pl.col("streak").cast(pl.Float64).clip(0, 10),
        )
    )
    cand = _still_with_team(cand, dressed, tg)
    cand = _goalie_history(cand, apps)
    cand = cand.with_columns(
        pl.lit(0.0).alias("is_other"),
        pl.col("started_last").cast(pl.Float64),
        pl.col("b2b_2nd").cast(pl.Float64),
        (pl.col("b2b_2nd") & pl.col("started_last")).cast(pl.Float64).alias("b2b_2nd_x_last"),
        (pl.col("b2b_1st") & pl.col("started_last")).cast(pl.Float64).alias("b2b_1st_x_last"),
        (pl.col("playoff") & pl.col("started_last")).cast(pl.Float64).alias("playoff_x_last"),
        (pl.col("playoff").cast(pl.Float64) * pl.col("share_10")).alias("playoff_x_share"),
        (pl.col("home").cast(pl.Float64) * pl.col("share_10")).alias("home_x_share"),
        (pl.col("opp_str") * pl.col("share_10")).alias("opp_x_share"),
        (pl.col("player_id") == pl.col("starter")).alias("chosen"),
    )
    other = (
        tg.select("game_id", "season", "game_date", "team_id", "n", "starter")
        .join(cand.group_by("game_id", "team_id").agg(pl.col("chosen").any().alias("covered")), on=["game_id", "team_id"], how="left")
        .with_columns(
            pl.lit(None, dtype=pl.Int64).alias("player_id"), pl.lit(1.0).alias("is_other"),
            (pl.col("starter").is_not_null() & ~pl.col("covered").fill_null(False)).alias("chosen"),
        )
        .drop("covered")
    )
    out = pl.concat([cand.select(other.columns + [f for f in FEATURES if f != "is_other"]), other], how="diagonal_relaxed")
    return out.with_columns(pl.col(f).cast(pl.Float64).fill_null(0.0) for f in FEATURES).sort("game_date", "game_id", "team_id", "is_other", "player_id")


def _still_with_team(cand: pl.DataFrame, dressed: pl.DataFrame, tg: pl.DataFrame) -> pl.DataFrame:
    """Drop candidates whose most recent dressing before the game was for another team
    (traded, signed elsewhere, claimed): mostly season-boundary leftovers."""
    last = (
        dressed.join(tg.select("game_id", "team_id", "game_date"), on=["game_id", "team_id"])
        .select("player_id", "game_date", pl.col("team_id").alias("last_team"))
        .sort("game_date")
    )
    j = cand.with_row_index("_row").sort("game_date").join_asof(
        last, on="game_date", by="player_id", strategy="backward", allow_exact_matches=False, check_sortedness=False,
    )
    return j.filter(pl.col("last_team").is_null() | (pl.col("last_team") == pl.col("team_id"))).sort("_row").drop("_row", "last_team")


def _goalie_history(cand: pl.DataFrame, apps: pl.DataFrame) -> pl.DataFrame:
    """Add ``log_rest``, ``long_absence``, ``starts_7d`` and ``quality`` from every appearance
    strictly before the game date."""
    daily = apps.group_by("player_id", "game_date").agg(
        pl.col("started").sum().cast(pl.Float64).alias("st"), pl.col("gsax").sum(), pl.col("xga").sum()
    ).sort("game_date").with_columns(
        pl.col("st", "gsax", "xga").cum_sum().over("player_id").name.suffix("_cum"),
        pl.col("game_date").alias("last_app"),
    )
    left = cand.with_row_index("_row").sort("game_date")
    j = left.join_asof(
        daily.select("player_id", "game_date", "last_app", "st_cum", "gsax_cum", "xga_cum"),
        on="game_date", by="player_id", strategy="backward", allow_exact_matches=False, check_sortedness=False,
    )
    week = left.select("_row", "player_id", (pl.col("game_date") - pl.duration(days=7)).alias("game_date")).sort("game_date").join_asof(
        daily.select("player_id", "game_date", pl.col("st_cum").alias("st_cum_7")),
        on="game_date", by="player_id", strategy="backward", allow_exact_matches=True, check_sortedness=False,
    ).select("_row", "st_cum_7")
    rest = (pl.col("game_date") - pl.col("last_app")).dt.total_days()
    return j.join(week, on="_row").sort("_row").with_columns(
        rest.clip(1, REST_CAP).fill_null(REST_CAP).cast(pl.Float64).log().alias("log_rest"),
        (rest.fill_null(999) > LONG_ABSENCE_DAYS).cast(pl.Float64).alias("long_absence"),
        (pl.col("st_cum").fill_null(0) - pl.col("st_cum_7").fill_null(0)).alias("starts_7d"),
        (pl.col("gsax_cum").fill_null(0) / (pl.col("xga_cum").fill_null(0) + QUALITY_PRIOR_XGA)).alias("quality"),
    ).drop("_row", "last_app", "st_cum", "gsax_cum", "xga_cum", "st_cum_7")


# --------------------------------------------------------------------------- model

@dataclass
class StarterModel:
    """Conditional-logit coefficients over ``features``."""

    features: list[str]
    coef: np.ndarray

    def predict(self, cand: pl.DataFrame) -> pl.DataFrame:
        """``cand`` with ``p_start`` (sums to 1 within each team-game)."""
        x = cand.select(self.features).to_numpy()
        groups = _group_index(cand)
        return cand.with_columns(pl.Series("p_start", _softmax(x @ self.coef, groups)))

    def to_dict(self) -> dict:
        return {"features": self.features, "coef": self.coef.tolist()}

    @classmethod
    def from_dict(cls, d: dict) -> StarterModel:
        return cls(d["features"], np.array(d["coef"]))


def _group_index(cand: pl.DataFrame) -> np.ndarray:
    return cand.select((pl.col("game_id") * 100 + pl.col("team_id")).rank("dense") - 1).to_series().to_numpy().astype(np.int64)


def _softmax(score: np.ndarray, groups: np.ndarray) -> np.ndarray:
    m = np.full(groups.max() + 1, -np.inf)
    np.maximum.at(m, groups, score)
    e = np.exp(score - m[groups])
    z = np.bincount(groups, weights=e)
    return e / z[groups]


def fit(cand: pl.DataFrame, features: list[str]) -> StarterModel:
    """Maximum likelihood (with a small ridge) on team-games that have a starter."""
    cand = cand.filter(pl.col("starter").is_not_null())
    x = cand.select(features).to_numpy()
    y = cand["chosen"].to_numpy().astype(float)
    groups = _group_index(cand)
    n_groups = groups.max() + 1

    def loss(beta: np.ndarray) -> tuple[float, np.ndarray]:
        p = _softmax(x @ beta, groups)
        nll = -np.sum(y * np.log(np.clip(p, 1e-12, None))) / n_groups + L2 * beta @ beta
        grad = x.T @ (p - y) / n_groups + 2 * L2 * beta
        return nll, grad

    res = minimize(loss, np.zeros(len(features)), jac=True, method="L-BFGS-B")
    if not res.success:
        logger.warning("starter model did not converge: %s", res.message)
    return StarterModel(list(features), res.x)


def log_loss(pred: pl.DataFrame) -> float:
    """Mean −log P(actual starter) per team-game."""
    p = pred.filter(pl.col("chosen"))["p_start"].to_numpy()
    return float(-np.mean(np.log(np.clip(p, 1e-12, None))))


def train_seasons(season: int) -> list[int]:
    """The ``TRAIN_SEASONS`` seasons before ``season``."""
    y = int(str(season)[:4])
    return [int(f"{k}{k + 1}") for k in range(y - TRAIN_SEASONS, y)]


def evaluate(cand: pl.DataFrame, test: list[int]) -> pl.DataFrame:
    """Per test season: the model and baselines, each fitted on the ``TRAIN_SEASONS`` seasons
    before it. Reports log loss, top-pick accuracy and the share of starters who were
    ``other``."""
    rows = []
    for season in test:
        tr = cand.filter(pl.col("season").is_in(train_seasons(season)))
        te = cand.filter((pl.col("season") == season) & pl.col("starter").is_not_null())
        models = {"model": fit(tr, FEATURES), **{k: fit(tr, f) for k, f in BASELINES.items()}}
        for name, m in models.items():
            pred = m.predict(te)
            top = pred.sort("p_start", descending=True).group_by("game_id", "team_id", maintain_order=True).first()
            rows.append({
                "season": season, "name": name, "team_games": top.height,
                "log_loss": log_loss(pred), "accuracy": float(top["chosen"].mean()),
                "other_share": float(pred.filter(pl.col("is_other") == 1)["chosen"].mean()),
            })
    return pl.DataFrame(rows).sort("season", "log_loss")


def build_candidates(store: Store, seasons: list[int], today: date | None = None,
                     assumed: pl.DataFrame | None = None) -> pl.DataFrame:
    """Candidate features for ``seasons`` (plus the season before, for the lag windows).

    Args:
        store: Where the schedule, rosters and goalie starts live.
        seasons: Seasons to return candidates for.
        today: Also include unplayed team-games on or after this date.
        assumed: Unplayed team-games to treat as played (see :func:`assume`), so a later
            game's features (started last, streak, rest, back-to-back) follow from them.
    """
    first = int(str(min(seasons))[:4])
    span = sorted({int(f"{first - 1}{first}"), *seasons} & set(_available(store)))
    tg, dressed, apps = team_games(store, span, today), dressed_goalies(store, span), appearances(store, span)
    if assumed is not None and assumed.height:
        tg, dressed, apps = assume(tg, dressed, apps, assumed)
    cand = candidate_features(tg, dressed, apps)
    return cand.filter(pl.col("season").is_in(seasons))


def assume(tg: pl.DataFrame, dressed: pl.DataFrame, apps: pl.DataFrame,
           assumed: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """``tg``, ``dressed`` and ``apps`` as if each unplayed team-game in ``assumed``
    (``game_id, team_id, game_date, starter, backup``) had been played: ``starter`` started
    (with no GSAx evidence) and both goalies dressed.

    Used to price a game when the team plays before it (tonight, then tomorrow): the
    later game's starter is a mixture over who starts the earlier one.
    """
    a = assumed.select(pl.col("game_id").cast(pl.Int64), pl.col("team_id").cast(pl.Int64),
                       pl.col("starter").cast(pl.Int64).alias("_assumed"))
    tg = tg.with_columns(pl.col("game_id", "team_id").cast(pl.Int64)).join(a, on=["game_id", "team_id"], how="left")
    prev_starter = pl.col("starter").shift(1).over("team_id")
    prev_streak = pl.col("consecutive_starts").shift(1).over("team_id")
    tg = tg.sort("team_id", "n").with_columns(
        pl.coalesce("starter", "_assumed").alias("starter"),
        pl.when(pl.col("_assumed").is_not_null())
        .then(pl.when(prev_starter == pl.col("_assumed")).then(prev_streak.fill_null(0) + 1).otherwise(1))
        .otherwise(pl.col("consecutive_starts")).alias("consecutive_starts"),
    ).drop("_assumed")
    both = pl.concat([assumed.select("game_id", "team_id", pl.col(c).alias("player_id")) for c in ("starter", "backup")])
    dressed = pl.concat([dressed, both.drop_nulls("player_id").cast(dressed.schema)]).unique()
    apps = pl.concat([apps, assumed.select(pl.col("starter").alias("player_id"), "game_date", pl.lit(True).alias("started"),
                                           pl.lit(0.0).alias("gsax"), pl.lit(0.0).alias("xga")).cast(apps.schema)])
    return tg, dressed, apps


def _available(store: Store) -> list[int]:
    return [int(k.rsplit("/", 1)[1].split(".")[0]) for k in store.list_keys("processed/goalie_starts/")]


def train(store: Store, season: int) -> StarterModel:
    """Fit the model for ``season`` on the seasons before it and save it."""
    cand = build_candidates(store, train_seasons(season))
    model = fit(cand, FEATURES)
    store.put_bytes(keys.starter_model(season), json.dumps({**model.to_dict(), "train": train_seasons(season)}).encode())
    return model


def load(store: Store, season: int) -> StarterModel:
    return StarterModel.from_dict(json.loads(store.get_bytes(keys.starter_model(season))))


def write_report(results: pl.DataFrame, coef: StarterModel, path: str) -> str:
    """Markdown report of :func:`evaluate` results and the latest model's coefficients."""
    wide = results.pivot(on="name", index="season", values="log_loss").sort("season")
    acc = results.pivot(on="name", index="season", values="accuracy").sort("season")
    lines = [
        "# M5 starting-goalie model",
        "",
        f"Conditional logit over each team-game's candidates (goalies dressed in the last {WINDOW} team games "
        f"plus `other`), fitted on the {TRAIN_SEASONS} seasons before each test season. Bar: log loss below "
        "\"last game's starter\".",
        "",
        "## Log loss (per team-game)",
        "",
        "| season | model | last_starter_b2b | last_starter |",
        "|---|---|---|---|",
        *[f"| {r['season']} | {r['model']:.4f} | {r['last_starter_b2b']:.4f} | {r['last_starter']:.4f} |" for r in wide.iter_rows(named=True)],
        "",
        "## Top-pick accuracy",
        "",
        "| season | model | last_starter_b2b | last_starter |",
        "|---|---|---|---|",
        *[f"| {r['season']} | {r['model']:.3f} | {r['last_starter_b2b']:.3f} | {r['last_starter']:.3f} |" for r in acc.iter_rows(named=True)],
        "",
        "## Coefficients (latest model)",
        "",
        "| feature | coef |",
        "|---|---|",
        *[f"| {f} | {c:+.3f} |" for f, c in zip(coef.features, coef.coef)],
        "",
    ]
    text = "\n".join(lines)
    with open(path, "w") as fh:
        fh.write(text)
    return text
