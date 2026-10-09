"""Command-line entry point: ``nhl <command> [options]`` (or ``python -m nhl``)."""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import date, datetime

from nhl import config


def _current_start_year() -> int:
    today = date.today()
    return today.year if today.month >= 9 else today.year - 1


def _default_seasons() -> str:
    return f"{config.FIRST_SEASON}-{_current_start_year()}"


def cmd_catalog(args: argparse.Namespace) -> None:
    """Refresh games, teams and players."""
    from nhl.ingest.catalog import refresh_games, refresh_players
    from nhl.ingest.http import NHLClient
    from nhl.storage.s3 import Store

    store, client = Store(), NHLClient()
    games = refresh_games(store, client)
    logging.info("games catalog: %d games", games.height)
    from nhl.teams import validate_against_api

    problems = validate_against_api(client.teams(), set(games["home_team_id"]) | set(games["away_team_id"]))
    for problem in problems:  # e.g. a new expansion team: add it to src/nhl/reference/teams.csv
        logging.error("teams.csv out of date: %s", problem)
    players = refresh_players(store, client, config.parse_seasons(args.seasons), force=args.force)
    logging.info("players: %d", players.height)


def cmd_ingest(args: argparse.Namespace) -> None:
    """Fetch raw play-by-play and shifts for final games not yet stored."""
    from nhl.ingest.catalog import refresh_games
    from nhl.ingest.games import ingest_season
    from nhl.ingest.http import NHLClient
    from nhl.storage import keys
    from nhl.storage.s3 import Store

    store, client = Store(), NHLClient()
    games = refresh_games(store, client) if not args.skip_catalog else store.read_parquet_required(keys.GAMES)
    reports = [
        ingest_season(store, client, games, year, workers=args.workers, refetch=args.refetch)
        for year in config.parse_seasons(args.seasons)
    ]
    print("\n".join(r.summary() for r in reports))
    if any(r.failed for r in reports):
        sys.exit(1)


def cmd_build(args: argparse.Namespace) -> None:
    """Rebuild processed event tables from raw files."""
    from nhl.storage.s3 import Store
    from nhl.transform.build import build_season

    store = Store()
    for year in config.parse_seasons(args.seasons):
        build_season(store, year, workers=args.workers)


def cmd_features(args: argparse.Namespace) -> None:
    """Rebuild shot feature tables from processed events."""
    from nhl.features.shots import build_shot_features
    from nhl.storage import keys
    from nhl.storage.s3 import Store

    import polars as pl

    store = Store()
    players = store.read_parquet_required(keys.PLAYERS)
    for year in config.parse_seasons(args.seasons):
        sid = config.season_id(year)
        # Fatigue norms (a player's typical shift length) look back 20 games, so the prior
        # season's shifts are included to make early-season values point-in-time correct.
        shift_frames = [store.get_parquet(keys.shifts(config.season_id(y))) for y in (year - 1, year)]
        shift_frames = [f for f in shift_frames if f is not None]
        shifts = pl.concat(shift_frames, how="vertical_relaxed") if shift_frames else None
        shots = build_shot_features(store.read_parquet_required(keys.events(sid)), players, shifts)
        store.put_parquet(keys.shots(sid), shots)


def cmd_rink_adjust(args: argparse.Namespace) -> None:
    """Estimate arena scorer-bias maps and write adjusted shot tables."""
    import polars as pl

    from nhl.features import rink
    from nhl.storage import keys
    from nhl.storage.s3 import Store

    store = Store()
    years = config.parse_seasons(args.seasons)
    shots = pl.concat([store.read_parquet_required(keys.shots(config.season_id(y))) for y in years], how="vertical_relaxed")
    venues = store.read_parquet_required(keys.GAME_VENUES)
    if args.apply_only:
        # Nightly path: apply the stored maps; re-estimating from one season would overwrite them.
        maps = store.read_parquet_required(keys.RINK_MAPS)
    else:
        maps = rink.estimate_maps(shots, venues)
        store.put_parquet(keys.RINK_MAPS, maps)
        print(rink.validation_report(shots, venues, rink.apply_maps(shots, venues, maps, include_tracking_era=True)))
    adjusted = rink.apply_maps(shots, venues, maps)
    for (season,), part in adjusted.partition_by("season", as_dict=True).items():
        store.put_parquet(keys.shots_rink_adjusted(int(season)), part.drop("arena_id"))


def cmd_train_xg(args: argparse.Namespace) -> None:
    """Tune, evaluate, refit and save the xG models; write out-of-fold historical xG."""
    import polars as pl

    from nhl.features.shots import STRENGTH_GROUPS
    from nhl.models import xg
    from nhl.storage import keys
    from nhl.storage.s3 import Store

    store = Store()
    split = xg.SeasonSplit(
        train=config.parse_seasons(args.train), valid=config.parse_seasons(args.valid), test=config.parse_seasons(args.test)
    )
    shots = xg.load_shots(store, split.all, rink_adjusted=args.rink_adjusted)
    base_features = xg.feature_set(args.feature_set)
    architecture = xg.ARCHITECTURES[args.architecture]
    names = args.groups.split(",") if args.groups else list(architecture)
    results = []
    for name in names:
        covers = architecture[name]
        budget = args.timeout if "EV" in covers else max(60, args.timeout // 3)
        results.append(xg.tune_group(name, shots, split, n_trials=args.trials, timeout=budget, covers=covers,
                                     features=xg.model_features(base_features, covers)))
    version = args.version or datetime.now().strftime("v%Y%m%d-%H%M")
    prefix = xg.save_models(store, version, results, split, promote=args.promote, architecture=args.architecture,
                            feature_set_name=args.feature_set, rink_adjusted=args.rink_adjusted)
    print(xg.report(results))
    print(f"\nsaved models -> {store.uri(prefix)}")

    if not args.promote:
        print("not promoted: LATEST and stored predictions are unchanged (re-run with --promote to adopt)")
        return
    if not args.skip_oof:
        oof = [xg.out_of_fold(r.group, shots, split.all, r.params, covers=r.covers, features=r.features) for r in results]
        _write_predictions(store, shots, pl.concat(oof), version)


def _write_predictions(store, shots, preds, version: str) -> None:
    import polars as pl

    from nhl.storage import keys

    scored = shots.select("season", "game_id", "game_date", "event_idx", "period", "period_seconds", "event_type",
                          "event_team_abbr", "shooter_id", "goalie_id", "strength_state", "strength_group",
                          "event_distance", "event_angle", "is_goal").join(preds, on=["game_id", "event_idx"], how="inner")
    scored = scored.with_columns(pl.lit(version).alias("model_version"))
    for (season,), part in scored.partition_by("season", as_dict=True).items():
        store.put_parquet(keys.xg_predictions(season), part.sort("game_id", "event_idx"))


def cmd_score_xg(args: argparse.Namespace) -> None:
    """Score seasons with the production xG model (use for the current season)."""
    from nhl.models import xg
    from nhl.storage.s3 import Store

    store = Store()
    models, meta = xg.load_models(store, args.version)
    shots = xg.load_shots(store, config.parse_seasons(args.seasons), rink_adjusted=meta.get("rink_adjusted", False))
    _write_predictions(store, shots, xg.predict_xg(shots, models, meta), meta["version"])


def cmd_xg_monitor(args: argparse.Namespace) -> None:
    """Season-to-date xG calibration by strength state (flags drift worth a retrain)."""
    from nhl.models.monitor import run_monitor
    from nhl.storage.s3 import Store

    rows = run_monitor(Store(), config.season_id(args.season or _current_start_year()), write=not args.no_write)
    for r in rows:
        print(f"{r['state']:4} goals/xG {r['ratio']:.3f}  [{r['lo']:.3f}, {r['hi']:.3f}]  "
              f"{r['goals']:>5} goals / {r['shots']:>6} shots{'  FLAG' if r['flagged'] else ''}")


def cmd_train_freeze(args: argparse.Namespace) -> None:
    """Train the frozen-puck model, mark it LATEST, write historical and current predictions."""
    from datetime import datetime, timezone

    from nhl.ratings.freeze import train_and_store
    from nhl.storage.s3 import Store

    version = args.version or "f" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
    result = train_and_store(
        Store(), config.parse_seasons(args.train), config.parse_seasons(args.valid), config.parse_seasons(args.test),
        config.parse_seasons(args.score), version,
    )
    t = result.test
    print(f"{version}: test log loss {t['log_loss']:.5f} (skill {t['log_loss_skill']:.4f}), "
          f"AUC {t['auc']:.3f}, freezes/expected {t['goals_per_xg']:.3f}")


def cmd_score_freeze(args: argparse.Namespace) -> None:
    """Score seasons with the production frozen-puck model."""
    from nhl.ratings.freeze import score_seasons
    from nhl.storage.s3 import Store

    score_seasons(Store(), config.parse_seasons(args.seasons))


def cmd_build_priors(args: argparse.Namespace) -> None:
    """Chain complete seasons and store the priors entering each next season (all M3 models)."""
    from nhl.ratings import finishing, penalties, rapm
    from nhl.storage.s3 import Store

    store, years = Store(), config.parse_seasons(args.seasons)
    finishing.build_priors(store, years)
    penalties.build_priors(store, years)
    ev_curve = rapm.load_curve(store, "EV")
    st_curve = rapm.AgeCurve(coef=ev_curve.coef, scale=rapm.ST_AGE_SCALE) if ev_curve else None
    rapm.build_priors(store, years, "EV", curve=ev_curve)
    rapm.build_priors(store, years, "ST", curve=st_curve)
    print(f"priors stored for {config.season_id(years[-1] + 1)} and earlier")


def cmd_ratings(args: argparse.Namespace) -> None:
    """Write point-in-time rating snapshots (one date, or a backfill)."""
    from nhl.ratings import snapshots
    from nhl.storage.s3 import Store

    store = Store()
    if args.backfill:
        n = snapshots.backfill(store, config.parse_seasons(args.backfill), every_days=args.every)
        print(f"{n} snapshots written")
    else:
        day = date.fromisoformat(args.as_of) if args.as_of else date.today()
        print(snapshots.snapshot(store, day))


def cmd_evaluate_ratings(args: argparse.Namespace) -> None:
    """Rest-of-season test of EV ratings vs baselines; writes the M3 report."""
    from pathlib import Path

    from nhl.ratings.evaluate import evaluate, write_report
    from nhl.storage.s3 import Store

    results, summary = evaluate(Store(), config.parse_seasons(args.seasons))
    print(write_report(results, summary, Path(args.report)))


def cmd_sim_constants(args: argparse.Namespace) -> None:
    """Estimate and store simulator league constants (point-in-time) per season."""
    from nhl.sim.constants import build
    from nhl.storage.s3 import Store

    build(Store(), [config.season_id(y) for y in config.parse_seasons(args.seasons)])


def cmd_train_starters(args: argparse.Namespace) -> None:
    """Fit and store the starting-goalie model per season; evaluate and write the report."""
    from nhl.pregame import goalies
    from nhl.storage.s3 import Store

    store = Store()
    seasons = [config.season_id(y) for y in config.parse_seasons(args.seasons)]
    model = None
    for season in seasons:
        model = goalies.train(store, season)
        logging.info("starter model %s fitted on %s", season, goalies.train_seasons(season))
    tests = [config.season_id(y) for y in config.parse_seasons(args.evaluate)]
    first = int(str(min(goalies.train_seasons(min(tests))))[:4])
    cand = goalies.build_candidates(store, [config.season_id(y) for y in range(first, int(str(max(tests))[:4]) + 1)])
    results = goalies.evaluate(cand, tests)
    print(results)
    if model is not None:
        goalies.write_report(results, model, args.report)


def cmd_backtest_pregame(args: argparse.Namespace) -> None:
    """M5: backtest with projected lineups and the starter mixture; write the report."""
    from pathlib import Path

    from nhl.pregame import backtest
    from nhl.storage import keys
    from nhl.storage.s3 import Store

    store = Store()
    seasons = [config.season_id(y) for y in config.parse_seasons(args.seasons)]
    results = backtest.run(store, seasons, n_sims=args.sims)
    for season, part in results.partition_by("season", as_dict=True).items():
        store.put_parquet(keys.pregame_backtest(season[0]), part)
    print(backtest.write_report(results, backtest.lineup_accuracy(store, seasons), Path(args.report)))


def cmd_pregame_history(args: argparse.Namespace) -> None:
    """M6: honest pregame prices with score matrices for past seasons."""
    from nhl.pregame import backtest
    from nhl.storage.s3 import Store

    seasons = [config.season_id(y) for y in config.parse_seasons(args.seasons)]
    print(backtest.run_history(Store(), seasons, n_sims=args.sims))


def cmd_props_backtest(args: argparse.Namespace) -> None:
    """M9: player props projections vs baselines on past seasons: goals / assists / points
    (``scoring``, phase B) and shots / blocks / saves (``volume``, phase C)."""
    from pathlib import Path

    from nhl.props import backtest
    from nhl.storage.s3 import Store

    seasons = [config.season_id(y) for y in config.parse_seasons(args.seasons)]
    if args.what in ("scoring", "all"):
        results = backtest.run(Store(), seasons)
        backtest.write_report(results, Path(args.report))
        print(f"report -> {args.report}")
    if args.what in ("volume", "all"):
        from nhl.props import backtest_volume

        results = backtest_volume.run(Store(), seasons)
        backtest_volume.write_report(results, Path(args.volume_report))
        print(f"report -> {args.volume_report}")


def cmd_fit_blend(args: argparse.Namespace) -> None:
    """M6: fit the model/market blend on the pregame history."""
    from nhl.betting import blend
    from nhl.storage.s3 import Store

    out = blend.fit(Store(), [config.season_id(y) for y in config.parse_seasons(args.seasons)])
    for market, segs in out["markets"].items():
        for seg, f in segs.items():
            print(f"{market:9s} {seg:7s} market {f['coef'][1]:.3f}  model {f['coef'][2]:.3f}  (n={f['n']})")


def cmd_edges(args: argparse.Namespace) -> None:
    """M6: edges and stakes for today's games from the latest pregame snapshot and odds."""
    from datetime import date

    from nhl.betting import edges
    from nhl.storage.s3 import Store

    e = edges.run(Store(), date.fromisoformat(args.date) if args.date else None, write=not args.no_write)
    print(edges.render(e))


def cmd_props_edges(args: argparse.Namespace) -> None:
    """M9: player-prop edges and stakes for today's games (adds new paper prop bets)."""
    from datetime import date

    from nhl.props import live
    from nhl.storage.s3 import Store

    e = live.run(Store(), date.fromisoformat(args.date) if args.date else None, write=not args.no_write)
    print(live.render(e))


def cmd_record_bet(args: argparse.Namespace) -> None:
    """M6: record a bet you placed (graded with the paper bets)."""
    from nhl.betting import ledger
    from nhl.storage.s3 import Store

    side = {"home": 1, "over": 1, "away": 2, "under": 2}[args.side]
    bet_id = ledger.record_real(Store(), args.game_id, args.market, side, args.price, args.units, args.book,
                                line=args.line, note=args.note)
    print(f"recorded {bet_id}")


def cmd_grade_bets(args: argparse.Namespace) -> None:
    """M6: grade finished bets (CLV against our captured close, result, units) and summarise."""
    from nhl.betting import ledger
    from nhl.props import ledger as props_ledger
    from nhl.storage.s3 import Store

    store = Store()
    print(f"graded {ledger.grade(store)} bets")
    print(ledger.summary(store))
    # Player props need the night's game logs, so the nightly job grades them after `nhl update`.
    print(f"graded {props_ledger.grade(store)} prop bets")
    print(props_ledger.summary(store))


def cmd_site_tables(args: argparse.Namespace) -> None:
    """Build the site's precomputed ratings boards (also run after every pregame run)."""
    from datetime import date

    from nhl.api.serialize import today_et
    from nhl.site import tables
    from nhl.storage.s3 import Store

    print(tables.build(Store(), date.fromisoformat(args.date) if args.date else today_et()))


def cmd_pregame(args: argparse.Namespace) -> None:
    """M5: project lineups and starters, price today's games, write pregame snapshots."""
    from datetime import date

    from nhl.pregame import price
    from nhl.storage.s3 import Store

    from nhl.pregame import slate

    out = price.run(Store(), date.fromisoformat(args.date) if args.date else None, n_sims=args.sims, write=not args.no_write)
    if out is not None:
        print(slate.render(out.slate, out.freshness))


def cmd_evaluate_deployment(args: argparse.Namespace) -> None:
    """M5 diagnostic: projected vs actual ice time by role; write the report."""
    from pathlib import Path

    from nhl.pregame import evaluate
    from nhl.storage.s3 import Store

    seasons = [config.season_id(y) for y in config.parse_seasons(args.seasons)]
    summary = evaluate.summarize(evaluate.run(Store(), seasons))
    print(evaluate.write_report(summary, seasons, Path(args.report)))


def cmd_evaluate_betting(args: argparse.Namespace) -> None:
    """M6: model vs market on history (information test, blend, bets at the open)."""
    from pathlib import Path

    from nhl.betting import evaluate
    from nhl.storage.s3 import Store

    seasons = [config.season_id(y) for y in config.parse_seasons(args.seasons)]
    print(evaluate.run(Store(), seasons, Path(args.report)))


def cmd_backtest_sim(args: argparse.Namespace) -> None:
    """Backtest the game simulator and write the M4 report."""
    from pathlib import Path

    from nhl.sim.backtest import run, write_report
    from nhl.storage.s3 import Store

    seasons = [config.season_id(y) for y in config.parse_seasons(args.seasons)]
    results = run(Store(), seasons, n_sims=args.sims)
    print(write_report(results, Path(args.report)))


def cmd_game_state(args: argparse.Namespace) -> None:
    """Build stints, lineups, goalie starts, coaches and game logs (M2)."""
    from nhl.gamestate.build import build_season
    from nhl.storage.s3 import Store

    store = Store()
    reports = [build_season(store, year, workers=args.workers) for year in config.parse_seasons(args.seasons)]
    print("\n".join(r.summary() for r in reports))


def cmd_usage(args: argparse.Namespace) -> None:
    """Deployment tiers, TOI by strength, PP/PK units and zone starts per skater-game."""
    from nhl.storage.s3 import Store
    from nhl.usage.tiers import build_season

    store = Store()
    for year in config.parse_seasons(args.seasons):
        checks = build_season(store, config.season_id(year))
        print(config.season_id(year), "  ".join(f"{k} {v:.3f}" for k, v in checks.items()))


def cmd_usage_matchups(args: argparse.Namespace) -> None:
    """Line matchups: matching matrices, intensity, hard minutes and the interaction test."""
    from nhl.storage.s3 import Store
    from nhl.usage import matchups

    res = matchups.run(Store(), [config.season_id(y) for y in config.parse_seasons(args.seasons)])
    print(matchups.write_report(res, args.report))


def cmd_usage_projection(args: argparse.Namespace) -> None:
    """Phase D: does context improve rest-of-season on-ice forecasts?"""
    from nhl.storage.s3 import Store
    from nhl.usage import projection

    _, scores = projection.run(Store(), [config.season_id(y) for y in config.parse_seasons(args.seasons)])
    print(projection.write_report(scores, args.report))


def cmd_onice(args: argparse.Namespace) -> None:
    """5v5 on-ice decomposition per skater-game: own, teammates, competition, context, residual."""
    from nhl.storage.s3 import Store
    from nhl.usage.onice import build_season

    store = Store()
    for year in config.parse_seasons(args.seasons):
        checks = build_season(store, config.season_id(year))
        print(config.season_id(year), "  ".join(f"{k} {v:.4f}" for k, v in checks.items()))


def cmd_style(args: argparse.Namespace) -> None:
    """Style features per skater (archetypes plan phase A): counts, shrunk features, reliability."""
    from nhl.archetypes.features import build_season
    from nhl.storage.s3 import Store

    store = Store()
    for year in config.parse_seasons(args.seasons):
        checks = build_season(store, config.season_id(year))
        print(config.season_id(year), "  ".join(f"{k} {v:.3f}" for k, v in checks.items()))


def cmd_archetypes(args: argparse.Namespace) -> None:
    """Style axes, forward archetypes and comps (archetypes plan phase B); ``--fit`` refits the model."""
    from nhl.archetypes import model as am
    from nhl.storage.s3 import Store

    store = Store()
    if args.fit:
        print("fitted", am.save(store, am.fit(store), args.version))
    mdl = am.load(store, args.version)
    pool = am.comp_pool(store, mdl)
    seasons = [config.season_id(y) for y in config.parse_seasons(args.seasons)]
    for season in seasons:
        checks = am.build_season(store, season, mdl, pool)
        print(season, "  ".join(f"{k} {v:.3f}" for k, v in checks.items()))
    if len(seasons) > 1:
        print("persistence", "  ".join(f"{k} {v:.2f}" for k, v in am.persistence(store, seasons).items()))


def cmd_archetypes_tests(args: argparse.Namespace) -> None:
    """Archetypes phase C: point attribution, line chemistry and aging tests; writes the numbers report."""
    from nhl.archetypes import usefulness
    from nhl.storage.s3 import Store

    res = usefulness.run(Store(), [config.season_id(y) for y in config.parse_seasons(args.seasons)])
    print(usefulness.write_report(res, args.report))


def cmd_validate_game_state(args: argparse.Namespace) -> None:
    """Check the M2 tables against official scores and NHL boxscores; write the report."""
    from pathlib import Path

    from nhl.gamestate.validate import validate, write_report
    from nhl.storage.s3 import Store

    results = validate(Store(), config.parse_seasons(args.seasons), sample_games=args.sample)
    print(write_report(results, Path(args.report)))


def cmd_update(args: argparse.Namespace) -> None:
    """Nightly: catalog -> ingest -> rebuild -> features -> score -> game state -> usage -> ratings snapshot -> on-ice -> style -> archetypes."""
    year = str(args.season or _current_start_year())
    cmd_catalog(argparse.Namespace(seasons=f"{config.FIRST_SEASON}-{year}", force=False))
    cmd_ingest(argparse.Namespace(seasons=year, workers=6, refetch=False, skip_catalog=True))
    cmd_build(argparse.Namespace(seasons=year, workers=8))
    cmd_features(argparse.Namespace(seasons=year))
    cmd_rink_adjust(argparse.Namespace(seasons=year, apply_only=True))
    cmd_score_xg(argparse.Namespace(seasons=year, version=None))
    cmd_xg_monitor(argparse.Namespace(season=int(year), no_write=False))
    cmd_score_freeze(argparse.Namespace(seasons=year))
    cmd_game_state(argparse.Namespace(seasons=year, workers=16))
    cmd_usage(argparse.Namespace(seasons=year))
    cmd_ratings(argparse.Namespace(backfill=None, as_of=None, every=7))
    cmd_onice(argparse.Namespace(seasons=year))
    cmd_style(argparse.Namespace(seasons=year))
    cmd_archetypes(argparse.Namespace(seasons=year, fit=False, version=None))


POLL_TARGETS = ("odds", "props", "props_lowvig", "goalies", "lines", "injuries", "transactions", "officials")
#: Targets whose changes move pregame prices (odds don't: the model never reads them).
REPRICE_TARGETS = ("goalies", "lines", "injuries", "transactions", "officials")


def cmd_serve(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run("nhl.api.main:app", host=args.host, port=args.port, reload=args.reload)


def minutes_to_next_game(games, now: datetime | None = None) -> float | None:
    """Minutes until the next scheduled, not-yet-final game (negative = started already).

    Uses ``start_time_et`` from the games table. Games that started within the last
    20 minutes still count, so a late puck drop keeps the window open.

    Args:
        games: ``processed/games.parquet``.
        now: Current time (defaults to now, America/New_York).

    Returns:
        Minutes to the nearest upcoming start, or None if nothing is scheduled.
    """
    import polars as pl
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    now = (now or datetime.now(et)).astimezone(et).replace(tzinfo=None)
    starts = (
        games.filter(~pl.col("is_final"))
        .select(pl.col("start_time_et").str.to_datetime(strict=False).alias("t"))
        .drop_nulls()
        .with_columns(((pl.col("t") - pl.lit(now)).dt.total_seconds() / 60).alias("m"))
        .filter(pl.col("m") > -20)
    )
    return None if starts.is_empty() else float(starts["m"].min())


def cmd_poll(args: argparse.Namespace) -> None:
    """Poll third-party sources (odds, starting goalies, lines, injuries, transactions,
    officials) once.

    With ``--window N`` the poll only runs when a game starts within N minutes, so cron
    can fire every few minutes and the pollers only work near puck drop. With
    ``--reprice``, any change to a source in :data:`REPRICE_TARGETS` reruns ``nhl pregame``
    for today's games in the same run.
    """
    from nhl.ingest.http import SourceUnavailable
    from nhl.storage import keys
    from nhl.storage.s3 import Store

    store = Store()
    games = store.read_parquet_required(keys.GAMES)
    if args.window is not None:
        mins = minutes_to_next_game(games)
        if mins is None or mins > args.window:
            print(f"no game within {args.window} min (next in {mins if mins is None else round(mins)} min) — skipping")
            return

    targets = POLL_TARGETS if args.what == "all" else tuple(args.what.split(","))
    results: dict[str, str] = {}
    failed = False
    for target in targets:
        try:
            if target == "odds":
                from nhl.sources import espn_odds, fourcasters, lowvig

                n = {"lowvig": lowvig.poll(store, games), "fourcasters": fourcasters.poll(store, games),
                     "espn": espn_odds.poll(store, games)}
                results[target] = ", ".join(f"{k} {v}" for k, v in n.items())
            elif target == "props":
                # Player props from books polled over plain HTTP (DraftKings props come with ESPN odds).
                from nhl.sources import fanduel

                results[target] = f"fanduel {fanduel.poll(store, games)}"
            elif target == "props_lowvig":
                # LowVig/BetOnline props need a headless-browser walk (~minutes), so they poll on their own.
                from nhl.sources import dst

                results[target] = f"lowvig {dst.poll(store, games)}"
            elif target == "goalies":
                from nhl.sources import dailyfaceoff

                results[target] = str(dailyfaceoff.poll_goalies(store, games))
            elif target == "lines":
                from nhl.sources import dailyfaceoff

                results[target] = str(dailyfaceoff.poll_lines(store))
            elif target == "injuries":
                from nhl.sources import espn_injuries

                results[target] = str(espn_injuries.poll_injuries(store))
            elif target == "transactions":
                from nhl.sources import transactions

                results[target] = str(transactions.poll_transactions(store))
            elif target == "officials":
                from nhl.sources import officials

                results[target] = str(officials.poll_assignments(store, games) + officials.poll_right_rail(store, games))
            else:
                raise ValueError(f"unknown poll target {target!r}; choose from {POLL_TARGETS}")
        except SourceUnavailable as exc:  # refusals are skips, not failures
            results[target] = f"skipped ({exc})"
        except Exception as exc:  # noqa: BLE001 - one source failing must not block the rest
            logging.exception("poll %s failed", target)
            results[target] = f"FAILED ({exc!r})"
            failed = True
    print(" | ".join(f"{k}: {v}" for k, v in results.items()))
    changed = [k for k in REPRICE_TARGETS if results.get(k, "").isdigit() and int(results[k]) > 0]
    repriced = False
    if args.reprice and changed:
        from nhl.pregame import price

        try:
            out = price.run(store)
            repriced = out is not None
            print(f"repriced after {', '.join(changed)}: {0 if out is None else out.prices.height} games")
        except Exception:  # noqa: BLE001 - a failed reprice is reported, polls already stored
            logging.exception("reprice failed")
            failed = True
    odds_moved = "odds" in results and any(int(n) > 0 for n in re.findall(r"\b(\d+)\b", results["odds"]))
    props_moved = any(int(n) > 0 for t in ("props", "props_lowvig") for n in re.findall(r"\b(\d+)\b", results.get(t, "")))
    if args.edges and (repriced or odds_moved or props_moved):
        from nhl.props import live as props_live

        try:
            p = props_live.run(store)
            print(f"prop edges: {0 if p.is_empty() else int(p['flagged'].sum())} flagged")
        except Exception:  # noqa: BLE001 - reported; game-line edges still run
            logging.exception("prop edges failed")
            failed = True
    if args.edges and (repriced or odds_moved):
        from nhl.betting import edges

        try:
            e = edges.run(store)
            print(f"edges: {0 if e.is_empty() else int(e['flagged'].sum())} flagged")
        except Exception:  # noqa: BLE001 - reported; polls and prices are already stored
            logging.exception("edges failed")
            failed = True
    if failed:
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser."""
    parser = argparse.ArgumentParser(prog="nhl", description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("catalog", help="refresh games, teams and player bios")
    p.add_argument("--seasons", default=_default_seasons())
    p.add_argument("--force", action="store_true", help="re-download past seasons' bios")
    p.set_defaults(func=cmd_catalog)

    p = sub.add_parser("ingest", help="fetch raw play-by-play + shifts into S3")
    p.add_argument("--seasons", default=_default_seasons(), help='e.g. "2024" or "2010-2025"')
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--refetch", action="store_true", help="re-download games already stored")
    p.add_argument("--skip-catalog", action="store_true", help="use the stored games table")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("build", help="raw -> processed/events/{season}.parquet")
    p.add_argument("--seasons", default=_default_seasons())
    p.add_argument("--workers", type=int, default=8)
    p.set_defaults(func=cmd_build)

    p = sub.add_parser("features", help="events -> processed/shots/{season}.parquet")
    p.add_argument("--seasons", default=_default_seasons())
    p.set_defaults(func=cmd_features)

    p = sub.add_parser("rink-adjust", help="arena scorer-bias adjustment of shot locations")
    p.add_argument("--seasons", default=_default_seasons())
    p.add_argument("--apply-only", action="store_true", help="apply stored maps instead of re-estimating them")
    p.set_defaults(func=cmd_rink_adjust)

    p = sub.add_parser("train-xg", help="tune/evaluate/refit xG models and write historical xG")
    p.add_argument("--train", default="2010-2023", help="season start years used for fitting")
    p.add_argument("--valid", default="2024", help="seasons for early stopping + tuning")
    p.add_argument("--test", default="2025", help="untouched seasons for the final report")
    p.add_argument("--groups", default=None, help="subset of the architecture's model names")
    p.add_argument("--architecture", default="pooled", choices=["pooled", "split", "hybrid", "ev_st"],
                   help="which strength groups share a model (pooled won the 2026-10-05 comparison)")
    p.add_argument("--feature-set", default="v2e", help="feature set from nhl.features.shots.FEATURE_SETS")
    p.add_argument("--trials", type=int, default=40)
    p.add_argument("--timeout", type=int, default=1800, help="EV tuning seconds (others get a third)")
    p.add_argument("--version", default=None)
    p.add_argument("--skip-oof", action="store_true", help="skip out-of-fold historical predictions")
    p.add_argument("--rink-adjusted", action="store_true",
                   help="train on arena scorer-bias-adjusted shot locations (run `nhl rink-adjust` first)")
    p.add_argument("--promote", action="store_true",
                   help="make this version LATEST and rewrite stored xG predictions (default: evaluate only)")
    p.set_defaults(func=cmd_train_xg)

    p = sub.add_parser("score-xg", help="score seasons with the production xG model")
    p.add_argument("--seasons", default=str(_current_start_year()))
    p.add_argument("--version", default=None)
    p.set_defaults(func=cmd_score_xg)

    p = sub.add_parser("poll", help="poll odds / props / goalies / lines / injuries / transactions / officials once")
    p.add_argument("--what", default="all", help=f"comma list of {', '.join(POLL_TARGETS)} or 'all'")
    p.add_argument("--window", type=int, default=None,
                   help="only run if a game starts within this many minutes")
    p.add_argument("--reprice", action="store_true", help="rerun `nhl pregame` when a lineup/goalie/officials source changed")
    p.add_argument("--edges", action="store_true", help="recompute edges (and paper bets) after a reprice or an odds change")
    p.set_defaults(func=cmd_poll)

    p = sub.add_parser("xg-monitor", help="season-to-date xG calibration by strength state")
    p.add_argument("--season", type=int, default=None, help="season start year (default: current)")
    p.add_argument("--no-write", action="store_true", help="don't snapshot the summary to S3")
    p.set_defaults(func=cmd_xg_monitor)

    p = sub.add_parser("train-freeze", help="train the frozen-puck model and write freeze predictions")
    p.add_argument("--train", default="2010-2023")
    p.add_argument("--valid", default="2024")
    p.add_argument("--test", default="2025")
    p.add_argument("--score", default=_default_seasons(), help="seasons to write predictions for")
    p.add_argument("--version", default=None)
    p.set_defaults(func=cmd_train_freeze)

    p = sub.add_parser("score-freeze", help="score seasons with the production frozen-puck model")
    p.add_argument("--seasons", default=str(_current_start_year()))
    p.set_defaults(func=cmd_score_freeze)

    p = sub.add_parser("build-priors", help="season-start priors for every M3 rating model")
    p.add_argument("--seasons", default=f"{config.FIRST_SEASON}-{_current_start_year() - 1}",
                   help="complete seasons to chain (priors are written for the season after each)")
    p.set_defaults(func=cmd_build_priors)

    p = sub.add_parser("ratings", help="point-in-time rating snapshots -> ratings/{date}/")
    p.add_argument("--as-of", default=None, help="YYYY-MM-DD (default today)")
    p.add_argument("--backfill", default=None, help='seasons, e.g. "2015-2025"')
    p.add_argument("--every", type=int, default=7, help="days between backfill snapshots")
    p.set_defaults(func=cmd_ratings)

    p = sub.add_parser("evaluate-ratings", help="M3 bar: rest-of-season prediction vs baselines")
    p.add_argument("--seasons", default="2015-2025")
    p.add_argument("--report", default="docs/reports/m3-evaluation.md")
    p.set_defaults(func=cmd_evaluate_ratings)

    p = sub.add_parser("sim-constants", help="simulator league constants per season (point-in-time)")
    p.add_argument("--seasons", default=f"2016-{_current_start_year()}")
    p.set_defaults(func=cmd_sim_constants)

    p = sub.add_parser("backtest-sim", help="M4 bar: backtest the game simulator vs results and a Poisson baseline")
    p.add_argument("--seasons", default="2016-2025")
    p.add_argument("--sims", type=int, default=1000)
    p.add_argument("--report", default="docs/reports/m4-backtest.md")
    p.set_defaults(func=cmd_backtest_sim)

    p = sub.add_parser("train-starters", help="M5: starting-goalie model per season (+ evaluation report)")
    p.add_argument("--seasons", default=f"2015-{_current_start_year()}", help="seasons to fit a model for")
    p.add_argument("--evaluate", default="2018-2025", help="test seasons for the report")
    p.add_argument("--report", default="docs/reports/m5-starters.md")
    p.set_defaults(func=cmd_train_starters)

    p = sub.add_parser("backtest-pregame", help="M5: backtest with projected lineups and the starter mixture")
    p.add_argument("--seasons", default="2021-2025")
    p.add_argument("--sims", type=int, default=1000)
    p.add_argument("--report", default="docs/reports/m5-pregame-backtest.md")
    p.set_defaults(func=cmd_backtest_pregame)

    p = sub.add_parser("props-backtest", help="M9: player goals/assists/points projections vs baselines")
    p.add_argument("--seasons", default="2016-2025")
    p.add_argument("--what", choices=("scoring", "volume", "all"), default="all")
    p.add_argument("--report", default="docs/reports/props-backtest.md")
    p.add_argument("--volume-report", default="docs/reports/props-volume-backtest.md")
    p.set_defaults(func=cmd_props_backtest)

    p = sub.add_parser("pregame-history", help="M6: honest pregame prices with score matrices for past seasons")
    p.add_argument("--seasons", default="2016-2025")
    p.add_argument("--sims", type=int, default=1000)
    p.set_defaults(func=cmd_pregame_history)

    p = sub.add_parser("fit-blend", help="M6: fit the model/market blend on the pregame history")
    p.add_argument("--seasons", default="2016-2025")
    p.set_defaults(func=cmd_fit_blend)

    p = sub.add_parser("edges", help="M6: edges and stakes for today's games (adds new paper bets)")
    p.add_argument("--date", help="game date (default: today, Eastern)")
    p.add_argument("--no-write", action="store_true", help="don't snapshot or touch the ledger")
    p.set_defaults(func=cmd_edges)

    p = sub.add_parser("props-edges", help="M9: player-prop edges and stakes for today's games (adds paper prop bets)")
    p.add_argument("--date", help="game date (default: today, Eastern)")
    p.add_argument("--no-write", action="store_true", help="don't cache projections, snapshot or touch the ledger")
    p.set_defaults(func=cmd_props_edges)

    p = sub.add_parser("record-bet", help="M6: record a bet you placed")
    p.add_argument("game_id", type=int)
    p.add_argument("market", choices=["moneyline", "puckline", "total"])
    p.add_argument("side", choices=["home", "away", "over", "under"])
    p.add_argument("price", type=float, help="American odds, e.g. -115 or +130")
    p.add_argument("units", type=float, help="stake in units (bankroll = 100)")
    p.add_argument("book")
    p.add_argument("--line", type=float, help="puck line (home handicap, e.g. -1.5) or total")
    p.add_argument("--note")
    p.set_defaults(func=cmd_record_bet)

    p = sub.add_parser("grade-bets", help="M6: grade finished bets and print the ledger summary")
    p.set_defaults(func=cmd_grade_bets)

    p = sub.add_parser("pregame", help="M5: project lineups/starters and price today's games (snapshots)")
    p.add_argument("--date", help="game date (default: today, Eastern)")
    p.add_argument("--sims", type=int, default=4000)
    p.add_argument("--no-write", action="store_true", help="don't write snapshots")
    p.set_defaults(func=cmd_pregame)

    p = sub.add_parser("evaluate-deployment", help="M5: projected vs actual ice time by role")
    p.add_argument("--seasons", default="2023-2025")
    p.add_argument("--report", default="docs/reports/m5-deployment.md")
    p.set_defaults(func=cmd_evaluate_deployment)

    p = sub.add_parser("evaluate-betting", help="M6: model vs closing market, blend, bets at the open")
    p.add_argument("--seasons", default="2016-2025")
    p.add_argument("--report", default="docs/reports/m6-model-vs-market.md")
    p.set_defaults(func=cmd_evaluate_betting)

    p = sub.add_parser("game-state", help="stints, lineups, goalie starts, coaches, game logs (M2)")
    p.add_argument("--seasons", default=_default_seasons())
    p.add_argument("--workers", type=int, default=16, help="parallel reads of per-game raw payloads")
    p.set_defaults(func=cmd_game_state)

    p = sub.add_parser("usage", help="deployment tiers and usage per skater-game (usage plan phase A)")
    p.add_argument("--seasons", default=_default_seasons())
    p.set_defaults(func=cmd_usage)

    p = sub.add_parser("usage-matchups", help="line matchup study and per-team matching tables (usage plan phase C)")
    p.add_argument("--seasons", default=f"2015-{_current_start_year() - 1}")
    p.add_argument("--report", default="docs/reports/usage-matchups-numbers.md")
    p.set_defaults(func=cmd_usage_matchups)

    p = sub.add_parser("usage-projection", help="rest-of-season on-ice forecast test (usage plan phase D)")
    p.add_argument("--seasons", default=f"2015-{_current_start_year() - 1}")
    p.add_argument("--report", default="docs/reports/usage-projection-numbers.md")
    p.set_defaults(func=cmd_usage_projection)

    p = sub.add_parser("onice", help="5v5 on-ice decomposition and QoT/QoC per skater-game (usage plan phase B)")
    p.add_argument("--seasons", default=_default_seasons())
    p.set_defaults(func=cmd_onice)

    p = sub.add_parser("style", help="style features per skater, shrunk, with split-half reliability (archetypes phase A)")
    p.add_argument("--seasons", default=_default_seasons())
    p.set_defaults(func=cmd_style)

    p = sub.add_parser("archetypes", help="style axes, forward archetypes and style comps (archetypes phase B)")
    p.add_argument("--seasons", default=_default_seasons())
    p.add_argument("--fit", action="store_true", help="refit the model on the pooled fit seasons first")
    p.add_argument("--version", default=None, help="model version to fit as / assign with (default LATEST)")
    p.set_defaults(func=cmd_archetypes)

    p = sub.add_parser("archetypes-tests", help="point attribution, line chemistry and aging tests (archetypes phase C)")
    p.add_argument("--seasons", default=f"2011-{_current_start_year() - 1}")
    p.add_argument("--report", default="docs/reports/archetypes-numbers.md")
    p.set_defaults(func=cmd_archetypes_tests)

    p = sub.add_parser("validate-game-state", help="validate M2 tables vs official scores and boxscores")
    p.add_argument("--seasons", default=_default_seasons())
    p.add_argument("--sample", type=int, default=20, help="boxscore-checked games per season (0 to skip)")
    p.add_argument("--report", default="docs/reports/m2-validation.md")
    p.set_defaults(func=cmd_validate_game_state)

    p = sub.add_parser("update", help="nightly incremental update of the current season")
    p.add_argument("--season", type=int, default=None)
    p.set_defaults(func=cmd_update)

    p = sub.add_parser("site-tables", help="precompute the site's ratings boards -> site/ratings/")
    p.add_argument("--date", default=None, help="YYYY-MM-DD (default today, Eastern)")
    p.set_defaults(func=cmd_site_tables)

    p = sub.add_parser("serve", help="run the site API (frontend/ talks to it)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8010)
    p.add_argument("--reload", action="store_true", help="restart on code changes")
    p.set_defaults(func=cmd_serve)

    return parser


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and dispatch."""
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    args.func(args)


if __name__ == "__main__":
    main()
