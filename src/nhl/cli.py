"""Command-line entry point: ``nhl <command> [options]`` (or ``python -m nhl``)."""

from __future__ import annotations

import argparse
import logging
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


def cmd_update(args: argparse.Namespace) -> None:
    """Nightly: catalog -> ingest new games -> rebuild current season -> features -> score."""
    year = str(args.season or _current_start_year())
    cmd_catalog(argparse.Namespace(seasons=f"{config.FIRST_SEASON}-{year}", force=False))
    cmd_ingest(argparse.Namespace(seasons=year, workers=6, refetch=False, skip_catalog=True))
    cmd_build(argparse.Namespace(seasons=year, workers=8))
    cmd_features(argparse.Namespace(seasons=year))
    cmd_rink_adjust(argparse.Namespace(seasons=year, apply_only=True))
    cmd_score_xg(argparse.Namespace(seasons=year, version=None))


POLL_TARGETS = ("odds", "goalies", "lines", "injuries")


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
    """Poll third-party sources (odds, starting goalies, lines, injuries) once.

    With ``--window N`` the poll only runs when a game starts within N minutes, so cron
    can fire every few minutes and the pollers only work near puck drop.
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
            elif target == "goalies":
                from nhl.sources import dailyfaceoff

                results[target] = str(dailyfaceoff.poll_goalies(store, games))
            elif target == "lines":
                from nhl.sources import dailyfaceoff

                results[target] = str(dailyfaceoff.poll_lines(store))
            elif target == "injuries":
                from nhl.sources import espn_injuries

                results[target] = str(espn_injuries.poll_injuries(store))
            else:
                raise ValueError(f"unknown poll target {target!r}; choose from {POLL_TARGETS}")
        except SourceUnavailable as exc:  # refusals are skips, not failures
            results[target] = f"skipped ({exc})"
        except Exception as exc:  # noqa: BLE001 - one source failing must not block the rest
            logging.exception("poll %s failed", target)
            results[target] = f"FAILED ({exc!r})"
            failed = True
    print(" | ".join(f"{k}: {v}" for k, v in results.items()))
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
    p.add_argument("--feature-set", default="v1", help="feature set from nhl.features.shots.FEATURE_SETS")
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

    p = sub.add_parser("poll", help="poll odds / starting goalies / lines / injuries once")
    p.add_argument("--what", default="all", help=f"comma list of {', '.join(POLL_TARGETS)} or 'all'")
    p.add_argument("--window", type=int, default=None,
                   help="only run if a game starts within this many minutes")
    p.set_defaults(func=cmd_poll)

    p = sub.add_parser("update", help="nightly incremental update of the current season")
    p.add_argument("--season", type=int, default=None)
    p.set_defaults(func=cmd_update)

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
