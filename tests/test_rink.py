from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from nhl.features import rink


def _shots(rng, n_per_team=400):
    """Two arenas; visiting teams' shots at arena 'biased' are recorded 5 ft closer."""
    rows = []
    for season in (20152016, 20222023):
        for arena, home in (("fair", "AAA"), ("biased", "BBB"), ("other", "CCC")):
            for team in ("T1", "T2", "T3"):
                d = rng.gamma(4.0, 9.0, n_per_team)
                if arena == "biased":
                    d = np.clip(d - 5.0, 0.5, None)
                for i, dist in enumerate(d):
                    rows.append(dict(game_id=hash((season, arena, team, i // 20)) % 10**9, season=season, arena_id=arena,
                                     event_team_abbr=team, is_home=0.0, strength_group="EV", event_distance=float(dist),
                                     event_angle=float(rng.uniform(0, 80))))
    df = pl.DataFrame(rows)
    venues = df.select("game_id", "arena_id").unique()
    return df.drop("arena_id"), venues


def test_biased_arena_shifted_back_in_scorer_era_only(monkeypatch):
    monkeypatch.setattr(rink, "MIN_SHOTS", 100)
    shots, venues = _shots(np.random.default_rng(1))
    maps = rink.estimate_maps(shots, venues)
    biased = maps.filter((pl.col("arena_id") == "biased") & (pl.col("season") == 20152016)).row(0, named=True)
    median_shift = (biased["dist_to"][25] - biased["dist_from"][25]) * biased["shrink"]
    assert median_shift == pytest.approx(5.0 * biased["shrink"], abs=1.5)
    fair = maps.filter((pl.col("arena_id") == "fair") & (pl.col("season") == 20152016)).row(0, named=True)
    assert abs((fair["dist_to"][25] - fair["dist_from"][25]) * fair["shrink"]) < 1.5

    full = shots.with_columns(
        pl.lit(None, pl.Float32).alias(c) for c in ("x_abs", "y_abs", "x_abs_last", "y_abs_last", "distance_from_last",
                                                  "event_angle_change", "event_angle_change_speed",
                                                  "puck_speed_since_last", "off_wing", "shooter_shoots_r")
    ).with_columns(pl.lit(5.0).alias("y_abs"), pl.lit(2.0).alias("seconds_since_last"))
    adj = rink.apply_maps(full, venues, maps)
    tracked = adj.filter(pl.col("season") == 20222023)
    assert (tracked["event_distance"] - tracked["event_distance_raw"]).abs().max() < 1e-4  # tracking era untouched (float32 round-trip)
    scorer = adj.filter(pl.col("season") == 20152016).join(venues, on="game_id").filter(pl.col("arena_id") == "biased")
    assert scorer["rink_shift_ft"].mean() > 2.5
