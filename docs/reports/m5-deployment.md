# M5 deployment accuracy (projected vs actual ice time)

Seasons 20232024, 20242025, 20252026. Projections as of the morning, last game + transactions (no DailyFaceoff history). Minutes = share × the team's actual skater time in the state.

## End to end, per player-game (minutes)

| state | player_games | mean_actual_min | mae_min | mae_min_dressed_and_projected |
|---|---|---|---|---|
| s5 | 159768 | 13.556 | 2.630 | 1.467 |
| spp | 114893 | 2.040 | 0.480 | 0.621 |
| spk | 108882 | 1.715 | 0.456 | 0.581 |

## 5v5 by role in the previous game (projected players; `none` = no role last game, mostly projected players who didn't dress)

| role_ev | n | actual_min | proj_min | bias_min | mae_min |
|---|---|---|---|---|---|
| D1 | 16294 | 17.746 | 17.644 | -0.103 | 1.526 |
| D2 | 16021 | 16.713 | 16.578 | -0.135 | 1.567 |
| D3 | 14547 | 14.477 | 14.573 | 0.096 | 1.682 |
| F1 | 24619 | 13.923 | 13.813 | -0.110 | 1.326 |
| F2 | 24328 | 12.931 | 12.889 | -0.042 | 1.347 |
| F3 | 23805 | 11.976 | 11.961 | -0.015 | 1.386 |
| F4 | 20564 | 10.367 | 10.500 | 0.133 | 1.493 |
| none | 10613 | 1.577 | 11.842 | 10.265 | 10.468 |

## Power play by previous unit

| role_pp | n | actual_min | proj_min | bias_min | mae_min |
|---|---|---|---|---|---|
| PP1 | 39193 | 3.066 | 3.087 | 0.021 | 0.654 |
| PP2 | 34263 | 1.542 | 1.624 | 0.082 | 0.596 |
| none | 23872 | 0.405 | 0.836 | 0.431 | 0.701 |

## Penalty kill by previous unit

| role_pk | n | actual_min | proj_min | bias_min | mae_min |
|---|---|---|---|---|---|
| PK1 | 30939 | 2.204 | 2.326 | 0.122 | 0.638 |
| PK2 | 23257 | 1.731 | 1.790 | 0.059 | 0.569 |
| none | 38238 | 0.856 | 1.123 | 0.267 | 0.639 |

## Share predictors on dressed players (MAE, minutes)

| state | last | last10 | ewma | season |
|---|---|---|---|---|
| s5 | 1.854 | 1.527 | 1.487 | 1.550 |
| spp | 0.708 | 0.579 | 0.578 | 0.597 |
| spk | 0.721 | 0.549 | 0.548 | 0.563 |

## Team 5v5 offence composite (what moves a price)

Σ share × EV offence rating per team-game, ×5 (xG/60 units). `sd_error` vs `sd_actual_composite` says how much deployment error blurs team strength.

| team_games | sd_actual_composite | sd_error | mean_error | corr |
|---|---|---|---|---|
| 8384 | 0.210 | 0.066 | -0.001 | 0.950 |
