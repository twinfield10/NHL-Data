# M4 backtest: game simulator

Generated 2026-10-05 by `nhl backtest-sim`. Actual lineups, point-in-time ratings (latest snapshot before each game) and constants (three prior seasons). Calibration (pace 1.0, strength 1.2, game shock 0.3) and the team terms (xG residual, 30 h; defence goals residual, 500 expected goals) tuned on 2016-17..2019-20 only; 2020-21..2025-26 are out of sample.

## Overall

| metric | value |
|---|---|
| games | 13187.0000 |
| logloss_sim | 0.6675 |
| logloss_poisson | 0.6739 |
| brier_sim | 0.2375 |
| margin2_sim | 0.5699 |
| margin2_actual | 0.5844 |
| ot_sim | 0.2099 |
| ot_actual | 0.2249 |
| goals_sim | 6.0490 |
| goals_actual | 6.0541 |
| over_5.5_sim | 0.5304 |
| over_5.5_actual | 0.5403 |
| logloss_over_5.5_sim | 0.6833 |
| logloss_over_5.5_poisson | 0.6857 |
| over_6.5_sim | 0.4263 |
| over_6.5_actual | 0.4300 |
| logloss_over_6.5_sim | 0.6755 |
| logloss_over_6.5_poisson | 0.6826 |
| logloss_home_puckline_sim | 0.6144 |
| max_calibration_error | 0.0237 |

## By season

| season | games | logloss_sim | logloss_poisson | margin2_sim | margin2_actual | ot_sim | ot_actual | goals_sim | goals_actual |
|---|---|---|---|---|---|---|---|---|---|
| 20162017 | 1317 | 0.6710 | 0.6768 | 0.5252 | 0.5292 | 0.2203 | 0.2399 | 5.4804 | 5.5065 |
| 20172018 | 1355 | 0.6782 | 0.6820 | 0.5531 | 0.5779 | 0.2068 | 0.2258 | 5.9587 | 5.9395 |
| 20182019 | 1358 | 0.6753 | 0.6818 | 0.5700 | 0.5876 | 0.2042 | 0.2121 | 6.2112 | 6.0007 |
| 20192020 | 1212 | 0.6817 | 0.6875 | 0.5596 | 0.5677 | 0.2134 | 0.2294 | 6.0706 | 5.9777 |
| 20202021 | 952 | 0.6565 | 0.6719 | 0.5500 | 0.5651 | 0.2228 | 0.2332 | 5.6855 | 5.8361 |
| 20212022 | 1401 | 0.6470 | 0.6628 | 0.5791 | 0.6117 | 0.2002 | 0.2163 | 6.1002 | 6.2912 |
| 20222023 | 1400 | 0.6605 | 0.6628 | 0.5985 | 0.5929 | 0.1940 | 0.2329 | 6.5507 | 6.3543 |
| 20232024 | 1400 | 0.6584 | 0.6630 | 0.5921 | 0.6107 | 0.2058 | 0.2057 | 6.1553 | 6.1943 |
| 20242025 | 1398 | 0.6628 | 0.6707 | 0.5717 | 0.6216 | 0.2236 | 0.2082 | 5.9216 | 6.0887 |
| 20252026 | 1394 | 0.6824 | 0.6816 | 0.5891 | 0.5674 | 0.2123 | 0.2496 | 6.2109 | 6.2346 |

## Regular season vs playoffs

| type | games | logloss_sim | logloss_poisson | margin2_sim | margin2_actual | ot_sim | ot_actual | goals_sim | goals_actual |
|---|---|---|---|---|---|---|---|---|---|
| R | 12282 | 0.6659 | 0.6730 | 0.5707 | 0.5861 | 0.2094 | 0.2247 | 6.0559 | 6.0735 |
| P | 905 | 0.6880 | 0.6868 | 0.5601 | 0.5613 | 0.2165 | 0.2276 | 5.9546 | 5.7901 |

## Calibration (P(home win), all seasons)

| bin | games | predicted | actual |
|---|---|---|---|
| (-inf, 0.3] | 168 | 0.267 | 0.262 |
| (0.3, 0.4] | 903 | 0.364 | 0.355 |
| (0.4, 0.45] | 1295 | 0.428 | 0.433 |
| (0.45, 0.5] | 1992 | 0.477 | 0.463 |
| (0.5, 0.55] | 2540 | 0.526 | 0.530 |
| (0.55, 0.6] | 2490 | 0.574 | 0.562 |
| (0.6, 0.7] | 3059 | 0.642 | 0.638 |
| (0.7, inf] | 740 | 0.744 | 0.768 |

**Bar** ([M4 plan](../plans/m4-simulator.md)): calibration within 2 points in bins with ≥100 games; 2+ goal margins, OT rate and total goals within sampling error; moneyline log loss below the Poisson baseline.
