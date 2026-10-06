# M4 backtest: game simulator

Generated 2026-10-05 by `nhl backtest-sim`. Actual lineups, point-in-time ratings (latest snapshot before each game) and constants (three prior seasons). Calibration (pace 1.0, strength 1.2, game shock 0.3) tuned on 2016-17..2019-20 only; 2020-21..2025-26 are out of sample.

## Overall

| metric | value |
|---|---|
| games | 13187.0000 |
| logloss_sim | 0.6688 |
| logloss_poisson | 0.6739 |
| brier_sim | 0.2382 |
| margin2_sim | 0.5669 |
| margin2_actual | 0.5844 |
| ot_sim | 0.2117 |
| ot_actual | 0.2249 |
| goals_sim | 5.9938 |
| goals_actual | 6.0541 |
| over_5.5_sim | 0.5216 |
| over_5.5_actual | 0.5403 |
| logloss_over_5.5_sim | 0.6835 |
| logloss_over_5.5_poisson | 0.6857 |
| over_6.5_sim | 0.4175 |
| over_6.5_actual | 0.4300 |
| logloss_over_6.5_sim | 0.6757 |
| logloss_over_6.5_poisson | 0.6826 |
| logloss_home_puckline_sim | 0.6154 |
| max_calibration_error | 0.0295 |

## By season

| season | games | logloss_sim | logloss_poisson | margin2_sim | margin2_actual | ot_sim | ot_actual | goals_sim | goals_actual |
|---|---|---|---|---|---|---|---|---|---|
| 20162017 | 1317 | 0.6715 | 0.6768 | 0.5219 | 0.5292 | 0.2224 | 0.2399 | 5.4245 | 5.5065 |
| 20172018 | 1355 | 0.6782 | 0.6820 | 0.5426 | 0.5779 | 0.2135 | 0.2258 | 5.7345 | 5.9395 |
| 20182019 | 1358 | 0.6773 | 0.6818 | 0.5601 | 0.5876 | 0.2107 | 0.2121 | 5.9788 | 6.0007 |
| 20192020 | 1212 | 0.6815 | 0.6875 | 0.5554 | 0.5677 | 0.2163 | 0.2294 | 5.9690 | 5.9777 |
| 20202021 | 952 | 0.6567 | 0.6719 | 0.5566 | 0.5651 | 0.2182 | 0.2332 | 5.8360 | 5.8361 |
| 20212022 | 1401 | 0.6506 | 0.6628 | 0.5733 | 0.6117 | 0.2036 | 0.2163 | 6.0179 | 6.2912 |
| 20222023 | 1400 | 0.6623 | 0.6628 | 0.5873 | 0.5929 | 0.2015 | 0.2329 | 6.3034 | 6.3543 |
| 20232024 | 1400 | 0.6591 | 0.6630 | 0.5959 | 0.6107 | 0.2029 | 0.2057 | 6.2786 | 6.1943 |
| 20242025 | 1398 | 0.6661 | 0.6707 | 0.5807 | 0.6216 | 0.2169 | 0.2082 | 6.1491 | 6.0887 |
| 20252026 | 1394 | 0.6836 | 0.6816 | 0.5864 | 0.5674 | 0.2144 | 0.2496 | 6.1510 | 6.2346 |

## Regular season vs playoffs

| type | games | logloss_sim | logloss_poisson | margin2_sim | margin2_actual | ot_sim | ot_actual | goals_sim | goals_actual |
|---|---|---|---|---|---|---|---|---|---|
| R | 12282 | 0.6674 | 0.6730 | 0.5675 | 0.5861 | 0.2113 | 0.2247 | 6.0008 | 6.0735 |
| P | 905 | 0.6891 | 0.6868 | 0.5582 | 0.5613 | 0.2177 | 0.2276 | 5.8995 | 5.7901 |

## Calibration (P(home win), all seasons)

| bin | games | predicted | actual |
|---|---|---|---|
| (-inf, 0.3] | 96 | 0.267 | 0.260 |
| (0.3, 0.4] | 887 | 0.364 | 0.356 |
| (0.4, 0.45] | 1215 | 0.429 | 0.413 |
| (0.45, 0.5] | 2073 | 0.477 | 0.479 |
| (0.5, 0.55] | 2664 | 0.527 | 0.520 |
| (0.55, 0.6] | 2544 | 0.575 | 0.572 |
| (0.6, 0.7] | 3052 | 0.641 | 0.634 |
| (0.7, inf] | 656 | 0.739 | 0.768 |

**Bar** ([M4 plan](../plans/m4-simulator.md)): calibration within 2 points in bins with ≥100 games; 2+ goal margins, OT rate and total goals within sampling error; moneyline log loss below the Poisson baseline.
