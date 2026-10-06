# M4 backtest: game simulator

Generated 2026-10-06 by `nhl backtest-sim`. Actual lineups, point-in-time ratings (latest snapshot before each game) and constants (three prior seasons). Calibration (pace 1.0, strength 1.2, game shock 0.3), team terms (xG residual 30 h; defence goals residual 500 expected goals) and score-state multipliers (lead x period / 3rd-period time left, from the three prior seasons) tuned or estimated on data before each season; 2020-21..2025-26 are out of sample.

## Overall

| metric | value |
|---|---|
| games | 13187.0000 |
| logloss_sim | 0.6673 |
| logloss_poisson | 0.6739 |
| brier_sim | 0.2374 |
| margin2_sim | 0.5769 |
| margin2_actual | 0.5844 |
| ot_sim | 0.2110 |
| ot_actual | 0.2249 |
| goals_sim | 6.0445 |
| goals_actual | 6.0541 |
| over_5.5_sim | 0.5297 |
| over_5.5_actual | 0.5403 |
| logloss_over_5.5_sim | 0.6834 |
| logloss_over_5.5_poisson | 0.6857 |
| over_6.5_sim | 0.4255 |
| over_6.5_actual | 0.4300 |
| logloss_over_6.5_sim | 0.6754 |
| logloss_over_6.5_poisson | 0.6826 |
| logloss_home_puckline_sim | 0.6142 |
| max_calibration_error | 0.0235 |

## By season

| season | games | logloss_sim | logloss_poisson | margin2_sim | margin2_actual | ot_sim | ot_actual | goals_sim | goals_actual |
|---|---|---|---|---|---|---|---|---|---|
| 20162017 | 1317 | 0.6708 | 0.6768 | 0.5314 | 0.5292 | 0.2221 | 0.2399 | 5.4742 | 5.5065 |
| 20172018 | 1355 | 0.6779 | 0.6820 | 0.5515 | 0.5779 | 0.2133 | 0.2258 | 5.9623 | 5.9395 |
| 20182019 | 1358 | 0.6758 | 0.6818 | 0.5682 | 0.5876 | 0.2111 | 0.2121 | 6.2150 | 6.0007 |
| 20192020 | 1212 | 0.6816 | 0.6875 | 0.5742 | 0.5677 | 0.2103 | 0.2294 | 6.0658 | 5.9777 |
| 20202021 | 952 | 0.6558 | 0.6719 | 0.5611 | 0.5651 | 0.2166 | 0.2332 | 5.6782 | 5.8361 |
| 20212022 | 1401 | 0.6472 | 0.6628 | 0.5840 | 0.6117 | 0.2016 | 0.2163 | 6.0926 | 6.2912 |
| 20222023 | 1400 | 0.6599 | 0.6628 | 0.6034 | 0.5929 | 0.1962 | 0.2329 | 6.5431 | 6.3543 |
| 20232024 | 1400 | 0.6585 | 0.6630 | 0.5965 | 0.6107 | 0.2127 | 0.2057 | 6.1540 | 6.1943 |
| 20242025 | 1398 | 0.6625 | 0.6707 | 0.5875 | 0.6216 | 0.2191 | 0.2082 | 5.9122 | 6.0887 |
| 20252026 | 1394 | 0.6820 | 0.6816 | 0.6018 | 0.5674 | 0.2095 | 0.2496 | 6.2021 | 6.2346 |

## Regular season vs playoffs

| type | games | logloss_sim | logloss_poisson | margin2_sim | margin2_actual | ot_sim | ot_actual | goals_sim | goals_actual |
|---|---|---|---|---|---|---|---|---|---|
| R | 12282 | 0.6658 | 0.6730 | 0.5775 | 0.5861 | 0.2106 | 0.2247 | 6.0514 | 6.0735 |
| P | 905 | 0.6883 | 0.6868 | 0.5681 | 0.5613 | 0.2167 | 0.2276 | 5.9511 | 5.7901 |

## Calibration (P(home win), all seasons)

| bin | games | predicted | actual |
|---|---|---|---|
| (-inf, 0.3] | 165 | 0.266 | 0.255 |
| (0.3, 0.4] | 936 | 0.364 | 0.361 |
| (0.4, 0.45] | 1282 | 0.428 | 0.431 |
| (0.45, 0.5] | 1984 | 0.477 | 0.465 |
| (0.5, 0.55] | 2553 | 0.526 | 0.530 |
| (0.55, 0.6] | 2444 | 0.574 | 0.564 |
| (0.6, 0.7] | 3067 | 0.642 | 0.635 |
| (0.7, inf] | 756 | 0.744 | 0.767 |

**Bar** ([M4 plan](../plans/m4-simulator.md)): calibration within 2 points in bins with ≥100 games; 2+ goal margins, OT rate and total goals within sampling error; moneyline log loss below the Poisson baseline.
