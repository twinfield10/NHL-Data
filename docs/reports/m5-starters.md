# M5 starting-goalie model

Conditional logit over each team-game's candidates (goalies dressed in the last 10 team games plus `other`), fitted on the 4 seasons before each test season. Bar: log loss below "last game's starter".

## Log loss (per team-game)

| season | model | last_starter_b2b | last_starter |
|---|---|---|---|
| 20182019 | 0.6623 | 0.8364 | 0.8829 |
| 20192020 | 0.6822 | 0.8445 | 0.8853 |
| 20202021 | 0.7678 | 0.9692 | 1.0108 |
| 20212022 | 0.7577 | 0.9406 | 0.9872 |
| 20222023 | 0.7368 | 0.9161 | 0.9655 |
| 20232024 | 0.7037 | 0.8894 | 0.9407 |
| 20242025 | 0.6838 | 0.8822 | 0.9382 |
| 20252026 | 0.6961 | 0.8700 | 0.9258 |

## Top-pick accuracy

| season | model | last_starter_b2b | last_starter |
|---|---|---|---|
| 20182019 | 0.704 | 0.622 | 0.550 |
| 20192020 | 0.674 | 0.583 | 0.493 |
| 20202021 | 0.664 | 0.554 | 0.485 |
| 20212022 | 0.676 | 0.567 | 0.499 |
| 20222023 | 0.682 | 0.556 | 0.473 |
| 20232024 | 0.684 | 0.539 | 0.449 |
| 20242025 | 0.711 | 0.550 | 0.452 |
| 20252026 | 0.692 | 0.538 | 0.435 |

## Coefficients (latest model)

| feature | coef |
|---|---|
| is_other | -2.669 |
| share_5 | +1.052 |
| share_10 | +0.294 |
| share_30 | +0.847 |
| started_last | -0.463 |
| streak | -0.145 |
| b2b_2nd | +0.411 |
| b2b_2nd_x_last | -2.465 |
| b2b_1st_x_last | -0.061 |
| log_rest | -0.188 |
| long_absence | -0.933 |
| starts_7d | +0.173 |
| quality | +0.980 |
| playoff_x_last | +1.249 |
| playoff_x_share | +0.387 |
| home_x_share | +0.510 |
| opp_x_share | +0.454 |
