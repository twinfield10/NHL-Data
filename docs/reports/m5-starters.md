# M5 starting-goalie model

Conditional logit over each team-game's candidates (goalies dressed in the last 10 team games plus `other`), fitted on the 4 seasons before each test season. Bar: log loss below "last game's starter".

## Log loss (per team-game)

| season | model | last_starter_b2b | last_starter |
|---|---|---|---|
| 20182019 | 0.6788 | 0.8509 | 0.8976 |
| 20192020 | 0.7021 | 0.8615 | 0.9019 |
| 20202021 | 0.7927 | 0.9871 | 1.0289 |
| 20212022 | 0.7877 | 0.9586 | 1.0056 |
| 20222023 | 0.7631 | 0.9342 | 0.9837 |
| 20232024 | 0.7169 | 0.9007 | 0.9522 |
| 20242025 | 0.7071 | 0.9022 | 0.9591 |
| 20252026 | 0.7079 | 0.8831 | 0.9393 |

## Top-pick accuracy

| season | model | last_starter_b2b | last_starter |
|---|---|---|---|
| 20182019 | 0.705 | 0.620 | 0.550 |
| 20192020 | 0.671 | 0.578 | 0.492 |
| 20202021 | 0.648 | 0.550 | 0.485 |
| 20212022 | 0.663 | 0.564 | 0.499 |
| 20222023 | 0.667 | 0.552 | 0.472 |
| 20232024 | 0.677 | 0.537 | 0.449 |
| 20242025 | 0.702 | 0.547 | 0.452 |
| 20252026 | 0.686 | 0.536 | 0.435 |

## Coefficients (latest model)

| feature | coef |
|---|---|
| is_other | -2.415 |
| share_5 | +1.301 |
| share_10 | +0.291 |
| share_30 | +0.757 |
| started_last | -0.349 |
| streak | -0.148 |
| b2b_2nd | +0.398 |
| b2b_2nd_x_last | -2.371 |
| b2b_1st_x_last | -0.063 |
| log_rest | -0.061 |
| long_absence | -0.915 |
| starts_7d | +0.141 |
| quality | +0.981 |
| playoff_x_last | +1.245 |
| playoff_x_share | +0.356 |
| home_x_share | +0.501 |
| opp_x_share | +0.446 |
