# M5 pregame backtest

M4 engine with point-in-time ratings. `actual`: lineups and starters as played (the M4 bar). `lineup`: projected lineups (last game + transactions; ESPN injury history doesn't exist before 2026-10-05, so this is the pessimistic case). `goalie`: the starter mixture. `pregame`: both.

## All seasons

| variant | games | moneyline LL | Poisson LL | Brier | max cal. err | O/U 5.5 LL | O/U 6.5 LL | puck line LL |
|---|---|---|---|---|---|---|---|---|
| actual | 6993 | 0.6620 | 0.6682 | 0.2350 | 0.029 | 0.6807 | 0.6864 | 0.6115 |
| lineup | 6993 | 0.6607 | 0.6682 | 0.2344 | 0.024 | 0.6806 | 0.6865 | 0.6097 |
| goalie | 6993 | 0.6625 | 0.6682 | 0.2352 | 0.026 | 0.6808 | 0.6868 | 0.6120 |
| pregame | 6993 | 0.6612 | 0.6682 | 0.2346 | 0.033 | 0.6807 | 0.6870 | 0.6104 |

## Moneyline log loss by season

| season | actual | lineup | goalie | pregame | Poisson |
|---|---|---|---|---|---|
| 20212022 | 0.6472 | 0.6468 | 0.6483 | 0.6478 | 0.6628 |
| 20222023 | 0.6599 | 0.6564 | 0.6601 | 0.6565 | 0.6628 |
| 20232024 | 0.6585 | 0.6576 | 0.6587 | 0.6581 | 0.6630 |
| 20242025 | 0.6625 | 0.6619 | 0.6632 | 0.6626 | 0.6707 |
| 20252026 | 0.6820 | 0.6809 | 0.6821 | 0.6809 | 0.6816 |

## Lineup projection accuracy

Share of actually dressed skaters that were projected (`weighted`: weighted by their 5v5 share).

| season | method | found | weighted | all 18 right |
|---|---|---|---|---|
| 20212022 | last game + events | 0.924 | 0.934 | 0.262 |
| 20212022 | last game only | 0.927 | 0.935 | 0.276 |
| 20222023 | last game + events | 0.939 | 0.946 | 0.320 |
| 20222023 | last game only | 0.942 | 0.949 | 0.339 |
| 20232024 | last game + events | 0.937 | 0.945 | 0.310 |
| 20232024 | last game only | 0.940 | 0.947 | 0.328 |
| 20242025 | last game + events | 0.945 | 0.952 | 0.358 |
| 20242025 | last game only | 0.942 | 0.949 | 0.348 |
| 20252026 | last game + events | 0.940 | 0.948 | 0.322 |
| 20252026 | last game only | 0.938 | 0.946 | 0.317 |
