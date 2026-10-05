# M3 evaluation: rest-of-season prediction

Generated 2026-10-05 by `nhl evaluate-ratings`; seasons 20152016-20252026, cutoffs Nov 15, Jan 1 and Feb 15 (when inside the season). Each team's 5v5 xG for/against per 60 after the cutoff, predicted on the stints actually played. See `nhl.ratings.evaluate`.

## All cutoffs

| model | n | rmse_xgf60 | rmse_xga60 | rmse_diff | corr_diff | corr_goal_diff |
|---|---|---|---|---|---|---|
| ratings | 845 | 0.160 | 0.177 | 0.242 | 0.749 | 0.396 |
| last_season | 845 | 0.164 | 0.184 | 0.258 | 0.710 | 0.360 |
| team_to_date | 845 | 0.197 | 0.201 | 0.285 | 0.685 | 0.464 |
| league | 845 | 0.218 | 0.229 | 0.365 | nan | nan |

## Nov 15

| model | n | rmse_xgf60 | rmse_xga60 | rmse_diff | corr_diff | corr_goal_diff |
|---|---|---|---|---|---|---|
| ratings | 188 | 0.158 | 0.168 | 0.240 | 0.689 | 0.379 |
| last_season | 188 | 0.155 | 0.169 | 0.242 | 0.678 | 0.377 |
| league | 188 | 0.200 | 0.202 | 0.329 | nan | nan |
| team_to_date | 188 | 0.233 | 0.227 | 0.340 | 0.595 | 0.389 |

## Jan 1

| model | n | rmse_xgf60 | rmse_xga60 | rmse_diff | corr_diff | corr_goal_diff |
|---|---|---|---|---|---|---|
| ratings | 313 | 0.151 | 0.174 | 0.232 | 0.765 | 0.447 |
| last_season | 313 | 0.159 | 0.182 | 0.251 | 0.719 | 0.399 |
| team_to_date | 313 | 0.177 | 0.187 | 0.256 | 0.733 | 0.528 |
| league | 313 | 0.217 | 0.227 | 0.360 | nan | nan |

## Feb 15

| model | n | rmse_xgf60 | rmse_xga60 | rmse_diff | corr_diff | corr_goal_diff |
|---|---|---|---|---|---|---|
| ratings | 344 | 0.168 | 0.185 | 0.251 | 0.763 | 0.365 |
| last_season | 344 | 0.174 | 0.193 | 0.272 | 0.719 | 0.324 |
| team_to_date | 344 | 0.192 | 0.196 | 0.278 | 0.716 | 0.469 |
| league | 344 | 0.228 | 0.244 | 0.388 | nan | nan |

**Bar** ([M3 plan](../plans/m3-ratings.md)): `ratings` beats `team_to_date` and `last_season` on rest-of-season xG differential.
