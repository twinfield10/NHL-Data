# M2 validation report

Generated 2026-10-05 by `nhl validate-game-state`. Bars from [the M2 plan](../plans/m2-game-state.md); a cell is marked ✗ below its bar.

| Season | Games | games ≥99% valid | valid_time_share | goals_match | xg_match | faceoff_order_ok | toi_within_30s | goals_exact | assists_exact | starter_match |
|---|---|---|---|---|---|---|---|---|---|---|
| 20102011 | 1319 | 99.24% | 99.94% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20112012 | 1316 | 99.47% | 99.98% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20122013 | 806 | 99.38% | 99.99% | 100.00% | 100.00% | 100.00% | 99.58% | 100.00% | 100.00% | 100.00% |
| 20132014 | 1323 | 99.24% | 99.98% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20142015 | 1319 | 99.24% | 99.96% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20152016 | 1321 | 99.47% | 99.93% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20162017 | 1317 | 99.70% | 99.99% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20172018 | 1355 | 99.93% | 99.99% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20182019 | 1358 | 99.56% | 99.99% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20192020 | 1212 | 95.96% | 99.88% | 99.83% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20202021 | 952 | 93.91% | 99.80% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20212022 | 1401 | 98.86% | 99.97% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20222023 | 1400 | 99.50% | 99.98% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20232024 | 1400 | 98.64% | 99.95% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20242025 | 1398 | 99.36% | 99.97% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20252026 | 1394 | 99.35% | 99.97% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |
| 20262027 | 39 | 97.44% | 99.94% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% |

## Inferred units vs DailyFaceoff

Last DailyFaceoff version published before puck drop. Bar: forward trios ≥ 85% exact.

| Season | Team-games | F trios | D pairs | PP units | PK units |
|---|---|---|---|---|---|
| 20262027 | 4 | 81.2% | 83.3% | 62.5% | 0.0% |

PK units are a weak test: a team kills only a few penalties a game, and its PK forward and D pairs rotate independently, so exact four-man sets from one game rarely repeat. Treat fewer than ~100 team-games as anecdotal.

Bars: `valid_time_share` ≥ 99.5%, `goals_match` ≥ 99.5%, `xg_match` ≥ 100.0%, `faceoff_order_ok` ≥ 100.0%, `toi_within_30s` ≥ 98.0%, `goals_exact` ≥ 100.0%, `assists_exact` ≥ 100.0%, `starter_match` ≥ 100.0%.

`valid_time_share` is the share of ice time with a plausible on-ice count; the rest are NHL shift-chart errors (a late exit briefly puts 6-8 skaters on), flagged per stint by `valid_personnel` for M3 to drop. *games ≥99% valid* is informational: the plan's original bar (99.5% of games) is stricter than the source allows in 2019-21, when the charts were worst. Boxscore columns use a random sample of games per season.
