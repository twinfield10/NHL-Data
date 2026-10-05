# Plan: xG feature engineering, pass 1

**Status:** complete 2026-10-05.
- **Promoted:** `v2-pooled-20261005` (pooled, arena-adjusted, v2 features). It beats the
  previous production model in every state: EV −0.6%, PP −1.6%, SH −1.8%, EN −4.2%
  test log loss.
- **Recording-era flags** (`v2e` feature set) are in the pipeline for the next retrain.
- **In-season calibration** is monitored by `nhl xg-monitor` (nightly).
- **Retrain each offseason**, or earlier if the monitor flags a state.
**Scope:** `src/nhl/features/shots.py`, plus feature-set versioning in `src/nhl/models/xg.py`.
**Principle:** xG stays talent-neutral. It describes the chance, not who took it or how
good their team is. Talent (finishing, goaltending, xG generation and suppression) and
context (rest, travel, home, referees, win %) belong to the ratings and game model (see
[roadmap](roadmap.md), M3/M4). Every feature below is computed from events strictly
before the shot (point-in-time), in the shooter's frame (the shooting team attacks +x).

---

## 1. Cross-ice ("royal road") movement
A pass across the slot forces the goalie to move laterally and is the best-known
pre-shot predictor. We have no passes, but the previous event's location is a proxy.

| Feature | Definition |
|---|---|
| `lateral_ft` | `abs(y_abs - y_abs_last)` |
| `lateral_speed` | `lateral_ft / seconds_since_last` |
| `crossed_slot` | previous event within 3 s, on the opposite side of `y = 0`, both at least 3 ft off the centre line, and the previous event in the offensive zone (`x_abs_last > 25`) |
| `crossed_slot_same_team` | `crossed_slot` and the previous event was by the shooting team (a pass-like sequence rather than a block or turnover) |

## 2. Event sequences (beyond one previous event)
Over the context events (shot attempts, faceoffs, hits, giveaways, takeaways) in the
same game and period:

| Feature | Definition |
|---|---|
| `prev2_*`, `prev3_*` | type flags (own shot attempt, opponent shot attempt, faceoff won/lost, turnover for/against, hit), seconds since, and location for events 2 and 3 back |
| `secs_since_faceoff` | time since the last faceoff |
| `faceoff_zone_oz` | that faceoff was in the shooting team's offensive zone |
| `attempts_since_faceoff_for` / `_against` | shot attempts by each team since that faceoff (cycle length, sustained pressure) |
| `attempts_for_10s`, `_30s` | shooting team's attempts in the trailing 10 / 30 s (scrambles, sustained pressure) |

## 3. Net-angle geometry
Raw angle treats a sharp-angle shot from 10 ft like one from 40 ft. The angle the net
subtends is the physical quantity. Posts are at `(89, +/-3)`.

| Feature | Definition |
|---|---|
| `net_angle_deg` | `abs(atan2(3 - y, 89 - x) - atan2(-3 - y, 89 - x))` in degrees (0 behind the line) |
| `dist_near_post` | distance to the nearer post |
| `behind_net` | `x_abs > 89` |
| `in_slot` | inside the home-plate area (between the posts and the faceoff dots, below the circles' tops) |

## 4. Goalie workload (rolling)
Owner's idea: recent workload rather than only game totals. For the *defending* goalie,
counting events strictly before the shot:

| Feature | Definition |
|---|---|
| `goalie_sog_10s`, `_60s`, `_120s` | shots on goal faced in the trailing window |
| `goalie_att_10s`, `_60s` | unblocked attempts faced (screens and scrambles) |
| `goalie_sog_game` | shots on goal faced so far this game |
| `goalie_secs_since_save` | time since his last save |
| `goalie_mins_in_game` | minutes since he entered (cold-off-the-bench relief), from shifts |

Same-second events are ordered by `event_idx`; a rebound 1 s later sees the first save.

## 5. Shift fatigue (beyond team averages)
v1 already has each team's average elapsed shift time, their difference, and the time in
the current strength state. In model `b` these rank 8th of 29 features for SH and 18th
of 47 for EV. Averages blur the case that matters: one exhausted player, and *why* he's
tired.

| Feature | Definition |
|---|---|
| `def_max_shift_secs_d` / `_f` | longest current shift among defending defensemen / forwards |
| `def_shift_vs_norm_max` | max over defending skaters of current shift ÷ that player's median shift length (point-in-time, trailing 20 games) |
| `def_rest_before_shift_min` | shortest bench rest before the current shift among defending skaters (double shifts) |
| `def_toi_last5` | defending skaters' max ice time in the trailing 5 game-minutes (heavy PK use) |
| `icing_trap` | shot within 30 s of a defensive-zone faceoff that followed an icing by the defending team, with no line change since (they can't change after icing; stoppage `reason` is in events) |
| `shooter_shift_secs` | the shooter's own elapsed shift time |

Fatigue right now describes the chance, so it belongs in xG. Season-level fatigue
(back-to-backs, travel) stays in the game-model context layer. Games without shift
charts (57 late in 2024-25) get nulls; see the HTML time-on-ice fallback follow-up.

---

## Engineering
1. **Feature-set versioning.** `FEATURES` becomes `FEATURE_SETS = {"v1": [...], "v2": [...]}`.
   Every model's metadata already records its feature list, so `predict_xg` should read
   the list from the model's metadata, not a global. Models `b`–`d` keep scoring
   correctly after v2 lands.
2. **Rolling counts** use as-of joins on `(game_id, game_seconds)` per team and per
   goalie: vectorized, no per-row loops.
3. **Unit tests** on the synthetic game fixture: a hand-built cross-ice sequence, a
   three-shot scramble (rolling counts 0/1/2), a relief goalie entering mid-period, and
   net angle at known points (slot center, sharp angle, behind the net).

## Evaluation
Same protocol as the pooling experiment: arena-adjusted shots, train 2010-11..2023-24,
tune on 2024-25, test on 2025-26, 60 trials, **pooled architecture**.

**Architecture decided 2026-10-05.** One pooled model with state flags (`is_pp`, `is_sh`,
`is_en`) beat four separate models in every state on identical data and tuning budget.
Test 2025-26 log loss, split → pooled:

| State | Split | Pooled |
|---|---|---|
| EV | 0.20014 | 0.19951 |
| PP | 0.29470 | 0.29281 |
| SH | 0.20942 | 0.19954 |
| EN | 0.58718 | 0.56309 |
| All shots | 0.21959 | 0.21834 |

The hybrid (EV, PP separate; SH+EN pooled) captured only part of the gain.
`nhl train-xg --architecture pooled` is the default; split/hybrid remain available.

1. **v1 vs v2:** per-state test log loss, AUC and calibration (bar: v2 better on EV and
   PP log loss, no worse on calibration).
2. **Ablation:** drop each of the four groups in turn to see what earns its place.
3. **Drift check:** does the tuned recency half-life lengthen? Timing-based features
   are a suspect for the recency preference.
4. **Feature importance** and partial-dependence plots in the exploration notebook.

**Done when:** a v2 model beats `v20261005b` on the test season and is promoted with
`--promote`.

## Outcome and findings (2026-10-05)
- **v2 features vs v1** (both pooled): small gains. EV −0.02%, PP −0.39%, EN −0.70%,
  all shots −0.11%. Geometry (24% of gain) and goalie workload (21%) largely re-express
  information already in distance, angle, rebound and timing features.
- **Drift diagnosis.** The recency preference is a *recording* change, not a hockey
  change. From 2022-23 the feed logs far more quick-succession attempts:
  - missed-shot share 27% → 35%;
  - rebound share 3.8% → 5.7%, with rebound goal rate 18% → 11%;
  - a 2015-19 model's goals ÷ xG on shots within 1 s fell to 0.30.

  Snap/wrist labels were also redrawn in 2024-25 (snap 13% → 25%). An era classifier
  separates 2017-19 from 2022-24 shots with AUC 0.94.
- **Era flags** raised the tuned half-life from 0.20 to 0.61 seasons and improved EV
  log loss and calibration. Merging wrist and snap improved overall calibration but hurt
  log loss, so it was not adopted.
- **Ceiling.** Further xG gains from public play-by-play are tenths of a percent. The
  next meaningful gains for betting come from M2/M3 (lines, goalies, talent and
  context).
