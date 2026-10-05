# Plan: M3 — ratings (players, goalies, team context)

**Status:** planned 2026-10-05. Phases A-F below; A first.
**Depends on:** M1 xG (`predictions/xg`, shot features), M2 (`stints`, `goalie_starts`,
`rosters`, `coaches`, `game_logs`, `schedule_context`), the player catalog (`birth_date`).
**Feeds:** M4 simulator (scoring rates by unit, goalie adjustment, penalty rates, context),
M5 projected lineups, M7 season simulator, site pages (ratings, power rankings).
**Methods:** HockeyViz Magnus 8 (xG layers, shooter/goalie terms) and Magnus 9 (stint
ridge regression); see the [roadmap](roadmap.md) M3 section and references.

---

## TL;DR

| Phase | Output | Method |
|---|---|---|
| A | `p_freeze` per shot on goal; goalie rebound control | talent-neutral companion to xG (gradient boosting) |
| B | shooter finishing and goalie save skill | logistic GLM with offset logit(xG) and shrunk shooter + goalie terms |
| C | skater EV offence/defence, with zone, score × venue, rest and coach terms | weighted ridge regression on 5v5 stints, xG per 60 |
| D | skater PP offence / PK defence | same, on 5v4 / 5v3 / 4v3 stints |
| E | penalty drawing / taking rates | shrunk Poisson rates per 60 |
| F | daily point-in-time snapshots and the evaluation bar | refits as of date *D* |

**Decisions taken (owner may override):**
- **Season-to-season priors, not one long fit.** Each season starts from last season's
  estimates, aged; within a season the fit uses only that season's data plus the prior.
  This is how Magnus 9 keeps old information without letting it dominate, and the closed
  form makes daily refits cheap.
- **Special teams is its own model (phase D),** fit on its own stints, as in Magnus 9 ST.
  PP and PK skills are different skills; pooling them with EV would let the much larger
  EV sample swamp them.

All ratings are on the xG scale (talent-neutral chance quality), so they compose with
finishing and goaltending (phase B) without double counting. Goal-based versions are a
later diagnostic, not the product.

---

## A. Frozen-puck model (xG companion)
**Why:** under neutral xG, a goalie who gives up rebounds faces more xG, and his GSAx
credits him for it. Magnus 8 finds a real freeze-vs-stop trade-off (layer correlation −0.28).

- **Sample:** saved shots on goal (`SHOT`, goalie in net, not a penalty shot).
- **Label:** the next event is a `STOPPAGE` with reason `goalie-stopped-after-sog`.
  Present in every season since 2010-11. The rate is about 0.22 through 2018-19 and
  0.24 since, a recording change, so the model gets an era flag at 2019-20 alongside the
  xG era flags.
- **Features:** the xG feature set (`v2e`, rink-adjusted). Same principle: describe the
  shot, never who took it or who faced it.
- **Training:** like xG. Gradient boosting, season split (train 2010-2023, tune 2024, test
  2025), recency weights, out-of-fold predictions for historical seasons.
- **Outputs:**
  - `predictions/freeze/{season}`: `p_freeze` per saved shot on goal;
  - `goalie_starts.xfreeze`: expected freezes, beside the existing `sog_frozen`.
- **Done when:** test-season calibration is within ±3% overall and by era, and the model
  beats a constant-rate baseline on log loss.
- **Goalie use (phase B):** freezes above expected, shrunk, become the goalie's
  rebound-control rating.

## B. Finishing and goaltending
One logistic GLM over unblocked, non-empty-net shots:

    logit P(goal) = logit(xG) + shooter_s + goalie_g

- Ridge-penalized, fitted by iteratively reweighted least squares with sparse design
  matrices.
- **Priors:** last season's estimate for each player (centred at 0 for unknown players).
  The penalty is proportional to √(last season's shots), with floors for newcomers (Magnus
  8 uses 200 imaginary shots for skaters and 1000 for goalies). Newcomer priors are
  slightly below average.
- **Talent-adjusted xG** = the fitted probability. It is what the simulator and goal props
  use. Neutral xG stays unchanged.
- **Goalie rating** = save term (from B) + rebound control (from A), both shrunk; workload
  and rest effects are estimated from `goalie_starts` as a separate step.
- **Evaluation:** next-season log loss on goals, vs neutral xG alone and vs raw
  goals-minus-xG shrinkage.

## C. Skater EV ratings (Magnus 9 EV)
- **Rows:** each 5v5 stint with `valid_personnel`, twice: once per attacking team.
- **Response:** the attacking team's xG per 60. **Weight:** duration.
- **Design columns:**
  - offence terms for the attacking skaters, defence terms for the defending skaters;
  - zone start: OZ/NZ/DZ/on-the-fly × seconds 0-34 since the last faceoff (a stint's
    duration is split across those seconds), with on-the-fly from
    `*_changed_since_faceoff`;
  - score state (−3+ … +3+) × period × home/away, by minute, smoothed;
  - rest (well-rested / normal / back-to-back) for both teams, offence and defence;
  - head coach: overall plus score-state terms, heavily penalized;
  - `post_penalty_5v5`.
- **Penalties:**
  - each group of terms sums to zero;
  - smoothness on the per-second and per-minute terms;
  - player priors are last season's estimate adjusted by an age curve. Prior tightness
    varies by age (looser when young and old), and newcomers start below average
    (Magnus 9: −10% offence, +10% defence).
- **Solve** β = (XᵀWX + Λ + K)⁻¹(XᵀWY + Λβ₀) with sparse Cholesky / conjugate gradient.
  About 1,000 skaters × 2 plus about 1,000 context columns: seconds per fit.
- **Hyperparameters:** prior strength and context penalties, tuned on out-of-sample stint
  xG (fit season *s*, predict season *s+1*'s stints).
- **Diagnostics:**
  - year-to-year correlation of residuals (target about 0);
  - the spread of teammate- and opposition-quality terms;
  - the context terms themselves (home, rest, score), which are M4 inputs.

## D. Skater PP / PK ratings (Magnus 9 ST)
Same machinery on 5v4, 5v3 and 4v3 stints, attacking team on the power play only:
PP offence for the attackers, PK defence for the defenders. Context terms: score state,
period × home/away, rest, coach, zone start. Short-handed offence is not modelled, as in
Magnus 9; the simulator uses a league rate for it.

## E. Penalties
Per player per 60 minutes, by strength:
- drawn rate;
- taken rate.

Each is a gamma-Poisson posterior with a position prior and last season's rate as the
prior mean. Referee crew effects (from `officials`) are a later refinement.

## F. Snapshots and evaluation
- **Snapshot.** `ratings/{date}/{kind}.parquet` for skaters EV/ST, finishing, goalies,
  penalties and context, using only games before *date*.
  - Daily for the current season.
  - Weekly for the 2015-2026 backfill, which is enough for backtests and history pages.
- **Bar (from the roadmap).** Take ratings at dates *D* (Nov 15, Jan 1, Feb 15) for every
  season 2015-2025. Combine them with each team's actual remaining TOI by player.
  - The predicted rest-of-season team xG differential must beat both:
    - (a) team-level xG rates to date;
    - (b) last season's ratings.
  - Also report it for goal differential.
- **Products unlocked:** player and goalie rating pages, lineup-weighted power rankings,
  rating history.

## Package and interfaces
- `src/nhl/ratings/`, one module each: `freeze.py` (A), `finishing.py` (B),
  `design.py` (sparse design matrices), `rapm.py` (C/D solve, priors, aging),
  `penalties.py` (E), `snapshots.py` (F), `evaluate.py`.
- `storage/keys.py`: `freeze_predictions(season)`, `ratings(date, kind)`.
- **CLI:**
  - `nhl train-freeze`;
  - `nhl ratings --as-of DATE` / `--backfill 2015-2025 --every 7d`;
  - `nhl evaluate-ratings`.

## Open questions
- **Aging curve:** estimate it from our own season-to-season rating changes (the delta
  method), or fix Magnus 9's parabola (peak 24) to start. Recommendation: start fixed,
  then estimate.
- **Goal-based RAPM** next to xG-based. Diagnostic only, unless it helps the bar.
