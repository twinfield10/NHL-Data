# Plan: M3 — ratings (players, goalies, team context)

**Status:** A-F built 2026-10-05. The bar passes on xG ([evaluation](../reports/m3-evaluation.md)).
Results, tuned settings and open items are in [Results](#results-2026-10-05).
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

## Results (2026-10-05)
Every model is tuned the same point-in-time way: chain full seasons, fit the evaluation
season through Dec 31, score the rest, 2012-2025. 2012-13 and 2020-21 started in
January, so they are split at their median game date instead.

| Phase | Result | Settings |
|---|---|---|
| A freeze | test freezes/expected 1.004, AUC 0.61, out-of-fold within ±2.2% every season | xG features + era flag at 2019-20 |
| B finishing | +0.19% log loss vs neutral xG (with intercept and position effect); goalie terms +0.04% on their own | shooter decay 0.8, 1k-shot newcomer prior at −0.05; goalie decay 0.8, 10k shots at +0.025 |
| C EV | +0.17% stint-level weighted MSE vs no player terms; aging adds a little | decay 0.7, 100k-s newcomer prior at league average, coach ridge 2e5, aging curve ×2 |
| D ST | +0.65% (power-play skill is concentrated) | decay 0.9, 20k-s newcomer prior, no aging yet |
| E penalties | +7.4% Poisson log likelihood vs position rates | decay 0.7, 5 h of position evidence |
| F bar | **pass on xG.** Rest-of-season 5v5 xG differential RMSE 0.242 vs 0.258 (last season) and 0.285 (team to date); correlation 0.75 | cutoffs Nov 15 / Jan 1 / Feb 15, 2015-2025 |

**Findings**
- **xG overrates defensemen's shots.** Defensemen convert about 7% fewer goals than their
  xG (2010-2026), so finishing has an unpenalized defenseman effect (−0.05 logits).
  Without it, every defenseman's term carries the gap.
- **Centering matters.** Without a zero-sum constraint on each group of talent terms,
  the free intercept and the talent terms drift against each other. The first finishing
  tuning showed a spurious +1.5% gain for exactly this reason.
- **Newcomers start at league average, by position.** Below-average newcomer priors for
  everyone (Magnus 9's ±10%) scored worse in both the EV and the finishing tuning.
  Defencemen are the exception (2026-10-08, re-chained `chain_eval`, 13 seasons): new D
  start at offence −0.25 and defence +0.12 xG/60. That gains +0.93 bp on late-season MSE
  and wins every season (the age curve adds +0.29 bp re-chained). Optimum is flat from
  −0.20 to −0.30 offence and +0.08 to +0.16 defence. A per-season shift on returning D priors
  on top adds < 0.1 bp, so it isn't used.
- **Special teams: D newcomers and aging (2026-10-08).** Same re-chained test. New D start
  at −1.0 PP xG/60 (+15 bp alone; −0.75 to −1.25 within 1.5 bp; forwards best at 0; PK
  defence shows nothing), and ST priors age with the EV curve's shape at scale 4 (+2.8 bp
  alone; flat over 3-6). Together **+19.3 bp, 13 of 13 seasons** (9-32 bp each), twenty
  times the EV gain: power-play skill is concentrated and few defencemen run a PP.
- **Finishing aging (2026-10-08).** Shooter and goalie terms age into each season with a
  delta-method quadratic per role, ×2 (`finishing.AGE_CURVES`): shooters +0.015 logit a
  season at 21, −0.014 at 33; goalies decline after about 30. Re-chained fit-to-Dec-31
  log loss: **+1.68 bp, 13 of 13** (shooters +1.27, 13/13; goalies +0.41, 10/13).
  Rejected: a D-specific newcomer shooter mean (the `d` term already carries the gap).
- **Goalie rest and workload: no effect (2026-10-08).** Pooled 2010-2026 logit effects on
  goals allowed: second of a back-to-back +0.015 ± 0.016, 4+ starts in 7 days
  +0.034 ± 0.027, 10+ days' rest −0.009 ± 0.011. Refit each half-season they cost 1-4 bp;
  held fixed they add +0.04 bp. Relief appearances do allow more (+0.077 ± 0.018, 14 of 16
  seasons), but that's in-game information the pregame simulator can't use; held fixed it
  adds +0.47 bp (10 of 13) to the ratings. Not adopted.
- **Tested and rejected: Marcel-style per-lag weights (2026-10-08).** Each season's prior
  rebuilt as one joint fit of the previous seasons' stints, season *y−L* weighted by w_L,
  newcomer prior added once, then aged (verified to reproduce a single-season fit exactly).
  Same re-chained test against the production chain:
  - **EV: loses in every season, every shape.** Geometric 0.5-0.95 −2.1 to −0.9 bp;
    Marcel 5/4/3 −1.0 to −1.4; truncated at 3/2/1 lags −1.5/−1.8/−2.7; flat −1.0 (all 0/13).
    Longer memory is consistently better, so Marcel's 3-season cutoff hurts.
  - **ST: +1.4 to +1.7 bp, 9 of 13, for any shape** (geometric 0.6/0.7, 3 lags, Marcel).
    A faster chain decay instead costs −2.7 bp (0.8) and −8.6 bp (0.7), so the gain is the
    joint fit across seasons (plausibly separating PP linemates who share units), not the
    weights. Too small for a separate ST prior builder; revisit if ST accuracy becomes a focus.
  - **Finishing: not tested.** One shooter and one goalie per shot leaves nothing for a
    joint fit to untangle, and the weights didn't matter elsewhere.
- **EV context terms (2024-25).** Home ice is +0.12 xG/60 (about 5%). The attacking team
  on a back-to-back is −0.08; an opponent on a back-to-back is +0.12. An offensive-zone
  faceoff adds +5.6 xG/60 in its first second, decaying over about 10 s. Trailing teams
  generate more.
- **Aging:** offence rises about 0.012 xG/60 per season at 19-21, peaks at 25-26, then
  falls 0.01-0.02 per season. Defence worsens after about 27. The curve is fitted on all
  seasons: a league-level curve with six parameters, so the leakage is negligible.

**Open items (carried to M4 unless noted)**
- **Goals vs xG at team level.** On rest-of-season *goal* differential, team xG to date
  correlates better (0.46) than the ratings (0.40), and a 75/25 ratings/team blend lifts
  the ratings to 0.43 at no xG cost. Weaker coach shrinkage made both worse, so coach
  terms are not the cause. M4 adds finishing and goaltending to the ratings anyway; test
  there whether a shrunk team term is still needed.
- **Line chemistry** (roadmap hypothesis): not tested yet.
- **Goalie workload and back-to-back effects** on save skill: tested 2026-10-08, no
  effect (see the results above).
- **Goal-based RAPM**: not built (diagnostic only).
- **ST aging**: done 2026-10-08 (EV curve shape × 4).
- **Season rollover:** run `nhl build-priors` once each season is complete, so the
  next season starts from it.

