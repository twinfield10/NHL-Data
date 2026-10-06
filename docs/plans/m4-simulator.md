# Plan: M4 — game simulator (the pricing engine)

**Status:** built 2026-10-05; the log-loss bar passes out of sample, with calibration, OT
rate and margins slightly off ([backtest](../reports/m4-backtest.md)). See
[Results](#results-2026-10-05).
**Depends on:** M3 snapshots (`ratings/{date}/`: EV, ST, context, finishing, penalties),
M2 (`player_game_logs` for deployment, `goalie_starts`, `stints` for league constants),
`schedule_context`.
**Feeds:** M6 pricing (moneyline, puck line, totals), M7 season simulator, props later.

---

## TL;DR
A vectorized Monte Carlo of the game clock. Every simulated game steps through regulation
(and overtime) in 5-second steps, carrying the score, the manpower state, penalty timers
and whether a net is empty. At each step:
- each team scores with a probability given by its **goal rate in the current state**;
- each team takes a penalty with a probability given by its **penalty rate**.

All the rates come from M3 ratings applied to the two lineups, plus league constants for
the rare states. Thousands of games (all teams × N simulations) run together as numpy arrays.

## 1. Inputs per game (`GameInputs`)
- **Lineups with deployment.** Each skater's share of the team's 5v5, PP and PK time.
  - Backtest: the shares actually played. Only the shares are used, not the total PP/PK
    time, which the simulator generates from penalties.
  - Forward: M5 projections.
- **Starting goalie** for each team. Backtest: the actual starter. Forward: M5's
  starter-probability mixture, priced per goalie and weighted.
- **Context:** home team, rest category per team, head coaches.
- **Ratings as of the game date:** the latest snapshot strictly before the game.

## 2. Rates
- **5v5 xG for team A** = EV intercept
  - plus Σ share × offence over A's skaters;
  - plus Σ share × defence over B's skaters;
  - plus A's home, rest and coach terms.

  Zone-start terms average out over a game and are omitted.
- **Score effects:** add the EV `score:{lead}:p{period}` terms for the current lead and
  period. These make trailing teams press.
- **Goals from xG:** multiply by exp(intercept + defenseman effect × A's defenseman shot
  share + Σ shot share × shooter term + B's starting-goalie term), from the finishing
  ratings. Shot shares come from each skater's individual xG to date (point-in-time, from
  the logs), falling back to TOI share.
- **PP / PK:** the same composition with ST ratings and PP/PK unit shares.
  - Short-handed goals, 4v4 and 3v3 rates are league constants scaled by the teams'
    relative EV strength.
- **Empty net:** league goal rates for and against with a net empty. The pull time is a
  league hazard by deficit (1 or 2) and time remaining, fitted on recent seasons from
  stints.
- **Penalties:** team A's rate of taking a PP-creating penalty.
  - Its components: league rate × (Σ share × A's taken rates ÷ league) × (Σ share × B's
    drawn rates ÷ league).
  - Duration mix (2-minute, 4-minute double, 5-minute major) from data.
  - A minor ends early on a power-play goal; a major doesn't.
- **League constants** (`models/sim/constants/{season}.json`): estimated only from
  seasons before the one being simulated, point-in-time.

## 3. Game flow
- **Regulation:** 3 × 20 minutes.
- **Regular-season overtime:** 3v3 sudden death for 5 minutes, then a shootout. From
  2015-16 on; 4v4 before that.
- **Playoff overtime:** 5v5 sudden-death periods of 20 minutes.
- **Shootout:** won by the home team with a league-average probability (about 0.5).
  Shooter and goalie shootout skill is a later refinement.

## 4. Outputs and markets
Per game:
- the joint distribution of final scores, split by regulation / OT / shootout;
- **moneyline:** P(home wins), including OT and the shootout;
- **puck line:** P(home wins by 2+), P(away wins by 2+);
- **totals:** P(total > line) for lines 4.5 to 7.5, with OT goals counted and a shootout
  win counting as one goal (the usual book rule, configurable).

## 5. Backtest and bar (from the roadmap)
2015-16 to 2025-26 regular seasons and playoffs, actual lineups, point-in-time ratings.
- **Calibration:** binned P(home win) vs actual within 2 points in every bin with
  enough games.
- **Distributions:** the share of games decided by 2+ goals, the total-goals distribution
  and the OT rate match reality within sampling error.
- **Log loss** on the moneyline beats a team-xG Poisson baseline (team xGF/60 and xGA/60
  to date, home ice, 50/50 overtime).
- **M3 open item:** test whether a shrunk team term (team xG to date) still improves the
  goal prediction once finishing and goaltending are in.

## Package
`src/nhl/sim/`:
- `constants.py`: league constants;
- `inputs.py`: GameInputs from ratings and lineups;
- `engine.py`: the vectorized simulator;
- `markets.py`: prices from simulations;
- `backtest.py`: the backtest and report.

CLI: `nhl sim-constants`, `nhl backtest-sim`.

## Results (2026-10-05)
Backtest of 2016-17..2025-26: 13,187 games, 1,000 simulations each. Calibration was tuned on
2016-17..2019-20 only; 2020-21..2025-26 (7,945 games) are out of sample.

| Bar | Result |
|---|---|
| Moneyline log loss below the Poisson baseline | ✅ test 0.6634 vs 0.6686; better in 9 of 10 seasons (2025-26: 0.6836 vs 0.6816) |
| Calibration within 2 pts per bin | ⚠️ max 2.9 pts, in the top bin, and the simulator is *under*confident there (0.739 predicted vs 0.768 actual); every other bin within 1.6 |
| 2+ goal margins | ⚠️ 56.7% vs 58.4% actual |
| OT rate | ⚠️ 21.2% vs 22.5% |
| Total goals | ✅ 5.99 vs 6.05; over/under 5.5 and 6.5 log loss beat Poisson (0.6835 vs 0.6857; 0.6757 vs 0.6826) |
| Puck line | home −1.5 log loss 0.615 (no baseline yet) |

**What it took** (each found by comparing simulated with actual aggregates):
- **Empty-net goals.** A goalie-pull hazard by deficit and time left, from first pulls
  that last at least 10 s, re-applied after every goal. That brought empty-net goals from
  0.51 to 0.33 per game (actual 0.34).
- **3v3 overtime rates from regular-season OT.** Regulation 3v3 is shift-chart noise:
  41.9 goals/60.
- **Power-play rate and penalty mix.** The rate comes from actual PP opportunities;
  fighting majors are excluded from the mix.
- **Point-in-time league scoring level.** Season-to-date goals per game, shrunk toward last
  season. Without it, 2016-19 ran 6.6% low. The calibration centres on the
  level-adjusted league rate, so the pace scale can't shrink the level away.
- **Calibration** (tuned on 2016-2019):
  - strength 1.2: ratings are shrunk, so team gaps need stretching; 1.4 overshoots;
  - pace 1.0;
  - game shock 0.3: σ 0.35 had the best log losses but missed calibration by 3 points
    and pushed OT lower; 0.2 the reverse.
- **Playoffs.**
  - Point-in-time scoring factor (about 0.96-0.98) and home factor, from five prior
    playoffs.
  - Playoff penalties: calls run 1.16× regular overall but 0.82× in tied 3rd periods. PP
    opportunities are about equal because the extra calls are often offsetting.
- **Penalty context.**
  - Home teams take about 4% fewer.
  - Season type × period × the penalized team's lead, from PP starts: leading teams take
    up to 1.3× more; 3rd periods are quieter.
  - Referee crew factor: each referee shrunk with 86 games of league evidence;
    point-in-time correlation with actual PP opportunities 0.08 (the ceiling is about 0.15).
    Linesmen show no effect.

**Findings / next accuracy work (ranked)**
1. ~~A team-level signal is missing.~~ **Resolved with team terms (2026-10-05)**; see
   [Team terms](#team-terms-2026-10-05). Out of sample: 0.6634 → 0.6615, against 0.6611 for
   the blend.
2. **Late-game dynamics.** OT is 1.3 points low and 2+ margins 1.7 points low together:
   too many one-goal regulation finishes. Score effects are per period; Magnus 9 uses
   per-minute terms, and tied-late conservatism is a known effect. Add 3rd-period
   per-minute score terms.
3. **Playoffs.** The simulator trails Poisson on the 905 playoff games (0.6891 vs 0.6868)
   and over-favours home teams (53.9% vs 51.4%). The playoff home factor is noisy; shrink
   it more, or use a fixed reduction.
4. **2025-26.** Home teams won 51.9% and 25% of games went to OT. The simulator's
   predictions spread more than Poisson's (sd 0.098 vs 0.063), which costs in a parity
   season. Monitor before reacting.
5. **Not modelled:** shootout skill, penalties in OT, 5v3 detail beyond a scale factor, and
   in-game goalie changes.

## Tested and rejected: team style matchups (2026-10-05)
**Question.** Do teams that generate certain kinds of chances (rush, off turnovers, off
faceoff wins, rebounds, cycle) score more against teams vulnerable to them, beyond each
team's overall xG for and against?

**Method.**
- 5v5 shots are classified by the play before them:
  - rebound: `is_rebound`;
  - rush: `is_rush_play`;
  - turnover: opponent giveaway or own takeaway ≤5 s before;
  - faceoff: faceoff win ≤5 s before;
  - cycle: everything else.
- For each type, a Poisson model of team-game xG = league × attacker × defender × arena,
  shrunk; the arena terms absorb scorer bias.
- Fit on games before Jan 1, scored on 5v5 goals for the rest of the season, 2016-17..2025-26
  (Poisson deviance).

**Results.**
- **About half the apparent style is arena scorer bias.** Split-half persistence of a team's
  rush / turnover / faceoff xG falls from 0.64 / 0.52 / 0.62 (all games) to 0.38 / 0.31 /
  0.36 (road games only). Each arena's share of shots tagged as rush varies by about 47%
  between arenas, turnovers by about 30%.
- **No matchup interaction.** The full model (Σ over types of attacker × defender) and a
  separable one (attacker's overall style × defender's overall style) differ by ±0.02
  deviance at every shrinkage level.
- **No separable style value either.** Type-decomposed team ratings first appeared to beat
  aggregate ones (−103 deviance), but only because the aggregate model was over-shrunk:
  tuned (λ 15 instead of 200), the aggregate model scores 14,802 against 14,808 for the
  best type-decomposed one.

**Conclusion.** With play-by-play chance types, team style adds nothing to goal prediction
beyond overall xG for and against. Revisit only with tracking-based chance types (true
rushes, passes, zone entries), which the NHL doesn't publish at shot level.

## Team terms (2026-10-05)
**Where the signal was.** A blend test on the out-of-sample seasons added features to the
simulator's log-odds one at a time:
- the team's 5v5 xG differential to date: 0.6633 → **0.6612**, the whole gain;
- special-teams xG: 0.6629;
- goal differential: 0.6625;
- last-10 xG differential: 0.6616.

So the player ratings, shrunk toward their priors, credit too little of a team's realised
5v5 xG. That's system, chemistry or in-season change.

**What went in** (`nhl.sim.engine.team_term`; both from the residuals in `nhl.sim.inputs`):
- **xG team term.** A team's running 5v5 xG for and against, minus what its ratings predicted
  for those games, per hour, shrunk with 30 hours of zero evidence. Multiplies its 5v5 goal
  rates; the opponent's defence residual counts too.
- **Defence goals term.** A team's running 5v5 goals against, minus the talent-adjusted
  expectation (shooter and goalie terms from the snapshot in force at each game), shrunk
  with 500 expected goals. Multiplies the opponent's 5v5 goals.
- Both were tuned on 2016-2019 only.

**Team finishing and defence beyond the players** (split-half persistence 2012-2025, after
full-season player talent):
- offence ≈ 0 (−0.02 all games, 0.03 road): finishing talent lives in the shooters;
- defence 0.13 (0.12 road): some teams allow better chances than xG and their goalie
  explain, plausibly the passes public xG can't see. Worth about 0.0004 log loss in-sample
  and about 0 out of sample, but it improves calibration a little.

**Result (test seasons 2020-21..2025-26, 7,945 games):**

| Metric | Before | After |
|---|---|---|
| Moneyline log loss (Poisson 0.6686) | 0.6634 | 0.6615 |
| Puck line | 0.6124 | 0.6110 |
| Over 5.5 | 0.6827 | 0.6824 |
| Over 6.5 | 0.6842 | 0.6840 |

The remaining calibration miss is in the top bin, and it's *under*confidence: favourites
predicted at 74.8% win 78.3% (about 2 SE). The game shock (σ 0.3) probably pulls heavy
favourites toward 50%. Next: test a smaller shock for lopsided games, or tune σ jointly
with the team terms.

