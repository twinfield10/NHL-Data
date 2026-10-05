# Plan: NHL game-prediction platform (roadmap)

**Status:** M1 complete (2026-10-05). Data sources for M5/M6 built early. M2 planned ([m2-game-state.md](m2-game-state.md)). Everything else is proposed.
**Written:** 2026-10-05.
**Audience:** the owner and a few friends. **Use:** betting the main game markets
(moneyline, puck line ±1.5, totals), with player props (goals, assists, points) later.
**Delivery:** a static, password-gated site in the style of `football-pool-reports`
(React/Vite, Parquet read from S3, Cloudflare Worker). This repo stays data and models only.

---

## TL;DR

- **Rate players, not teams.** Hockey is played in units: forward lines of 3,
  defense pairs of 2, PP and PK units, and one goalie who is the biggest single
  input, much like a starting pitcher. A game projection starts from a *projected
  lineup* and weights each player's rating by the ice time and role they will get.
- **One engine for every market.** Moneyline, puck line and totals all need the
  full distribution of final scores, including empty-net goals, overtime and the
  shootout. A Monte Carlo **game simulator** produces that distribution. Props
  later reuse the same simulations, assigning goals and assists to players.
- **Everything is point-in-time.** Every rating, projection and price is computed
  "as of" a date using only earlier data. The same code then serves the season-history
  pages (rankings on Nov 15) and an honest backtest.
- **Nothing ships to the site without a backtest bar.** Each forward-looking output
  has a metric and a threshold. For anything we bet, that includes closing-line value.

## Architecture

```
NHL API ──> raw/ ──> events, shots, xG                       (M1, done/in progress)
                         │
                         ▼
              stints · lineups · game logs · goalie starts    (M2)
                         │
                         ▼
     player ratings (EV off/def, PP, PK, finishing, penalties)
     goalie ratings · team context (home, rest, travel)       (M3, daily point-in-time snapshots)
                         │
   projected lineup + starting goalie (M5) ──┐
                         ▼                   ▼
                game simulator ──> score distribution per game   (M4)
                         │
          ┌──────────────┼───────────────────────┐
          ▼              ▼                       ▼
   market prices     season simulator      player props (later)
   + odds, edges,    (playoff odds,
   CLV (M6)          projections, M7)
          └──────────────┴──────────> products/{date}/ ──> site (M8)
```

---

## M1: Data foundation and xG (in progress)

Done: `src/nhl` package, S3 store, idempotent ingest, event and shot tables,
four xG models with the legacy bugs fixed. Remaining: finish the backfill, full
tuned training, out-of-fold historical xG, nightly `nhl update`.

**Done when:** test-season (2024-25, 2025-26) xG is calibrated within ±3% of goals
for each strength group and the EV AUC is at least the legacy model's ~0.78.

## M2: Game-state data model (stints, lineups, game logs)

The tables everything else stands on. All are derived from data we already store.

| Table | Grain | Contents |
|---|---|---|
| `processed/stints/{season}` | constant on-ice personnel and strength | 10 skaters + goalies, duration, xGF/xGA, GF/GA, CF/CA, zone start, score state |
| `processed/lineups/{season}` | team-game | dressed players, forward lines and D pairs inferred from shared TOI, PP1/PP2 and PK units, TOI by role |
| `processed/goalie_starts/{season}` | team-game | starting goalie, whether he finished, rest days, consecutive starts |
| `processed/game_logs/{season}` | team-game and player-game | goals, xG, shots, TOI by strength, penalties drawn/taken |
| `processed/schedule_context` | team-game | rest days, back-to-backs, travel distance, time-zone shift, home/away |

Line inference: within a game, cluster forwards by pairwise even-strength TOI
together. A trio's shared TOI is high, so a greedy assignment recovers the lines.
Validate against published line charts for a sample of games.

**Products unlocked (backward-looking):** game recaps (xG timeline, who was on
for what), team and line pages, player and goalie game logs.

**Done when:** stint durations sum to game length (within 1%) for at least 99% of
games, and inferred lines match published lines in at least 85% of sampled games.

## M3: Ratings (players, goalies, team context)

**Layer separation (agreed 2026-10-05).** xG measures the chance and stays
talent-neutral. Talent is performance *relative to expected*, in two parts:
- how many and how good chances a player or team creates and allows (xG for/against
  per 60);
- how well they convert or stop them (finishing = goals − xG; goaltending = goals
  saved above expected, GSAx).

Context (rest, travel, home ice, referees, starter) is a separate adjustment in the game
model. The simulator combines all three, so each factor's effect on a given game is
visible and explainable. Talent estimates are shrunk toward priors by reliability:
over/under-performing xG is mostly noise in small samples, especially for shooters and
goalies. **Inputs to the talent priors, never to xG:**
- team win % and records (as baselines for the xG-based ratings to beat);
- NHL EDGE player tracking (shot speed → finishing; skating and zone time → xG
  generation and suppression).

All ratings are fit on data before date *D* and saved as daily snapshots
`ratings/{date}/...`. That makes history pages and backtests fall out naturally.

- **Skater impact.** Regularized (ridge or Bayesian) regression on stints, the
  standard RAPM setup, by state: EV offense, EV defense (xG and goal rates per 60),
  PP offense, PK defense. Priors come from the previous season's estimate, so early
  season ratings are stable. Time-decayed weighting.
- **Finishing.** Shooter goals above xG, heavily shrunk. This is small for team
  totals but central for goal props later.
- **Penalties.** Penalty drawing and taking rates per player. These drive PP and PK
  time in the simulator.
- **Goalies.** Goals saved above expected per shot, with strong shrinkage (a
  goalie's save skill is noisy over a few hundred shots), age curve, workload and
  back-to-back effects. Goalies are the starting pitchers of this model.
- **Line chemistry.** Hypothesis to test, not assumed: do known units (a trio or
  pair) outperform the sum of their individual ratings, out of sample? If they do,
  add a shrunk unit term. If not, the additive model plus deployment is the answer.
- **Team context.** Home ice, rest, travel, schedule density, fitted as
  adjustments to scoring rates.

**Products unlocked:** power rankings (lineup-weighted team strength), player
and goalie ratings, rating history charts.

**Done when:** ratings at date *D* predict the rest-of-season on-ice xG
differential better than (a) team-level xG rates and (b) last season's ratings.

## M4: Game simulator (the pricing engine)

Input: two projected lineups with expected TOI by unit, starting goalies,
context. Output: 10k+ simulated games giving the joint distribution of final
scores (regulation, OT, shootout).

Mechanics (all rates fitted from history):

- **Scoring rates** by strength state: the TOI-weighted sum of on-ice ratings, then
  adjusted for the opposing goalie.
- **Penalties.** Arrival rates from player penalty rates create PP/PK segments.
- **Score effects.** Trailing teams shoot more, leading teams less.
- **Goalie pulls.** Pull timing by deficit and time left, with empty-net scoring
  rates for and against. This is what makes the puck line and totals honest.
- **Overtime and shootout.** Regular-season 3-on-3 OT, then shootout rates by
  shooter/goalie. Playoff OT is 5-on-5 and continuous.

**Markets from one simulation:** moneyline (P(win), including OT/SO); puck line
P(margin ≥ 2) and P(lose by ≤ 1); totals P(goals > line), including how books
grade OT and the shootout. Later, player props by assigning simulated goals to
players in proportion to their on-ice share and finishing.

**Done when:** across a 2015-2025 point-in-time backtest the following hold (using
actual lineups for now):

- calibration error under 2 points on win probability;
- the share of games decided by 2+ goals and the distribution of total goals match
  reality within sampling error;
- log loss beats a team-xG Poisson baseline.

## M5: Pregame inputs (projected lineups and starting goalies)

The forward-looking weak spot. The NHL API confirms lineups only close to puck
drop, and starting goalies are often confirmed only after the morning skate.

- **Default projection:** each team's most recent lineup and lines from M2,
  adjusted for known absences.
- **Starting goalie:** a probability model (rest days, back-to-back, recent
  starts, the opponent) used as a *mixture*, i.e. price the game under each
  possible starter and weight the results. When a starter is confirmed, collapse
  the mixture and reprice.
- **External confirmations:** DailyFaceoff starting goalies and line combinations,
  with the source tweet text and post time kept for each.
- **Lineup validation and reconciliation.** Lineups have fixed shapes: forward
  lines of 3, D pairs of 2, PP units of 5 (sometimes 4), PK units of 4, and 12F/6D
  dressed (11F/7D is legitimate). Externally published lines are checked against
  those shapes and for players listed as both active and injured. Example: on
  2026-10-05 DailyFaceoff's TBL page had F4 with two forwards and Lilleberg both on
  D2 and on IR, while the practice tweet it cited listed full lines. Nothing is
  imputed silently. A projected lineup reconciles three sources: DailyFaceoff's
  structured lines, the parsed source tweet, and the team's last actual lineup from
  shift data. Each projected slot records which source it came from and how
  confident it is.

**Done when:** the starter model's log loss beats "last game's starter", and the
pregame pipeline reprices a game within minutes of a confirmed change.

## M6: Odds, pricing and betting

- **Odds capture.** Reuse the Pinnacle tooling in `SportsbookScrapers` and Rebirtha
  for NHL: snapshots into `s3://tmw-nhl-data/odds/...` (or the shared sportsbook
  bucket), one table across many books, opening and closing lines.
- **Pricing.** Remove the vig (devig) to get the market's fair price. Edge is the
  model probability minus the fair probability for each market and side. Stakes use
  fractional Kelly with a minimum edge and maximum exposure.
- **Measurement.** Closing-line value (CLV) on every model pick is the primary
  metric. ROI is secondary because it's noisy. Results are tracked live, and on
  history wherever odds exist.

**Done when:** a historical (or, failing that, forward paper-traded) sample shows
positive CLV on the bets the model would have placed, and the site only flags bets
that pass the gate.

## M7: Season simulator and power rankings

Simulate the rest of the regular season game by game with M4 (fast mode), then
apply NHL standings rules, tiebreakers and the playoff format, then the playoffs.
Ratings evolve within each simulated season, so the uncertainty in team strength
carries through.

**Products:** projected points, division and conference finishing odds, playoff and
Cup probabilities, daily power rankings, all snapshotted daily for "odds over time".

**Done when:** backtested preseason and midseason point projections beat a
points-to-date regression baseline.

## M8: Site

A clone of the `football-pool-reports` architecture reading `s3://tmw-nhl-data/products/`.
Pages:

- **Today:** games, fair odds vs market, edges, projected lineups and goalies,
  confidence.
- **Standings and projections:** playoff odds and how they've moved.
- **Power rankings:** with history.
- **Teams:** lines, deployment, unit ratings.
- **Players and goalies:** ratings and game logs.
- **Game recaps:** xG timeline and who was on the ice.
- **Model performance:** calibration and CLV tracker.

## Later: player props

Goals, assists, points: reuse M4 simulations and attribute goals and assists by
on-ice share, finishing, and playmaking rates from M3. Needs a prop odds feed.

---

## Cross-cutting rules

1. **Point-in-time or it doesn't ship.** Every model input carries an `as_of`. Backtests
   must reproduce exactly what would have been known pregame.
2. **Snapshot, don't overwrite,** for every daily product (`products/{date}/...`).
3. **Data contracts first.** Each milestone starts by fixing its output schemas in
   `storage/keys.py` and a schema doc, so site work can proceed in parallel.
4. **Each milestone gets its own detailed plan** (`docs/plans/mN-*.md`) before
   implementation: inputs/outputs, method, evaluation bar, done definition.

## Open decisions

| Decision | Options | Recommendation |
|---|---|---|
| Historical odds for backtests (none in the bucket today) | free archived closing lines (e.g. sportsbookreviewsonline, coverage ends ~2021-22) · paid historical API (e.g. The Odds API, ~2020+) · forward-only capture | Archive for 2015-2022 plus start Pinnacle capture now. Check licensing and ToS first |
| Projected lineups and starting goalies | external line-combination and starting-goalie sites (check robots/ToS) · team and beat-reporter feeds · NHL API only (late) | Decide in M5. Start capturing *something* soon, because history of projections is useful for measuring M5 |
| Odds storage | `tmw-nhl-data/odds/` · shared `tmw-sportsbook-data/nhl/` | Shared bucket if the scraper already writes there, otherwise this bucket |
| Rating method | ridge RAPM · Bayesian hierarchical (slower, gives uncertainty) | Ridge first, Bayesian if the simulator needs rating uncertainty |
