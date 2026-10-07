# Plan: M5 — pregame inputs (projected lineups and starting goalies)

**Status:** phases A-D done 2026-10-06 (inputs refactor; starter model, bar passed; projected lineups and pregame backtest; live pregame pipeline). E (forward calibration) waits for ~300 settled team-games.
**Depends on:** M2 (`rosters`, `lineups`, `goalie_starts`, `player_game_logs`, `coaches`),
M3 snapshots, M4 (`sim/inputs.py`, `engine.py`), `schedule_context`, and the external
sources already captured: DailyFaceoff goalies and lines, source tweets, ESPN injuries,
transactions.
**Feeds:** forward pricing (M6), M7 season simulator (default lineups), site "Today" page (M8).

---

## TL;DR
M4 prices a game from two things per team: each dressed skater's **deployment shares**
(5v5, PP, PK, shots) and **the starting goalie**. In the backtest both come from the game
itself. M5 produces them *before* the game, point-in-time, from:

1. **A default lineup.** The team's recent dressed roster and deployment from M2, with
   known absences removed (injuries, transactions).
2. **A starter-probability model** fitted on 16 seasons of `goalie_starts`. The game is
   priced once per possible starter pair and the prices are weighted (a mixture).
3. **External confirmations.** DailyFaceoff lines and goalie status override or sharpen the default. Every slot records where it came from.

The honest output is a **pregame backtest**: M4 re-run with projected lineups and the
starter mixture instead of actual ones. The gap between that and the actual-lineup log loss
(0.6613) is what lineup and goalie uncertainty costs.

## 1. Outputs (data contracts first)
All snapshots are keyed by `as_of` (UTC) and never overwritten.

**`pregame/lineups/{date}/{stamp}.parquet`**: one row per projected dressed player.
- `game_id, team_id, player_id, position, as_of`
- `slot`: f1..f4, d1..d3, g, extra (the 19th skater when 11F/7D is uncertain), or null
- `pp_unit` (1, 2, null), `pk_unit` (1, 2, null)
- `p_dressed`: 1.0 for the projected 18, lower for game-time decisions
- `s5, spp, spk, sshot`: deployment shares as the simulator consumes them
- `source`: `last_game`, `dfo`, `nhl_official`, `fill`
- `confidence`: high / medium / low, plus `issues` text (shape problems, conflicts)

**`pregame/goalies/{date}/{stamp}.parquet`**: one row per candidate goalie per team-game.
- `game_id, team_id, goalie_id, as_of, p_start`
- `p_model` (the model alone), `dfo_status` (Confirmed / Likely / null), `source`

**`pregame/prices/{date}/{stamp}.parquet`**: M4 market prices per game, mixed over
starter pairs, with the lineup and goalie snapshot `as_of` they used.

Keys go in `storage/keys.py`: `pregame_lineups(date, stamp)`, `pregame_goalies(...)`,
`pregame_prices(...)`.

## 2. Starting goalie model
**Candidates:** the goalies the team dressed in its last 5 games, plus any goalie a recent
transaction adds (recalled, claimed, activated from IR), minus goalies ruled out (IR,
assigned, traded).

**Model:** conditional logit over a team-game's candidates (softmax of a linear score),
fitted on `goalie_starts` + `rosters` 2010-11 to 2024-25, tested on 2025-26. Features, all
known before the game:
- share of the team's starts season-to-date and over the last 10 (shrunk early season);
- started the team's previous game; consecutive starts *before* this game
  (`consecutive_starts` includes the game itself, so it's lagged);
- team on the 2nd night of a back-to-back × started the 1st night (the biggest single
  signal); goalie rest days; starts in the last 7 days;
- home/away, opponent strength (teams save the starter for tougher games);
- days since the goalie last appeared (returning from injury);
- point-in-time GSAx per shot from the M3 goalie term (who the coach trusts).

**Baseline:** "last game's starter starts" (and on a b2b's 2nd night, the other goalie).
**Bar (roadmap):** log loss beats that baseline on 2025-26.

**Result (2026-10-06, [report](../reports/m5-starters.md)).** `src/nhl/pregame/goalies.py`,
`nhl train-starters`, models at `models/starters/{season}.json`.
- 2025-26 log loss **0.708** vs 0.883 (last starter, b2b-aware) and 0.939 (last starter);
  top pick right 68.6% vs 53.6% / 43.5%. The model wins in every season 2018-19..2025-26.
- Calibrated within ~3 points in every probability bin (2023-2026).
- Repeat starts fell from 59% (2015-16) to 43% (2025-26) as teams moved to tandems, so each
  season's model is fitted on the 4 seasons before it (better than all history in each of
  4 test seasons, by 0.01-0.02).
- A starter outside the last-10-games candidate set (`other`) happens in ~1% of team-games.
- Home and opponent strength (× share of recent starts) each add a small, consistent gain.
  Dropping goalie quality or the playoff terms hurts.
- Candidates and features come from the schedule, so future games work: a team's next game
  uses only its completed games.

**DailyFaceoff status:** `Confirmed` and `Likely` override the model with calibrated
probabilities. There is no DFO history before 2026-10-05, so we start with priors
(Confirmed 0.98, Likely 0.85) and re-estimate from our own captures once a few hundred
team-games have settled. A null status means DFO's projection only and is used as one
feature, not an override.

## 3. Projected lineup
**Default (`last_game`):** the dressed skaters from the team's last game. Each player's
deployment is a recency-weighted average of his own recent games, on any team, so a traded
player brings his role: half-life 5 games for 5v5/PP/PK shares, 15 for shot shares. The
shares are renormalised over the projected 18.

**Status events** (built 2026-10-06, `src/nhl/pregame/lineups.py`): ESPN injuries and
transactions become `out` / `in` events with the time they were known. Only events after
the last game count, because that lineup already reflects everything known before it.
- **ESPN** `Injured Reserve`, `Out`, `Suspension`: out **until the expected return date**.
  On a game after that date he is back (owner's request, 2026-10-06), so a projection for
  any future date uses the return date in force. `Removed` from the report: back.
  `Day-To-Day`: no change.
- **Transactions:** placed on IR, assigned, suspended, retired, traded away: out. Activated
  from IR, recalled, claimed, traded in: available. A backfilled transaction counts as known
  the day after its date. Missing player ids are matched by name to the team's rosters.
- **A regular back from injury** (ESPN return date passed, or activated from IR) replaces
  the lowest-usage player at his position when his own 5v5 share is clearly higher
  (`source=return`, medium confidence). A recall alone only fills a vacancy.

**Replacing a removed player (decided 2026-10-06):** first a player back from injury, then
the most recently dressed or added player at the same position (a healthy scratch, a
recall, a trade), tagged `source=fill`, `confidence=low`. If nobody qualifies, a
placeholder (no player id, the departed player's deployment, league-average ratings), also
flagged. The simulator needs 18 skaters, so something must fill the slot, but it is never
silent.

**DailyFaceoff lines (`dfo`):** used when the team's latest DFO version was updated after
its last game. Validation already exists (`validate_lines`):
- a valid version replaces the 18 dressed skaters and their slots;
- an invalid one contributes only its complete, conflict-free groups; the rest stays from
  `last_game`, and the issues are kept.

Shares for a DFO lineup: each player's own recent shares, re-weighted toward the typical
share of the slot he's in (e.g. a 4th-liner promoted to f2 gets f2-like 5v5 time). Slot
shares are league averages from M2 `lineups` by rank, fitted on recent seasons.

**Tweet:** deferred (decided 2026-10-06). The tweet text stays captured and linked to each
DFO version, but isn't parsed in M5. DFO validation plus the last game's lineup carry the
reconciliation; an incomplete DFO group falls back to `last_game` and is flagged.

**Validation on output:** every projected lineup is checked for 12F/6D or 11F/7D, 3 per
forward line, 2 per pair, no player in two EV groups, nobody active and injured.

## 4. Pricing with the mixture
Refactor `sim/inputs.py` so the rate builder takes a deployment frame and starters from
any source (today it reads `player_game_logs` and `goalie_starts` inside `build_season`):
- `build_inputs(store, games, deployment, starters, constants, snapshot)` shared by the
  backtest and the forward path;
- for the forward path, each game expands to up to 4 rows (home starter × away starter
  with p ≥ 0.02, renormalised), is simulated once per row, and the market probabilities
  are averaged with weight p_home × p_away.

Team residuals and the scoring level already depend only on games before the date, so
they work for future games unchanged.

## 5. Pregame backtest (the honest number)
Run M4 on 2023-24..2025-26 with:
1. projected lineups from `last_game` + transactions (injury and DFO history don't exist,
   so this is the pessimistic case), and
2. the starter mixture from the model,

and report log loss, calibration and totals against the actual-lineup run. Expected: a
small moneyline cost from lineups, a larger one from goalies. It also tells us how much a
correct confirmed starter is worth, which decides how hard to chase late confirmations.

**Result (2026-10-06, [report](../reports/m5-pregame-backtest.md)).** 2021-22..2025-26,
6,993 games, 1,000 simulations, `nhl backtest-pregame`:

| variant | moneyline LL | max cal. err | O/U 5.5 LL | puck line LL |
|---|---|---|---|---|
| actual lineups and starters | 0.6620 | 0.029 | 0.6807 | 0.6115 |
| projected lineups | 0.6607 | 0.024 | 0.6806 | 0.6097 |
| starter mixture | 0.6625 | 0.026 | 0.6808 | 0.6120 |
| **pregame (both)** | **0.6612** | 0.033 | 0.6807 | 0.6104 |
| Poisson baseline | 0.6682 | | | |

- **Pregame inputs cost nothing.** Projected lineups are slightly *better* than the actual
  ones. The actual variant takes 5v5 shares from the game itself, which carry in-game noise
  (injuries, blowouts, penalties), while the projection averages recent games.
- **The starter mixture costs 0.0005** of log loss against knowing the starter. Collapsing
  the mixture on a confirmed starter is worth little on average; it matters on the
  individual games where the backup starts.
- **The pregame model beats Poisson in every season but 2025-26** (0.6809 vs 0.6816, the
  season where M4 also lost).
- **Lineup accuracy:** 92-95% of dressed skaters projected (93-95% weighted by ice time).
  Transactions help in 2024-26 but slightly hurt in 2021-24 (−0.3 points), mostly
  conditioning-loan "assigned" moves that don't remove a player. Most misses are
  healthy-scratch rotation and unannounced injuries; only lineup news (phase D) fixes
  those. ESPN injury history starts 2026-10-05, so its value is measured forward.

## 6. Pregame command and repricing
`nhl pregame [--date D]`:
1. read the latest captured DFO, tweets, injuries and transactions;
2. build lineups and starter probabilities, validate, write snapshots;
3. price every game not yet started, write `pregame/prices`.

**Repricing:** `nhl poll --what goalies,lines` reports whether anything changed (it
already stores only transitions). When it did, it runs `pregame` for the affected games.
The roadmap bar ("reprice within minutes of a confirmed change") is met by the poll
cadence plus a pregame run of a few seconds. The schedule is designed in
[docs/scheduler.md](../scheduler.md) (2026-10-06) and installed with
`scripts/install_cron.sh --apply`.

**Officials:** add the ~20-minutes-before-puck-drop officials check from the roadmap to
`poll --window`, so the referee crew factor uses the late assignment.

**Transactions** join `nhl poll --what` (they're built but not polled today).

**Result (phase D, 2026-10-06).** `nhl pregame` (`src/nhl/pregame/price.py`) and
`nhl poll --reprice`:
- **Today's slate in ~15 s:** lineups as of now (ESPN return dates, transactions,
  DailyFaceoff lines), starter probabilities with DailyFaceoff overrides, prices mixed over
  starter pairs, three snapshots under `pregame/{lineups,goalies,prices}/{date}/{stamp}`.
- **DailyFaceoff lines:** listed players' shares are pulled halfway to their slot's typical
  share (F1-F4, D1-D3, PP1/2, PK1/2, from M2 lines 2024-26). The first validator flagged 11
  of 32 versions on 2026-10-06; both causes were our reading of DFO's conventions, fixed
  the same day:
  - **"f4 has 2/3; dressed 11F/6D" (COL, DET, LAK, NSH, TBL):** DFO's `d4` group is the
    seventh defenseman. All five dressed 11F/7D in their last game and every `d4` player
    dressed. f4 of two plus a `d4` is now a complete 11F/7D lineup.
  - **"active + IR" (8 players):** DFO's injury list includes day-to-day players still
    expected to play (7 of 8 tagged `dtd`). Such a player is now **questionable**, not a
    conflict: he stays in the lineup unless ESPN rules him out past the game (Lilleberg: ESPN
    out until 10/13 while DFO still had him on d2). Only a player in two EV groups
    invalidates a version.
  - **Game-time decisions:** a questionable player whom ESPN lists day-to-day, or one DFO
    flags as a game-time decision, is priced at `GTD_P_DRESSED` = 0.75: his deployment is
    split 75/25 with the likeliest replacement (`source = gtd_backup`, `p_dressed` on both
    rows). Owner's starting value, to be measured in phase E.
  - **Goalies:** a starter candidate ESPN rules out for the game (IR / Out past the game
    date) is dropped and the rest renormalised (Annunen, NSH, IR until 10/13).

  After the fixes, all 31 teams' next lineups on 10/6-10/8 are 12F/6D (26) or 11F/7D (5):
  7 game-time decisions, 4 fills (ESPN outs: Lilleberg, Kane, Veleno, and one shortfall).
- **Goalie candidates fix:** a goalie whose latest appearance was for another team is no
  longer a candidate (season-boundary leftovers, e.g. a departed starter still listed). The
  starter model improved in every season; 2025-26 log loss 0.708 → 0.696.
- **Forward-only inputs:** each team's latest head coach; referee crews from Scouting the
  Refs and the NHL right-rail (~40 minutes before puck drop, `nhl poll --what officials`),
  through an as-of crew factor that reproduces history exactly.
- **Repricing:** `nhl poll --reprice` reruns `pregame` in the same run when goalies,
  lines, injuries, transactions or officials wrote new rows (odds don't move model prices).
  Transactions are now a poll target. No cron installed.
- **Open:** the pollers report row counts, not which games changed, so a change for another
  date also reprices today (harmless, ~15 s). Early-season model-vs-market gaps of 8-9
  points on a few games (TOR, DET, CHI on 10/6) are more likely priors than edges, given
  M6's finding that the model doesn't beat the close.

**Additions (2026-10-06, after phase D):**
- **Input freshness:** every pregame run reports each input's last capture and age against a
  limit (DailyFaceoff goalies 6 h, lines 24 h, ESPN injuries 12 h, transactions 24 h,
  referee assignments 24 h, odds 6 h, rating snapshot and game state 36 h), logs stale ones
  and lists them on every slate row. The first run caught 25-hour-old odds, so the "market"
  column was yesterday's line.
- **Slate summary and site contract:** one row per game per run (grain date → `game_id`),
  `pregame/slate/{date}/{stamp}`, with the detail in that stamp's lineups / goalies / prices
  files and a `pregame/latest/{date}.json` pointer. See `src/nhl/pregame/slate.py`.
  `nhl pregame` prints it.
- **Replacement level:** players missing from a rating snapshot (debuts, call-ups,
  placeholders, an unknown starting goalie) get the mean term of low-usage players in that
  snapshot instead of league average (`sim/inputs.py`, `REPLACEMENT_LEVEL`). Backtest effect
  is noise (2022-26 moneyline log loss −0.0001 to +0.0002) because ratings refresh daily;
  it matters forward.

**Deployment accuracy (2026-10-06, [report](../reports/m5-deployment.md), `nhl
evaluate-deployment`).** Projected vs actual ice time per player-game, 2023-26, morning
projections without DailyFaceoff history:
- **Shares are unbiased by role.** Grouped by the player's role in his previous game, the
  bias is within ±0.13 min at 5v5 for every line and pair and +0.02 min for PP1. (Grouping
  by that night's role makes top lines look 1-1.5 min under-projected; that is selection.)
- **Errors are mostly noise and roster misses.** 5v5 MAE 1.47 min (of 13.6) for players
  projected who dressed, 2.6 min including roster misses; PP 0.6 min, PK 0.6 min.
- **The recency-weighted average is the best simple predictor** (5v5 MAE 1.487 vs last 10
  games 1.527, season 1.550, last game 1.854).
- **Effect on prices is small.** The team 5v5 offence composite (Σ share × rating) correlates
  0.95 with the actual one; its error (sd 0.066 xG/60, no bias) is about a third of the
  spread between teams (sd 0.21), roughly a point of win probability per team.
- **Small leak:** players without a PP or PK unit last game are projected about double their
  actual PP/PK time (0.84 vs 0.41 PP minutes). DailyFaceoff's units fix this forward.
- **Not measured:** DailyFaceoff's slot adjustment (no history; phase E), in-game deployment
  by score state, and an explicit TOI model (needed for props).

## 7. Decisions (owner, 2026-10-06)
1. **Fill policy:** the most recent available player at the position, flagged low
   confidence; a placeholder only when there's none.
2. **Tweet parser:** skipped for now.
3. **Storage:** `pregame/` for M5 snapshots; `products/{date}/` stays for the site-facing
   tables M6/M8 assemble.

## 8. Done when
- Starter model log loss beats "last game's starter" on 2025-26.
- The pregame backtest is reported against the actual-lineup run.
- Every projected lineup passes the shape checks or carries its issues; no silent fills.
- `nhl pregame` prices the day's games from live captures, and a DFO goalie change triggers
  a reprice in the same poll run.
- Once enough 2026-27 games settle: DFO Confirmed/Likely calibration, and line agreement of
  projected vs actual lineups (feeds the M2 open item `compare_dailyfaceoff`).

## Package
`src/nhl/pregame/`:
- `goalies.py`: candidates, features, conditional logit, the DFO override;
- `lineups.py`: default lineup, absences, DFO reconciliation, shares, validation;
- `price.py`: mixture pricing on the refactored `sim/inputs.py`;
- `backtest.py`: the pregame backtest.

CLI: `nhl train-starters`, `nhl pregame`, `nhl backtest-pregame`.

## Build order
| Phase | What | Gate |
|---|---|---|
| A | Refactor `sim/inputs.py` to take deployment + starters; actual-lineup backtest unchanged | identical M4 numbers |
| B | Starter model + baseline | beats baseline on 2025-26 |
| C | Default lineup + absences; pregame backtest with lineups and the mixture | report written |
| D | DFO reconciliation, `nhl pregame`, poll → reprice, officials check, transactions polling | runs on today's slate |
| E | Forward calibration of DFO statuses (Confirmed/Likely), line agreement, and the game-time-decision rate (is 0.75 right? by source: DFO flag vs DFO injury list + ESPN day-to-day) | after ~300 team-games |
