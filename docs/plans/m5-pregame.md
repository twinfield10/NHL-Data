# Plan: M5 — pregame inputs (projected lineups and starting goalies)

**Status:** phases A (inputs refactor) and B (starter model, bar passed) done 2026-10-06; C-E open.
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
**Default (`last_game`):** the dressed skaters from the team's last game, with each player's
deployment from an exponentially weighted average of the team's last ~10 games (half-life
~4 games), so one odd game doesn't dominate. PP/PK and shot shares already come from
earlier games in M4; this reuses that logic.

**Absences:** a player is removed when, as of the snapshot, any of these hold:
- ESPN injury status `Out`, `Injured Reserve` or `Day-To-Day` *and* listed out by DFO;
- a transaction after his last game: placed on IR, assigned, traded, waived-and-claimed,
  suspended.
`Day-To-Day` alone keeps him in, with `p_dressed` < 1 and an issue flag.

**Replacing a removed player (decided 2026-10-06):** the team's most
recent dressed-but-now-available player at the same position (the healthy scratch or
recalled player from transactions), tagged `source=fill`, `confidence=low`. If nobody
qualifies, a replacement-level placeholder with league-average-of-depth ratings, also
flagged. The simulator needs 18 skaters, so something must fill the slot, but it is
never silent.

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

## 6. Pregame command and repricing
`nhl pregame [--date D]`:
1. read the latest captured DFO, tweets, injuries and transactions;
2. build lineups and starter probabilities, validate, write snapshots;
3. price every game not yet started, write `pregame/prices`.

**Repricing:** `nhl poll --what goalies,lines` reports whether anything changed (it
already stores only transitions). When it did, it runs `pregame` for the affected games.
The roadmap bar ("reprice within minutes of a confirmed change") is met by the poll
cadence plus a pregame run of a few seconds. **No cron is installed in M5**; scheduling
stays on hold for the scheduler design.

**Officials:** add the ~20-minutes-before-puck-drop officials check from the roadmap to
`poll --window`, so the referee crew factor uses the late assignment.

**Transactions** join `nhl poll --what` (they're built but not polled today).

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
| E | Forward calibration of DFO statuses and line agreement | after ~300 team-games |
