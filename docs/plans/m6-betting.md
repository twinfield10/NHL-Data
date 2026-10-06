# Plan: M6 — odds, pricing and betting

**Status:** phases A (lines), B (devig) and a first D (model vs market) built 2026-10-06; **the gate is not passed**. See [Results](#results-2026-10-06). C (score matrix) and E (live) open.
**Depends on:** captured odds (`external/odds/...`: LowVig, 4Casters, ESPN-listed books live;
SBR archive and ESPN history), M4 simulator, M5 pregame inputs (`pregame/prices`, and the
`pregame` variant of `predictions/pregame_backtest`).
**Feeds:** M8 site ("Today": fair odds vs market, edges; model performance: CLV tracker), props later.
**Books (owner's rules):** LowVig, 4Casters and the books ESPN lists (DraftKings, ESPN BET,
Caesars, MGM, ...). **No Pinnacle. No public betting splits.**

---

## TL;DR
1. **One tidy lines table:** every book's open, close and latest price per market, with
   canonical book names and bad history rows removed.
2. **Fair market prices:** remove the vig (devig) per book and combine the reference books
   into a consensus. Choose the devig method by how well the closing consensus predicts
   results, 2015-2026.
3. **Model prices at any line:** the simulator keeps each game's full score distribution, so
   any main or alternate puck line, total (including whole numbers with pushes), team total
   or 3-way is priced exactly.
4. **Does the model know something the close doesn't?** Test the model's price against the
   closing consensus on 11 seasons. The answer sets how much to trust the model when it
   disagrees with the market (a fitted blend).
5. **Bets:** edge = blended probability vs the best available price; fractional Kelly with a
   minimum edge and exposure caps. **CLV is the primary metric**, ROI secondary.
6. **Live:** after each `nhl pregame` run, price every market at every book, write edges and
   a paper-trade ledger; grade CLV and results nightly.

## 1. Lines table (phase A)
`odds/lines/{season}.parquet`, one row per (game_id, book, period, market, subject, side,
line):
- `price_open`, `price_close`, `price_last`, `captured_open`, `captured_close`, `source`
  (`live` / `espn_history` / `sbr`).
- **Close for live captures:** the last transition before puck drop (`start_time`), and
  only if captured within 30 minutes of it; otherwise `price_last` only, flagged. 4Casters
  keeps quoting in-play, so anything after puck drop is ignored.
- **Book names:** one canonical map (ESPN history's `MGM` → `BetMGM`, `Caesars Sportsbook
  (...)` state variants → `Caesars`, ...).
- **History cleaning** (found in the survey): totals outside 4-9 (≈354 rows where line and
  price were swapped), puck lines other than ±1.5 in history (≈88 at ±180, ≈200 at ±1.0).
  Dropped and counted, never repaired.
- **Coverage for the backtest** (games with a closing line):

  | seasons | moneyline | totals | puck line | open lines | source |
  |---|---|---|---|---|---|
  | 2015-16..2021-22 | all | all | all (±1.5) | ML + total | SBR consensus |
  | 2022-23 | 1,303 close, 1,399 last | 1,038 / 1,399 | 1,312 / 1,399 | — | ESPN multi-book |
  | 2023-24 | all | all | all | ML (1,389) | ESPN multi-book |
  | 2024-25..2025-26 | all | all | all | — | ESPN (ESPN BET, DraftKings) |

## 2. Fair prices (phase B)
- **Devig methods**, two-way and 3-way: multiplicative (proportional), additive, power and
  Shin. Pick the one whose closing fair probabilities have the lowest log loss on results
  (moneyline, puck line, totals separately; 2015-2026), since favourite-longshot bias
  differs by market.
- **Consensus:** median of the reference books' fair probabilities
  (`REFERENCE_BOOKS`: LowVig, DraftKings, FanDuel, Caesars, BetMGM, ESPN BET). LowVig and
  the 4Casters exchange are also reported on their own, as the sharpest single quotes we have.
- **Totals at different lines across books** are compared on one line by converting with the
  model's distribution (the probability shift between, say, 5.5 and 6.0), not by assuming
  books agree.

## 3. Model prices at any line (phase C)
`markets.py` gains a **score matrix** per game: P(home goals = i, away goals = j, ended in
regulation / OT / shootout), i, j ≤ 12, from the simulations (507 numbers per game). From it:
- moneyline (2-way incl. OT/SO);
- puck line ±1.5;
- totals at the book's main line, with push probability for whole numbers (graded the book
  way: OT goals count, a shootout adds one).

The same matrix prices alternates, team totals and regulation 3-way later without new
simulation.

Period markets need per-period scores, so they wait (the engine can record them later).
The pregame backtest is re-run once, storing matrices, so history is priced at the exact
book line.

## 4. Model vs market on history (phase D)
For each season 2015-16..2025-26, using the **`pregame`** variant only (projected lineups,
starter mixture: what we'd know that morning):
1. **Information test:** logistic regression of the result on logit(closing consensus) and
   logit(model). A positive, stable model coefficient means the model adds information
   beyond the close. This is the honest gate: without it, edges are noise.
2. **Blend:** p = σ(a·logit(market) + b·logit(model)), fitted per market on earlier
   seasons only, tested on later ones (rolling, as with the starter model).
3. **Simulated betting at the open** (seasons with opening lines): bet when the blended
   probability beats the opening price by the minimum edge; measure CLV against the
   devigged close and ROI on results. Thresholds and Kelly fraction are tuned on 2015-2019,
   reported on 2020 on.

**CLV per bet** = P_close_fair × decimal price taken − 1 (expected return at the closing
fair price). Also reported: share of bets that beat the close, and the close-to-open line
move in our direction.

## 5. Staking
- **Edge** = blended probability × decimal price − 1 at the best available price among
  bettable books.
- **Kelly fraction** (proposed ¼), **minimum edge** (proposed 2% ML, 3% puck line and totals,
  tuned in phase D), **caps**: per bet, per game (correlated sides: ML and puck line on the
  same team count together) and per day.
- Bets that pass are `flagged`; the site shows only flagged bets.

## 6. Live pipeline (phase E)
`nhl edges [--date D]`, run after `nhl pregame`:
1. latest lines from the live captures;
2. model prices from the latest `pregame/prices` snapshot (score matrices);
3. devig, consensus, blend, edges, stakes →
   `pregame/edges/{date}/{stamp}.parquet` (snapshots, never overwritten);
4. new flagged bets append to a **paper ledger** `bets/ledger.parquet`: time, book, market,
   line, price, model / market / blended probabilities, stake, and the pregame snapshot
   used.

**Nightly grading** (in `nhl update`): fill each ledger bet's closing fair probability,
CLV, result and P/L. `bets/clv/{season}.parquet` feeds the site's tracker.

No cron in M6 either: these are commands the scheduler will call.

## 7. Decisions (owner, 2026-10-06)
1. **Staking:** ¼ Kelly; caps 2% of bankroll per bet, 3% per game, 10% per day.
2. **Bettable books:** all of them: LowVig, 4Casters, DraftKings and the other ESPN-listed
   books (ESPN BET, Caesars, BetMGM, FanDuel). Best available price is taken across all.
3. **Markets in M6:** main markets only: moneyline, puck line ±1.5, main totals. The score
   matrix still prices whole-number totals (pushes) because main totals are often 6.0;
   alternates, team totals and periods come later.
4. **Ledger:** paper trades automatically, plus a command to record bets actually placed
   with the price obtained (`nhl record-bet`); both are graded the same way.

## 8. Done when (from the roadmap)
- The historical sample shows **positive CLV** on the bets the model would have placed
  (2020 onward, thresholds tuned before), or failing that, forward paper trades do.
- The information test shows whether the model adds to the close, per market.
- `nhl edges` runs on today's slate, and the site's flagged bets pass the gate.

## Package
`src/nhl/betting/`:
- `lines.py`: lines table, canonical books, open/close, cleaning;
- `devig.py`: methods and consensus;
- `prices.py`: score matrix → any market;
- `evaluate.py`: information test, blend, historical betting simulation;
- `stake.py`: edges, Kelly, caps;
- `ledger.py`: paper and real-bet ledger, grading.

CLI: `nhl build-lines`, `nhl evaluate-betting`, `nhl edges`, `nhl record-bet`, `nhl grade-bets`.

## Build order
| Phase | What | Gate |
|---|---|---|
| A | Lines table, canonical books, close selection, cleaning | coverage table reproduced |
| B | Devig methods, consensus | method chosen per market on log loss |
| C | Score matrices in the simulator; pregame backtest re-run storing them | prices match existing columns |
| D | Information test, blend, betting simulation at the open | report: CLV on 2020+ |
| E | `nhl edges`, ledger, nightly grading | runs on today's slate |

## Results (2026-10-06)
Code: `src/nhl/betting/` (`lines.py`, `devig.py`, `evaluate.py`), `nhl evaluate-betting`,
[report](../reports/m6-model-vs-market.md). The lines table is built in memory for now
(not yet stored at `odds/lines/`).

- **Lines:** closing prices for every game 2015-16..2025-26 in all three markets. 2,707
  bad history rows dropped, mostly ESPN 2022-23 closes with line and price swapped, so that
  season uses ESPN's last pregame quote.
- **Devig:** the method barely matters; per market the four methods are within 0.00015 log
  loss on 14k closes. Chosen: power (ML), multiplicative (puck line), Shin (totals). The
  close slightly overprices home teams (0.543 vs 0.538) and unders.
- **Model vs close (honest `pregame` prices, 2021-26):**
  - pooled, the model looks informative beside the close (moneyline coefficient 0.35 ± 0.12,
    puck line 0.26 ± 0.11, totals 0.43 ± 0.11);
  - **but out of sample a rolling blend is slightly worse than the close alone** (−0.0005
    to −0.0007 log loss in every market). The pooled effect doesn't hold season to season.
  - **Betting at the open: CLV ≈ 0** over 2,844 bets (moneyline +0.2% with 51% beating
    the close, puck line −0.3%, totals −0.65%). ROI +1.6% is noise.
- With the M4 actual-lineup prices, moneyline bets showed +2.7% CLV: knowing the starter
  before the market does. The gap is the value of early starter information; M5's
  confirmations (phase D there) can only capture it live.
- **Conclusion:** the model is about as good as the close but not better, so the site must
  not flag bets yet. Next accuracy work belongs in the model (the M4 calibration and
  2025-26 gaps, team terms, goalie terms) and in timing: bets placed when news (starters,
  lines) lands before the market moves, which only forward paper trading can measure.
