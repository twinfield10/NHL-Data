# Plan: M6 — odds, pricing and betting

**Status:** phases A-D built 2026-10-06. With 10 seasons of honest prices and exact-line pricing, **November-February moneylines pass a first gate** (CLV +1.0% ± 0.4%, positive in both eras); totals fail. See [Phase C/D results](#phase-cd-results-2026-10-06). **Phase E (live betting) built 2026-10-06**: see [Phase E](#phase-e-live-betting-2026-10-06).
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
- **By stretch of season (honest pregame prices, moneyline, 2021-26; model minus close log
  loss):** Nov-Feb **−0.0007** (the model slightly *beats* the close, 3,891 games); October
  +0.0047; March +0.0048; **April +0.0115; playoffs +0.0097**. Late in the season and in the
  playoffs the market spreads teams much further than the model (mean |p − 0.5| 0.124 vs
  0.104 in April; playoffs 0.086 vs 0.067, and the model is 1.4 points low on home teams).
  Likely causes: motivation (tanking, resting, races), deadline rosters, playoff home ice.
  The overall gap is mostly these stretches, so the next M6 test is a **Nov-Feb-only** gate,
  and the late-season and playoff gaps are M4/M3 work.
- **Conclusion:** the model is about as good as the close but not better, so the site must
  not flag bets yet. Next accuracy work belongs in the model (the M4 calibration and
  2025-26 gaps, team terms, goalie terms) and in timing: bets placed when news (starters,
  lines) lands before the market moves, which only forward paper trading can measure.

## Phase C/D results (2026-10-06)
**Phase C.** Each simulated game stores its score matrix (P(home = i, away = j, ended in
regulation / OT / shootout), i, j ≤ 12; `nhl.sim.markets.score_matrix`), so any line is priced
exactly; a whole-number total is P(over | no push), comparable with a devigged price.
`nhl pregame-history` writes honest pregame prices with matrices for 2016-17..2025-26
(`predictions/pregame_history/`, ~7 min per season), including the goalie-candidate fix,
same-day rating snapshots and replacement-level defaults. On 2021-26 moneyline log loss
moved by ≤ 0.0003 from the M5 run, as expected.

**Phase D, 10 seasons** (`nhl evaluate-betting`, [report](../reports/m6-model-vs-market.md)):

| | moneyline | puck line | totals |
|---|---|---|---|
| Blend vs close, out of sample, all games | +0.0005 | −0.0002 | +0.0008 |
| Blend vs close, out of sample, Nov-Feb | **+0.0009** | +0.0003 | **+0.0013** |
| CLV at the open, Nov-Feb (bets) | **+0.99% ± 0.40%** (1,621) | +0.27% ± 0.23% (557) | **−0.95% ± 0.34%** (1,211) |
| CLV at the open, all games | +0.26% ± 0.30% (2,636) | +1.40% ± 0.31% (319, 2023-26 only) | −1.03% ± 0.22% (1,991) |

- **Hold-out check.** Nov-Feb was chosen after looking at 2021-26, so 2016-21 is the honest
  test: moneyline CLV there is **+1.1%** (444 bets) vs +0.96% in 2021-26 (1,177); the
  out-of-sample blend gain +0.0005 vs +0.0013. Same sign, smaller in the hold-out. Totals CLV
  is negative in both eras (−1.7%, −0.4%).
- **ROI** is +6-11% on moneyline bets but noisy (2016-21: +5.9% ± 5.2%). CLV is the metric.
- **Reading:** mid-season moneylines are the one place the model reliably moves toward the
  close. For totals, the model's opening-line edges are the wrong way: the market moves
  against them. Don't bet totals at the open.
- **Caveats:** 2016-22 opening prices are the SBR consensus (not one bettable book); the edge
  thresholds and ¼ Kelly weren't tuned; ~1% CLV is thin after real-world limits and line
  shopping. Forward paper trading is the real test.
- **Next (phase E):** `nhl edges` and the paper ledger, flagging **Nov-Feb moneylines only**
  at first, graded on live CLV against our own captured closes.

## Phase E: live betting (2026-10-06)
The owner wants to bet now, in every month, not only where the backtest passes. Decisions:
stakes in **units** (bankroll 100 u); **¼ Kelly everywhere**, capped at 2 u per bet,
3 u per game, 10 u per day; **totals tracked only** (paper-traded, never flagged).

- **Blend** (`nhl fit-blend`, `models/betting/blend.json`): p = σ(c + a·logit(market) +
  b·logit(model)) per market and segment, on 2016-26 pregame history vs the close.
  Model weight b: moneyline 0.59 Nov-Feb / 0.32 other months; puck line 0.52 / 0.07;
  totals 0.62 / 0.32. So October edges lean on the market and are fewer and smaller.
- **Edges** (`nhl edges`, `src/nhl/betting/edges.py`): latest pregame snapshot (with score
  matrix) against every captured book's latest price; model at the exact line, market =
  consensus (or the book's own price at an off-consensus line), books more than 3 points
  off the consensus skipped as bad quotes; best book per side; flagged when the edge is
  ≥ 2% (moneyline) or ≥ 3% (puck line). **Tiers:** `validated` (Nov-Feb moneyline),
  `unvalidated` (other moneylines, puck lines), `track_only` (totals).
- **Ledger** (`bets/ledger.parquet`, `src/nhl/betting/ledger.py`): a paper bet the first
  time a (game, market, side) is flagged; `nhl record-bet` for real bets; `nhl grade-bets`
  (nightly) fills the closing consensus from our own captures, CLV, result and units, and
  prints CLV and ROI by kind, tier and market.
- **Schedule:** edges after every reprice and every odds change, the morning slate, nightly
  grading (see `docs/scheduler.md`).
- **First live flag (2026-10-06):** FLA moneyline −107 at LowVig, edge +2.2%, 0.58 u,
  unvalidated (model 56%, market 51%, blend 53%).
- **Watch:** CLV by tier after ~200 graded bets per tier; refit the blend after each season.
