# Plan: Player props (M9)

**Status:** planned 2026-10-08. **Phase A built 2026-10-08:** FanDuel (`sources/fanduel.py`,
`nhl poll --what props`, with every odds poll) and LowVig (`sources/dst.py`,
`nhl poll --what props_lowvig`, every 15 min on game days and ~7 min inside T−90). First
polls: FanDuel 4,064 prices over 14 games in 36 s, LowVig 4,227 over 10 games in 74 s, every
player resolved to an NHL id. **Phase B built 2026-10-08:** goals, assists and points
(`src/nhl/props/`, `nhl props-backtest`): gate passed on 6 of 7 targets in every season, and
on goals ≥ 2 in 9 of 10 (see [Phase B results](#phase-b-results-2026-10-08),
[report](../reports/props-backtest.md)). **Phase D built 2026-10-08:** live projections,
edges and a props paper ledger (`src/nhl/props/live.py`, `ledger.py`; `nhl props-edges`; run on
every odds / props poll that moves and every reprice; graded nightly by `nhl grade-bets`).
See [Phase D](#phase-d-live-edges-and-the-paper-ledger-2026-10-08). **Phase E built 2026-10-08:**
the site's Props tab, a Props sub-tab on every game page, and Model Results → Props
(`src/nhl/api/routers/props.py`; `frontend/src/app/props`, `components/GameProps.tsx`).
**Depends on:** M2 game logs (`processed/game_logs/player/{season}`), M3 ratings and
finishing terms, M4 simulator, M5 projected lineups and goalies, usage tiers and on-ice
projection ([usage-context.md](usage-context.md)), the prop table (`nhl.odds.props`).
**Feeds:** a Props tab on the site, a Props sub-tab on each game page, and a paper ledger for props.

---

## TL;DR
1. **Collection is mostly there.** DraftKings props already arrive through ESPN's
   `propBets` on every odds poll: 26 games and ~140 polls since 2026-10-05, stored as price
   transitions. 4Casters props arrive too, but are thin. Add **FanDuel** (public JSON, plain
   HTTP) and **BetOnline/LowVig** (one headless-browser walk; the two are the same feed).
2. **Model each stat as ice time × rate × opponent × game script, tied to the team
   simulation.** Goals and assists come from attributing the simulator's team goals to players, so
   prop prices agree with our moneyline and total. Shots on goal, blocks and saves need a
   shot-volume model the simulator doesn't have yet.
3. **The mean is the hard part, not the distribution.** Within a player-season, game-to-game
   variance over mean is 1.05-1.09 for shots on goal and blocks, and 0.96-0.97 for goals and
   assists. Poisson (or a light negative binomial for shots) is enough once the
   per-game mean is right.
4. **We can backtest the model but not the market.** There's no historical prop pricing,
   but there are 16 seasons of game logs. Score the projected distributions against
   outcomes (log loss at the book thresholds, calibration) before any price comparison.
   The market side is forward-only: store every price, every projection and every flagged
   edge, then read CLV and calibration after a few weeks.

## 1. Collection

### What each book gives us (probed 2026-10-08)

| Book | Access | NHL player markets | Two-way hold (median) | Notes |
|---|---|---|---|---|
| DraftKings | **Already captured** via ESPN `propBets` (`espn_odds.py`); direct DK API returns 403 | goals ladder, assists, points, shots, saves, blocks, PP points, first/last goal | 6.6-8.4% | Fine as is |
| FanDuel | `sbapi.va.sportsbook.fanduel.com/api/event-page?eventId=…&tab=…` with the public site key, plain HTTP, 200 | tabs: Goals, Points/Assists, Shots, Goalies, Period Player Props | ~6.6% on two-way SOG | Mostly ladders ("Player to Record 3+ Shots"); a few two-way per game |
| BetOnline / LowVig | DST widget (`troya.xyz/betbuilder?sb=lowvig`), signed per request, so a headless browser must drive it (as `ESPN_FFL/Scripts/bol_widget.py` does) | goals, assists, points, SOG ladders; O/U points, saves, SOG; first goalscorer; saves H2H | ~6.4% | **LowVig and BetOnline quote identical prices** (checked on one game). Priced by a third-party B2B provider (DST), probably the softest and slowest to move, but likely with low limits |
| 4Casters | Already captured | goals, assists, points, shots, saves | 4.8-8.1% | Little liquidity for props |

**Plan:**
- **FanDuel poller** (`sources/fanduel.py`):
  - The schedule page lists events; each game needs four tab calls.
  - Each poll stores both the two-way markets and the ladders, folded into `props_frame`'s
    vocabulary (`milestone_line`).
  - Runs on the odds schedule: every 15 min within 24 h, every 5 min inside T−90.
- **DST poller** (`sources/dst.py`):
  - Port `bol_widget.py`'s browser walk with `league="NHL"`, `sb=lowvig`, and store the
    data as book "LowVig".
  - A full walk takes ~75 s (one click per game tile, seven category tabs), so it runs
    every 15 min on game days and in between inside T−90.
  - Playwright becomes an optional dependency.
- **No new storage.** `external/odds/props/{season}/{source}.parquet` already stores price
  transitions (line movement) per book.

### Collection risks
- The FanDuel site key and DST's markup can change without notice. Each poller fails loudly
  (no silent empty scrape) and the nightly report counts props per book per game.
- Player name resolution: DST and FanDuel give names, and DST gives a team. Reuse
  `resolve_player`, and log the resolution rate per book (DK and 4C are at ≥99.5%).

## 2. Modeling

### The decomposition
For player *i* in game *g*:

  E[stat] = Σ over strengths (EV, PP, PK) of TOI_i,s × rate_i,s × opponent_s × script_s

- **TOI** comes from the projected lineup slot (F1-F4, D1-D3, PP1/PP2, PK1/PK2 from M5 and
  DailyFaceoff), the player's own recent TOI share within that slot, and expected special-teams
  time from the simulator's penalty rates (both teams' and the referee crew's). Score effects come
  from the simulator: a team expected to trail plays its top players more and has the net
  empty longer. This is the largest single source of error for every prop.
- **Rate** is the player's per-60 talent, shrunk to a position/tier prior, the same
  way the ratings are built:
  - **Shot attempts** (iCF/60) are very stable year to year.
  - **P(on goal | attempt)** is each player's own shooting accuracy plus the block companion
    model (roadmap M1 follow-ups).
  - **Goals per shot** come from the M3 finishing terms (talent-adjusted xG with
    the shooter's term and the opposing goalie's term).
  - **Primary/secondary assist share** of on-ice goals comes from the archetype
    point-attribution work ([archetypes.md](archetypes.md) §Point attribution).
- **Opponent:** the other team's shot suppression and xGA (team-level allowed rates, and
  their goalie for goals and saves), plus home/away and rest.

### Per market
| Market | Built from | Distribution |
|---|---|---|
| Goals (anytime, 2+) | Simulator team goals × player share; share = expected individual xG adjusted for finishing ÷ team expected xG (per strength, including empty-net goals for players likely to be on ice late) | Taken from the simulation draws, so the correlation with the team total carries through |
| Assists, points | Each simulated team goal → player on ice with P(on ice) ≈ TOI share by strength; involved with P(A1), P(A2), from playmaking talent | From the simulation draws |
| Shots on goal | Expected shot attempts × P(on goal \| attempt), with team pace from a new team shot-volume model | Negative binomial, dispersion fit on game logs (~1.07) |
| Blocks | Opponent's shot attempts × player's block share while on ice (D-heavy) × TOI | Negative binomial |
| Saves | Opponent's shots on goal × (1 − goals-against share), × P(start) × P(not pulled); voided if he doesn't start | From the simulation draws plus the shot model |
| First goalscorer | Player's share of team goal rate × P(team scores first) from the simulator; "no goal" is part of the outcome space | Closed form |

**The new piece the simulator needs:** a **team shot-volume layer**. Today the simulator
draws goals only. Give each team-state an expected shot-attempt rate and an on-target rate
from the same ratings (shot attempts for and against per 60, from a shot-count RAPM or
from xG ÷ league xG per attempt), so shots, blocks and saves come out of the same simulation as
goals.

### How we test it (no prop prices needed)
1. **Point-in-time backtest:** for every game 2016-2026, project each dressed skater and
   goalie using only the data available before that game (weekly rating snapshots already exist), and
   the **actual** lineup (an upper bound) and the **projected** lineup where we have it (2026 on).
2. **Score at the book thresholds:**
   - Log loss and Brier for P(goals ≥ 1), P(points ≥ 1), P(assists ≥ 1), P(SOG ≥ 2, 3, 4), P(saves ≥ 25.5 …).
   - Calibration plots by decile and by tier.
3. **Baselines it must beat:** (a) season-to-date per-game average; (b) a TOI-weighted
   recent rate (last 10 games); (c) the same model without opponent and script terms.
   Each term earns its place by the out-of-sample log loss it adds, the same bar as the
   ratings work.
4. **Market check, forward only:** from the first day of capture, devig each book (and
   the consensus) and compare log loss against the model on outcomes; fit the
   logit blend (model vs market) once ~4-6 weeks of graded props exist.

### Devigging props
- **Two-way markets:** the same methods as game lines (multiplicative by default; power for
  ladders).
- **One-sided ladders and yes-only markets** (anytime scorer, 3+ shots): no opposite price.
  - Estimate each book's margin by price level from its own two-way markets on the same
    stat.
  - Alternatively, fit a Poisson or negative binomial to the whole ladder and read the vig
    as the excess over 1.
  - Favourite-longshot bias is strong at long prices, so keep long rungs (≥ +400) out of
    play recommendations until the tracking says otherwise.

## 3. Edges and site

- **Edge** = model (later: blend) probability × decimal price − 1, at each book's best
  price for that side. Same ¼-Kelly sizing as the game lines, but with a lower unit cap and a
  higher minimum edge (proposed 5%) until the blend is fit.
- **Props tab:**
  - Today's best edges across all games.
  - Filters for market, book, team and minimum edge.
  - Each row shows: player, market, line/side, best book and price, model probability,
    devigged market probability, edge, RISK, and line movement since open.
- **Game page, Props sub-tab:** every dressed player for both teams, grouped by
  market, with the model's line (median and P(≥ each threshold)) beside each book's
  price. A player who is out shows as void.
- Precompute like the ratings boards: `site/props/{date}/` written after each pregame run.

## 4. Tracking (what we look at in a few weeks)

Three new tables, all append-only:
- `predictions/props/{date}.parquet`: each pregame run's projection per player × market
  × threshold, with `as_of`, the inputs (lineup slot, TOI, rate, opponent factor) and the
  rating snapshot date, so changes in edge can be explained later.
- `betting/props_edges/{season}.parquet`: every edge at or above a low reporting threshold
  (say 2%), with the price, book, devigged consensus and model probability, captured
  each time it changes.
- `betting/props_ledger`: paper bets placed at our thresholds. Graded nightly from game logs:
  - Result.
  - CLV against that book's last price before puck drop and against the consensus close.
  - Voids for scratches.

Weekly read (no backtest needed): CLV and ROI by market, book, edge bucket, price
range, tier and position. Model-vs-market log loss on outcomes. Calibration of model
probabilities that disagree with the market. That is how we decide which markets to trust and
how to weight the blend.

## Phases
| Phase | Work | Gate |
|---|---|---|
| A | FanDuel + LowVig pollers in the odds schedule; nightly coverage report per book | ≥95% of scheduled games with props from each book; ≥98% player resolution |
| B | Projection backtest for goals, assists, points (simulator attribution + TOI model) | Beats all three baselines in log loss at ≥1 thresholds, every season |
| C | Team shot-volume layer; SOG, blocks, saves | Same gate; shots calibrated by decile |
| D | Daily projections, edges, paper ledger, nightly grading | Runs in the pregame cron (**built 2026-10-08**) |
| E | Props tab and game sub-tab | — |
| F | After ~4-6 weeks: blend fit, thresholds, which markets/books to bet | CLV > 0 on flagged plays |

Phase A is worth starting now: every day not captured is lost, and the modeling
phases need that history for F.

## Phase B results (2026-10-08)

**Method** (`src/nhl/props/`):
- `rates.py`: point-in-time goal and assist rates per 60 by strength (even incl. empty net,
  PP, SH), from this season's earlier games plus two prior seasons weighted 1 / 0.6 / 0.35,
  shrunk toward the position group's league rate. Goals = individual xG rate × shrunk
  finishing. Shrinkage tuned on 2016-17 to 2018-19: flat optimum (50 min xG, 250 min assists,
  20 xG finishing).
- `project.py`: each team goal goes to a scorer in proportion to projected deployment share
  × goal rate (per strength, mixed by the league's goal share by strength) and to assisters
  the same way, scaled to the league's assists per goal. Given team goals G, the player's
  count is Binomial(G, p). G comes from the pregame score matrix with the shootout goal
  removed.
- `backtest.py`: 444,883 skater-games, 2016-17 to 2025-26, players who dressed and were in
  that morning's projected lineup. Deployment is the morning projection (cached at
  `predictions/pregame_deployment/`), outputs at `predictions/props_backtest/`.

**Pooled log loss** (lower is better):

| target | model | without the team term | season average | last 10 |
|---|---|---|---|---|
| goals ≥ 1 | **0.3926** | 0.3938 | 0.4105 | 0.4187 |
| goals ≥ 2 | **0.0763** | 0.0766 | 0.0786 | 0.0838 |
| assists ≥ 1 | **0.5186** | 0.5206 | 0.5331 | 0.5423 |
| assists ≥ 2 | **0.1533** | 0.1544 | 0.1565 | 0.1642 |
| points ≥ 1 | **0.5970** | 0.5997 | 0.6087 | 0.6178 |
| points ≥ 2 | **0.2633** | 0.2654 | 0.2671 | 0.2769 |
| points ≥ 3 | **0.0794** | 0.0804 | 0.0807 | 0.0847 |

- The model beats both naive baselines by a wide margin (0.012-0.018 log loss at the ≥ 1
  thresholds) and the version without the team term in every season, except goals ≥ 2 in
  2017-18 (0.0732 vs 0.0731).
- The team term (the simulator's goal distribution for this team against this opponent)
  is worth 0.001-0.003. That's small next to the player shares, but it is the part that ties
  props to the game lines.

**Tested and rejected:**
- Spreading players apart with rate^γ (γ 1.1-1.6): worse on every target.
- A beta-binomial for game-to-game variation in the player's share (κ 5-100): worse or
  equal on every target.
- A logit recalibration fitted on 2016-19 and tested on 2019-26: +0.0001 to +0.0005 (slopes
  1.03-1.08). Left to the market blend in phase F.

**Open items:**
- Stars' 2+ point nights are under-predicted (top decile 24.0% vs 26.7% realized), and
  low-usage players are slightly over-predicted. Candidates: team-specific PP goal share
  (stars carry the PP), and score-state ice time.
- Shots on goal, blocks and saves wait for the team shot-volume layer (phase C).

## Phase D: live edges and the paper ledger (2026-10-08)

- **Projections:** each game's last pregame snapshot (score matrix and projected lineup) plus
  rates from every game logged before today (`rates.rates_for_day`, the backtest's math).
  Then the backtest's logit calibration per (stat, k) (`project.CALIBRATION`, slopes
  1.01-1.11), which corrects the compression found in phase B. Cached per pregame run at
  `pregame/props/{date}/{stamp}.parquet`, with slot, PP unit, P(dressed) and lineup source.
- **Market:**
  - Two-way markets are devigged multiplicatively.
  - A one-sided ladder rung is divided by 1 + that book's median two-way margin on the same
    stat that day (7% without any).
  - The consensus is the median across books. A book more than 10 points from it is
    skipped as a bad quote.
- **Blend:** a 50/50 logit average of model and market. This is a placeholder until phase F
  fits it on graded props.
- **Flags:** edge ≥ 5%, price ≤ +400, ≥ 2 books, and the player projected to dress for
  certain with high lineup confidence.
- **Stakes:** ¼ Kelly, ≤ 0.5 u per bet, 1 u per player-game, 5 u per day of props (separate
  from the game-line caps).
- **Storage:**
  - Every best quote per (game, player, stat, line, side) is snapshotted at
    `pregame/props_edges/{date}/{stamp}.parquet`.
  - New flags go to `bets/props_ledger.parquet` (first price only).
  - `nhl grade-bets` grades them nightly from game logs: result (void if he didn't play),
    the book's closing price, the devigged closing consensus, CLV and units.
- **Known:** the props table stores price changes, not withdrawals, so a quote a book has
  pulled still looks live until it changes. First night (2026-10-08): 3,104 quotes, 21
  paper bets, 5.05 u.

## Phase E: site (2026-10-08)

- **API:**
  - `/api/props?date=`: each game's last edges snapshot. Includes every quote with an edge of
    at least −2%, every flagged one, and every bet already placed, even if its edge has
    since faded. Each row carries its opening price at the same book (line movement) and the
    ledger bet as placed.
  - `/api/games/{id}/props`: projections per player, every book's latest quote (devigged,
    with the consensus; closing quotes once the game starts) and the game's edges.
  - `/api/props/bets`: the props ledger, with totals and a breakdown by stat and side.
- **Props tab:**
  - Plays, positive edges at +400 or shorter, or all edges.
  - Filters for stat, book and player/team.
  - Each row: model vs market, edge, best price with its opening price, and the RISK badge
    with the stake as placed.
- **Game page, Props sub-tab:**
  - Best edges for the game.
  - Each team's board: expected G/A/Pts and 1+ G, 1+ A, 1+ and 2+ Pts. Each cell shows the
    model %, the best price and the consensus, highlighted when it's a play.
  - Click a player for every book's prices.
- **Model Results → Props:** the graded ledger (result, close, CLV, units).
- **Stake caps fix:** a bet already in the props ledger now keeps its placed stake and uses up
  the per-player and per-day caps; only new bets are sized in what's left. Before this, the
  first night's later runs could take the day past 5 u (2026-10-08 ended at 5.87 u).

## Open questions
- **Limits:** LowVig/BetOnline prop limits are probably small. FanDuel and DraftKings limit
  winning prop bettors quickly. CLV per book will tell us where an edge is real and where
  it's only a stale price.
- **Late scratches and goalie changes:** books void props for players who don't play. A
  goalie who starts but is pulled still has action, so saves need P(pulled).
- **DST H2H and period props:** out of scope until the base markets are tracked.
