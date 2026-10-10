# Plan: Price from the open, ladder the official stake, track every checkpoint

**Status:** planned 2026-10-10. **Phase A1 + A2 built 2026-10-10:** `nhl.pregame.horizon`; `nhl pregame` / `edges` / `props-edges` and every poll loop over the horizon; odds polls price newly listed dates; the nightly job prices the horizon and the 09:14 entry is gone from `scripts/crontab.nhl` (installed 2026-10-10). **Phase A3 built 2026-10-10:** hourly odds poll at :03 with a 72 h window. **Phase B built 2026-10-10:** `edges.LADDER` / `ladder_tier`, fills in the ledger (`fill`, `ladder`; the old `tier` column still means validated/unvalidated), `ledger.positions` for the API (totals and the market breakdown count positions, the timing/info breakdown counts fills), a fills marker on the site's ledger. Props keep their own rule (2× before T-4h), not this ladder. **Phase C built 2026-10-10:** `nhl.betting.checkpoints` (`nhl checkpoints`, nightly for yesterday; `--all` backfills), `bets/checkpoints.parquet` / `bets/props_checkpoints.parquet`, `nhl checkpoint-report` (strategies, edge decay, ladder tiers). Props checkpoints stake by the props rules at the moment (2× at least 4 h out). Building it surfaced the back-to-back problem in A1's *Verify*: a starter two days out is now a mixture over who starts the game in between (`price.chained_model_probs`). **Depends on:** M5 pregame (`nhl pregame`,
`--reprice`), M6 edges and the paper ledger, M9 prop edges, the edges snapshots
(`pregame/edges/`, `pregame/props_edges/`) and `nhl replay-ledger` (PR #36).

**Decided (owner, 2026-10-10):**
- no daily caps while tracking; props staked at a tenth of game-line units;
- price a game **as soon as its odds appear**, however far ahead;
- the official stake is **laddered**: a small ceiling at the open, filling toward the full
  per-bet cap at later tiers;
- the official ledger never takes both sides of a market;
- checkpoints include **T-24h**;
- **the 09:14 slate run is dropped.**

---

## TL;DR
1. **We price games almost a day after the books do.** Books hang a game's moneyline a median
   of **31.6 h** before puck drop (max 53 h, 47 games this season). We first price it a median
   of **21 h** after that (max 42 h), because `nhl pregame` only prices *today* and its first run
   is 09:14. Odds are already captured; nothing reads them until the next morning.
2. **Fix the schedule, not the model.** `nhl pregame --date` already prices any date, and the
   rating inputs are as-of joins. The work: price every date that has odds for a game not yet
   started (the *horizon*, typically 2-3 dates); price a date the moment its first odds arrive;
   run the morning price at the end of the nightly rebuild; poll odds hourly up to 72 h out.
3. **The official stake is laddered by time to puck drop.** Early prices are often the best,
   but they rest on projected goalies and lineups, and limits are low. So a bet can take
   only **25% of the per-bet cap before T-24h, 50% until T-6h, and 100% after**. It
   fills up at each tier only if it still flags then, at the price then, and never shrinks.
   That's at most three fills per bet, and still never more than 2 u per bet or 3 u per game.
4. **The site shows one position per bet.** The fills are rolled up into total units and
   average price, and the fills show on expand.
5. **Checkpoints are rebuilt nightly from the snapshots.** For each game they record the best
   quote per side at `open`, `first_flag`, `goalies_confirmed`, `T-24h`, `T-6h`, `T-3h`,
   `T-60m` and `close`, flagged or not. Each checkpoint is sized at the **full** cap, as its own
   all-in strategy, and the checkpoints are never added together. That keeps "what if we'd gone all in at the open" measurable next to the
   ladder.
6. **The payoff:** the report tells us whether the ladder beats all-in at any single timing,
   and the ladder's tiers and fractions are settings to tune from that.

---

## Phase A: price from the open (scheduling)

### A1. Horizon: every date with odds
- `horizon(store, now)` returns every game date that has a game **not yet started with at
  least one captured odds quote**. Today that is 2-3 dates (books post ~30-53 h ahead).
- `nhl.pregame.price.run`, `nhl.betting.edges.run` and `nhl.props.live.run` loop over the
  horizon. A reprice is ~15 s per date, so ~30-45 s for the whole horizon.
- **New trigger:** an odds poll that stores a quote for a game with **no pregame snapshot
  yet** prices that game's date. The trigger is a newly listed game, not a price change, so
  odds still never move the model.
- Lineup news (`--reprice`) reprices every date in the horizon. Injuries and transactions
  matter for tomorrow too.
- Prices more than a day out use the latest rating snapshot, which is missing the games in
  between. That's one or two games in 80+ per team, after shrinkage. Each nightly run
  reprices the whole horizon with the new snapshot.
- **Verify:** `lineups.project` and `starter_probs` for a date 2-3 days out. With no
  DailyFaceoff data they should fall back to `Model` starters and the last-game lineup.
  Check the back-to-back logic, since tomorrow's starter depends on tonight's.

### A2. Nightly price; drop 09:14
`nightly.sh` ends with `nhl pregame` (whole horizon), then `nhl edges` and
`nhl props-edges`, at ~04:30 right after the new rating snapshot. The `14 9 * * *` entry and
`pregame.sh`'s cron line are removed. Morning news is still picked up by the 15-minute
lineup polls from 08:07.

### A3. Catch openers on off days
Today the odds poll runs only when a game starts within 24 h, so on an off day the next
night's openers wait. Change one cron entry:

```
3 * * * *          poll.sh odds,props,novig --window 4320 --edges   # hourly, games within 72 h
18,33,48 * * * *   poll.sh odds,props,novig --window 1440 --edges   # as today
```

The minutes don't change (residue 3), so there are no new collisions. Installing it needs
the owner's OK (`scripts/install_cron.sh --apply`).

---

## Phase B: the laddered official ledger

### B1. Tiers
The ceiling is a fraction of the per-bet cap, chosen by the time to puck drop when the run
happens:

| Tier | When | Game lines (cap 2 u) | Props (cap 0.05 u) |
|---|---|---|---|
| `early` | more than 24 h out | 25% → 0.5 u | 0.0125 u |
| `day` | 24 h to 6 h out | 50% → 1.0 u | 0.025 u |
| `late` | under 6 h | 100% → 2.0 u | 0.05 u |

These are `LADDER = [(24 h, 0.25), (6 h, 0.5), (0, 1.0)]` in `edges.py` and `props/live.py`.
Props usually open on game day, so they mostly start in `day`.

### B2. Fill rule
On each edges run, for each flagged, unblocked side:

1. Work out the tier and its ceiling.
2. If this side has **no fill in this tier yet**: `target = min(¼-Kelly stake now, ceiling)`,
   and `fill = target − units already placed`. Place it if it's at least 0.1 u (props:
   0.0025 u) and fits the per-game / per-player room.
3. Otherwise do nothing. Each tier fills at most once, at its first flagged run, so the
   ledger doesn't change with every 5-minute poll.

What follows from this:
- **A line moving our way shrinks the edge.** If the side no longer flags, it isn't added to,
  and that's the CLV we already captured.
- **The stake never shrinks.** A later, smaller Kelly just means no fill.
- **The other side flagging later stays blocked.** Existing fills stand.
- **A top-up may be at a different line** (total 6.5 at the open, 6 at T-6h). Each fill
  keeps its own line and price, and grades on its own.

### B3. Ledger shape
`bets/ledger.parquet` gains `fill` (1-3) and `tier`. `bet_id` hashes the key plus `fill`.
Grading is unchanged: it works per row, with CLV at each fill's own line. Existing rows
become `fill = 1`.

The laddered history is rebuilt with `nhl replay-ledger`. Snapshots carry `start_utc`, so
the tier at each run is known.

### B4. Site
The bet lists group fills into one **position** per (game, market, side): total units,
unit-weighted average price, CLV and P&L summed over the fills, and the fills on expand
(time, tier, line, price, units). Labels are in Title Case. The API router and the Model
Results totals sum fills, so nothing is counted twice.

---

## Phase C: the checkpoint ledger (backend)

### C1. Checkpoints
The quote at each checkpoint is the **latest edges snapshot at or before that moment**.
Snapshots are written only when something changed, so that is exactly what we would have seen.

| Checkpoint | Moment |
|---|---|
| `open` | the game's first snapshot with both a model price and a market |
| `first_flag` | for each side, the first snapshot where it flags |
| `goalies_confirmed` | the first snapshot whose pregame slate has both starters `Confirmed` |
| `T-24h`, `T-6h`, `T-3h`, `T-60m` | the last snapshot at or before puck drop minus 24 h / 6 h / 3 h / 60 min |
| `close` | the last snapshot before puck drop |

A game with no snapshot by a checkpoint's moment has no row for it. `close` shows CLV ≈ 0. Its
value is the *result*: does the model beat the closing line on outcomes?

### C2. Storage and stakes
`bets/checkpoints.parquet` and `bets/props_checkpoints.parquet` use the ledger schemas plus
`checkpoint`, `flagged` and `snapshot_stamp`. There is one row per (checkpoint, bet key) for
the best quote per side:
- **Flagged rows** are staked at the full cap: ¼ Kelly, 2 u per bet, 3 u per game; props ×0.1.
- **Unflagged rows** have stake 0 but still get CLV, so the edge threshold can be studied too.

### C3. Build, grade, backfill
- `nhl checkpoints [--dates]` reads a finished day's snapshots and slates, writes its rows
  and grades them. It reuses `ledger.grade` / `props.ledger.grade`, refactored to take a
  frame.
- It runs in `nightly.sh` after `nhl grade-bets`, for yesterday.
- The backfill covers every day with snapshots: game lines from 2026-10-06, props from
  2026-10-08. Before Phase A ships, those days have no `T-24h` rows, and `open` means "first
  morning price".

### C4. Report
`nhl checkpoint-report` shows, for each strategy × market × tier (props: × stat):
- **Strategies:** the ladder (official) and all-in at each checkpoint.
- **For each strategy:** bets, units, average edge at placement, average CLV ± SE, ROI.
- **Edge decay:** for each official position, its edge at each later checkpoint.
- **Ladder diagnostics:** how often each tier fills, and the average CLV of the `early`
  fills vs the `late` ones. Together these show whether the fractions or tier times should
  move.

Backend and terminal only for now. A Model Results → Timing page is an optional follow-up.

---

## Phase D: tune (later)
- The tiers and fractions (`LADDER`) are the knobs. Change them when a few hundred graded
  positions say so. For example, if `early` fills carry most of the CLV, raise 25%; if they
  lose to `late`, lower it.
- `nhl fit-blend` by checkpoint. The early market is less efficient, so the model may deserve
  more weight at `open`.

## Open points (defaults chosen; say if you want otherwise)
- **Tier times** are 24 h and 6 h. 12 h was the other option you mentioned. Goalie
  confirmation usually lands around T-8h to T-3h, so 6 h puts the full tier close to it.
- **Props use the same fractions** as game lines.
- **The minimum top-up is 0.1 u** (props 0.0025 u), so fills of a few cents are skipped.

## Build order
1. **Phase A1 + A2** (horizon, new-game trigger, nightly price; drop 09:14). Code and
   tests; dry run on tomorrow's games.
2. **Phase A3** (cron change, with the owner's OK).
3. **Phase B** (ladder, fills, replay the official ledger, site positions).
4. **Phase C** (checkpoints, grading, backfill, report).
5. **Phase D** once the data supports it.
