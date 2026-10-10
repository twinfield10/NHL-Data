# Plan: Price from the open, and track paper bets by checkpoint

**Status:** planned 2026-10-10, not built. **Depends on:** M5 pregame (`nhl pregame`,
`--reprice`), M6 edges and the paper ledger, M9 prop edges, the edges snapshots
(`pregame/edges/`, `pregame/props_edges/`) and `nhl replay-ledger` (PR #36).
**Decided already (owner, 2026-10-10):** no daily caps while tracking; props staked at a tenth
of game-line units; the official ledger shows one bet per market (the first flag) and never
takes both sides; every other timing is tracked on the backend only.

---

## TL;DR
1. **We price games almost a day after the books do.** Books hang a game's moneyline a median
   of **31.6 h** before puck drop (max 53 h, 47 games this season). We first price it a median
   of **21 h** after that (max 42 h), because `nhl pregame` only prices *today* and its first run
   is 09:14. Odds are already being captured. Nothing reads them until the next morning.
2. **Fix the schedule, not the model.** `nhl pregame --date` already prices any date, and the
   rating inputs are as-of joins. The work is: run the morning price right after the nightly
   rebuild; price **today and tomorrow** (the *horizon*) on every reprice and edges run; and
   poll odds hourly up to 72 h out, so openers posted on off days are caught.
3. **Earlier pricing changes what the official ledger shows.** The official bet is the first
   flag, so it moves toward the open, with projected goalies and lineups. That's consistent
   with "show the first bet", and each bet already carries its information grade (A/B/C).
   Calling it out here because the site will look different (decision 1 below).
4. **Checkpoints are rebuilt from snapshots, not placed live.** Every edges run already
   snapshots each quote it priced. A nightly job reads those snapshots and records, for each
   game, the best quote per side at each checkpoint (`open`, `first_flag`,
   `goalies_confirmed`, `T-6h`, `T-3h`, `T-60m`, `close`), flagged or not, with a stake where it
   flagged. One code path, idempotent, and new checkpoints can be added later and backfilled.
5. **Each checkpoint is its own strategy.** It's sized as if it were the only bet (same caps
   as the official ledger), graded the same way (CLV at the bet's line, result, units), and
   never summed with the others. The report compares checkpoints with each other and with
   the official ledger.
6. **The payoff is a setting.** `OFFICIAL_CHECKPOINT` (default `first_flag`) picks which timing
   the live official ledger follows. Once a few hundred bets per checkpoint are graded, the
   official bet can move to whichever timing earns the best CLV.

---

## Phase A: price from the open (scheduling)

### A1. Morning price after the nightly rebuild
`nightly.sh` ends with `nhl pregame` and then `nhl edges` / `nhl props-edges` (~04:30
instead of 09:14), right after the new rating snapshot. **Drop the 09:14 run.** Lineup,
goalie, injury and referee news already reprices on every 15-minute poll from 08:07, so
09:14 adds nothing once the 04:30 run exists.

### A2. A two-day horizon
- `nhl.pregame.price.run`, `nhl.betting.edges.run` and `nhl.props.live.run` take a list of
  dates. A new `horizon(store, now)` returns **today plus tomorrow**, limited to dates that
  have a game not yet started.
- `nhl poll --reprice` reprices every date in the horizon. That's ~15 s per date, so a
  two-date reprice is ~30 s.
- **New trigger:** an odds poll that stores a quote for a game with **no pregame snapshot
  yet** prices that game's date. This keeps the rule that odds never move the model: the
  trigger is a newly listed game, not a price change.
- Tomorrow's prices use today's rating snapshot, so tonight's games are missing. That's one
  game in 80+ per team, after shrinkage. The 04:30 run reprices with the new snapshot, and the
  `open` checkpoint carries that staleness in its information grade.
- **Verify:** that `lineups.project` and `starter_probs` behave for a date more than 24 h
  out. With no DailyFaceoff data, they should fall back to `Model` starters and the
  last-game lineup.

### A3. Catch openers on off days
Today the odds poll runs only when a game starts within 24 h. On an off day the next
night's openers (posted ~30 h ahead) wait for that window. Change the cron entry:

```
3 * * * *          poll.sh odds,props,novig --window 4320 --edges   # hourly, games within 72 h
18,33,48 * * * *   poll.sh odds,props,novig --window 1440 --edges   # as today
```

The minutes don't change (residue 3), so there are no new collisions. Installing it needs
the owner's OK (`scripts/install_cron.sh --apply`).

### A4. Site
The date picker already handles any date. Tomorrow's games show model prices and edges from
the first run. Official bets for tomorrow appear as soon as they flag (see decision 1).

---

## Phase B: the checkpoint ledger

### B1. What a checkpoint is
Each checkpoint is a moment per game. The quote it uses is the **latest edges snapshot at or
before that moment**. Snapshots are written only when something changed, so that is exactly
what we would have seen then.

| Checkpoint | Moment |
|---|---|
| `open` | the game's first snapshot with both a model price and a market |
| `first_flag` | for each side, the first snapshot where it flags (the official rule without the both-sides block) |
| `goalies_confirmed` | the first snapshot whose pregame slate has both starters `Confirmed` |
| `T-6h`, `T-3h`, `T-60m` | the last snapshot at or before puck drop minus 6 h / 3 h / 60 min |
| `close` | the last snapshot before puck drop |

A game missing a moment (goalies never confirmed, or first priced after T-6h) has no row for
that checkpoint. `close` should show CLV ≈ 0. Its value is the *result*: does the model beat the
closing line on outcomes?

### B2. Storage
`bets/checkpoints.parquet` (game lines) and `bets/props_checkpoints.parquet`. They use the
official ledgers' schemas plus `checkpoint`, `flagged` and `snapshot_stamp`. There is one row
per (checkpoint, bet key) for the **best quote per side**, flagged or not. Unflagged rows have
stake 0 and still get CLV, so the edge threshold itself can be studied.

### B3. Stakes
Within a checkpoint, the official rules apply as if it were the only strategy:
- game lines: ¼ Kelly, 2 u per bet, 3 u per game;
- props: ×0.1, 0.05 u per bet, 0.1 u per player-game;
- when both sides of a market flag, the bigger edge wins.

Nothing carries over between checkpoints.

### B4. Build and grade
- `nhl checkpoints [--dates]` reads a finished day's snapshots and slates, writes its
  checkpoint rows and grades them. It reuses `ledger.grade` / `props.ledger.grade`, refactored
  to take a frame.
- It runs in `nightly.sh` after `nhl grade-bets`, for yesterday.
- The **backfill** is the same command over every day with snapshots: game lines from
  2026-10-06, props from 2026-10-08. Before Phase A ships, `open` there means "first morning
  price".

### B5. Report
`nhl checkpoint-report` shows, for each checkpoint × market × tier (props: × stat):
- bets, units staked, average edge at placement, average CLV ± SE, ROI;
- how often the official bet and the checkpoint's bet are the same bet;
- an **edge-decay** table: for each official bet, its edge at each later checkpoint. This
  answers "the model still liked it at 6 pm" without placing anything.

Backend and terminal only for now. A Model Results → Timing page is an optional follow-up.

---

## Phase C: choose the official timing (later)
- `OFFICIAL_CHECKPOINT` in `edges.py` / `props/live.py`, default `first_flag`. Live, a
  checkpoint means "place at the first run at or after its moment". The `close` checkpoint
  isn't eligible, because the close isn't known until puck drop. `T-60m` is the latest
  usable timing.
- **Decision rule:** at least 300 graded bets per checkpoint in a market. Switch when a
  checkpoint's CLV beats `first_flag`'s by more than 2 SE.
- The checkpoint data also says whether the **model/market blend** should depend on timing.
  The early market is less efficient, so the model may deserve more weight at `open`.
  `nhl fit-blend` could take a checkpoint.

---

## Decisions for the owner
1. **Official bets move earlier.** With a two-day horizon, the first flag often lands the
   evening before, on projected goalies. *Recommendation:* accept it (it is "show the first
   bet"), and show the information grade on the site's bet rows. Alternative: official bets
   wait for game-day morning until Phase C says otherwise.
2. **Horizon:** today + tomorrow (recommended; it covers the median 31.6 h lead), or every
   game with odds (up to ~53 h, a three-day horizon)?
3. **Checkpoint offsets:** T-6h / T-3h / T-60m, or add T-24h for evening-before openers?
4. **Drop the 09:14 slate** once the nightly price exists (recommended)?

## Build order
1. Phase A1 + A2 (code, tests; dry run on tomorrow's games).
2. Phase A3 (cron change, owner's OK).
3. Phase B1-B4 + backfill.
4. Phase B5 report.
5. Phase C once the data supports it.
