# NHL-Data scheduler: what runs when, and why

**Where:** this Mac's user crontab, in one managed block between `# BEGIN NHL-Data` and
`# END NHL-Data`. The source of truth is `scripts/crontab.nhl`; install it with
`scripts/install_cron.sh` (see [Install, update, remove](#install-update-remove)). Times are
local, and this machine runs on **America/New_York**, so every time below is ET.

**Requirement:** cron doesn't run while a Mac sleeps. This machine has system sleep off
(`pmset -g`: `sleep 0`); keep it that way, or jobs during sleep are silently skipped.

## Principles
1. **A missed capture is permanent.** DailyFaceoff, ESPN injuries and odds only show the
   present. Pollers store only *changes*, so a poll where nothing moved costs a few
   requests and writes nothing. That's why polling is dense near puck drop.
2. **Poll only when it matters.** Every poll takes a `--window N`: it runs only if a game
   starts within N minutes. Off days and the offseason are quiet without editing cron.
3. **Model prices follow the news.** A lineup, goalie, injury, transaction or referee change
   reprices today's games in the same run (`--reprice`). Odds never reprice: the model
   doesn't read them.
4. **Stay out of other jobs' way.** This machine runs ~80 other cron jobs, and between
   them every minute that is a multiple of 5 is taken (:00, :05, :10 … :55), plus a cluster
   from 05:35 to 07:15. NHL jobs use only the other minutes, one residue class per job type
   (below), and the heavy nightly job runs at 04:19, the quietest slot.

## A game day, hour by hour

| Time (ET) | What runs | Why then |
|---|---|---|
| **04:19** | `nightly.sh`: `nhl update` (catalog → ingest last night's games → events, xG, freeze → game state → **rating snapshot dated today**), then a transactions + injuries poll, then **`nhl site-tables`** (the site's ratings boards from the new snapshot, so off days stay current, plus team matchup views and the archetype history), then **`nhl grade-bets`** (CLV against our captured closes, results, units), then **`nhl site-views --force`** for yesterday and today (final scores, graded bets) | The last West Coast games end ~01:30 and the NHL posts shift data soon after; done before the 05:35 cluster. The snapshot dated today uses only games before today, so it's point-in-time for tonight. |
| 00:00-24:00 | Odds and FanDuel props every 15 min (:03 :18 :33 :48) while a game is within 24 h; DraftKings props come with the ESPN odds | Captures **opening lines** whenever books post them (often the evening before) and the drift through the day. |
| **08:07** | First lineup poll (DailyFaceoff goalies, ESPN injuries, transactions, referee crews), then every 15 min (:07 :22 :37 :52) until 23:52 | News starts with morning reports. On an off day (no game within 16 h) it skips. |
| **08:11** | DailyFaceoff line combinations, hourly at :11 until 23:11 | Lines change after practices and morning skates; 32 pages take ~65 s, so hourly is polite. |
| **09:14** | `pregame.sh`: the **morning slate**, always, then **`nhl edges`** | The first full set of prices with the new ratings snapshot, even if no source changed overnight, and the first edges of the day against the morning odds. Every pregame run that writes (this one and each `--reprice`) also rebuilds the site's ratings boards under `site/ratings/` (~5 s, non-fatal; see `nhl.site.tables`). |
| 10:00-12:00 | Morning skates: DailyFaceoff "Likely"/"Confirmed" starters arrive | Picked up by the 15-minute lineup polls; each change reprices. |
| 10:30-15:30 | Scouting the Refs posts tonight's crews | Picked up by the officials part of the lineup poll; reprices with the crew's penalty factor. |
| **T−90 min → puck drop** | Lineup poll every 5 min (:02 :12 :17 :27 :32 :42 :47 :57, plus the 15-min slots); odds every 5 min (:08 :13 :23 :28 :38 :43 :53 :58, plus the 15-min slots); lines at :26 :41 :56 | Confirmed starters, scratches from warmups, the **closing line**. |
| ~T−40 min | The NHL right-rail lists the officials | Caught by the 5-minute lineup poll (`officials` includes the right-rail check); late referee swaps reprice. |
| Puck drop + 20 min | Window closes for that game | A late puck drop still counts. With later games, the windows stay open until the last one starts (~22:00-22:30). |
| After the last puck drop | Only odds (for tomorrow's openers) | Nothing else moves until morning. |

**Edges** (`--edges` on every poll): after a reprice, or whenever an odds poll stores a
change, `nhl edges` compares the latest prices with the latest odds from every book, sizes
stakes (¼ Kelly, 2 u per bet, 3 u per game, 10 u per day; bankroll 100 u) and writes
`pregame/edges/{date}/{stamp}`. A newly flagged bet (moneyline, puck line or total) goes into the paper ledger `bets/ledger.parquet` at the price available then.
Real bets go in with `nhl record-bet`. See `src/nhl/betting/edges.py`.

**Site views** (every poll that passes its window, plus `nhl pregame`, `edges`, `props-edges` and
`record-bet`): the site's heavier API responses are prebuilt as JSON under `site/views/` and
served as stored bytes. Each step rebuilds what it changed (`markets` after odds or a reprice,
`props` after prop quotes, `lineups` after a reprice) plus any view that is missing or went
stale at a puck drop; a started game's tabs are frozen once rebuilt at its close. The API
builds live only when a view is missing or invalid. A full day is ~5-10 s; a poll with nothing to
do costs one index read. See `src/nhl/site/views.py` and `src/nhl/site/publish.py`.

**Prop edges** run alongside: after a reprice, an odds poll that stores a change (ESPN's
DraftKings props come with it), or a props poll that stores one, `nhl props-edges` prices
goals / assists / points props, writes `pregame/props_edges/{date}/{stamp}` and puts new flags
in `bets/props_ledger.parquet` (caps 0.5 u per bet, 1 u per player-game, 5 u per day). The
nightly `nhl grade-bets` grades both ledgers. See `src/nhl/props/live.py`.

**Each reprice** takes ~15 s and writes a new pregame snapshot: lineups, goalies, prices,
the one-row-per-game slate and input freshness, plus `pregame/latest/{date}.json`
(see `src/nhl/pregame/slate.py`). One reprice runs at a time; a second waits for the
first and then prices with everything captured so far.

## Jobs

| Job | Cron | Script / command | Window | Lock | Log | Typical run |
|---|---|---|---|---|---|---|
| Nightly rebuild | `19 4 * * *` | `nightly.sh` → `nhl update`, `nhl poll --what transactions,injuries`, `nhl site-tables`, `nhl grade-bets`, `nhl site-views --force` (yesterday, today) | always | `nightly` | `logs/nightly.log` | 40 s - minutes ¹ |
| Morning slate | `14 9 * * *` | `pregame.sh` → `nhl pregame` (+ site tables), `nhl edges` | always (no-op without games) | `pregame` + run lock | `logs/pregame.log` | ~20-35 s |
| Lineups, game day | `7,22,37,52 8-23 * * *` | `poll.sh goalies,injuries,transactions,officials --window 960 --reprice` | game within 16 h | per source list | `logs/poll_lineups.log` | ~30-60 s (+15 s if repriced) |
| Lineups, pregame | `2,12,17,27,32,42,47,57 * * * *` | same, `--window 90` | game within 90 min | same lock as above | `logs/poll_lineups.log` | same |
| Lines, game day | `11 8-23 * * *` | `poll.sh lines --window 960 --reprice` | game within 16 h | `lines` | `logs/poll_lines.log` | ~65-80 s |
| Lines, pregame | `26,41,56 * * * *` | same, `--window 90` | game within 90 min | `lines` | `logs/poll_lines.log` | same |
| Odds, baseline | `3,18,33,48 * * * *` | `poll.sh odds,props --window 1440` | game within 24 h | `odds,props` | `logs/poll_odds.log` | ~55-75 s (FanDuel props ~35 s of it) |
| Odds, closing | `8,13,23,28,38,43,53,58 * * * *` | `poll.sh odds,props --window 90` | game within 90 min | `odds,props` | `logs/poll_odds.log` | same |
| LowVig props, game day | `4,19,34,49 9-23 * * *` | `poll.sh props_lowvig --window 960 --edges` | game within 16 h | `props_lowvig` | `logs/poll_props.log` | ~75 s (headless Chromium) |
| LowVig props, pregame | `9,24,39,54 * * * *` | same, `--window 90 --edges` | game within 90 min | `props_lowvig` | `logs/poll_props.log` | same |
| Novig | `14,29,44,59 * * * *` | `poll.sh novig --window 1440 --edges` | game within 24 h | `novig` | `logs/poll_novig.log` | ~5 s signed (one websocket snapshot); up to 7 min public (`TIME_BUDGET_S`) ² |

¹ 40 s early in the season; a few minutes later on. See [Timing](#timing).

² With the `trading::read` key, every order book arrives in one websocket snapshot. Without
it (or if the websocket fails) books are read over REST, one request per market (~250 per
game) at ~2 requests/s on the public routes, so a poll reads game lines first, then props for
the soonest games, and leaves the rest for the next poll. See `src/nhl/sources/novig.py`.

**Minute map.** Each job type has its own residue mod 5, so NHL jobs never start on the
same minute as each other or as any other job on this machine:

| Minute mod 5 | Used by |
|---|---|
| 0 | other jobs (Rebirtha, rebirtha-nfl/cfb, ESPN FFL, Iterable, FLS reports, …) |
| 1 | NHL lines (:11 :26 :41 :56) |
| 2 | NHL lineups (:02 :07 :12 … :57) |
| 3 | NHL odds (:03 :08 :13 … :58) |
| 4 | NHL nightly (04:19), morning slate (09:14), LowVig props (:04 :09 :19 :24 … :54), Novig (:14 :29 :44 :59) |

Other jobs that run every minute (`rebirtha-cfb/pool_lock_watch.sh`) or for minutes at a
time (the NFL refreshes at :30) can still overlap in time; NHL polls are light (HTTP plus a
small S3 write), and only the nightly job is heavy.

## Off days and the offseason
- **No game within the window:** the poll prints `no game within N min — skipping` and
  exits 0. On an off day only the nightly job and the morning slate run (the slate finds
  no games and exits).
- **News on off days:** the nightly job polls transactions and injuries, so IR moves and
  recalls from an off day are in place before the next morning.
- **Offseason (July-September):** the windows keep the pollers quiet. The nightly job still
  runs (cheap without new games). To stop everything, `scripts/install_cron.sh --remove`.

## Input freshness (what the schedule guarantees)
Every pregame run reports each input's age and warns when it passes its limit
(`MAX_AGE_HOURS` in `src/nhl/pregame/slate.py`); stale inputs are listed on every slate row.

| Input | Limit | Normally refreshed |
|---|---|---|
| DailyFaceoff starting goalies | 6 h | every 15 min from 08:07 on game days |
| DailyFaceoff lines | 24 h | hourly from 08:11 on game days |
| ESPN injuries | 12 h | every 15 min on game days, and nightly |
| Transactions | 24 h | every 15 min on game days, and nightly |
| Referee assignments | 24 h | every 15 min on game days |
| Odds | 6 h | every 15 min while a game is within 24 h |
| Rating snapshot | 36 h | nightly at 04:19 |
| Game state (player logs) | 36 h | nightly at 04:19 |

A stale warning on a game day therefore means a job failed or didn't run. Check its log.

## Logs and health checks
- Each run starts with a `=== <job> <UTC time> ===` header, so the last run is easy to find
  (`grep -n '^===' logs/poll_lineups.log | tail -1`). OpsView discovers these cron jobs and
  reads the same header.
- **Healthy outcomes:**
  - `no game within N min … skipping`;
  - `another run is in progress … exiting` (the previous run of the same job hasn't
    finished);
  - a source refusing (`skipped (…)`, exit 0).
- **Failures:**
  - a non-zero exit;
  - `FAILED (...)` for a source;
  - exit 124 (timeout: 600 s for polls, 900 s for the slate, 5400 s for the nightly
    job);
  - `reprice failed`.
- **Quick checks:**
  - `nhl pregame --no-write` prints today's slate and the stale-input warnings without
    writing anything;
  - `grep -E 'FAILED|Traceback|exit 1' logs/*.log | tail`.
- A crashed run leaves a stale lock directory in `/tmp/nhl_data_*.lock.d`; the next run
  clears it automatically when its pid is gone.

## Install, update, remove
```bash
scripts/install_cron.sh            # dry run: shows the diff against the current crontab
scripts/install_cron.sh --apply    # backs up to ~/crontab_backups/, then installs the block
scripts/install_cron.sh --remove   # backs up, then removes only the NHL-Data block
```
To change the schedule, edit `scripts/crontab.nhl` (and this guide), then run
`--apply` again. The installer replaces only the lines between the NHL-Data markers and
never touches other jobs. Don't edit the block with `crontab -e`; the next install
overwrites it.

## Once a season (not scheduled)
Before the first regular-season game:
1. `nhl build-priors`: season-start rating priors from the completed season.
2. `nhl sim-constants`: league constants for the new season.
3. `nhl train-starters --seasons <new season>`: the starting-goalie model, fitted on the
   four previous seasons.

At the end of the season, re-run the evaluations (`nhl backtest-pregame`,
`nhl evaluate-betting`, `nhl evaluate-deployment`) for the season just finished.

## Not scheduled yet
- **M5 phase E calibration** (DailyFaceoff statuses, line agreement, the 0.75 game-time
  rate): a monthly report once ~300 team-games have settled.
- **Log rotation:** polls append a few lines per run (~1-2 MB per month across all logs).
  Trim yearly, or add a `newsyslog` rule.

## Timing
| Job | Measured |
|---|---|
| Lineup poll + reprice | ~45 s (2026-10-06: 1 injury and 8 transactions changed, 9 games repriced) |
| Pregame (no poll) | ~15 s |
| Nightly | **40 s** on 2026-10-06 (4 new games, early season, first run by hand from a bare cron environment). Expect a few minutes late in the season as the season's game state and rating fits grow; the timeout is 90 min and the next busy slot on this machine is 05:35. |
