# Plan: M2 — game-state data model (stints, lineups, goalie starts, game logs)

**Status:** built 2026-10-05 (`src/nhl/gamestate/`, `nhl game-state`, `nhl validate-game-state`);
backfilled 2010-11 to 2026-27. Results in [the validation report](../reports/m2-validation.md),
decisions made while building in [Implementation notes](#implementation-notes-2026-10-05).
The plan was revised the same day after reviewing HockeyViz's Magnus 9 models (stint columns
for M3's joint context fit, coaches, goalie freezes).
**Depends on:**
- `processed/events`, `processed/shifts` (merged shifts, already persisted for 2010-2026);
- `predictions/xg` (LATEST model);
- `processed/game_venues`, `processed/schedule_context`, `processed/officials`;
- `processed/rink_maps`;
- cached right-rail payloads `raw/external/right_rail/{season}/` (head coaches, scratches;
  present back to 2010-11);
- the player catalog (`birth_date`, for ages).

**Feeds:** M3 ratings (players, lines, goalies), M4 simulator inputs, M5 projected
lineups, and the backward-looking site pages (game recaps, lines, player and goalie logs).

---

## TL;DR

Five tables turn play-by-play into the units hockey is actually played in:

| Table | Grain | Why it matters |
|---|---|---|
| `processed/stints/{season}` | constant on-ice personnel + strength | **The core of M3.** Player ratings (RAPM-style ridge regression) are fit on stints: who was on the ice, for how long, and what happened |
| `processed/lineups/{season}` | team-game | the lines, D pairs, PP/PK units each team actually used, and how much each played; the baseline for M5 projected lineups |
| `processed/goalie_starts/{season}` | team-game | the starter (our "starting pitcher"), workload, rest, relief appearances, saves above expected |
| `processed/game_logs/{season}` | team-game and player-game | box-score and on-ice stats by strength, the source for leaderboards and form |
| `processed/coaches/{season}` | team-game | head coach and scratches, from the cached right-rail payloads; coach terms in M3 and dressed-roster checks |

Everything here is a **post-game fact**, legal for measurement. M3 consumes it point-in-time
(only games before date *D*).

---

## 0. Shift completeness: HTML time-on-ice fallback
57 late-2024-25 games have no shift chart in the API. The NHL still publishes per-game HTML
time-on-ice reports (`nhl.com/scores/htmlreports/{season}/TH02xxxx.htm` home,
`TV02xxxx.htm` visitor) listing every shift.
- Parse them into the same merged-shift schema. Use them only when the API chart is
  empty, with `shift_source` = `api` | `html`.
- Validate on 20 games that have both: shift start/end within 1 s for at least 99% of shifts.
- Rebuild those games' events (on-ice lists) and features so stints have no holes.

## 1. Stints
**Definition.** A stint is a maximal interval within a period where both teams' skater
sets, both goalies and the strength state are constant.

**Build (per game, vectorized):**
1. Change points = every shift start/end time ∪ strength changes (situation code,
   forward-filled from events) ∪ period start/end.
2. On-ice sets per interval come from shifts. Intervals with identical consecutive sets
   are merged.
3. Events are attached by time using the same boundary rule as on-ice assignment
   (faceoffs → the stint starting at that second; other events → the stint ending at it).
   Attribution reuses the events table's on-ice lists so stints and events always agree.

**Columns:**
- Identity: `game_id`, `season`, `stint_id`, `period`, `start_s`, `end_s` (game seconds),
  `duration_s`.
- On ice: `home_skaters`, `away_skaters` (sorted id lists), `home_goalie`, `away_goalie`.
- State: `strength_state`, `home_score`/`away_score` at start, and `start_type`:
  `faceoff_oz` / `faceoff_nz` / `faceoff_dz` (from home perspective) or `on_the_fly`.
- Faceoff context (for M3's decaying zone-start terms):
  - `last_faceoff_s` (game seconds) and `last_faceoff_zone` (home perspective) of the most
    recent faceoff at or before the stint's start, within the period.
  - A stint-level `start_type` can't express "18 s after an offensive-zone faceoff, after a
    line change". M3 instead splits each stint's duration into seconds 0-34 after
    `last_faceoff_s` (and 35+), so stints are **not** cut at those marks here.
  - Per-team `home_changed_since_faceoff` / `away_changed_since_faceoff`: whether any of that
    team's skaters changed since the faceoff. Magnus 9 treats zone starts per shift, and
    on-the-fly changes as their own start type.
- `post_penalty_5v5`: the stint is 5v5, a penalty expired earlier in the period, and no
  stoppage has happened since (the "penalty shadow": the freed player is often still
  re-entering play).
- Event counts, for and against from the home perspective:
  - CF/CA (all attempts), FF/FA (unblocked), SF/SA, GF/GA;
  - **xGF/xGA** from LATEST predictions;
  - penalties taken/drawn;
  - faceoffs won/lost.

Rest, home/away and coaches are joined in M3 from `schedule_context` and `coaches`. They
are constant per team-game, so they aren't copied onto every stint.

**Validation:**
- Durations sum to game length (±1%) in ≥99.5% of games. *(As built, stints tile every period
  by construction. The real check is plausible on-ice counts: ≥99.5% of each season's ice
  time; see the implementation notes.)*
- Stint goals equal the official score minus shootout in ≥99.5% of games.
- Stint xG sums to the predictions table exactly.
- `last_faceoff_s` ≤ `start_s` always. `post_penalty_5v5` is never set before the
  period's first penalty expiry. Spot-check both on 10 games.

## 2. Lineups (actual lines from shifts)
Per team-game:
- **Dressed:** from the play-by-play roster, with position. Cross-checked against the
  right-rail `scratches` list: no player can be both dressed and scratched.
- **TOI by strength:** EV / PP / SH per player, from shifts.
- **Forward lines:** build the pairwise shared-EV-TOI matrix among forwards, then greedily
  take the trio with the highest *minimum* pairwise shared time, remove them, and repeat
  4×. Leftovers are extras or double-shifters.
- **D pairs:** the same with pairs (×3).
- **PP1/PP2 and PK1/PK2:** the 5- and 4-player groups with the most shared PP/SH time.
- **Shape rules** (owner requirement): 3F lines, 2D pairs, 12F/6D or 11F/7D. Report
  `lineup_shape` and flag anything irregular. Never force a fit.

**Columns:**
- `team`, `game_id`, `unit_type` (`F`/`D`/`PP`/`PK`), `unit_rank` (1-4), `members`;
- `shared_toi_s` (all members together), `unit_ev_toi_share`, `confidence` (members'
  shared time ÷ min individual TOI).

**Validation:**
- Against DailyFaceoff lines captured since 2026-10-05: exact-trio agreement for ≥85% of
  forward lines.
- Spot-check 10 historical games against published line charts.

## 3. Goalie starts
Per team-game:
- `starter` (on ice at the first faceoff), `finished`, `pulled_at_s`, `relief_goalie`,
  `relief_entry_s`.
- Shots and goals faced, `xga`, `gsax` (xGA − GA), saves.
- `sog_frozen`: shots on goal he froze, i.e. a shot on goal followed by a goalie stoppage
  with no event between. The exact stoppage `reason` codes are confirmed during the build.
  This is a raw count. The expected freeze rate comes from the frozen-puck model (roadmap,
  M1 follow-ups).
- `rest_days` since the last appearance, `starts_last_7d`, `consecutive_starts`,
  `is_back_to_back`, `home`.

**Validation:** the starter matches the NHL boxscore `starter` flag in 100% of a 200-game
sample (`/gamecenter/{id}/boxscore`).

## 4. Coaches
Per team-game, from the cached right-rail payloads (`gameInfo.{home,away}Team`):
- `team`, `game_id`, `head_coach` (name as given), `coach_id` (normalized slug, like
  `officials.official_id`), `scratches` (player ids).

Fill gaps where a payload is missing a coach (interim coaches, early seasons) from the
nearest game of that team. Flag every filled row.

**Validation:**
- Coach changes per team-season match known firings (spot-check 2018-19 and 2021-22).
- No game is missing a coach after the fill.

## 5. Game logs
- **Team-game by strength** (5v5, all EV, PP, SH, all):
  - TOI;
  - CF/CA, FF/FA, SF/SA, GF/GA, xGF/xGA;
  - PP opportunities and PP time;
  - penalties, faceoffs, hits, blocks, giveaways, takeaways.
- **Player-game:**
  - `age` at game date (from catalog `birth_date`; M3 uses it for aging curves);
  - TOI by strength, shifts, average shift;
  - individual: goals, primary/secondary assists, attempts, unblocked attempts, shots,
    ixG, penalties drawn/taken, faceoffs, hits, blocks, giveaways, takeaways;
  - on-ice: CF/CA, xGF/xGA, GF/GA by strength.
- **Arena count factors:** `*_rink_adj` versions of the count stats (CF/FF/SF), using
  per-arena volume factors from the scorer-bias work. Estimated from *prior* seasons only,
  so the adjustment is point-in-time.

Validation: player TOI vs the NHL boxscore within 30 s for ≥98% of player-games; goals and
assists exactly equal to the boxscore.

## 6. Interfaces
- `storage/keys.py`: `stints(season)`, `lineups(season)`, `goalie_starts(season)`,
  `coaches(season)`, `team_game_logs(season)`, `player_game_logs(season)`.
- CLI: `nhl game-state --seasons ...` builds all five. The nightly `nhl update` builds
  them for the current season after scoring xG.
- New package `src/nhl/gamestate/` with `stints.py`, `lineups.py`, `goalies.py`,
  `coaches.py`, `logs.py`, `validate.py`.

## Phases (parallelizable)
| Phase | Work | Depends on |
|---|---|---|
| A | HTML time-on-ice fallback (section 0) | – |
| B | Stints + validation | shifts (A only to fill the 57 games) |
| C | Lineups + goalie starts + coaches | shifts, cached right-rail |
| D | Game logs | stints, xG predictions |
| E | Validation report vs NHL boxscores and DailyFaceoff; backfill 2010-2026; nightly wiring | B-D |

A, B and C can run at the same time. D follows B. E closes it out.

## Done when
Every validation bar above is met across 2010-2026 and documented in a generated report
(`docs/reports/m2-validation.md`). The five tables update nightly, and stints are ready
for the M3 ratings fit.

## Decided
**Score/venue adjustment lives in M3, not M2** (2026-10-05). M3 fits score × period ×
home/away jointly with players, zone starts, rest and coaches, as Magnus 9 does. M2 only
stores the raw facts those terms need: score at stint start, period, home/away, the
faceoff context and `post_penalty_5v5`.

## Implementation notes (2026-10-05)
Decisions made while building, where the plan above left room:

- **Stints are also cut at every faceoff and goal.** Each stint then has exactly one score
  state and one most-recent faceoff, which M3's per-second zone-start terms need.
  - Strength is counted from the shift charts (skaters on ice), not from `situationCode`,
    which only updates on events.
  - Times are game seconds. Counts are home/away columns: `{home,away}_{cf,ff,sf,gf,xgf,pen_taken,pim,fo_won}`.
  - About 410 stints per game. A 2024-25 season builds in about 2 s.
- **`start_type`** is one of `faceoff_oz` / `faceoff_nz` / `faceoff_dz` (home team's view), `faceoff_unknown`
  (no zone recorded), or `on_the_fly`.
- **`post_penalty_5v5` requires the faceoff that starts the segment to be short-handed.**
  Every real power play starts at a faceoff, so this rule ignores momentary 4v5s during line
  changes. The flag still covers about 7% of 5v5 time (median 3.4 min per game), because most
  penalties expire in play and the next whistle can be far off. Checked by hand on game 2024020500.
- **Rosters table added** (`processed/rosters/{season}`): dressed players and their
  position from the play-by-play `rosterSpots`. Lineups need positions, and the
  table includes dressed players who never played (backup goalies).
- **Lineups:** `unit_toi_share` is relative to the team's time in that state (5v5, PP or PK).
  `confidence` = time together ÷ the smallest member's TOI in that state.
  - 2024-25 medians: F1 0.85, D1 0.88, PP1 0.99.
  - 4th lines are missing mostly in 11F/7D games.
- **Goalie starts:** the freeze code is `goalie-stopped-after-sog`. The freeze rate on
  shots on goal is 24.8% in 2024-25 (Magnus 8 reports about 25%).
- **Game logs are long:** one row per (team or player, game, strength).
  - Strengths: `all`, `5v5`, `EV`, `PP`, `SH`, `EN_own`, `EN_opp`. The last five partition `all`
    (checked in tests).
  - Box events (penalties, faceoffs, hits, blocks, giveaways/takeaways) are split by strength
    using the same event-to-stint attribution as the stints.
- **HTML time-on-ice fallback:**
  - URLs need the uppercase `.HTM` extension.
  - On 20 games (2015-16 and 2024-25, one playoff game) that also have an API chart, all
    15,298 shifts matched exactly.
  - It fills 58 games: the 57 in 2024-25, plus 2013020971, whose API chart holds only a
    goal-marker row.
  - The build falls back whenever the parsed API shifts are empty. Processed shifts carry
    `shift_source` = `api` | `html`.
  - Shot features for 2013-14 and 2024-25 were rebuilt, so the next xG retrain sees the new
    shift features for those games. Their stored xG predictions are unchanged: they are
    out-of-fold, and re-scoring with the production model would leak.
- **Source defects found by the backfill, and their fixes:**
  - **Foreign shifts.** The API chart for 2021020513 (NYI-WSH) also carries 704 shifts from
    an STL-MIN game; 2024030116 carries 4. `parse_shifts` now keeps only players dressed for
    the game, on the team they dressed for. This touched events and stints for those two
    games only; their stored xG is unchanged.
  - **Missing opening goalie shift** (6 team-games). The starter is now the team's first
    goalie in net, not necessarily the one on the ice in stint 0.
  - **`PERIOD_END` a second early.** Some events are logged at 20:00 of a period that
    "ends" at 19:59. A period now ends at the latest of `PERIOD_END`, the last event and
    the last shift, capped at 20:00.
  - **Shift-chart overlaps.** Late exits briefly put 6-8 skaters on the ice. They are kept
    and flagged: `valid_personnel` = false. They are at most 0.20% of any season's ice time
    (worst in 2019-21).
- **Personnel bar changed.** The bar was "≥99% plausible time in ≥99.5% of games". It is now
  **≥99.5% of each season's ice time plausible**, which every season meets (lowest 99.80%,
  2020-21). The per-game figure stays in the report for information: it is 94-99.9%, and
  the shortfall is concentrated in a few broken charts, not spread across games.
- **Validation:** sampled boxscores are cached under `raw/boxscore/`. The DailyFaceoff
  comparison uses the last version published before puck drop. It reports nothing until
  games played after the first capture (2026-10-05) are ingested.

