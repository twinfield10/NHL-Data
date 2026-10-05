# Plan: M2 — game-state data model (stints, lineups, goalie starts, game logs)

**Status:** planned 2026-10-05.
**Depends on:**
- `processed/events`, `processed/shifts` (merged shifts, already persisted for 2010-2026);
- `predictions/xg` (LATEST model);
- `processed/game_venues`, `processed/schedule_context`, `processed/officials`;
- `processed/rink_maps`.

**Feeds:** M3 ratings (players, lines, goalies), M4 simulator inputs, M5 projected
lineups, and the backward-looking site pages (game recaps, lines, player and goalie logs).

---

## TL;DR

Four tables turn play-by-play into the units hockey is actually played in:

| Table | Grain | Why it matters |
|---|---|---|
| `processed/stints/{season}` | constant on-ice personnel + strength | **The core of M3.** Player ratings (RAPM-style ridge regression) are fit on stints: who was on the ice, for how long, and what happened |
| `processed/lineups/{season}` | team-game | the lines, D pairs, PP/PK units each team actually used, and how much each played; the baseline for M5 projected lineups |
| `processed/goalie_starts/{season}` | team-game | the starter (our "starting pitcher"), workload, rest, relief appearances, saves above expected |
| `processed/game_logs/{season}` | team-game and player-game | box-score and on-ice stats by strength, the source for leaderboards and form |

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
- Event counts, for and against from the home perspective:
  - CF/CA (all attempts), FF/FA (unblocked), SF/SA, GF/GA;
  - **xGF/xGA** from LATEST predictions;
  - penalties taken/drawn;
  - faceoffs won/lost.

**Validation:**
- Durations sum to game length (±1%) in ≥99.5% of games.
- Stint goals equal the official score minus shootout in ≥99.5% of games.
- Stint xG sums to the predictions table exactly.

## 2. Lineups (actual lines from shifts)
Per team-game:
- **Dressed:** from the play-by-play roster, with position.
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
- `rest_days` since the last appearance, `starts_last_7d`, `consecutive_starts`,
  `is_back_to_back`, `home`.

Validation: the starter matches the NHL boxscore `starter` flag in 100% of a 200-game
sample (`/gamecenter/{id}/boxscore`).

## 4. Game logs
- **Team-game by strength** (5v5, all EV, PP, SH, all):
  - TOI;
  - CF/CA, FF/FA, SF/SA, GF/GA, xGF/xGA;
  - PP opportunities and PP time;
  - penalties, faceoffs, hits, blocks, giveaways, takeaways.
- **Player-game:**
  - TOI by strength, shifts, average shift;
  - individual: goals, primary/secondary assists, attempts, unblocked attempts, shots,
    ixG, penalties drawn/taken, faceoffs, hits, blocks, giveaways, takeaways;
  - on-ice: CF/CA, xGF/xGA, GF/GA by strength.
- **Arena count factors:** `*_rink_adj` versions of the count stats (CF/FF/SF), using
  per-arena volume factors from the scorer-bias work. Estimated from *prior* seasons only,
  so the adjustment is point-in-time.

Validation: player TOI vs the NHL boxscore within 30 s for ≥98% of player-games; goals and
assists exactly equal to the boxscore.

## 5. Interfaces
- `storage/keys.py`: `stints(season)`, `lineups(season)`, `goalie_starts(season)`,
  `team_game_logs(season)`, `player_game_logs(season)`.
- CLI: `nhl game-state --seasons ...` builds all four. The nightly `nhl update` builds
  them for the current season after scoring xG.
- New package `src/nhl/gamestate/` with `stints.py`, `lineups.py`, `goalies.py`,
  `logs.py`, `validate.py`.

## Phases (parallelizable)
| Phase | Work | Depends on |
|---|---|---|
| A | HTML time-on-ice fallback (section 0) | – |
| B | Stints + validation | shifts (A only to fill the 57 games) |
| C | Lineups + goalie starts | shifts |
| D | Game logs | stints, xG predictions |
| E | Validation report vs NHL boxscores and DailyFaceoff; backfill 2010-2026; nightly wiring | B-D |

A, B and C can run at the same time. D follows B. E closes it out.

## Done when
Every validation bar above is met across 2010-2026 and documented in a generated report
(`docs/reports/m2-validation.md`). The four tables update nightly, and stints are ready
for the M3 ratings fit.

## Open question
**Score/venue-adjusted team metrics** (the standard correction for trailing teams
out-shooting leading ones): build them in M2's game logs, or treat them as part of M3's
team ratings? Recommendation: M3, where they're fit jointly with the other context
effects.
