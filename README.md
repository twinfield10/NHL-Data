# NHL-Data

NHL play-by-play pipeline and expected goals (xG) model, the base layer for NHL game prediction.

- **Ingest**: raw play-by-play and shift charts from the public NHL APIs, 2010-11 to today.
- **Store**: S3 is the system of record (`s3://tmw-nhl-data`), with a local read-through cache.
- **Transform**: one tidy event table per season, with on-ice players, running score, strength and normalized coordinates.
- **Model**: four XGBoost xG models (even strength, power play, shorthanded, empty net), tuned and tested on season-based splits, with calibration reported.

The original scripts and notebook pipeline (2024) are preserved at git tag `legacy-v1`. Their data is archived at `s3://tmw-nhl-data/legacy/v1/`.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,notebooks]"
cp .env.example .env          # optional; defaults point at tmw-nhl-data / us-east-2
```

AWS credentials come from the standard boto3 chain (`~/.aws/credentials` or env vars).

## Usage

```bash
nhl catalog                    # games, teams, player bios -> processed/
nhl ingest --seasons 2010-2026 # raw play-by-play + shifts for final games not yet stored
nhl build  --seasons 2010-2026 # raw -> processed/events/{season}.parquet
nhl features --seasons 2010-2026
nhl train-xg                   # tune on 2010-22, validate on 2023-24, test on 2024-26, refit, out-of-fold xG
nhl score-xg --seasons 2026    # score the current season with the production model
nhl game-state --seasons 2010-2026   # stints, lineups, goalie starts, coaches, rosters, game logs (M2)
nhl validate-game-state --seasons 2010-2026   # vs official scores + NHL boxscores -> docs/reports/m2-validation.md
nhl train-freeze               # frozen-puck model (M3 phase A) + freeze predictions
nhl build-priors               # season-start priors for finishing, EV/ST ratings, penalties (after each season)
nhl ratings [--as-of DATE | --backfill 2015-2026]   # point-in-time ratings -> ratings/{date}/
nhl evaluate-ratings           # M3 bar -> docs/reports/m3-evaluation.md
nhl sim-constants              # simulator league constants per season (point-in-time)
nhl backtest-sim               # M4 bar -> docs/reports/m4-backtest.md
nhl update                     # nightly: catalog -> ingest -> build -> features -> score -> game state (current season)
nhl poll --what odds,goalies,lines,injuries [--window 90]   # one poll of the live sources
```

Training defaults: fit on 2010-11..2023-24, tune on 2024-25, test on 2025-26. Training
seasons are weighted by recency, with the half-life tuned alongside the tree parameters,
because the shot-to-goal relationship drifts as shot tracking changes. Historical xG is
out-of-fold, one fold per season, weighted toward neighbouring seasons.

Every step is idempotent. `ingest` skips stored games, and `build`/`features` rebuild a season from raw in a few seconds.

Reading data in a notebook:

```python
from nhl.storage.s3 import Store
from nhl.storage import keys

store = Store()
events = store.get_parquet(keys.events(20242025))
xg = store.get_parquet(keys.xg_predictions(20242025))
```

## Site

A read-only web app over the pregame and betting outputs: the day's **games** (model vs
market in team colors, best prices, edges, starters, bets), a **game** page (price through the day,
edges, goalie probabilities, projected lines), **team** and **player** ratings from the latest
rating snapshot (teams composed from today's projected lineups, as the simulator does), live
**edges**, and the **bets** ledger.

- API: `src/nhl/api/` (FastAPI). It reads the S3 data contract in `nhl.pregame.slate` and
  caches stamped snapshots in-process.
- Frontend: `frontend/` (Next.js, Tailwind, react-query). It proxies `/api/*` to the API.

```bash
pip install -e ".[api]"
nhl serve                       # API on http://127.0.0.1:8010 (--reload while developing)

cd frontend && npm install
npm run dev                     # http://localhost:3000 (INTERNAL_API_URL overrides the API address)
```

## Third-party sources

| Source | Module | What | History |
|---|---|---|---|
| LowVig (BetOnline) | `sources/lowvig.py` | moneyline, puck line, totals, team totals, alternates, 1st period, regulation 3-way | live captures |
| 4Casters (exchange) | `sources/fourcasters.py` | the same markets, depth-weighted fill prices (VWAP), alternate rungs, player props | live captures |
| Novig (exchange) | `sources/novig.py` | moneyline, every puck-line and total rung, team totals, player props (goals, assists, points, SOG, saves, PP points, first goal); VWAP at $100 stake (game lines) / $50 (props), thinner sides omitted | live captures |
| ESPN (DraftKings, Caesars, MGM, ...) | `sources/espn_odds.py` | main lines, period markets, regulation 3-way, DraftKings player props | 2019-20 onward |
| SBR archive (via the Wayback Machine) | `sources/sbr_archive.py` | consensus open/close moneyline and total; puck line from 2014-15 | 2010-11 to 2022-23 (2022-23 partial) |
| DailyFaceoff | `sources/dailyfaceoff.py` | starting goalies (status, news, source tweet, post time); line combinations with lineup-shape validation | live captures |
| X oEmbed | `sources/tweets.py` | text and post time of the tweets DailyFaceoff cites | live captures |
| ESPN | `sources/espn_injuries.py`, `sources/transactions.py` | injury report; roster transactions | injuries live; transactions 2010 onward |
| NHL API | `sources/officials.py`, `sources/edge.py` | referees and linesmen per game; EDGE tracking (speed, shot speed, zone time, shot location) | officials 2010 onward; EDGE 2021-22 onward |
| Scouting the Refs | `sources/officials.py` | pregame referee assignments | live captures |
| Curated | `reference/venues.csv`, `reference/teams.csv` | arenas (coordinates, time zone, elevation); team/franchise/lineage map | static |

Live pollers store only *changes* (line moves, status changes), so polling densely is
nearly free. Scheduling is still to be designed. `scripts/poll.sh` and
`scripts/crontab.example` are a starting point and are not installed.

**Teams.** `nhl.teams.resolve_team(label, season)` maps any spelling to the tricode used
in that season: Phoenix is `PHX` through 2013-14, then `ARI`. Utah is team 59 in 2024-25
and team 68 from 2025-26. `franchise_id` is the NHL's official franchise (Utah is a
*new* franchise, 40). `lineage_id` is our continuity key: PHX → ARI → UTA share one
lineage because Utah received Arizona's players and hockey operations. `nhl catalog`
checks the table against the NHL API and logs an error for any unmapped team.

## Layout

```
src/nhl/
  config.py            settings (env / .env), season helpers
  cli.py               `nhl` command
  storage/s3.py        S3 store with ETag-validated local cache
  storage/keys.py      S3 key layout (documented there)
  teams.py             team / franchise / lineage resolution (reference/teams.csv)
  odds/                odds schema, per-source storage, props
  sources/             third-party pollers and importers (see the table above)
  reference/           curated CSVs (teams, venues) plus travel features
  ingest/http.py       NHL API client: retries, timeouts, global rate limit
  ingest/catalog.py    games, teams, player bios (bulk endpoints)
  ingest/games.py      raw per-game play-by-play + shift charts
  ingest/toi_html.py   fallback shifts from the NHL's HTML time-on-ice reports
  transform/events.py  play-by-play -> events (score, strength, coordinates)
  transform/shifts.py  shift charts -> on-ice players and shift fatigue
  transform/build.py   per-season event table
  features/shots.py    shot-level model features (single code path for all strengths)
  models/xg.py         tuning, testing, refit, out-of-fold predictions, save/load
  models/evaluate.py   log loss, Brier, AUC, calibration
  gamestate/           M2: stints, rosters/coaches, lineups, goalie starts, game logs, validation
  ratings/             M3: frozen-puck model, finishing/goaltending, EV/ST RAPM, penalties, snapshots
  sim/                 M4: game simulator (constants, inputs, engine, markets, backtest)
tests/                 unit tests on hand-built synthetic games
notebooks/             exploration
```

## Data sources

| Data | Endpoint |
|---|---|
| Play-by-play | `api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play` |
| Shift charts | `api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}` |
| All games | `api.nhle.com/stats/rest/en/game` |
| Teams | `api.nhle.com/stats/rest/en/team` |
| Player bios | `api.nhle.com/stats/rest/en/{skater,goalie}/bios?cayenneExp=seasonId={season}` |
| Shift fallback | `www.nhl.com/scores/htmlreports/{season}/T{H,V}{nnnnnn}.HTM` (only when the shift chart is empty) |
| Coaches, scratches | `api-web.nhle.com/v1/gamecenter/{game_id}/right-rail` |
| Validation | `api-web.nhle.com/v1/gamecenter/{game_id}/boxscore` (sampled) |

## Key definitions

- **Coordinates**: `x_abs`/`y_abs` are rotated so the event team attacks toward +x, with the goal line at x = 89. The attack direction comes from `homeTeamDefendingSide` when the API provides it (about 2019-20 on). Before that it is inferred per period from where each team's shots land.
- **Strength group** (shooting team's view): `EN` if the defending net is empty. Otherwise `EV`, `PP` or `SH` by comparing skater counts. An extra attacker with the shooter's own goalie pulled counts as `PP`, as in the legacy model.
- **Model sample**: unblocked shot attempts (Fenwick), excluding shootouts and penalty shots.
- **On-ice players**: at a line change, faceoffs are credited to players coming on and all other events to players going off.
- **Stint**: an interval of constant on-ice personnel within a period, also cut at every faceoff and goal, so each stint has one score state and one most-recent faceoff. Strength comes from the skaters on ice (shift charts), not `situationCode`.
- **Game-log strengths** (team's view): `5v5`, `EV`, `PP`, `SH`, `EN_own` (own net empty), `EN_opp`, and `all`. All except `5v5` and `all` partition the game.

## Changes from `legacy-v1`

| Legacy issue | Fix |
|---|---|
| `score_model` clipped raw log-odds to [0, 1] before computing AUC/log loss, so the learning-rate choices were based on wrong numbers | Metrics are computed on probabilities only (enforced in `evaluate`) |
| Shot-type imputer used `is_goal` as a feature (label leak) | No imputation. Unusual shot types go to `shot_other` |
| Random row split, so the same game appeared in train and test | Season-based train / valid / test split, plus out-of-fold historical xG |
| Empty-net model barely trained (eta 0.001, 24 rounds) | All groups tuned with Optuna on validation log loss with early stopping |
| Score state came from detail fields set only on goals, so it read 0-0 for nearly every shot | Running score of goals before each event |
| Goalie features used the shooting team's own goalie | Defending goalie (`goalieInNetId`, falling back to on-ice) |
| Calibration never checked | Reliability tables and goals/xG in every report |
| Models pickled | Native XGBoost `.ubj` plus metadata JSON in S3 |
| Data committed to git (~1 GB), hardcoded roster URLs, no Utah | S3 storage and bulk catalog endpoints |
