# Plan: Usage and context (teammate and opponent quality)

**Status:** planned 2026-10-07. **Phase A built 2026-10-07** (`src/nhl/usage/tiers.py`, `nhl usage`, nightly after game state; backfilled 2010-11 to 2026-27): see [Phase A results](#phase-a-results-2026-10-07). **Phase B built 2026-10-07** (`src/nhl/usage/onice.py`, `nhl onice`, nightly after the ratings snapshot; backfilled 2015-16 to 2026-27): see [Phase B results](#phase-b-results-2026-10-07). **Phase C done 2026-10-07**: matchups are real but don't move game prices; simulator unchanged ([report](../reports/usage-matchups.md)).
**Depends on:** M2 stints (`processed/stints/{season}`) and inferred units
(`processed/lineups/{season}`), M3 point-in-time rating snapshots (`ratings/{date}/`,
weekly 2015-16 on), M4 simulator inputs (`sim/inputs.py` deployment shares).
**Feeds:** the site (player, line and team pages), player props, and DANAH (the player
future-value model; see [Later](#later-danah-inputs)).

---

## TL;DR
1. **The adjustment already exists.** M3's RAPM is a regression over every stint with
   teammates, opponents, zone, score, rest and coaches as terms, so a player's rating is
   already net of the company he keeps. This plan does **not** add a second adjustment to
   the ratings; that would double count.
2. **What's missing is the explanation.** Because RAPM is linear, a player's predicted
   on-ice xGF/60 and xGA/60 split *exactly* into: his own term + his teammates' terms +
   his opponents' terms + context, plus a residual (luck and model miss). That split is the
   product: "his on-ice xGD is +0.40/60; his teammates account for +0.25, tough
   competition −0.10, his own play +0.30, luck −0.05".
3. **Deployment tiers from usage, not results.** Tiers (F1-F4, D1-D3) come from ice-time
   rank within each team-game. Quality comes from the ratings. Keeping the two separate
   avoids the circularity of "he's a top-line player because he has top-line numbers".
4. **Matchups don't move game prices under the current model.** The simulator sums
   ice-time-weighted player terms, so who faces whom cancels out at the team level. Matchups
   only matter for game prices if there's an interaction (a top line does worse than its
   additive sum against a shutdown unit). Phase C tests that. M4 found no style
   interaction, so expect "no" and treat this as mostly a site and player-projection product.
5. **Where it should help projections:** player-level numbers (props, DANAH). A player's
   future on-ice results depend on the teammates and opponents he'll get. Phase D tests
   whether projecting those beats the player's rating alone.

## Definitions
- **Scope:** 5v5 for tiers, competition and decomposition. Power play and penalty kill are
  reported as usage (share of team PP/PK time, PP1/PP2, PK1/PK2) from the existing
  special-teams units; PP/PK decomposition can follow the same method with `st` terms later.
- **Point-in-time:** each game uses the latest rating snapshot *before* its date, the same
  rule as the simulator. Season pages can also show an end-of-season view; both are stored,
  labelled.
- **Ratings in the split:** offence terms (O) and defence terms (D) from `ev`, in xG/60
  relative to average. Unrated players get the simulator's replacement levels
  (`replacement_levels`), flagged.

## Phase A: Deployment tiers
`processed/usage/{season}.parquet`, one row per (game_id, player_id):
- `toi_5v5_s`, `toi_pp_s`, `toi_pk_s`, team shares of each.
- `tier`: F1-F4 / D1-D3 by 5v5 TOI rank within the team-game, using the inferred line
  (forwards) or pair (defence) rather than the individual rank where the unit has
  `confidence` ≥ 0.6, so linemates share a tier. 11F/7D games are ranked as dressed.
- `pp_unit`, `pk_unit` (1, 2 or none), `oz_start_share` (share of 5v5 faceoff starts in the
  offensive zone, among O/D starts), `dz_start_share`.

Season aggregates (`processed/usage_summary/{season}`; as-of views filter the per-game table by date): games by tier,
TOI per game, the special-teams roles and zone starts. **Validation:** tier agreement with
DailyFaceoff lines for 2026-27 games (report the confusion matrix) and with the M2
inferred lines; tier stability game to game.

### Phase A results (2026-10-07)
- **Tier basis:** ice time only (owner). A confidently inferred line or pair is ranked as one
  block at its members' mean 5v5 TOI, so linemates share a tier; others are ranked alone.
- **Checks, every season 2010-11 to 2026-27:** Σ `share_5v5` is 3.00 for forwards and 2.00
  for defence per team-game; every 12F/6D team-game has tiers of exactly 3/3/3/3 and 2/2/2;
  84-89% of tiers come from a confident unit.
- **Per-game tiers are noisy by nature:** a skater keeps his tier from one game to the next
  about 50% of the time (D 56%, F 48%; within one tier 92% / 86%). The median 5v5 gap
  between the F1 and F2 lines is only ≈ 80 s a game, so the order flips often. Use the
  season tier mix (`processed/usage_summary`) or a rolling view to describe a player, not
  one game. Spot checks for 2025-26: MacKinnon F1 in 86 of 93 games, Crosby 59 of 74,
  Malkin mostly F2 (34 of 62).
- **Still open:** the DailyFaceoff comparison needs more 2026-27 games (only the first two
  nights so far).

## Phase B: On-ice decomposition
`processed/onice_context/{season}.parquet`, one row per (game_id, player_id), 5v5, built
stint by stint with the point-in-time snapshot. For **xGF** (his team attacking) and **xGA**
(defending), each per 60:

| component | xGF uses | xGA uses |
|---|---|---|
| `own` | his O term | his D term |
| `teammates` | sum of the other 4 skaters' O terms | their D terms |
| `competition` | sum of the opponents' D terms | their O terms |
| `context` | intercept + home, zone, score, rest, post-penalty, coach | same |
| `residual` | actual − the sum of the above | same |

`xGD` = xGF − xGA for each component. Also stored: **QoT** and **QoC** as the TOI-weighted
mean rating of teammates and opponents (net, O and D separately), and the classic
**TOI-based QoC** (opponents' average 5v5 TOI share) for comparison with public sites.

**Checks:** the five components reproduce the RAPM prediction for every stint to rounding;
the residual averages ≈ 0 by season and its spread shrinks with TOI as expected.

**Products** (season to date and per snapshot):
- **Lifted / dragged** = the `teammates` component (positive = lifted).
- **Competition** = the `competition` component, also as a percentile within position.
- **Luck** = the residual, with a TOI-based band so small samples aren't over-read.

### Phase B results (2026-10-07)
- **Tables:** `processed/onice_context/{season}` (skater-game) and
  `processed/onice_context_summary/{season}` (season, player, team; `*_xgd` parts,
  `qot_net` / `qoc_net`, and percentiles within F / D for skaters with ≥ 200 5v5 minutes).
  Context is split into `zone` (zone-start terms) and `ctx` (score, home, rest, coaches,
  post-penalty), with the intercept as `league`, so every other part is relative to average.
- **Checks:** every snapshot term matches a design column (2025-26: 238/238 context terms,
  1,880/1,880 players); the parts add up to the actual on-ice xG exactly; the TOI-weighted
  residual is within ±0.04 xG/60 (< 2%) every season 2015-16 to 2025-26 (2026-27: −0.19
  after two nights). Games before a season's first snapshot use the previous season's last
  one, as the simulator does.
- **By tier (2025-26, xGD/60):** competition runs from −0.07 for F1 to +0.05 for F4 and
  −0.05 for D1 to +0.03 for D3; teammates from +0.07 (F1) to −0.14 (F4) and +0.11 (D1) to
  0.00 (D3). Teammate effects spread about twice as wide as competition, consistent with
  public QoT/QoC research. Only F1 gets a clear zone-start tilt (+0.04).
- **Spot checks (2025-26):** toughest competition goes to Montreal's top line (Suzuki,
  Caufield, Slafkovský) and Matheson; the most lifted are mostly Carolina and Edmonton
  defencemen; Crosby's teammates cost him −0.24 xGD/60 while his own term is +0.26.

## Phase C: Matchup study
A report (`docs/reports/usage-matchups.md`), answering empirically:
1. **How much matching is there?** Share of each tier's 5v5 time against each opponent tier,
   against the baseline of no matching (the opponent tier's overall TOI share). Split by home
   (last change) and away.
2. **Who gets the hard minutes?** Do players with good D terms, or defensive zone starts, face
   higher QoC? By team and by coach; a **coach matching index** (how far a coach's QoC spread
   is from the no-matching baseline), shown on team pages.
3. **Is there an interaction?** Add attacker-O × defender-D interaction terms (by tier pair,
   then by rating product) to the stint model and score held-out seasons, as M4 did for
   style. **Gate:** if it improves held-out xG log likelihood beyond noise, matchups go into
   the simulator via projected matchup shares; otherwise they stay descriptive and the
   simulator is unchanged.

### Phase C results (2026-10-07)
See [usage-matchups.md](../reports/usage-matchups.md) (`src/nhl/usage/matchups.py`,
`nhl usage-matchups`; per-team tables in `processed/usage_matchups/{season}`).
- **Matching:** like plays like (F1 vs F1 1.18×, F4 vs F4 1.58×). The last change shows on
  defence: home D1 vs opponent F1 1.16× (away 1.10×), home D3 sheltered 0.75× (away 0.83×).
- **Hard minutes:** tier explains most of the competition a skater faces; beyond tier, DZ
  starts go with tougher competition; individual defensive ability adds little.
- **Gate: failed, as expected.** Tier × tier cells add nothing held out; an offence × defence
  product is consistent (10 of 11 seasons) but tiny, worth −0.0006 xG per team-game on
  average under actual deployment (99th percentile 0.012). No simulator change.

## Phase D: Player projection test
Does knowing a player's context improve the forecast of his future on-ice results?
- **Target:** on-ice 5v5 xGF/60, xGA/60 and points/60 over the rest of the season, fit at
  several cut dates (as M3's `chain_eval` does), 2016-2026.
- **Baseline:** his own rating plus league-average teammates and opponents.
- **Candidate:** his own rating plus his *projected* teammates and opponents (recent tier and
  linemates, the team's roster ratings, the opponent mix of the remaining schedule).
- **Gate:** candidate beats baseline on held-out seasons. If it passes, the method is the
  context input to props and DANAH; if not, those use the rating alone.

## Phase E: Site
API routes (`src/nhl/api/routers/`) and frontend panels:
- **Player page:** the decomposition as a waterfall (own, teammates, competition, context,
  luck → on-ice xGD), tier mix and special-teams roles, QoT/QoC percentiles, most common
  linemates and their ratings.
- **Lines tab:** each unit's tier, minutes, and the same decomposition for the unit.
- **Team page:** the matchup matrix (own tiers × opponent tiers, share of time vs no-matching
  baseline) and the coach matching index.

## Later: DANAH inputs
For the player future-value model, this plan supplies the projected deployment (tier and TOI,
which drive counting stats) and the projected-context method from Phase D. The age curve
already in `ratings/ev/age_curve.json` and the replacement levels are its other starting
points.

## Open questions
- **Tier basis:** ice-time rank (recommended above) or a mix of ice time and ratings, as the
  scratch-pad note suggested. Ratings already appear as QoT/QoC, so a usage-only tier keeps
  the two ideas separate.
- **Seasons:** decomposition needs rating snapshots, so 2015-16 on; tiers and TOI-based QoC
  can go back to 2010-11.
