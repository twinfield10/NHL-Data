# Plan: Player archetypes

**Status:** planned 2026-10-07. Decided (owner, 2026-10-07): style-only features, EDGE as an
add-on layer, run all three Phase C tests. **Phase A built 2026-10-07** ([results](#phase-a-results-2026-10-07)).
**Phase B built 2026-10-07** with a changed design: named style axes for every skater, archetypes
for forwards only, because defence has no stable clusters ([results](#phase-b-results-2026-10-07)).
**Phase C done 2026-10-08** ([report](../reports/archetypes.md)): the axes pass the point-attribution
test (small but in every season); labels, chemistry and archetype aging fail.
**Depends on:** M1 shots and events (`processed/shots`, `processed/events`), rink-adjusted
locations (`processed/shots_rink`), M2 stints and units, the usage tables
(`processed/usage_summary`, `processed/onice_context_summary`), M3 rating snapshots,
player bios (`processed/players`), and optionally NHL EDGE (`external/edge/skater`, 2021-22 on).
**Feeds:** the site (player and line pages, player comps), player props (who scores and who
assists), and DANAH (the player future-value model).

---

## TL;DR
1. **Archetype = how a player plays, not how well.** Quality is already measured by the
   M3 ratings. Archetypes come only from *style* features (where and how he shoots, how
   he creates, physical play, deployment). This keeps the two separate, the same way the
   usage plan kept tier (usage) apart from quality (ratings). A "sniper" can be good or bad.
2. **Soft membership, fitted per position.** Most players are blends, so each player gets
   probabilities over archetypes (a Gaussian mixture), not one hard label. Forwards and
   defence get separate models. The model is fitted once on a pooled set of seasons, so an
   archetype means the same thing in 2012 and 2026.
3. **Scorer bias first.** Hits, giveaways, takeaways and blocks vary a lot by arena; M4
   found that about half the apparent team style was arena bias. Biased counts are measured
   on road games only, or arena-adjusted, and every feature must pass a split-half
   reliability check before it can define an archetype.
4. **Earn a place in the models, or stay descriptive.** M4 (team style) and usage Phase C
   (line matchups) both found no interaction. So the default expectation is that archetypes
   are a site and explanation product. Phase C tests three specific uses, each with a gate:
   line chemistry, point attribution for props, and archetype-specific aging for DANAH.
   The point-attribution test is the one most likely to pass.

## Definitions
- **Grain:** player-season (regular season), with a 2-season weighted window as the
  default "current" view, so a player with 20 games this year is still placed. Season-to-date
  as-of views follow the usual point-in-time rule.
- **Sample:** skaters with ≥ 300 5v5 minutes in the window are fitted; others are assigned
  with their features shrunk toward the position mean and flagged low confidence.
- **Seasons:** play-by-play features from 2010-11; EDGE features only from 2021-22, so
  they are an add-on layer, not part of the core model.
- **Positions:** F and D modelled separately. C vs W is not a separate model; faceoff
  share is a feature, so a centre-type archetype can emerge.

## Phase A: Style features
`processed/style/{season}.parquet`, one row per (player_id, season, window), raw and
shrunk values. Rates are per 60 at 5v5 unless marked; shares are within the player's own
attempts or events.

| Group | Features |
|---|---|
| **Shooting volume** | individual attempts/60, iFF/60, ixG/60 |
| **Shot location** | share of unblocked attempts from the slot, net-front (< 15 ft), point/high (> 45 ft), off wing; mean shot distance (rink-adjusted) |
| **Shot type** | share wrist/snap, slap, backhand, tip/deflection |
| **Chance creation** | share of own shots that are rebounds, rush, off a turnover; primary assists/60 and the ratio A1 / (A1 + G) |
| **Puck play** | takeaways/60, giveaways/60 (road only) |
| **Physical** | hits/60, hits taken/60 (road only), penalties drawn/60 and taken/60 (from M3 penalties), height, weight |
| **Defence** | blocks/60 (road only), share of on-ice opponent attempts he blocks |
| **Deployment** | faceoffs taken/60 (centre signal), DZ start share, share of team PP and PK time, PP1 share |
| **EDGE add-on (2021-22 on)** | max speed, bursts over 20 mph/60, distance/60, top shot speed, OZ time share |

**Shrinkage:** each rate is shrunk to the position mean by its own reliability (empirical
Bayes, the prior weight set from split-half variance), so a 15-game player isn't an outlier.
**Arena:** counts with known scorer bias use road games only; if road-only halves the sample
too much, use a home-arena multiplier fitted like the M1 rink adjustment.
**Checks:** split-half (odd/even games) reliability for every feature by season; drop or
merge features with reliability < 0.3 at 500 minutes. Report year-to-year correlation as
well, since a style trait should persist.

### Phase A results (2026-10-07)
Built: `src/nhl/archetypes/features.py`, `nhl style` (nightly, after `onice`), tables
`processed/style_counts`, `processed/style`, `processed/style_priors`, backfilled 2010-11 to
2026-27. Early in a season, a position group with fewer than 100 skaters at 200+ minutes
borrows the previous season's priors.
- **Reliability:** the median feature split-half is 0.65-0.70 every season (0.58 in 2012-13 and
  0.60 in 2020-21, the short seasons). The EB-implied reliability at 500 minutes tracks the
  empirical split-half closely.
- **Noise (split-half < 0.3, excluded from clustering):** rush, turnover and transition shot
  shares (0.1-0.2), rebound share (F 0.25, D 0.15), A1 / (A1 + G) (≈ 0.1), and for D tips and
  faceoffs. This matches M4: play-by-play chance types carry little player signal.
- **Most reliable:** faceoffs (F), PP/PK shares, hits, zone starts, shot volume, slap share,
  penalties, shot location.
- **Excluded from clustering on judgement:** PP share and PP1 rate (coaches give PP time to
  the best players, so they measure quality as well as style; kept as profile descriptors);
  giveaways and takeaways (they dominated the first clusters, separating McDavid, MacKinnon
  and Bedard by "+1.8 SD giveaways", i.e. puck touches and scorer practice, not style).
- **Clustering exploration** (PCA 90% → Gaussian mixture, 500+ min player-seasons 2015-26,
  features standardized within season): forwards have real but limited structure, stable at
  K = 4-6 (bootstrap ARI 0.6-0.7, same top type next season ≈ 70-77%): two-way / PK centre,
  offensive centre, perimeter scoring winger, physical winger, plus a distance-shooter group
  at K = 6. **Defence has no stable clusters** at any K (ARI ≤ 0.36); D style is a continuum.

## Phase B: Archetype model
`models/archetypes/{version}/` (scaler, PCA, mixture parameters, archetype names) and
`processed/archetypes/{season}.parquet`, one row per (player_id, season, window):
`position_group`, `p_{archetype}` for each archetype, `top_archetype`, `confidence`
(max probability), `minutes`, and the PCA coordinates (for comps and plots).

- **Fit:** standardize the shrunk features within position, reduce with PCA (keep ~90% of
  variance), then a Gaussian mixture. Pool 2015-16 to 2025-26 player-seasons, weighted by
  minutes.
- **Choosing K:** BIC, plus bootstrap stability (adjusted Rand index of assignments across
  resamples ≥ 0.7), plus interpretability. Expect about 6-8 forward and 4-5 defence types.
- **Naming:** by each component's feature profile against the position mean (for example
  "net-front", "shooter from distance", "playmaker", "two-way/PK", "physical depth";
  for D "offensive/PP", "puck-mover", "shutdown/shot-blocker", "physical"). Names are
  decided after looking at the clusters, not before.
- **Comps:** nearest neighbours in PCA space within position (style only), shown alongside
  ratings, so "plays like X" and "rated like Y" stay separate.

**Checks:** archetype persistence year to year (share keeping the same top archetype, and
the correlation of membership vectors); spot checks on well-known players; archetype mix by
tier (expect PP-heavy types in F1/D1, but every type should appear at several tiers,
otherwise the feature set is measuring usage, not style).

### Phase B results (2026-10-07)
**Design change (owner, 2026-10-07): axes + forward archetypes.** Exploration showed the style
space is mostly continuous, so the product is:
- **Style axes for every skater** (`src/nhl/archetypes/model.py`, `AXES`): fixed, named
  composites. Each is the mean of signed features after `sqrt` (rates) / `logit` (shares) and
  standardisation within season and position. Forwards: perimeter, shooter, release, physical,
  size, defensive, centre. Defence: wrister, activation, shooter, physical, size, shot_blocker,
  defensive. Factor analysis (varimax) found similar factors but lost the centre and
  defensive-role axes, so the fixed composites were kept.
- **Forward archetypes:** a full-covariance mixture on the forward axes, K = 5, named by
  rule from the centroids: skill winger (27-31%), offensive centre (22-26%), two-way centre
  (16-20%), balanced winger (13-19%), power forward (11-17%); the shares are stable over 16
  seasons. K by BIC, bootstrap ARI and persistence: K = 3 is the most stable (ARI 0.86), and
  K = 5 (ARI 0.62) was chosen for readability. Mean top-type confidence is 0.80.
- **Defence: axes and comps only.** No K had bootstrap ARI above 0.36.
- **Comps:** the 5 nearest other players in axis space (their closest season, from 2010-11 on).
- **Tables:** `models/archetypes/v20261007/` (LATEST) and `processed/archetypes/{season}`
  (both windows: axes, `{axis}_pct`, `p_*`, `archetype`, `confidence`, `comps`). Built by
  `nhl archetypes [--fit]`, nightly after `nhl style`.

**Checks:**
- **Year-to-year axis correlation** (500+ min): F 0.59-1.00 (release lowest; centre 0.94,
  physical 0.85); D 0.55-1.00 (shot_blocker lowest). 71% of forwards keep their top archetype
  the next season.
- **Every archetype appears at every tier** (2025-26 F1: 46 skill wingers, 35 offensive
  centres, 15 two-way centres, 9 power forwards), so labels measure style, not usage.
- **Spot checks:**
  - Kucherov is a skill winger (comps Ehlers, Marchessault, Bjorkstrand).
  - Tom Wilson is a power forward.
  - Kopitar, Draisaitl and Matthews are centres.
  - Makar is +2.5 activation and +2.5 wrister.
  - Trouba is +2.0 shot_blocker (comps Girardi, Seabrook, Pulock).
  - Hughes is small and offensive (comps Drysdale, Barrie).
- **Limit:** labels are coarse at the edges. Brady Tkachuk is "balanced winger" despite
  physical +1.7 and shooter +2.7, and Hyman is "skill winger" despite perimeter −3.0
  (net-front). The axes carry the detail, so the site should lead with axes and show the
  label as a summary.

## Phase C: Usefulness tests
A report (`docs/reports/archetypes.md`). Each test has a gate; a failure keeps archetypes
descriptive for that use.

Phase B changed the inputs: tests use the forward archetype probabilities *and* the axes
(for defence, axes only).

1. **Line chemistry.** Add archetype-mix terms for forward lines and D pairs (for example
   "line has a net-front player and a playmaker") to the 5v5 stint model, as usage Phase C
   did with tier pairs. **Gate:** held-out xG log likelihood improves beyond noise in most
   seasons. Expected: fail, like M4 style and Phase C.
2. **Point attribution (props).** Given an on-ice goal for, predict whether this player
   scored it, got the primary assist, the secondary assist, or nothing. Baseline: his own
   shrunk history of G / A1 / A2 shares plus position. Candidate: add archetype membership
   as the prior his shares are shrunk toward. **Gate:** held-out log loss, 2016-2026, with
   the biggest gain expected for low-minute players. Usage Phase D found that points need
   their own model, and this is the core of it.
3. **Aging (DANAH).** Fit EV O and D age curves by archetype against the pooled curve in
   `ratings/ev/age_curve.json`. **Gate:** next-season rating prediction improves on held-out
   seasons (for example, do physical depth forwards decline earlier than playmakers?).

### Phase C results (2026-10-08)
See [archetypes.md](../reports/archetypes.md) (`src/nhl/archetypes/usefulness.py`, `nhl archetypes-tests`).
- **Point attribution: axes pass.** Position + axes as the G / A1 / A2 prior beats position
  alone by +0.12% (5v5) and +0.18% (PP) log loss, in 15 of 15 seasons each. Beyond the
  shot-volume axis alone: +0.05% (14 of 15) and +0.16% (15 of 15). Archetype labels add
  ≈ +0.02%. Use the axes prior in player props.
- **Line chemistry: fail.** +0.001% held out, 8 of 11 seasons; in-sample adjustments are
  about 0.05 xG/60 per stint. No simulator change.
- **Aging: fail for style.** Archetypes and axes add ≤ 0.01% on top of an F/D term (7-9 of
  13 seasons). Side finding: the F/D term itself helps (+0.03%, 12 of 13). D offence priors run
  ≈ 0.12 xG/60 high relative to forwards; a position-specific prior mean is a candidate
  M3 fix, to be tested with re-chaining.

## Phase D: Site
- **Players tab:** lead with the style axes (percentiles within F / D); show the forward
  archetype (top type and probability) as a summary badge; the five closest style comps
  with their ratings; membership over time.
- **Lines tab:** each unit's archetype mix.
- **Team page:** roster archetype composition against the league.
- API: `/api/ratings/players/{id}/style` and archetype fields on `/api/ratings/players`.

## Open decisions
| Decision | Options | Recommendation |
|---|---|---|
| Style vs quality | style features only · include ratings / production | **Decided: style only.** Quality stays in ratings; a cluster that mixes them turns into "good / bad" tiers |
| Labels | hard clusters · soft mixture | **Soft**, with `top_archetype` for display |
| EDGE | core feature · add-on layer · skip | **Decided: add-on**: only 5 seasons, and it would leave 2010-21 players unplaced. Test in Phase B whether it changes assignments much |
| Goalies | include · separate later | **Later.** Goalie style (rebound control, puck play) needs the frozen-puck and shot-against detail, a separate small plan |
| Refit cadence | per season · fixed pooled model | **Fixed pooled model**, refit yearly in the off-season and versioned, so labels are comparable over time |
