# Archetypes phase C: do style axes and archetypes earn a place in the models?

**Date:** 2026-10-08. **Code:** `src/nhl/archetypes/usefulness.py`, `nhl archetypes-tests`.
**Numbers:** [archetypes-numbers.md](archetypes-numbers.md). **Plan:** [archetypes.md](../plans/archetypes.md).

## Summary
| Use | Test | Verdict |
|---|---|---|
| Props: who gets the goal and the assists | Style as the prior that a player's G / A1 / A2 history shrinks toward | **Axes pass; archetype labels fail.** The gain is small but holds in every season |
| Lineups: line chemistry | Archetype pairings and within-unit spread on the stint residual | **Fail.** No simulator change |
| DANAH: aging | Archetype- or axis-specific age shifts on the rating priors | **Fail.** Side finding for M3: a position term helps |

Each test uses only style known before the season (the previous season's `2yr` archetypes)
and is scored on held-out seasons. The pattern matches M4 (team style) and usage Phase C
(matchups): play-by-play style describes players well, but it adds almost nothing to
predicting goals at the unit level.

## 1. Point attribution
Given a goal for with the skater on the ice, P(he scored), P(primary assist),
P(secondary assist), P(none). His shrunk history of those outcomes (the previous three seasons
weighted 1, ½, ¼, plus the season to date before the game) is pulled toward a prior from a
multinomial logit. Priors tested: position only; position + shot-volume axis; + archetype
probabilities; + all axes; + both. Log loss per skater-goal, 2011-12 to 2025-26, leaving
one season out.

- **Axes beat position:** +0.12% at 5v5 and +0.18% on the 5-on-4 PP, positive in 15 of 15
  seasons for each.
- **Not just shot rate:** a prior with only the shot-volume axes gets about half of the 5v5 gain
  and almost none of the PP gain. The full axes add +0.05% beyond it at 5v5 (14 of 15) and
  +0.16% on the PP (15 of 15). On the PP, where the shooter sits (net-front, point,
  off wing) decides who scores and who sets up.
- **Archetype labels add little:** +0.02% at 5v5 and +0.01% on the PP (9 of 15). The axes
  carry the information.
- **Small samples gain most:** +0.15-0.2% for skaters with fewer than 40 weighted on-ice goals
  of history. Without a previous-season style row (rookies), the gain is about zero, as expected.
- **Scale:** small in absolute terms; most of the log loss is irreducible (which of five
  skaters scores). It is a consistent improvement at no cost.

**Decision:** when player props are built, use position + style axes as the prior for
G / A1 / A2 shares. The tuned prior weights are k ≈ 320 skater-goals at 5v5 and ≈ 80 on the PP.

## 2. Line chemistry
Target: the stint residual (actual xG/60 − the point-in-time additive RAPM prediction) for
both the attacking and the defending unit, 2015-16 to 2025-26 (11 seasons, about 2,200 hours
of 5v5 a season).
- **Additive style beyond the ratings:** +0.016 bp of held-out MSE (8 of 11 seasons).
  This uses each unit's summed archetype probabilities and axes.
- **Chemistry beyond additive style:** +0.010 bp (8 of 11). This uses the 15 pairwise
  archetype co-occurrence terms per unit and the within-unit spread of every axis.
- **Size:** fitted in-sample on all seasons, the chemistry adjustment has an RMS of 0.05 xG/60
  per stint, against a residual sd of 10.3. That is about 0.01 xG per line per game, before
  out-of-sample shrinkage.

**Decision:** no chemistry term in the simulator or the lineup projections. The answer did
not depend on the ridge penalty (10² to 10⁶).

## 3. Aging
The production EV priors age with one league curve per side. The test adds a shift
Σθ·f to each rated player's prior mean entering a season, with f = (1, a, a²) [re-tuned
league curve], + F/D, + archetype probabilities, + axes (each with an age slope). It then
scores M3's chain test: fit to Dec 31, score the rest of the season. Late-season error is
quadratic in θ, so leave-one-season-out fits are exact. 13 seasons: 2012-13 and 2020-21
started in January, so their pre-Jan-1 fit is empty and they were dropped (left in, their
level shift swamped every other season).

- Re-tuning the league curve: +0.002 bp (8 of 13). The stored curve is fine.
- **Position term: +0.031% (12 of 13 seasons).** This is ten times what the whole age curve
  adds over no aging (+0.004%).
- Archetypes on top of position: +0.003% (9 of 13). Axes on top of position: +0.009% (7 of 13).
  Both are noise.

**Decision:** DANAH uses the league curve (plus position, see below), not archetype curves.

**Side finding for M3:** the fitted position term lowers defencemen's offence priors by about
0.12 xG/60 relative to forwards (θ: O:one +0.06, O:is_d −0.18), with a small age slope.
In other words, carried-forward D offence priors run high. The likely cause is that the
newcomer prior and shrinkage target is league average for everyone, while defencemen's
offence terms average below it. The fix is a position-specific prior mean (newcomers and
aging). Test it in the full re-chained `chain_eval` before adopting, since this one-step test
doesn't re-chain later seasons.

**Re-chained result (2026-10-08): adopted for newcomers.** D newcomer priors of offence −0.25,
defence +0.12 xG/60 gain +0.93 bp, 13 of 13 seasons. A per-season shift on returning D priors
adds < 0.1 bp on top and isn't used. See `rapm.Hyper` and the M3 plan.
