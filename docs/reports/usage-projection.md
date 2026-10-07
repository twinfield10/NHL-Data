# Context in player projections (usage plan phase D, 2026-10-07)

Does knowing a skater's teammates, competition and deployment improve the forecast of his
rest-of-season 5v5 on-ice results? Regular seasons 2015-16 to 2025-26, cut after 25%, 40% and
55% of each season's games (≈ 1,500 skater-cuts per season with ≥ 200 5v5 minutes before and
after). Everything is point-in-time: ratings from the snapshot in effect at the cut, context
from `processed/onice_context`. Scored by future-TOI-weighted MSE. Tables:
[usage-projection-numbers.md](usage-projection-numbers.md) (`nhl usage-projection`). Code:
`src/nhl/usage/projection.py`.

## Verdict
**Gate passed.** Rating + context beats the honest baseline (the raw on-ice rate shrunk to
league average with 300 minutes, tuned) in every season, for both ends:

| target | best method | error vs shrunk raw | seasons better |
|---|---|---|---|
| on-ice xGF/60 | blend | −15% | 11 of 11 |
| on-ice xGA/60 | blend | −11% | 11 of 11 |
| 5v5 points/60 | ipp | −1.4% | 7 of 11 |

- **The rating alone is the worst forecast** (35-56% more error than shrunk raw). A player's
  on-ice numbers are mostly about who he plays with and against, so a context-free rating
  can't forecast them. Context is what makes the ratings usable for player numbers.
- **Season-to-date context ≈ recent linemates.** Re-rating his last 10 games' linemates
  (`recent`) is no better than his season-to-date teammates (`todate`); line changes are
  noise more often than signal.
- **Blending in the raw rate helps** (`blend` = 2/3 `todate` + 1/3 shrunk raw, weight fitted
  leaving one season out, 0.30-0.40 in every fold). The M3 ratings are shrunk hard for
  game pricing; for player forecasts some of the raw signal is worth keeping.
- **Points barely move.** Converting the on-ice forecast to points through a shrunk share of
  on-ice goals (`ipp`) is only 1.4% better than shrunk raw points; points are dominated by
  finishing luck and the player's own share of goals, which context doesn't touch.

## What this means downstream
- **Props and DANAH** should forecast on-ice rates with `blend` (rating + season-to-date
  context + 1/3 shrunk raw), not with the rating alone or the raw rate.
- **Projected context for a new situation** (a trade, a promotion to the top line) needs the
  `recent`-style re-rating with the new linemates; it's no worse than season-to-date when
  the line is stable, so it's the right tool when the line changes.
- Points need their own model (shooting talent, the share of on-ice goals a player is in on,
  power-play time) before context can pay off. That belongs in DANAH.
