"""Vectorized Monte Carlo of NHL games (M4).

All games × simulations advance together through regulation in ``DT``-second steps, as
arrays of shape (games, sims). Each step:

1. **Manpower.** Each team has up to two penalty timers; skaters = 5 − active penalties
   (minimum 3). In the last 10 minutes of the 3rd period a trailing team with its goalie
   in pulls him with the league's per-step hazard for its deficit (1, 2, 3+) and time
   left (:func:`nhl.sim.constants._pull_hazard`). He stays out while the team trails,
   returns after any goal, and the hazard for the new deficit applies again.
2. **Rates.** The goal rate for the state:
   - an empty net: the extra-attacker or empty-net rate;
   - even: 5v5 (+ score effect by lead and period), 4v4 or 3v3;
   - otherwise the power-play / short-handed rates (a two-man advantage scales the PP
     rate up).
3. **Events.** At most one goal per step, and penalties at each team's rate (not while
   pulled or already two down), scaled by the home factor and by season type × period ×
   that team's lead (:func:`nhl.sim.constants._penalty_states`).
   - A power-play goal releases the shorthanded team's earliest-ending minor; a double
     minor loses its first two minutes; a major is never released.
   - Timers then tick down.

Ties after regulation go to overtime:
- regular season: 3v3 for 5 minutes from 2015-16 (4v4 before), sudden death, then a
  shootout won by the home team with the league rate;
- playoffs: 5v5 sudden death until a goal (capped at 6 periods, then a coin flip).

Penalties are not simulated in overtime.

Outputs follow the betting convention: a shootout win adds one goal to the winner.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from nhl.sim.inputs import SeasonInputs

DT = 5
REGULATION_S = 3600
OT_3V3_START = 20152016


@dataclass
class SimResult:
    """Final scores per game and simulation (shootout winner +1), and how each ended."""

    home: np.ndarray  # (G, N) int16
    away: np.ndarray
    ended: np.ndarray  # 0 regulation, 1 overtime, 2 shootout
    en_goals: np.ndarray  # (G, N) goals into an empty net (diagnostic)
    pulled_s: np.ndarray  # (G, N) seconds with a goalie pulled (diagnostic)


def _pull_thresholds(rng: np.random.Generator, constants: dict, shape: tuple[int, int]) -> np.ndarray:
    """(3, G, N) seconds remaining at which a team trailing by 1, 2, 3+ pulls (0 = never)."""
    levels = np.array(constants["pull_quantile_levels"])
    out = np.zeros((3,) + shape)
    for d in range(3):
        spec = constants["pull"][str(d + 1)]
        q = np.array(spec["quantiles"])
        u = rng.random(shape)
        never = rng.random(shape) < spec["never_share"]
        out[d] = np.where(never, 0.0, np.interp(u, levels, q))
    return out


def _penalty_length(rng: np.random.Generator, constants: dict, shape: tuple[int, int]) -> tuple[np.ndarray, np.ndarray]:
    """Seconds and major flag for new penalties."""
    mix = constants["penalty_mix"]
    p2, p4 = mix.get("2", 1.0), mix.get("4", 0.0)
    u = rng.random(shape)
    secs = np.where(u < p2, 120.0, np.where(u < p2 + p4, 240.0, 300.0))
    return secs, secs == 300.0


#: Calibration, tuned on 2016-17..2019-20 (see docs/plans/m4-simulator.md). Each team's log
#: rate relative to the league rate for the state is split into the game's **pace** (the two
#: teams' mean) and **strength** (half their difference): pace is multiplied by
#: ``PACE_SCALE`` (rating noise makes raw pace overconfident for totals), strength by
#: ``STRENGTH_SCALE``, and ``GAME_SIGMA`` adds a per-game swing in which team is better
#: that night. Penalty rates are shrunk by the pace scale too.
#: Tuning on 2016-17..2019-20 (500 sims per game): pace 1.0 beat 0.7 once the scoring level
#: was fixed; strength 1.2 beat 1.0 and 1.4 (1.4 broke calibration); game shock 0.35 gave
#: the best log losses (moneyline, totals, puck line) but a 3-point calibration miss and too
#: few overtimes, 0.2 the reverse; 0.3 is the compromise.
STRENGTH_SCALE = 1.2
PACE_SCALE = 1.0
GAME_SIGMA = 0.3
#: Team-level term: a team's running 5v5 residual (actual − rating-predicted xG) per hour,
#: shrunk with TEAM_PRIOR_H hours of zero evidence, added to its offence (and to the
#: opponent's offence via its defence residual). 0 hours of shrinkage = off.
TEAM_PRIOR_H = 30.0  # 2016-19 tuning: off 0.6773, 100 h 0.6772, 30 h 0.6770; out of sample 0.6633 -> 0.6615
#: Team defence beyond xG and the goalie: the running (goals against − talent-adjusted
#: expected) ratio, shrunk with TEAM_FIN_PRIOR_G expected goals of zero evidence, multiplies
#: the opponent's 5v5 goals. 0 = off. (Team *offence* beyond its shooters doesn't persist.)
TEAM_FIN_PRIOR_G = 500.0  # tuned on 2016-2019: off 0.6773, 1000 0.6769, 500 0.6765, 250 0.6766
_LEAGUE_KEYS = {"5v5": "goals60_5v5", "pp": "goals60_pp", "sh": "goals60_sh", "4v4": "goals60_4v4", "3v3": "goals60_3v3"}


def team_term(inputs: SeasonInputs, prior_h: float, fin_prior_g: float = 0.0) -> dict[str, np.ndarray]:
    """Per game and side, the multiplier on 5v5 goals from the shrunk team residuals:
    xG residuals (``prior_h``) and the opponent's defensive goals residual (``fin_prior_g``)."""
    g = inputs.games.height
    out = {"home": np.ones(g), "away": np.ones(g)}
    tr = inputs.team_res
    if tr is None:
        return out
    for side, opp in (("home", "away"), ("away", "home")):
        if prior_h:
            off = tr[f"off_{side}"] / (tr[f"hours_{side}"] + prior_h)
            dfn = tr[f"def_{opp}"] / (tr[f"hours_{opp}"] + prior_h)
            xg = inputs.xg60_5v5[side]
            out[side] = out[side] * np.clip((xg + off + dfn) / xg, 0.6, 1.6)
        if fin_prior_g and f"fin_def_{opp}" in tr:
            out[side] = out[side] * np.clip(1 + tr[f"fin_def_{opp}"] / (tr[f"fin_xdef_{opp}"] + fin_prior_g), 0.7, 1.4)
    return out


def _calibrated_rates(inputs: SeasonInputs, constants: dict, scale: float, pace: float, sigma: float,
                      rng: np.random.Generator, n_sims: int, team_prior_h: float = 0.0,
                      team_fin_prior_g: float = 0.0) -> dict[str, np.ndarray]:
    """(G, N) rates after the team term and the pace / strength / game-shock calibration
    (empty-net rates untouched)."""
    g = inputs.games.height
    team = team_term(inputs, team_prior_h, team_fin_prior_g)
    z = rng.normal(0.0, sigma, (g, n_sims)) if sigma > 0 else np.zeros((g, n_sims))
    out = {}
    # Centre on the level-adjusted league rate, so the pace scale shrinks team differences
    # in pace but not the league's scoring level for the date.
    level = inputs.level if inputs.level is not None else np.ones(g)
    for state, league_key in _LEAGUE_KEYS.items():
        league = constants[league_key] * level
        th, ta = (team["home"], team["away"]) if state in ("5v5", "4v4", "3v3") else (1.0, 1.0)
        xh = np.log(np.maximum(inputs.goals60[f"{state}_home"] * th, 1e-6) / league)
        xa = np.log(np.maximum(inputs.goals60[f"{state}_away"] * ta, 1e-6) / league)
        m, d = (xh + xa) / 2, (xh - xa) / 2
        out[f"{state}_home"] = league[:, None] * np.exp((pace * m + scale * d)[:, None] + z / 2)
        out[f"{state}_away"] = league[:, None] * np.exp((pace * m - scale * d)[:, None] - z / 2)
    for key in ("en_own_home", "en_own_away", "en_opp_home", "en_opp_away"):
        out[key] = np.broadcast_to(inputs.goals60[key][:, None], (g, n_sims))
    return out


def simulate(inputs: SeasonInputs, constants: dict, season: int, n_sims: int = 1000, seed: int = 7,
             scale: float | None = None, sigma: float | None = None, pace: float | None = None,
             team_prior_h: float | None = None, team_fin_prior_g: float | None = None) -> SimResult:
    """Simulate every game in ``inputs`` ``n_sims`` times."""
    rng = np.random.default_rng(seed)
    g = inputs.games.height
    shape = (g, n_sims)
    pace = PACE_SCALE if pace is None else pace
    r = _calibrated_rates(inputs, constants, STRENGTH_SCALE if scale is None else scale, pace,
                          GAME_SIGMA if sigma is None else sigma, rng, n_sims,
                          TEAM_PRIOR_H if team_prior_h is None else team_prior_h,
                          TEAM_FIN_PRIOR_G if team_fin_prior_g is None else team_fin_prior_g)
    league_pen = constants["penalty_rate60"]
    home_pen = constants.get("penalty_home_factor", 1.0)
    pen = {
        s: (league_pen * np.power(np.maximum(inputs.penalties60[s], 1e-6) / league_pen, pace)
            * (home_pen if s == "home" else 1 / home_pen))[:, None] * DT / 3600
        for s in ("home", "away")
    }
    # Penalty state multipliers: season type × period × the penalized team's lead (−2..+2).
    default_grid = [[1.0] * 5 for _ in range(3)]
    state_grid = constants.get("penalty_state_factor", {"R": default_grid, "P": default_grid})
    playoff_rows = (inputs.games["season_type"] == "P").to_numpy()
    pen_state = np.where(playoff_rows[:, None, None], np.array(state_grid.get("P", default_grid))[None],
                         np.array(state_grid["R"])[None])  # (G, 3 periods, 5 leads)
    score_terms = inputs.score_terms
    step = DT / 3600

    hs = np.zeros(shape, np.int16)
    as_ = np.zeros(shape, np.int16)
    # Penalty slots: remaining seconds and major flag, two per team.
    timers = {s: np.zeros((2,) + shape) for s in ("home", "away")}
    majors = {s: np.zeros((2,) + shape, bool) for s in ("home", "away")}
    hazard = {d: np.array(constants["pull_hazard"][str(d)]) for d in (1, 2, 3)}
    bucket_s = constants["pull_bucket_s"]
    is_pulled = {s: np.zeros(shape, bool) for s in ("home", "away")}
    en_goals = np.zeros(shape, np.int16)
    pulled_s = np.zeros(shape, np.int32)

    for t in range(0, REGULATION_S, DT):
        period = t // 1200
        remaining = REGULATION_S - t
        active = {s: (timers[s] > 0).sum(axis=0) for s in ("home", "away")}
        n = {s: np.clip(5 - active[s], 3, 5) for s in ("home", "away")}
        lead_h = (hs - as_).astype(np.int32)
        # Goalie pulls: a trailing team with its goalie in pulls him with the league hazard
        # for its deficit and the time left; he stays out while the team still trails.
        pulled = {}
        bucket = (remaining - 1) // bucket_s
        for s, lead in (("home", lead_h), ("away", -lead_h)):
            trailing = lead < 0
            if period == 2 and bucket < len(hazard[1]):
                h = np.select([lead == -1, lead == -2, lead <= -3], [hazard[1][bucket], hazard[2][bucket], hazard[3][bucket]], 0.0)
                is_pulled[s] = trailing & (is_pulled[s] | (rng.random(shape) < h))
            else:
                is_pulled[s] = np.zeros(shape, bool)
            pulled[s] = is_pulled[s]

        def even_rate(side: str, lead: np.ndarray) -> np.ndarray:
            k = n[side]
            sc = score_terms[np.clip(lead, -3, 3) + 3, period]
            five = np.maximum(r[f"5v5_{side}"] + sc, 0.05)
            return np.where(k == 5, five, np.where(k == 4, r[f"4v4_{side}"], r[f"3v3_{side}"]))

        rates = {}
        for s, o, lead in (("home", "away", lead_h), ("away", "home", -lead_h)):
            adv = n[s] - n[o]
            pp = r[f"pp_{s}"] * (1 + 0.8 * np.clip(adv - 1, 0, None))
            special = np.where(adv > 0, pp, r[f"sh_{s}"])
            base = np.where(adv == 0, even_rate(s, lead), special)
            rates[s] = np.where(pulled[s], r[f"en_own_{s}"], np.where(pulled[o], r[f"en_opp_{s}"], base))

        u = rng.random(shape)
        goal_h = u < rates["home"] * step
        goal_a = (~goal_h) & (rng.random(shape) < rates["away"] * step)
        hs += goal_h
        as_ += goal_a
        en_goals += (goal_h & pulled["away"]) | (goal_a & pulled["home"])
        pulled_s += DT * (pulled["home"] | pulled["away"])
        # Any goal sends the goalie back in for the centre-ice faceoff; a team still
        # trailing re-pulls under the hazard for its new deficit.
        for s in ("home", "away"):
            is_pulled[s] &= ~(goal_h | goal_a)

        # A power-play goal releases the shorthanded side's earliest-ending minor.
        for scorer, victim, goal in (("home", "away", goal_h), ("away", "home", goal_a)):
            on_pp = goal & (n[scorer] > n[victim])
            if not on_pp.any():
                continue
            tm, mj = timers[victim], majors[victim]
            minor_left = np.where((tm > 0) & ~mj, tm, np.inf)
            first = np.argmin(minor_left, axis=0)
            has = np.isfinite(np.min(minor_left, axis=0)) & on_pp
            for slot in (0, 1):
                sel = has & (first == slot)
                left = tm[slot]
                tm[slot] = np.where(sel, np.where(left > 120, left - 120, 0.0), left)

        for s, lead in (("home", lead_h), ("away", -lead_h)):
            state_f = np.take_along_axis(pen_state[:, period, :], np.clip(lead, -2, 2) + 2, axis=1)
            take = (rng.random(shape) < pen[s] * state_f) & ~pulled[s] & (active[s] < 2)
            if take.any():
                secs, major = _penalty_length(rng, constants, shape)
                slot = np.where(timers[s][0] <= 0, 0, 1)
                for k in (0, 1):
                    sel = take & (slot == k)
                    timers[s][k] = np.where(sel, secs, timers[s][k])
                    majors[s][k] = np.where(sel, major, majors[s][k])
            timers[s] = np.maximum(timers[s] - DT, 0.0)

    ended = np.zeros(shape, np.int8)
    tied = hs == as_
    playoff = (inputs.games["season_type"] == "P").to_numpy()[:, None]
    ot_state = "3v3" if season >= OT_3V3_START else "4v4"
    regular_ot_steps = 300 // DT
    playoff_ot_steps = 6 * 1200 // DT
    for i in range(playoff_ot_steps):
        if not tied.any():
            break
        in_regular = ~playoff & (i < regular_ot_steps)
        in_playoff = playoff
        live = tied & (in_regular | in_playoff)
        if not live.any():
            break
        rh = np.where(playoff, r["5v5_home"], r[f"{ot_state}_home"])
        ra = np.where(playoff, r["5v5_away"], r[f"{ot_state}_away"])
        gh = live & (rng.random(shape) < rh * step)
        ga = live & ~gh & (rng.random(shape) < ra * step)
        hs += gh
        as_ += ga
        ended = np.where(gh | ga, 1, ended).astype(np.int8)
        tied = tied & ~(gh | ga)
    # Remaining ties: regular-season shootouts (and capped playoff games) by coin.
    if tied.any():
        p_home = np.where(playoff, 0.5, constants["shootout_home_win"])
        home_wins = rng.random(shape) < p_home
        hs += tied & home_wins
        as_ += tied & ~home_wins
        ended = np.where(tied, np.where(playoff, 1, 2), ended).astype(np.int8)
    return SimResult(home=hs, away=as_, ended=ended, en_goals=en_goals, pulled_s=pulled_s)
