"""Player and team rankings for the site, built from one rating snapshot (``ratings/{date}/``).

**Players** are the snapshot's terms side by side, in the simulator's units:

* ``ev_off`` / ``ev_def``: 5v5 on-ice xG for / against per 60 relative to average
  (``ev_def`` is sign-flipped so higher is better at both ends); ``ev_net`` is their sum;
* ``pp_off`` / ``pk_def``: the same for the power play and penalty kill;
* ``finishing``: goals per xG relative to average, ``exp(shooter term) − 1`` (the
  simulator's finishing multiplier);
* goalies: ``save`` = ``1 − exp(goalie term)``, the share of expected goals he stops
  beyond an average goalie;
* penalties drawn and taken per 60.

**Teams** are composed exactly like :func:`nhl.sim.inputs._rates_for`: today's projected
lineup (:func:`nhl.pregame.lineups.project`) weights each skater's terms by his 5v5, PP,
PK and shot shares, unrated skaters get replacement level, and the goalie term is the
starts-weighted average of the team's recent starters still on the roster. Against a
league-average opponent on neutral ice that gives 5v5 xGF/60 and xGA/60, goals for and
against per 60 after finishing and goaltending, and PP / PK xG per 60. Rest, coaches and
the in-season team residuals are left out: these are talent rankings, not a game price.

**Lines** are every forward line, D pair, power-play and penalty-kill unit a team has iced
(:func:`observed_units`, from the play-by-play shifts), rated by summing their players' 5v5 or
special-teams terms (:func:`rate_units`)
and marked with their slot when they're in today's projected lineup (:func:`current_slots`).
"""

from __future__ import annotations

from datetime import date

import polars as pl

from nhl.sim.inputs import replacement_levels
from nhl.storage import keys
from nhl.storage.s3 import Store

#: A goalie's starts in his team's last this-many games set the team's goalie term.
GOALIE_LOOKBACK_GAMES = 10


def snapshot_tables(store: Store, snap: date) -> dict[str, pl.DataFrame]:
    """The ``ev``, ``st``, ``finishing`` and ``penalties`` tables of one snapshot."""
    base = f"ratings/{snap.isoformat()}/"
    return {k: store.read_parquet_required(f"{base}{k}.parquet") for k in ("ev", "st", "finishing", "penalties")}


def current_teams(store: Store, seasons: list[int], games: pl.DataFrame) -> pl.DataFrame:
    """Each player's latest team: ``player_id, team_id, team_abbr, last_game_date``.

    Uses the game rosters of ``seasons`` (oldest first), so a player who hasn't dressed this
    season keeps last season's team.
    """
    rosters = pl.concat([r.select("game_id", "player_id", "team_id") for s in seasons
                         if (r := store.get_parquet(keys.rosters(s))) is not None])
    sides = pl.concat([games.select("game_id", "game_date", pl.col(f"{s}_team_id").cast(pl.Int64).alias("team_id"),
                                    pl.col(f"{s}_abbr").alias("team_abbr")) for s in ("home", "away")])
    return (
        rosters.with_columns(pl.col("team_id").cast(pl.Int64))
        .join(sides, on=["game_id", "team_id"], how="inner")
        .sort("game_date", "game_id")
        .group_by("player_id").last()
        .select("player_id", "team_id", "team_abbr", pl.col("game_date").alias("last_game_date"))
    )


def _wide(frame: pl.DataFrame, cols: dict[str, str]) -> pl.DataFrame:
    """O/D rows -> one row per player; ``cols`` maps ``{side}_{column}`` to an output name."""
    out = frame.pivot(on="side", index="player_id", values=["mean", "sd_s", "prior_mean", "toi_s"])
    return out.select("player_id", *[pl.col(src).alias(dst) for src, dst in cols.items() if src in out.columns])


def player_board(tables: dict[str, pl.DataFrame], players: pl.DataFrame, teams: pl.DataFrame, today: date) -> pl.DataFrame:
    """One row per skater with a rating (see the module docstring for units).

    Args:
        tables: :func:`snapshot_tables`.
        players: the player catalog (``player_id, player_name, position, birth_date``).
        teams: :func:`current_teams`.
        today: for ages.
    """
    ev = _wide(tables["ev"], {"mean_O": "ev_off", "mean_D": "ev_def", "sd_s_O": "ev_off_sd", "sd_s_D": "ev_def_sd",
                              "prior_mean_O": "ev_off_prior", "prior_mean_D": "ev_def_prior", "toi_s_O": "ev_toi_s"})
    st = _wide(tables["st"], {"mean_O": "pp_off", "mean_D": "pk_def", "toi_s_O": "pp_toi_s", "toi_s_D": "pk_toi_s"})
    fin = tables["finishing"].filter(pl.col("role") == "shooter").select(
        "player_id", (pl.col("mean").exp() - 1).alias("finishing"), pl.col("shots").alias("shots"),
        pl.col("goals").alias("goals"), pl.col("xg").alias("ixg"))
    pen = tables["penalties"].pivot(on="kind", index="player_id", values="rate").select(
        "player_id", pl.col("drawn").alias("pen_drawn60"), pl.col("taken").alias("pen_taken60"))
    board = (
        ev.join(st, on="player_id", how="left").join(fin, on="player_id", how="left").join(pen, on="player_id", how="left")
        .with_columns(
            # Defence terms are xG allowed: flip so higher is better at both ends.
            (-pl.col("ev_def")).alias("ev_def"), (-pl.col("ev_def_prior")).alias("ev_def_prior"),
            (-pl.col("pk_def")).alias("pk_def"),
        )
        .with_columns(
            (pl.col("ev_off") + pl.col("ev_def")).alias("ev_net"),
            (pl.col("ev_off_prior") + pl.col("ev_def_prior")).alias("ev_net_prior"),
            (pl.col("ev_off_sd") ** 2 + pl.col("ev_def_sd") ** 2).sqrt().alias("ev_net_sd"),
        )
    )
    return _with_bio(board, players, teams, today).filter(pl.col("position") != "G")


def goalie_board(tables: dict[str, pl.DataFrame], players: pl.DataFrame, teams: pl.DataFrame, today: date) -> pl.DataFrame:
    """One row per goalie: ``save`` (share of xG stopped beyond average), its prior, sd and
    the shots, goals and xG behind it this season."""
    g = tables["finishing"].filter(pl.col("role") == "goalie").select(
        "player_id",
        (1 - pl.col("mean").exp()).alias("save"),
        (1 - pl.col("prior_mean").exp()).alias("save_prior"),
        pl.col("sd").alias("save_sd"),
        pl.col("shots").alias("shots_against"), pl.col("goals").alias("goals_against"), pl.col("xg").alias("xga"),
    ).with_columns((pl.col("xga") - pl.col("goals_against")).alias("gsax"))
    return _with_bio(g, players, teams, today)


def _with_bio(board: pl.DataFrame, players: pl.DataFrame, teams: pl.DataFrame, today: date) -> pl.DataFrame:
    """Attach name, position, age and current team."""
    bio = players.select(
        "player_id", "player_name", "position",
        ((pl.lit(today) - pl.col("birth_date").cast(pl.String).str.to_date(strict=False)).dt.total_days() / 365.25).alias("age"),
    )
    return board.join(bio, on="player_id", how="left").join(teams, on="player_id", how="left")


def goalie_weights(starts: pl.DataFrame, games: pl.DataFrame, teams: pl.DataFrame) -> pl.DataFrame:
    """``team_id, player_id, weight``: each goalie's share of his team's recent starts.

    Only regular-season and playoff games count, among each team's last
    :data:`GOALIE_LOOKBACK_GAMES`, and only goalies whose latest team (``teams``) is still
    that team, so a traded or released goalie drops out.
    """
    real = games.filter(pl.col("season_type").is_in(["R", "P"])).select("game_id")
    recent = (
        starts.join(real, on="game_id", how="semi")
        .select("game_id", "game_date", pl.col("team_id").cast(pl.Int64), pl.col("starter").alias("player_id"))
        .sort("game_date", "game_id", descending=True)
        .filter(pl.int_range(pl.len()).over("team_id") < GOALIE_LOOKBACK_GAMES)
        .join(teams.select("player_id", pl.col("team_id").alias("now")), on="player_id", how="left")
        .filter(pl.col("now") == pl.col("team_id"))
    )
    return recent.group_by("team_id", "player_id").len().with_columns(
        (pl.col("len") / pl.col("len").sum().over("team_id")).alias("weight")
    ).drop("len")


def team_board(tables: dict[str, pl.DataFrame], dep: pl.DataFrame, goalies: pl.DataFrame, constants: dict) -> pl.DataFrame:
    """One row per team in ``dep`` (a projected lineup per team), neutral opponent and ice.

    Args:
        tables: :func:`snapshot_tables`.
        dep: projected deployment (``team_id, player_id, s5, spp, spk, sshot, is_d``).
        goalies: :func:`goalie_weights`.
        constants: sim constants (:func:`nhl.sim.constants.estimate`).

    Returns:
        ``team_id``, the lineup sums (``off5, def5, offpp, defpk, shoot, dshare``), the
        goalie term, and per-60 rates: ``xgf60, xga60, xgd60, gf60, ga60, gd60`` (5v5),
        ``pp_xgf60, pk_xga60``, plus ``take_f`` / ``draw_f`` (penalties relative to league).
    """
    ev, st, fin, pen = tables["ev"], tables["st"], tables["finishing"], tables["penalties"]
    repl = replacement_levels(ev, st, fin)
    defense = float(fin["defense"][0]) if "defense" in fin.columns and fin.height else 0.0
    league_taken = float(pen.filter(pl.col("kind") == "taken")["rate"].mean())
    league_drawn = float(pen.filter(pl.col("kind") == "drawn")["rate"].mean())

    def side(frame: pl.DataFrame, s: str, name: str) -> pl.DataFrame:
        return frame.filter(pl.col("side") == s).select("player_id", pl.col("mean").alias(name))

    d = (
        dep.join(side(ev, "O", "o_ev"), on="player_id", how="left").join(side(ev, "D", "d_ev"), on="player_id", how="left")
        .join(side(st, "O", "o_st"), on="player_id", how="left").join(side(st, "D", "d_st"), on="player_id", how="left")
        .join(fin.filter(pl.col("role") == "shooter").select("player_id", pl.col("mean").alias("shoot")), on="player_id", how="left")
        .join(pen.pivot(on="kind", index="player_id", values="rate"), on="player_id", how="left")
        .with_columns(*[pl.col(k).fill_null(repl[k]) for k in ("o_ev", "d_ev", "o_st", "d_st", "shoot")],
                      pl.col("taken").fill_null(league_taken), pl.col("drawn").fill_null(league_drawn))
    )
    team = d.group_by("team_id").agg(
        (pl.col("s5") * pl.col("o_ev")).sum().alias("off5"), (pl.col("s5") * pl.col("d_ev")).sum().alias("def5"),
        (pl.col("spp") * pl.col("o_st")).sum().alias("offpp"), (pl.col("spk") * pl.col("d_st")).sum().alias("defpk"),
        (pl.col("sshot") * pl.col("shoot")).sum().alias("shoot"), (pl.col("sshot") * pl.col("is_d")).sum().alias("dshare"),
        ((pl.col("s5") * pl.col("taken")).sum() / (pl.col("s5").sum() * league_taken)).alias("take_f"),
        ((pl.col("s5") * pl.col("drawn")).sum() / (pl.col("s5").sum() * league_drawn)).alias("draw_f"),
    )
    g_terms = fin.filter(pl.col("role") == "goalie").select("player_id", pl.col("mean").alias("g"))
    goalie = goalies.join(g_terms, on="player_id", how="left").with_columns(pl.col("g").fill_null(repl["goalie"])).group_by(
        "team_id").agg((pl.col("weight") * pl.col("g")).sum().alias("goalie"))
    team = team.join(goalie, on="team_id", how="left").with_columns(pl.col("goalie").fill_null(repl["goalie"]))

    league_dshare = float(team["dshare"].mean())
    xg5, xgpp = constants["xg60_5v5"], constants["xg60_pp"]
    finish = (pl.col("shoot") + defense * (pl.col("dshare") - league_dshare)).exp()
    return team.with_columns(
        (xg5 + pl.col("off5")).alias("xgf60"),
        (xg5 + pl.col("def5")).alias("xga60"),
        ((xg5 + pl.col("off5")) * constants["goals_per_xg_5v5"] * finish).alias("gf60"),
        ((xg5 + pl.col("def5")) * constants["goals_per_xg_5v5"] * pl.col("goalie").exp()).alias("ga60"),
        (finish - 1).alias("finishing"),
        (1 - pl.col("goalie").exp()).alias("save"),
        (xgpp + pl.col("offpp")).alias("pp_xgf60"),
        (xgpp + pl.col("defpk")).alias("pk_xga60"),
    ).with_columns((pl.col("gf60") - pl.col("ga60")).alias("gd60"), (pl.col("off5") - pl.col("def5")).alias("xgd60"))


def lineup_targets(team_ids: list[int], day: date) -> pl.DataFrame:
    """One placeholder team-game per team on ``day`` for :func:`nhl.pregame.lineups.project`
    (``game_id`` is ``-team_id``; it never meets a real game)."""
    return pl.DataFrame({"game_id": [-t for t in team_ids], "team_id": team_ids, "game_date": [day] * len(team_ids)},
                        schema={"game_id": pl.Int64, "team_id": pl.Int64, "game_date": pl.Date})



#: Line slots from :func:`nhl.pregame.lineups.project`, forward lines then D pairs.
LINE_SLOTS = ("f1", "f2", "f3", "f4", "d1", "d2", "d3")


def line_board(tables: dict[str, pl.DataFrame], dep: pl.DataFrame) -> pl.DataFrame:
    """One row per projected forward line and D pair: its players and summed 5v5 ratings.

    Ratings are additive, so a line's on-ice impact is the sum of its players' terms: with
    average partners (the rest of the five) and opponents, ``xgf60`` / ``xga60`` are the 5v5
    xG for / against per 60 relative to average while it's on the ice (``xga60``: lower is
    better) and ``xgd60`` their difference. Unrated players get replacement level, as in the
    simulator. A game-time decision's backup (``p_dressed`` < 0.5) isn't counted.

    Args:
        tables: :func:`snapshot_tables`.
        dep: projected deployment with ``slot`` (``team_id, player_id, position, slot, s5``).

    Returns:
        ``team_id, slot, kind`` (``F`` | ``D``), ``player_ids`` (in lineup order),
        ``xgf60, xga60, xgd60`` and ``s5`` (the players' mean share of 5v5 time).
    """
    ev = tables["ev"]
    repl = replacement_levels(ev, tables["st"], tables["finishing"])
    if "p_dressed" in dep.columns:
        dep = dep.filter(pl.col("p_dressed").fill_null(1.0) >= 0.5)
    order = {"L": 0, "C": 1, "R": 2, "D": 3}
    d = (
        dep.filter(pl.col("slot").is_in(LINE_SLOTS))
        .join(ev.filter(pl.col("side") == "O").select("player_id", pl.col("mean").alias("o")), on="player_id", how="left")
        .join(ev.filter(pl.col("side") == "D").select("player_id", pl.col("mean").alias("d")), on="player_id", how="left")
        .with_columns(pl.col("o").fill_null(repl["o_ev"]), pl.col("d").fill_null(repl["d_ev"]),
                      pl.col("position").replace_strict(order, default=9, return_dtype=pl.Int8).alias("_pos"))
        .sort("team_id", "slot", "_pos", -pl.col("s5"))
    )
    return d.group_by("team_id", "slot", maintain_order=True).agg(
        pl.col("player_id").alias("player_ids"),
        pl.col("o").sum().alias("xgf60"), pl.col("d").sum().alias("xga60"), pl.col("s5").mean().alias("s5"),
    ).with_columns(
        pl.when(pl.col("slot").str.starts_with("d")).then(pl.lit("D")).otherwise(pl.lit("F")).alias("kind"),
        (pl.col("xgf60") - pl.col("xga60")).alias("xgd60"),
    )


#: Unit kinds: skaters per unit and the on-ice state (team skaters, opponent skaters) it comes from.
#: Forward lines and pairs are the forwards / defensemen in 5v5 stints; power-play and
#: penalty-kill units are all of a team's skaters at 5v4 / 4v5.
UNITS = {"F": (3, (5, 5)), "D": (2, (5, 5)), "PP": (5, (5, 4)), "PK": (4, (4, 5))}


def _stint_sides(stints: pl.DataFrame) -> pl.DataFrame:
    """Valid regular-season and playoff stints with both goalies in net, one row per team:
    ``game_id, stint_id, team_id, n_us, n_them, skaters, toi_s, xgf, xga, gf, ga`` (``n_*``:
    skaters on the ice). Empty-net time is dropped, so 5 skaters a side is true 5v5."""
    real = stints.filter(pl.col("valid_personnel") & ((pl.col("game_id") // 10_000) % 100).is_in([2, 3])
                         & pl.col("home_goalie").is_not_null() & pl.col("away_goalie").is_not_null())
    return pl.concat([
        real.select(
            "game_id", "stint_id", pl.col(f"{us}_team_id").cast(pl.Int64).alias("team_id"),
            pl.col(f"{us}_n").cast(pl.Int8).alias("n_us"), pl.col(f"{them}_n").cast(pl.Int8).alias("n_them"),
            pl.col(f"{us}_skaters").alias("skaters"), pl.col("duration_s").cast(pl.Float64).alias("toi_s"),
            pl.col(f"{us}_xgf").cast(pl.Float64).alias("xgf"), pl.col(f"{them}_xgf").cast(pl.Float64).alias("xga"),
            pl.col(f"{us}_gf").cast(pl.Int64).alias("gf"), pl.col(f"{them}_gf").cast(pl.Int64).alias("ga"),
        ) for us, them in (("home", "away"), ("away", "home"))
    ])


def observed_units(stints: pl.DataFrame, positions: pl.DataFrame) -> pl.DataFrame:
    """Every forward line, D pair, power-play and penalty-kill unit a team has iced, from the
    play-by-play shifts (see :data:`UNITS`).

    A 5v5 stint counts toward a forward line when the team has exactly three forwards on the
    ice (and toward a pair with exactly two defensemen); a 5v4 stint toward the power-play unit
    of its five skaters, a 4v5 stint toward the penalty-kill unit of its four. A player belongs
    to every combination he has played in. Regular season and playoffs only.

    Args:
        stints: one season's stints (:func:`nhl.storage.keys.stints`).
        positions: ``game_id, player_id, position`` (the game rosters).

    Returns:
        ``team_id, kind`` (``F`` | ``D`` | ``PP`` | ``PK``), ``player_ids`` (sorted), ``toi_s, xgf,
        xga, gf, ga, games``, plus ``team_toi_s`` (the team's total time in that state: 5v5, 5v4
        or 4v5) and ``toi_share`` (``toi_s / team_toi_s``, for a "qualified" cut that's fair
        across games played).
    """
    sides = _stint_sides(stints)
    keys_ = ["game_id", "stint_id", "team_id"]
    pos = positions.select("game_id", "player_id", (pl.col("position") == "D").alias("is_d"))
    groups = (
        sides.select(*keys_, pl.col("skaters").alias("player_id")).explode("player_id", empty_as_null=False)
        .join(pos, on=["game_id", "player_id"], how="left")
        .filter(pl.col("is_d").is_not_null())
        .group_by(keys_).agg(
            pl.col("player_id").filter(~pl.col("is_d")).sort().alias("F"),
            pl.col("player_id").filter(pl.col("is_d")).sort().alias("D"),
            pl.col("player_id").sort().alias("all"),
        )
        .join(sides.drop("skaters"), on=keys_, how="inner")
    )
    stats = [pl.col(c).sum() for c in ("toi_s", "xgf", "xga", "gf", "ga")] + [pl.col("game_id").n_unique().alias("games")]
    frames = []
    for kind, (size, (n_us, n_them)) in UNITS.items():
        state = (pl.col("n_us") == n_us) & (pl.col("n_them") == n_them)
        members = kind if kind in ("F", "D") else "all"
        team_toi = sides.filter(state).group_by("team_id").agg(pl.col("toi_s").sum().alias("team_toi_s"))
        frames.append(
            groups.filter(state & (pl.col(members).list.len() == size)).group_by("team_id", members).agg(*stats)
            .rename({members: "player_ids"}).with_columns(pl.lit(kind).alias("kind"))
            .join(team_toi, on="team_id", how="left")
        )
    return pl.concat(frames, how="diagonal").with_columns((pl.col("toi_s") / pl.col("team_toi_s")).alias("toi_share"))


def rate_units(tables: dict[str, pl.DataFrame], units: pl.DataFrame) -> pl.DataFrame:
    """Add each unit's summed ratings: ``xgf60, xga60, xgd60`` relative to average (xga60: lower
    is better). Forward lines and pairs use the 5v5 ratings (as in :func:`line_board`), power-play
    and penalty-kill units the special-teams ones; unrated players count at replacement level.
    ``units`` needs ``team_id, kind, player_ids``."""
    repl = replacement_levels(tables["ev"], tables["st"], tables["finishing"])
    rated = []
    for table, kinds, o_repl, d_repl in ((tables["ev"], ["F", "D"], repl["o_ev"], repl["d_ev"]),
                                         (tables["st"], ["PP", "PK"], repl["o_st"], repl["d_st"])):
        part = units.filter(pl.col("kind").is_in(kinds))
        if part.is_empty():
            continue
        terms = (
            part.select("team_id", "kind", "player_ids", pl.col("player_ids").alias("player_id"))
            .explode("player_id", empty_as_null=False)
            .join(table.filter(pl.col("side") == "O").select("player_id", pl.col("mean").alias("o")), on="player_id", how="left")
            .join(table.filter(pl.col("side") == "D").select("player_id", pl.col("mean").alias("d")), on="player_id", how="left")
            .group_by("team_id", "kind", "player_ids").agg(
                pl.col("o").fill_null(o_repl).sum().alias("xgf60"), pl.col("d").fill_null(d_repl).sum().alias("xga60"))
        )
        rated.append(part.join(terms, on=["team_id", "kind", "player_ids"], how="left"))
    out = pl.concat(rated, how="diagonal") if rated else units
    return out.with_columns((pl.col("xgf60") - pl.col("xga60")).alias("xgd60"))


def current_slots(lines: pl.DataFrame, dep: pl.DataFrame) -> pl.DataFrame:
    """``team_id, kind, player_ids`` (sorted) -> ``slot`` for today's projected units: lines and
    pairs from :func:`line_board` (``f1``.., ``d1``..) and power-play / penalty-kill units from
    the projection's ``pp_unit`` / ``pk_unit`` (``pp1``, ``pk2``..), so observed units can be
    marked as current."""
    full = lines.filter(pl.col("player_ids").list.drop_nulls().list.len() == pl.col("player_ids").list.len())
    parts = [full.select("team_id", "kind", pl.col("player_ids").list.sort(), "slot")]
    for kind, col in (("PP", "pp_unit"), ("PK", "pk_unit")):
        if col not in dep.columns:
            continue
        units = dep.filter(pl.col(col).is_not_null() & pl.col("player_id").is_not_null()).group_by("team_id", col).agg(
            pl.col("player_id").sort().alias("player_ids"))
        parts.append(units.select("team_id", pl.lit(kind).alias("kind"), "player_ids",
                                  (pl.lit(kind.lower()) + pl.col(col).cast(pl.String)).alias("slot")))
    return pl.concat(parts, how="vertical_relaxed")
