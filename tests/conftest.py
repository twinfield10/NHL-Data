"""Synthetic games small enough to reason about by hand."""

from __future__ import annotations

import polars as pl
import pytest

HOME, AWAY = 1, 2
H_SKATER, H_D, H_GOALIE = 101, 102, 199
A_SKATER, A_GOALIE = 201, 299


def play(idx, period, clock, type_key, owner=None, x=None, y=None, situation="1551", side=None, **details):
    """Build one raw play dict in the api-web gamecenter format."""
    d = {"eventOwnerTeamId": owner, "xCoord": x, "yCoord": y, **details}
    return {
        "eventId": idx,
        "sortOrder": idx,
        "periodDescriptor": {"number": period, "periodType": "REG" if period <= 3 else "OT"},
        "timeInPeriod": clock,
        "situationCode": situation,
        "homeTeamDefendingSide": side,
        "typeDescKey": type_key,
        "details": {k: v for k, v in d.items() if v is not None},
    }


def spot(pid, team, pos):
    return {"playerId": pid, "teamId": team, "positionCode": pos,
            "firstName": {"default": "P"}, "lastName": {"default": str(pid)}}


@pytest.fixture
def raw_pbp() -> dict:
    """Home attacks +x in period 1 (no defending-side field, so it must be inferred)."""
    plays = [
        play(1, 1, "00:00", "faceoff", HOME, 0, 0, winningPlayerId=H_SKATER, losingPlayerId=A_SKATER),
        play(2, 1, "00:10", "shot-on-goal", HOME, 70, 10, shootingPlayerId=H_SKATER, goalieInNetId=A_GOALIE, shotType="wrist"),
        play(3, 1, "00:12", "goal", HOME, 80, -5, scoringPlayerId=H_SKATER, goalieInNetId=A_GOALIE, shotType="snap"),
        play(4, 1, "00:12", "faceoff", AWAY, 0, 0, winningPlayerId=A_SKATER, losingPlayerId=H_SKATER),
        # Away shoots toward -x; blocked shot owned (per API) by the blocking home team.
        play(5, 1, "00:30", "blocked-shot", HOME, -60, 5, shootingPlayerId=A_SKATER, blockingPlayerId=H_D),
        play(6, 1, "00:32", "missed-shot", AWAY, -75, -3, shootingPlayerId=A_SKATER, shotType="slap"),
        play(7, 1, "00:40", "shot-on-goal", AWAY, -65, 20, shootingPlayerId=A_SKATER, goalieInNetId=H_GOALIE,
             shotType="backhand", situation="1451"),
        play(8, 1, "00:50", "goal", AWAY, -85, 0, scoringPlayerId=A_SKATER, situation="0651"),
    ]
    return {
        "id": 2024020001, "season": 20242025, "gameType": 2, "gameDate": "2024-10-08",
        "homeTeam": {"id": HOME, "abbrev": "HOM"}, "awayTeam": {"id": AWAY, "abbrev": "AWY"},
        "plays": plays,
        "rosterSpots": [spot(H_SKATER, HOME, "C"), spot(H_D, HOME, "D"), spot(H_GOALIE, HOME, "G"),
                        spot(A_SKATER, AWAY, "L"), spot(A_GOALIE, AWAY, "G")],
    }


@pytest.fixture
def raw_shifts() -> dict:
    def sh(pid, team, start, end):
        return {"typeCode": 517, "duration": "x", "playerId": pid, "teamId": team, "period": 1,
                "startTime": start, "endTime": end}
    return {"data": [
        sh(H_SKATER, HOME, "00:00", "00:12"),
        sh(H_SKATER, HOME, "00:12", "00:20"),   # back-to-back fragment: should merge
        sh(H_D, HOME, "00:12", "01:00"),         # comes on at the goal second
        sh(H_GOALIE, HOME, "00:00", "20:00"),
        sh(A_SKATER, AWAY, "00:00", "01:00"),
        sh(A_GOALIE, AWAY, "00:00", "00:45"),    # pulled before the last goal
    ]}


@pytest.fixture
def players() -> pl.DataFrame:
    return pl.DataFrame({
        "player_id": [H_SKATER, H_D, H_GOALIE, A_SKATER, A_GOALIE],
        "shoots_catches": ["R", "L", "L", "L", "R"],
        "position": ["C", "D", "G", "L", "G"],
    })
