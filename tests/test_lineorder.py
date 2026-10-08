from nhl.api.lineorder import order_lineup, order_unit


def _p(pid, pos, slot=None):
    return {"player_id": pid, "position": pos, "slot": slot}


def test_forwards_lw_c_rw_and_defense_left_shot_first():
    unit = [_p(1, "R"), _p(2, "D"), _p(3, "C"), _p(4, "L"), _p(5, "D")]
    out = order_unit(unit, {2: "R", 5: "L"})
    assert [p["player_id"] for p in out] == [4, 3, 1, 5, 2]


def test_extra_centre_fills_the_empty_wing_on_his_shooting_side():
    unit = [_p(1, "C"), _p(2, "C"), _p(3, "L")]  # two centres: one plays right wing
    assert [p["player_id"] for p in order_unit(unit, {1: "R", 2: "L"})] == [3, 2, 1]
    unit = [_p(1, "C"), _p(2, "C"), _p(3, "R")]  # no left winger: the left-shot centre moves there
    assert [p["player_id"] for p in order_unit(unit, {1: "L", 2: "R"})] == [1, 2, 3]


def test_order_lineup_reorders_lines_and_pairs_only():
    rows = [_p(1, "R", "f1"), _p(2, "C", "f1"), _p(3, "L", "f1"), _p(4, "D", "d1"), _p(5, "D", "d1"),
            _p(6, "C", None), _p(7, "L", None)]
    out = order_lineup(rows, {4: "R", 5: "L"})
    assert [p["player_id"] for p in out] == [3, 2, 1, 5, 4, 6, 7]
