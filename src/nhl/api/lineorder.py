"""Display order for units: forwards LW - C - RW, then defensemen LD - RD.

Positions come from the player catalog (``L``/``C``/``R``/``D``) and sides from
``shoots_catches``. A forward fills his natural slot first; an extra centre (or a winger
off his side) fills the empty wing, choosing by handedness. Defensemen go left shot first.
"""

from __future__ import annotations

F_SLOTS = ("L", "C", "R")
UNIT_SLOTS = ("f1", "f2", "f3", "f4", "d1", "d2", "d3")


def order_unit(players: list[dict], hands: dict[int, str] | None = None) -> list[dict]:
    """Players of one unit in display order (forwards LW-C-RW, then D left shot first).

    Args:
        players: Rows with ``player_id`` and ``position``.
        hands: ``player_id -> "L" | "R"`` (shoots); missing hands count as neither side.
    """
    hands = hands or {}
    hand = lambda p: hands.get(p.get("player_id"))  # noqa: E731
    fwd = [p for p in players if p.get("position") != "D"]
    dmen = [p for p in players if p.get("position") == "D"]
    slots: dict[str, dict] = {}
    centres, extra = [], []
    for p in fwd:
        pos = p.get("position")
        if pos == "C":
            centres.append(p)
        elif pos in ("L", "R") and pos not in slots:
            slots[pos] = p
        else:
            extra.append(p)
    for side in ("L", "R"):  # empty wing: a spare winger on his side, else a spare centre on his side
        if side in slots:
            continue
        pick = next((p for p in extra if hand(p) == side), None)
        if pick is None and len(centres) > 1:
            pick = next((p for p in reversed(centres) if hand(p) == side), None)
        if pick is None:
            pick = extra[0] if extra else (centres[-1] if len(centres) > 1 else None)
        if pick is not None:
            slots[side] = pick
            (extra if pick in extra else centres).remove(pick)
    if centres:
        slots["C"] = centres.pop(0)
    elif extra:
        slots["C"] = extra.pop(0)
    rest = centres + extra
    ordered_f = [slots[s] for s in F_SLOTS if s in slots] + rest
    ordered_d = sorted(dmen, key=lambda p: 0 if hand(p) == "L" else 1 if hand(p) == "R" else 2)
    return ordered_f + ordered_d


def order_lineup(rows: list[dict], hands: dict[int, str] | None = None) -> list[dict]:
    """A lineup already grouped by ``slot``: each line and pair (f1..d3) put in display order,
    everything else left where it is."""
    out, i = [], 0
    while i < len(rows):
        slot = rows[i].get("slot")
        j = i
        while j < len(rows) and rows[j].get("slot") == slot:
            j += 1
        group = rows[i:j]
        out.extend(order_unit(group, hands) if slot in UNIT_SLOTS else group)
        i = j
    return out
