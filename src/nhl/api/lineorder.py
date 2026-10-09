"""Display order for units: forwards LW - C - RW, then defensemen LD - RD.

Positions come from the player catalog (``L``/``C``/``R``/``D``) and sides from
``shoots_catches``. A forward fills his natural slot first; an extra centre (or a winger
off his side) fills the empty wing, choosing by handedness. Defensemen go left shot first.
"""

from __future__ import annotations

F_SLOTS = ("L", "C", "R")
UNIT_SLOTS = ("f1", "f2", "f3", "f4", "d1", "d2", "d3")


#: Catalog position -> display position (defensemen get a side from :func:`natural_role`).
POSITION_LABEL = {"L": "LW", "C": "C", "R": "RW"}


def natural_role(p: dict, hands: dict[int, str] | None = None) -> str:
    """A player's own position for display: ``LW``/``C``/``RW``, or ``LD``/``RD`` by shooting
    hand (``D`` when unknown)."""
    pos = p.get("position")
    if pos == "D":
        side = (hands or {}).get(p.get("player_id"))
        return f"{side}D" if side in ("L", "R") else "D"
    return POSITION_LABEL.get(pos, pos or "")


def order_unit(players: list[dict], hands: dict[int, str] | None = None) -> list[dict]:
    """Players of one unit in display order (forwards LW-C-RW, then D left shot first).

    Args:
        players: Rows with ``player_id`` and ``position``.
        hands: ``player_id -> "L" | "R"`` (shoots); missing hands count as neither side.
    """
    return [p for p, _ in _arrange(players, hands)]


def unit_roles(players: list[dict], hands: dict[int, str] | None = None, five_on_five: bool = True) -> list[str]:
    """The position each player fills in his unit, in :func:`order_unit` order. At 5v5 a forward
    takes the LW / C / RW slot he was placed in and a pair is LD then RD; on special teams (and
    for anyone beyond a full line) it is his own position (:func:`natural_role`)."""
    arranged = _arrange(players, hands)
    dmen = [p for p, _ in arranged if p.get("position") == "D"]
    out = []
    for p, slot in arranged:
        if five_on_five and slot:
            out.append(POSITION_LABEL[slot])
        elif five_on_five and len(dmen) == 2 and p in dmen:
            out.append(("LD", "RD")[dmen.index(p)])
        else:
            out.append(natural_role(p, hands))
    return out


def _arrange(players: list[dict], hands: dict[int, str] | None) -> list[tuple[dict, str | None]]:
    """``(player, slot)`` in display order; ``slot`` is the forward slot filled (``L``/``C``/``R``)
    or None for spare forwards and defensemen."""
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
    ordered_f = [(slots[s], s) for s in F_SLOTS if s in slots] + [(p, None) for p in rest]
    ordered_d = sorted(dmen, key=lambda p: 0 if hand(p) == "L" else 1 if hand(p) == "R" else 2)
    return ordered_f + [(p, None) for p in ordered_d]


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
