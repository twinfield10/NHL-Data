"use client";

import Link from "next/link";
import { ArrowDownRight, ArrowUpRight } from "lucide-react";
import SortTable, { type Column } from "@/components/SortTable";
import { TeamTag } from "@/components/TeamLogo";
import { american, pct, signedPct, timeET } from "@/lib/format";
import { isPlay, playStake, propBet, SLOT_LABEL } from "@/lib/props";
import type { PropEdge } from "@/lib/types";
import { cn } from "@/lib/utils";

/** "RISK 0.25u" for a staked play (the same badge the game cards use). */
export function RiskBadge({ stake }: { stake: number }) {
  return (
    <span className="whitespace-nowrap rounded bg-emerald-600 px-1.5 text-[11px] font-bold leading-4 tracking-wide text-white">
      RISK {stake.toFixed(2)}u
    </span>
  );
}

/** Opening price at the same book -> now, with the direction it moved for the bettor. */
function Movement({ e }: { e: PropEdge }) {
  if (e.open_price == null) return <span className="text-muted-foreground">–</span>;
  if (e.open_price === e.price) return <span className="text-muted-foreground">{american(e.open_price)}</span>;
  // A longer price is better for the side we'd bet: green when it lengthened, red when it shortened.
  const better = e.price > e.open_price;
  const Icon = better ? ArrowUpRight : ArrowDownRight;
  return (
    <span className={cn("inline-flex items-center gap-0.5", better ? "text-positive" : "text-negative")}
      title={`opened ${american(e.open_price)}, ${e.moves ?? 0} move${e.moves === 1 ? "" : "s"}`}>
      {american(e.open_price)} <Icon className="h-3 w-3" />
    </span>
  );
}

const started = (e: PropEdge) => Date.parse(e.start_utc) <= Date.now();

/** Sortable list of prop edges: one row per (player, stat, line, side) at its best book. */
export default function PropTable({ rows, showGame = true }: { rows: PropEdge[]; showGame?: boolean }) {
  const columns: Column<PropEdge>[] = [
    ...(showGame ? [{
      key: "time", label: "Start", sort: (e: PropEdge) => e.start_utc,
      render: (e: PropEdge) => <span className="text-muted-foreground">{started(e) ? "Started" : timeET(e.start_utc)}</span>,
    }, {
      key: "game", label: "Game", sort: (e: PropEdge) => `${e.away_abbr}@${e.home_abbr}`,
      render: (e: PropEdge) => (
        <Link href={`/games/${e.game_id}?tab=props`} className="inline-flex items-center gap-1.5 hover:text-accent">
          <TeamTag abbr={e.away_abbr} /> @ <TeamTag abbr={e.home_abbr} />
        </Link>
      ),
    }] : []),
    {
      key: "player", label: "Player", sort: (e) => e.player_name ?? "",
      render: (e) => (
        <span>
          <span className="font-medium">{e.player_name}</span>
          <span className="ml-1.5 text-xs text-muted-foreground">
            {e.team} · {e.position}{e.slot ? ` · ${SLOT_LABEL[e.slot] ?? e.slot}` : ""}{e.pp_unit ? ` · PP${e.pp_unit}` : ""}
          </span>
        </span>
      ),
    },
    { key: "bet", label: "Bet", sort: (e) => `${e.prop_type}${e.line}${e.side}`, render: (e) => <span className="font-medium">{propBet(e.prop_type, e.line, e.side)}</span> },
    {
      key: "price", label: "Best Price", align: "right", sort: (e) => e.price,
      render: (e) => <span>{american(e.price)} <span className="text-xs text-muted-foreground">{e.book}</span></span>,
    },
    { key: "open", label: "Open", title: "Opening price at the same book (arrow: how it moved for this side)", align: "right", sort: (e) => e.open_price, render: (e) => <Movement e={e} /> },
    { key: "model", label: "Model", title: "Model probability for this side", align: "right", sort: (e) => e.p_model_side, render: (e) => pct(e.p_model_side) },
    { key: "market", label: "Market", title: "Devigged consensus across books", align: "right", sort: (e) => e.p_market_side, render: (e) => pct(e.p_market_side) },
    { key: "books", label: "Books", align: "right", sort: (e) => e.books, render: (e) => e.books },
    {
      key: "edge", label: "Edge", title: "Blend × decimal price − 1 at the best book", align: "right", sort: (e) => e.edge,
      render: (e) => (
        <span className={cn("font-semibold", e.flagged ? "text-positive" : e.edge > 0 ? "text-foreground" : "text-muted-foreground")}>
          {signedPct(e.edge)}
        </span>
      ),
    },
    {
      key: "risk", label: "", sort: (e) => (isPlay(e) ? playStake(e) : null),
      render: (e) =>
        isPlay(e) ? (
          <span className="inline-flex items-center gap-1.5" title={e.bet_price != null ? `placed ${american(e.bet_price)} at ${e.bet_book}` : "not placed yet"}>
            <RiskBadge stake={playStake(e)} />
            {e.bet_price != null && e.bet_price !== e.price && (
              <span className="text-[11px] text-muted-foreground">@ {american(e.bet_price)}</span>
            )}
          </span>
        ) : null,
    },
  ];
  return (
    <SortTable
      rows={rows}
      columns={columns}
      rowKey={(e) => `${e.game_id}-${e.player_id}-${e.prop_type}-${e.line}-${e.side}`}
      initialSort={{ key: "edge", desc: true }}
      rowClassName={(e) => cn(isPlay(e) && "bg-emerald-500/5", started(e) && "opacity-60")}
    />
  );
}
