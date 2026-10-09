"use client";

import Link from "next/link";
import { ArrowDownRight, ArrowUpRight } from "lucide-react";
import SortTable, { type Column } from "@/components/SortTable";
import { ClvCell, PlacedBet } from "@/components/BetLedger";
import { TeamTag } from "@/components/TeamLogo";
import { Signed } from "@/components/ui";
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
    {
      key: "clv", label: "Fair CLV", title: "Placed bets: the price taken vs. the devigged consensus now", align: "right",
      sort: (e) => e.bet_clv ?? null, render: (e) => <ClvCell v={e.bet_clv} />,
    },
    {
      key: "price_clv", label: "Price CLV", title: "Placed bets: the price taken vs. the same book's price now", align: "right",
      sort: (e) => e.bet_price_clv ?? null,
      render: (e) => <ClvCell v={e.bet_price_clv} title={e.bet_book_now != null ? `${e.bet_book} now ${american(e.bet_book_now)}` : undefined} />,
    },
    { key: "model", label: "Model", title: "Model probability for this side", align: "right", sort: (e) => e.p_model_side, render: (e) => pct(e.p_model_side) },
    { key: "market", label: "Market", title: "Devigged consensus across books", align: "right", sort: (e) => e.p_market_side, render: (e) => pct(e.p_market_side) },
    {
      key: "books", label: "Books", title: "Books quoting the line (quoting both sides); plays need 2 two-way books", align: "right",
      sort: (e) => e.books,
      render: (e) => (
        <span>{e.books}{e.two_way_books != null && <span className="ml-1 text-xs text-muted-foreground">({e.two_way_books})</span>}</span>
      ),
    },
    {
      key: "edge", label: "Edge", title: "Blend × decimal price − 1 at the best book", align: "right", sort: (e) => e.edge,
      render: (e) => <span className="font-medium"><Signed value={e.edge}>{signedPct(e.edge)}</Signed></span>,
    },
    {
      key: "stake", label: "Stake", title: "Units at risk: as placed, else the current stake", align: "right",
      sort: (e) => (isPlay(e) ? playStake(e) : null),
      render: (e) => (isPlay(e) ? `${playStake(e).toFixed(2)}u` : ""),
    },
    {
      key: "placed", label: "Placed", title: "Paper bet already placed: price, book, time (ET)", sort: (e) => e.placed_at ?? null,
      render: (e) => <PlacedBet price={e.bet_price} book={e.bet_book} stake={e.bet_stake} at={e.placed_at} />,
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
