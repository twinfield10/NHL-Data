"use client";

import Link from "next/link";
import type { Edge } from "@/lib/types";
import { american, pct, signedPct, timeET } from "@/lib/format";
import { MARKET_LABEL } from "@/lib/markets";
import { cn } from "@/lib/utils";
import { ClvCell, PlacedBet } from "./BetLedger";
import SortTable, { type Column } from "./SortTable";
import TeamLogo from "./TeamLogo";
import { Signed } from "./ui";

/** Best book per side; flagged rows are highlighted and carry a stake. With ledger data, a Placed
 *  column shows the paper bet already taken on each side. Every column sorts. */
export default function EdgeTable({ edges, showGame = true }: { edges: Edge[]; showGame?: boolean }) {
  const withBets = edges.some((e) => e.bet_price !== undefined);
  const columns: Column<Edge>[] = [
    ...(showGame ? [{
      key: "game", label: "Game", sort: (e: Edge) => `${e.start_utc} ${e.away_abbr}@${e.home_abbr}`,
      render: (e: Edge) => (
        <>
          <Link href={`/games/${e.game_id}`} className="inline-flex items-center gap-1.5 hover:text-accent">
            <TeamLogo abbr={e.away_abbr} className="h-5 w-5" /> {e.away_abbr} @ {e.home_abbr}
            <TeamLogo abbr={e.home_abbr} className="h-5 w-5" />
          </Link>
          <span className="ml-2 text-xs text-muted-foreground">{timeET(e.start_utc)}</span>
        </>
      ),
    }] : []),
    {
      key: "bet", label: "Bet", sort: (e) => `${MARKET_LABEL[e.market]} ${e.selection}`,
      render: (e) => (
        <>
          <span className="mr-2 text-xs text-muted-foreground">{MARKET_LABEL[e.market]}</span>
          <span className="font-medium">{e.selection}</span>
        </>
      ),
    },
    { key: "price", label: "Price", align: "right", sort: (e) => e.price, render: (e) => american(e.price) },
    { key: "book", label: "Book", sort: (e) => e.book, className: "text-muted-foreground", render: (e) => e.book },
    ...(withBets ? [{
      key: "clv", label: "Fair CLV", title: "Placed bets: the price taken vs. the devigged consensus now", align: "right" as const,
      sort: (e: Edge) => e.bet_clv ?? null, render: (e: Edge) => <ClvCell v={e.bet_clv} />,
    }, {
      key: "price_clv", label: "Price CLV", title: "Placed bets: the price taken vs. the same book's price now", align: "right" as const,
      sort: (e: Edge) => e.bet_price_clv ?? null,
      render: (e: Edge) => <ClvCell v={e.bet_price_clv} title={e.bet_book_now != null ? `${e.bet_book} now ${american(e.bet_book_now)}` : undefined} />,
    }] : []),
    { key: "model", label: "Model", align: "right", sort: (e) => e.p_model_side, render: (e) => pct(e.p_model_side) },
    { key: "market", label: "Market", align: "right", sort: (e) => e.p_market_side, render: (e) => pct(e.p_market_side) },
    { key: "blend", label: "Blend", align: "right", sort: (e) => e.p, render: (e) => pct(e.p) },
    {
      key: "edge", label: "Edge", align: "right", sort: (e) => e.edge, className: "font-medium",
      render: (e) => <Signed value={e.edge}>{signedPct(e.edge)}</Signed>,
    },
    {
      key: "stake", label: "Stake", align: "right", sort: (e) => (e.stake_units > 0 ? e.stake_units : null),
      render: (e) => (e.stake_units > 0 ? `${e.stake_units.toFixed(2)}u` : ""),
    },
    ...(withBets ? [{
      key: "placed", label: "Placed", title: "Paper position already held: price (average over its fills), first book and time (ET)",
      sort: (e: Edge) => e.placed_at ?? null,
      render: (e: Edge) => <PlacedBet price={e.bet_price} book={e.bet_book} stake={e.bet_stake} at={e.placed_at} fills={e.bet_fills} />,
    }] : []),
  ];
  return (
    <SortTable
      rows={edges}
      columns={columns}
      rowKey={(e) => `${e.game_id}-${e.market}-${e.side}-${e.line}`}
      rowClassName={(e) => cn(e.flagged && "bg-positive/10")}
      rank={false}
    />
  );
}
