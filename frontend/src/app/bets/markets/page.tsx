"use client";

import { Suspense, useState } from "react";
import { useSearchParams } from "next/navigation";
import DateNav from "@/components/DateNav";
import EdgeTable from "@/components/EdgeTable";
import { Card, Empty, ErrorState, Loading, Pills, Stat } from "@/components/ui";
import { useEdges } from "@/lib/api";
import { dateTimeET, stampToIso, todayET } from "@/lib/format";
import { MARKET_FILTERS, type MarketFilter } from "@/lib/markets";

const FILTERS = [
  { key: "flagged", label: "Flagged" },
  { key: "placed", label: "Placed" },
  { key: "positive", label: "Positive Edge" },
  { key: "all", label: "All" },
] as const;

function GameMarkets() {
  const date = useSearchParams().get("date") ?? todayET();
  const [filter, setFilter] = useState<(typeof FILTERS)[number]["key"]>("positive");
  const [market, setMarket] = useState<MarketFilter>("all");
  const { data, isLoading, error } = useEdges(date);

  const all = (data?.edges ?? []).filter((e) => market === "all" || e.market === market);
  const flagged = all.filter((e) => e.flagged);
  const placed = all.filter((e) => e.bet_price != null);
  const shown = filter === "flagged" ? flagged : filter === "placed" ? placed
    : filter === "positive" ? all.filter((e) => e.edge > 0 || e.bet_price != null) : all;

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-xl font-semibold">Game Markets</h1>
        <DateNav date={date} />
      </div>

      {isLoading && <Loading />}
      {error && <ErrorState error={error} />}
      {data && (
        <>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Stat label="Flagged bets" value={flagged.length} />
            <Stat label="Placed" value={`${placed.length} · ${placed.reduce((s, e) => s + (e.bet_stake ?? 0), 0).toFixed(2)}u`} />
            <Stat label="Sides priced" value={all.length} />
            <Stat label="Snapshot (ET)" value={<span className="text-base">{dateTimeET(data.stamp ? stampToIso(data.stamp) : null)}</span>} />
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Pills options={FILTERS} value={filter} onChange={setFilter} />
            <span className="h-5 w-px bg-border" />
            <Pills options={MARKET_FILTERS} value={market} onChange={setMarket} />
          </div>

          {shown.length ? (
            <Card>
              <EdgeTable edges={shown} />
            </Card>
          ) : (
            <Empty>{data.edges.length ? "Nothing matches these filters." : "No edges for this date (no snapshot, no odds, or all games started)."}</Empty>
          )}
        </>
      )}
    </div>
  );
}

export default function GameMarketsPage() {
  return (
    <Suspense fallback={<Loading />}>
      <GameMarkets />
    </Suspense>
  );
}
