"use client";

import { Suspense, useState } from "react";
import { BreakdownTable, InfoBreakdown, type LedgerRow, LedgerTable } from "@/components/BetLedger";
import ResultsHeader, { OpenStats, GradedStats, useLedgerView } from "@/components/ResultsHeader";
import { Badge, ErrorState, Loading, Pills, SectionTitle } from "@/components/ui";
import { useBets } from "@/lib/api";
import type { Bet } from "@/lib/types";

const KINDS = [
  { key: "all", label: "All" },
  { key: "paper", label: "Paper" },
  { key: "real", label: "Real" },
] as const;
const MARKET = { moneyline: "ML", puckline: "PL", total: "Total" } as Record<string, string>;

const toRow = (b: Bet): LedgerRow => ({
  id: b.bet_id, placedAt: b.placed_at, window: b.window, leadMinutes: b.lead_minutes, gameId: b.game_id,
  gameHref: `/games/${b.game_id}`, away: b.away_abbr, home: b.home_abbr,
  bet: (
    <span className="inline-flex items-center gap-1.5">
      <span className="text-xs text-muted-foreground">{MARKET[b.market] ?? b.market}</span>
      <span className="font-medium">{b.selection}</span>
      {b.kind === "real" && <Badge tone="accent">real</Badge>}
    </span>
  ),
  book: b.book, price: b.price, stake: b.stake_units, edge: b.edge, nowPrice: b.now_price, nowBook: b.now_book,
  closePrice: b.close_price, clv: b.clv, clvNow: b.clv_now, priceClv: b.price_clv, bookNow: b.book_now, status: b.status, result: b.result, pnl: b.pnl_units, info: b,
});

function MarketResults() {
  const [view, setView] = useLedgerView();
  const [kind, setKind] = useState<(typeof KINDS)[number]["key"]>("all");
  const { data, isLoading, error } = useBets(kind === "all" ? null : kind);
  const pending = data?.bets.filter((b) => b.status !== "graded") ?? [];
  const graded = data?.bets.filter((b) => b.status === "graded") ?? [];

  return (
    <div className="space-y-5">
      <ResultsHeader title="Game Markets" view={view} onView={setView} pending={pending.length} graded={graded.length}
        right={<Pills options={KINDS} value={kind} onChange={setKind} />} />
      {isLoading && <Loading />}
      {error && <ErrorState error={error} />}
      {data && view === "pending" && (
        <>
          <OpenStats open={data.open} />
          <LedgerTable rows={pending.map(toRow)} view="pending" />
        </>
      )}
      {data && view === "graded" && (
        <>
          <GradedStats totals={data.totals} />
          {data.breakdown.length > 0 && (
            <div>
              <SectionTitle>By Market</SectionTitle>
              <BreakdownTable rows={data.breakdown} labels={[
                { h: "Kind", cell: (r) => <span className="capitalize">{r.kind}</span> },
                { h: "Market", cell: (r) => <span className="capitalize">{r.market}</span> },
              ]} />
            </div>
          )}
          {data.info.length > 0 && (
            <div>
              <SectionTitle>By Timing and Info at Placement</SectionTitle>
              <InfoBreakdown rows={data.info} />
            </div>
          )}
          <div>
            <SectionTitle>Ledger</SectionTitle>
            <LedgerTable rows={graded.map(toRow)} view="graded" />
          </div>
        </>
      )}
    </div>
  );
}

export default function MarketResultsPage() {
  return (
    <Suspense fallback={<Loading />}>
      <MarketResults />
    </Suspense>
  );
}
