"use client";

import { Suspense, useState } from "react";
import { BreakdownTable, InfoBreakdown, type LedgerRow, LedgerTable } from "@/components/BetLedger";
import ResultsHeader, { OpenStats, GradedStats, useLedgerView } from "@/components/ResultsHeader";
import { Badge, ErrorState, Loading, Pills, SectionTitle } from "@/components/ui";
import { useBets } from "@/lib/api";
import { gradedTotals, groupTotals, openTotals } from "@/lib/ledger";
import { MARKET_FILTERS, MARKET_LABEL, type MarketFilter } from "@/lib/markets";
import type { Bet } from "@/lib/types";

const KINDS = [
  { key: "all", label: "All" },
  { key: "paper", label: "Paper" },
  { key: "real", label: "Real" },
] as const;

const toRow = (b: Bet): LedgerRow => ({
  id: b.bet_id, placedAt: b.placed_at, window: b.window, leadMinutes: b.lead_minutes, gameId: b.game_id,
  gameHref: `/games/${b.game_id}`, away: b.away_abbr, home: b.home_abbr,
  bet: (
    <span className="inline-flex items-center gap-1.5">
      <span className="text-xs text-muted-foreground">{MARKET_LABEL[b.market] ?? b.market}</span>
      <span className="font-medium">{b.selection}</span>
      {b.kind === "real" && <Badge tone="accent">real</Badge>}
    </span>
  ),
  betSort: `${MARKET_LABEL[b.market] ?? b.market} ${b.selection}`,
  book: b.book, price: b.price, stake: b.stake_units, fills: b.fills, edge: b.edge, nowPrice: b.now_price, nowBook: b.now_book,
  closePrice: b.close_price, clv: b.clv, clvNow: b.clv_now, priceClv: b.price_clv, bookNow: b.book_now, status: b.status, result: b.result, pnl: b.pnl_units, info: b,
});

function MarketResults() {
  const [view, setView] = useLedgerView();
  const [kind, setKind] = useState<(typeof KINDS)[number]["key"]>("all");
  const [market, setMarket] = useState<MarketFilter>("all");
  const { data, isLoading, error } = useBets(kind === "all" ? null : kind);
  const bets = data?.bets.filter((b) => market === "all" || b.market === market) ?? [];
  const pending = bets.filter((b) => b.status !== "graded");
  const graded = bets.filter((b) => b.status === "graded");
  // The server's totals and breakdowns cover every market; with a market picked, recompute them
  // from the filtered bets (same definitions as nhl.api.routers.betting;
  // the info breakdown leaves out pushes).
  const all = market === "all";
  const settled = graded.filter((b) => b.result !== "push");
  const open = !data ? null : all ? data.open : openTotals(bets);
  const totals = !data ? null : all ? data.totals : gradedTotals(graded);
  const breakdown = !data ? [] : all ? data.breakdown : groupTotals(graded, ["kind", "market"], data.breakdown);
  const info = !data ? [] : all ? data.info : groupTotals(settled, ["window", "info_grade"], data.info);

  return (
    <div className="space-y-5">
      <ResultsHeader title="Game Markets" view={view} onView={setView} pending={pending.length} graded={graded.length}
        right={
          <div className="flex flex-wrap items-center gap-3">
            <Pills options={MARKET_FILTERS} value={market} onChange={setMarket} />
            <span className="h-5 w-px bg-border" />
            <Pills options={KINDS} value={kind} onChange={setKind} />
          </div>
        } />
      {isLoading && <Loading />}
      {error && <ErrorState error={error} />}
      {open && view === "pending" && (
        <>
          <OpenStats open={open} />
          <LedgerTable rows={pending.map(toRow)} view="pending" />
        </>
      )}
      {totals && view === "graded" && (
        <>
          <GradedStats totals={totals} />
          {breakdown.length > 0 && (
            <div>
              <SectionTitle>By Market</SectionTitle>
              <BreakdownTable rows={breakdown} labels={[
                { h: "Kind", sort: (r) => r.kind, cell: (r) => <span className="capitalize">{r.kind}</span> },
                { h: "Market", sort: (r) => r.market, cell: (r) => <span className="capitalize">{r.market}</span> },
              ]} />
            </div>
          )}
          {info.length > 0 && (
            <div>
              <SectionTitle>By Timing and Info at Placement</SectionTitle>
              <InfoBreakdown rows={info} />
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
