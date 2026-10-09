"use client";

import { Suspense } from "react";
import { BreakdownTable, InfoBreakdown, type LedgerRow, LedgerTable } from "@/components/BetLedger";
import ResultsHeader, { GradedStats, OpenStats, useLedgerView } from "@/components/ResultsHeader";
import { ErrorState, Loading, SectionTitle } from "@/components/ui";
import { usePropBets } from "@/lib/api";
import { PROP_LABEL, propBet } from "@/lib/props";
import type { PropBet } from "@/lib/types";

const toRow = (b: PropBet): LedgerRow => ({
  id: b.bet_id, placedAt: b.placed_at, window: b.window, leadMinutes: b.lead_minutes, gameId: b.game_id,
  gameHref: `/games/${b.game_id}?tab=props`, away: b.away_abbr, home: b.home_abbr,
  bet: (
    <span>
      <span className="font-medium">{b.player_name}</span>
      <span className="ml-1 text-xs text-muted-foreground">{b.team}</span>
      <span className="ml-2 font-medium">{propBet(b.prop_type, b.line, b.side)}</span>
      {b.stat != null && <span className="ml-1.5 text-xs text-muted-foreground">(had {b.stat})</span>}
    </span>
  ),
  book: b.book, price: b.price, stake: b.stake_units, edge: b.edge, nowPrice: b.now_price, nowBook: b.now_book,
  closePrice: b.close_price, clv: b.clv, clvNow: b.clv_now, priceClv: b.price_clv, bookNow: b.book_now, status: b.status, result: b.result, pnl: b.pnl_units, info: b,
});

function PropResults() {
  const [view, setView] = useLedgerView();
  const { data, isLoading, error } = usePropBets();
  const pending = data?.bets.filter((b) => b.status !== "graded") ?? [];
  const graded = data?.bets.filter((b) => b.status === "graded") ?? [];

  return (
    <div className="space-y-5">
      <ResultsHeader title="Props" view={view} onView={setView} pending={pending.length} graded={graded.length} />
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
          {data.info.length > 0 && (
            <div>
              <SectionTitle>By Timing and Info at Placement</SectionTitle>
              <InfoBreakdown rows={data.info} />
            </div>
          )}
          {data.timing.length > 0 && (
            <div>
              <SectionTitle>By Time Before Puck Drop</SectionTitle>
              <BreakdownTable rows={data.timing} labels={[{ h: "Placed", cell: (r) => `${r.lead_bucket} before` }]} />
            </div>
          )}
          {data.breakdown.length > 0 && (
            <div>
              <SectionTitle>By Stat and Side</SectionTitle>
              <BreakdownTable rows={data.breakdown} labels={[
                { h: "Stat", cell: (r) => PROP_LABEL[r.prop_type] },
                { h: "Side", cell: (r) => <span className="capitalize">{r.side}</span> },
              ]} />
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

export default function PropResultsPage() {
  return (
    <Suspense fallback={<Loading />}>
      <PropResults />
    </Suspense>
  );
}
