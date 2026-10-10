"use client";

import { Suspense, useState } from "react";
import { BreakdownTable, InfoBreakdown, type LedgerRow, LedgerTable } from "@/components/BetLedger";
import ResultsHeader, { GradedStats, OpenStats, useLedgerView } from "@/components/ResultsHeader";
import { ErrorState, Loading, Pills, SectionTitle } from "@/components/ui";
import { usePropBets } from "@/lib/api";
import { gradedTotals, groupTotals, openTotals } from "@/lib/ledger";
import { PROP_FILTERS, PROP_LABEL, propBet } from "@/lib/props";
import type { PropBet, PropLeadBucket, PropType } from "@/lib/types";

const LEAD_ORDER: PropLeadBucket[] = ["<1h", "1-3h", "3-6h", "6-12h", "12h+"];

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
  betSort: `${b.player_name ?? ""} ${propBet(b.prop_type, b.line, b.side)}`,
  book: b.book, price: b.price, stake: b.stake_units, edge: b.edge, nowPrice: b.now_price, nowBook: b.now_book,
  closePrice: b.close_price, clv: b.clv, clvNow: b.clv_now, priceClv: b.price_clv, bookNow: b.book_now, status: b.status, result: b.result, pnl: b.pnl_units, info: b,
});

function PropResults() {
  const [view, setView] = useLedgerView();
  const [stat, setStat] = useState<PropType | "all">("all");
  const { data, isLoading, error } = usePropBets();
  const bets = data?.bets.filter((b) => stat === "all" || b.prop_type === stat) ?? [];
  const pending = bets.filter((b) => b.status !== "graded");
  const graded = bets.filter((b) => b.status === "graded");
  // The server's totals and breakdowns cover every stat; with one picked, recompute them from the
  // filtered bets (same definitions as nhl.api.routers.props: voids excluded, pushes too for info).
  const all = stat === "all";
  const counted = graded.filter((b) => b.result !== "void");
  const open = !data ? null : all ? data.open : openTotals(bets);
  const totals = !data ? null : all ? data.totals : gradedTotals(counted);
  const info = !data ? [] : all ? data.info
    : groupTotals(counted.filter((b) => (b.result as string) !== "push"), ["window", "info_grade"], data.info);
  const timing = !data ? [] : all ? data.timing
    : groupTotals(counted.filter((b) => b.lead_bucket != null), ["lead_bucket"], data.timing).map((r) => ({ ...r, lead_bucket: r.lead_bucket! }));
  const breakdown = !data ? [] : all ? data.breakdown : groupTotals(counted, ["prop_type", "side"], data.breakdown);

  return (
    <div className="space-y-5">
      <ResultsHeader title="Prop Markets" view={view} onView={setView} pending={pending.length} graded={graded.length}
        right={<Pills options={PROP_FILTERS} value={stat} onChange={setStat} />} />
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
          {info.length > 0 && (
            <div>
              <SectionTitle>By Timing and Info at Placement</SectionTitle>
              <InfoBreakdown rows={info} />
            </div>
          )}
          {timing.length > 0 && (
            <div>
              <SectionTitle>By Time Before Puck Drop</SectionTitle>
              <BreakdownTable rows={timing} labels={[
                { h: "Placed", sort: (r) => LEAD_ORDER.indexOf(r.lead_bucket), cell: (r) => `${r.lead_bucket} before` },
              ]} />
            </div>
          )}
          {breakdown.length > 0 && (
            <div>
              <SectionTitle>By Stat and Side</SectionTitle>
              <BreakdownTable rows={breakdown} labels={[
                { h: "Stat", sort: (r) => PROP_LABEL[r.prop_type], cell: (r) => PROP_LABEL[r.prop_type] },
                { h: "Side", sort: (r) => r.side, cell: (r) => <span className="capitalize">{r.side}</span> },
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
