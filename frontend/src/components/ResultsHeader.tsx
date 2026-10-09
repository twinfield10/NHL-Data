"use client";

import type { ReactNode } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import type { LedgerView } from "@/components/BetLedger";
import { Stat } from "@/components/ui";
import { pct, signedPct, units } from "@/lib/format";
import type { BetTotals, OpenTotals } from "@/lib/types";
import { cn } from "@/lib/utils";

const tone = (v: number | null | undefined) => (v == null || v === 0 ? null : v > 0 ? "pos" : "neg");

/** Pending / Graded, kept in the URL (``?view=graded``) so either can be linked. */
export function useLedgerView(): [LedgerView, (v: LedgerView) => void] {
  const params = useSearchParams();
  const router = useRouter();
  const path = usePathname();
  const view: LedgerView = params.get("view") === "graded" ? "graded" : "pending";
  return [view, (v) => router.replace(v === "pending" ? path : `${path}?view=${v}`, { scroll: false })];
}

/** Page title, the Pending / Graded toggle with counts, and any extra controls. */
export default function ResultsHeader({ title, view, onView, pending, graded, right }: {
  title: string; view: LedgerView; onView: (v: LedgerView) => void; pending: number; graded: number; right?: ReactNode;
}) {
  const opts: { key: LedgerView; label: string; n: number }[] = [
    { key: "pending", label: "Pending", n: pending },
    { key: "graded", label: "Graded", n: graded },
  ];
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div className="flex items-center gap-4">
        <h1 className="text-xl font-semibold">{title}</h1>
        <div className="inline-flex rounded-md border border-border bg-card p-0.5">
          {opts.map((o) => (
            <button
              key={o.key}
              onClick={() => onView(o.key)}
              aria-pressed={view === o.key}
              className={cn("rounded px-2.5 py-1 text-xs font-medium transition-colors",
                view === o.key ? "bg-accent/15 text-foreground" : "text-muted-foreground hover:text-foreground")}
            >
              {o.label} <span className="tabular text-muted-foreground">{o.n}</span>
            </button>
          ))}
        </div>
      </div>
      {right}
    </div>
  );
}

/** Ungraded bets against the market now. */
export function OpenStats({ open }: { open: OpenTotals }) {
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
      <Stat label="Pending bets" value={`${open.bets} · ${open.staked.toFixed(2)}u`} />
      <Stat label="Live CLV" value={signedPct(open.mean_clv, 2)} tone={tone(open.mean_clv)} />
      <Stat label="Beating the market" value={pct(open.beating, 0)} />
      <Stat label="Still +EV" value={open.value} />
      <Stat label="Faded / Pulled / Closed" value={`${open.faded} / ${open.gone} / ${open.closed}`} />
    </div>
  );
}

/** Headline numbers for graded bets. */
export function GradedStats({ totals }: { totals: BetTotals }) {
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
      <Stat label="Graded bets" value={totals.bets} />
      <Stat label="Units won" value={units(totals.pnl)} tone={tone(totals.pnl)} />
      <Stat label="ROI" value={signedPct(totals.roi)} tone={tone(totals.roi)} />
      <Stat label="Mean CLV" value={signedPct(totals.mean_clv, 2)} tone={tone(totals.mean_clv)} />
      <Stat label="Beat the close" value={pct(totals.beat_close, 0)} />
    </div>
  );
}
