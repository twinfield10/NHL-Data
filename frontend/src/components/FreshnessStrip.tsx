import type { Freshness } from "@/lib/types";
import { cn } from "@/lib/utils";

const LABELS: Record<string, string> = {
  dailyfaceoff_goalies: "Goalies",
  dailyfaceoff_lines: "Lines",
  espn_injuries: "Injuries",
  transactions: "Transactions",
  ref_assignments: "Refs",
  odds: "Odds",
  ratings: "Ratings",
  game_state: "Game state",
};

const age = (h: number | null) => (h == null ? "never" : h < 1 ? `${Math.round(h * 60)}m` : `${h.toFixed(h < 10 ? 1 : 0)}h`);

/** One chip per pregame input: how old it was at the latest run, red when past its limit. */
export default function FreshnessStrip({ rows }: { rows: Freshness[] }) {
  if (!rows.length) return null;
  return (
    <div className="flex flex-wrap gap-1.5">
      {rows.map((r) => (
        <span
          key={r.source}
          title={`${r.source}: ${age(r.age_hours)} old (limit ${r.max_age_hours}h)${r.note ? ` · ${r.note}` : ""}`}
          className={cn(
            "inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-[11px]",
            r.stale ? "border-negative/40 text-negative" : "border-border text-muted-foreground"
          )}
        >
          <span className={cn("h-1.5 w-1.5 rounded-full", r.stale ? "bg-negative" : "bg-positive")} />
          {LABELS[r.source] ?? r.source} <span className="tabular">{age(r.age_hours)}</span>
        </span>
      ))}
    </div>
  );
}
