"use client";

import { Suspense, useState } from "react";
import { useSearchParams } from "next/navigation";
import { ChevronDown, ChevronUp, Loader2 } from "lucide-react";
import DateNav from "@/components/DateNav";
import FreshnessStrip from "@/components/FreshnessStrip";
import GameCard, { maxEdge, UnpricedGameCard } from "@/components/GameCard";
import { ErrorState } from "@/components/ui";
import { useSlate } from "@/lib/api";
import { stampToIso, todayET } from "@/lib/format";
import type { Bet, Edge, MarketLine, ThreeWay } from "@/lib/types";
import { cn } from "@/lib/utils";

type SortBy = "TIME" | "ML" | "OU";
const SORT_OPTIONS: [SortBy, string][] = [
  ["TIME", "Game Start"],
  ["ML", "ML Edge"],
  ["OU", "O/U Edge"],
];

function groupBy<T extends { game_id: number }>(rows: T[]): Record<number, T[]> {
  const out: Record<number, T[]> = {};
  for (const r of rows) (out[r.game_id] ??= []).push(r);
  return out;
}

function Games() {
  const date = useSearchParams().get("date") ?? todayET();
  const [sortBy, setSortBy] = useState<SortBy>("TIME");
  // Cards show moneyline and total; expanding adds the puck line and the regulation three-way.
  const [expanded, setExpanded] = useState(false);
  const { data, isLoading, error } = useSlate(date);

  const edgesByGame: Record<number, Edge[]> = data ? groupBy(data.edges) : {};
  const betsByGame: Record<number, Bet[]> = data ? groupBy(data.bets) : {};
  const linesByGame: Record<number, MarketLine[]> = data ? groupBy(data.lines) : {};
  const threeWay: Record<number, ThreeWay> = Object.fromEntries((data?.three_way ?? []).map((t) => [t.game_id, t]));
  // Game start, or the largest blended edge on either side of the moneyline / total (ties by start).
  const games = [...(data?.games ?? [])].sort((a, b) => {
    if (sortBy !== "TIME") {
      const market = sortBy === "ML" ? "moneyline" : "total";
      const [ea, eb] = [maxEdge(edgesByGame[a.game_id] ?? [], market), maxEdge(edgesByGame[b.game_id] ?? [], market)];
      if (ea !== eb) return eb > ea ? 1 : -1; // guard: −∞ − −∞ is NaN
    }
    return Date.parse(a.start_time) - Date.parse(b.start_time) || a.game_id - b.game_id;
  });
  const plays = data ? new Set(data.edges.filter((e) => e.flagged && e.point !== "close").map((e) => e.game_id)).size : 0;
  const total = games.length + (data?.unpriced.length ?? 0);

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-bold">Games</h1>
        <DateNav date={date} />
      </div>

      {isLoading && (
        <div className="flex justify-center py-20">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      )}
      {error && <ErrorState error={error} />}
      {data && total === 0 && <div className="py-20 text-center text-muted-foreground">No games found for {date}</div>}

      {data && total > 0 && (
        <>
          <div className="mb-4 flex flex-wrap items-center justify-between gap-y-2 text-sm text-muted-foreground">
            <div className="flex items-center gap-3">
              <span>
                {total} game{total !== 1 ? "s" : ""} / {plays} play{plays !== 1 ? "s" : ""}
              </span>
              <div className="inline-flex items-center gap-1.5">
                <span className="text-xs">Sort</span>
                <div className="inline-flex rounded-md border border-border bg-card p-0.5">
                  {SORT_OPTIONS.map(([key, label]) => (
                    <button
                      key={key}
                      onClick={() => setSortBy(key)}
                      aria-pressed={sortBy === key}
                      className={cn(
                        "rounded px-2.5 py-1 text-xs font-medium transition-colors",
                        sortBy === key ? "bg-accent/15 text-foreground" : "text-muted-foreground hover:text-foreground"
                      )}
                    >
                      {label}
                    </button>
                  ))}
                </div>
              </div>
              <button
                onClick={() => setExpanded((e) => !e)}
                aria-pressed={expanded}
                className="inline-flex items-center gap-1 rounded-md border border-border bg-card px-2.5 py-1 text-xs font-medium text-muted-foreground transition-colors hover:text-foreground"
              >
                {expanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
                {expanded ? "Collapse Bets" : "Expand All Bets"}
              </button>
            </div>
            {data.stamp && (
              <span>
                Last update:{" "}
                {new Date(stampToIso(data.stamp)).toLocaleString([], {
                  month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit", timeZoneName: "short",
                })}
              </span>
            )}
          </div>

          {data.freshness.length > 0 && (
            <div className="mb-4">
              <FreshnessStrip rows={data.freshness} />
            </div>
          )}

          <div className="grid grid-cols-1 gap-4 min-[1280px]:grid-cols-2">
            {games.map((g) => (
              <GameCard key={g.game_id} g={g} edges={edgesByGame[g.game_id] ?? []} bets={betsByGame[g.game_id] ?? []} teams={data.teams}
                threeWay={threeWay[g.game_id]} expanded={expanded} />
            ))}
            {data.unpriced.map((g) => (
              <UnpricedGameCard key={g.game_id} g={g} teams={data.teams} lines={linesByGame[g.game_id] ?? []} threeWay={threeWay[g.game_id]} expanded={expanded} />
            ))}
          </div>
        </>
      )}
    </div>
  );
}

export default function GamesPage() {
  return (
    <Suspense fallback={null}>
      <Games />
    </Suspense>
  );
}
