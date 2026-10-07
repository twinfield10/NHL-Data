"use client";

import { Suspense, useState } from "react";
import { useSearchParams } from "next/navigation";
import DateNav from "@/components/DateNav";
import EdgeTable from "@/components/EdgeTable";
import { Card, Empty, ErrorState, Loading, Stat } from "@/components/ui";
import { useEdges } from "@/lib/api";
import { dateTimeET, stampToIso, todayET } from "@/lib/format";
import { cn } from "@/lib/utils";

const FILTERS = [
  { key: "flagged", label: "Flagged" },
  { key: "positive", label: "Positive edge" },
  { key: "all", label: "All" },
] as const;

function Edges() {
  const date = useSearchParams().get("date") ?? todayET();
  const [filter, setFilter] = useState<(typeof FILTERS)[number]["key"]>("positive");
  const { data, isLoading, error } = useEdges(date);

  const all = data?.edges ?? [];
  const flagged = all.filter((e) => e.flagged);
  const shown = filter === "flagged" ? flagged : filter === "positive" ? all.filter((e) => e.edge > 0) : all;

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-xl font-semibold">Edges</h1>
        <DateNav date={date} />
      </div>

      {isLoading && <Loading />}
      {error && <ErrorState error={error} />}
      {data && (
        <>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Stat label="Flagged bets" value={flagged.length} />
            <Stat label="Units staked" value={`${flagged.reduce((s, e) => s + e.stake_units, 0).toFixed(2)}u`} />
            <Stat label="Sides priced" value={all.length} />
            <Stat label="Snapshot (ET)" value={<span className="text-base">{dateTimeET(data.stamp ? stampToIso(data.stamp) : null)}</span>} />
          </div>

          <div className="flex gap-1">
            {FILTERS.map((f) => (
              <button
                key={f.key}
                onClick={() => setFilter(f.key)}
                className={cn(
                  "rounded-md px-3 py-1 text-sm",
                  filter === f.key ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground"
                )}
              >
                {f.label}
              </button>
            ))}
          </div>

          {shown.length ? (
            <Card>
              <EdgeTable edges={shown} />
            </Card>
          ) : (
            <Empty>{all.length ? "Nothing matches this filter." : "No edges for this date (no snapshot, no odds, or all games started)."}</Empty>
          )}
        </>
      )}
    </div>
  );
}

export default function EdgesPage() {
  return (
    <Suspense fallback={<Loading />}>
      <Edges />
    </Suspense>
  );
}
