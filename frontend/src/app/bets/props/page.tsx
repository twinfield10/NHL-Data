"use client";

import { Suspense, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import DateNav from "@/components/DateNav";
import PropTable from "@/components/PropTable";
import { Card, Empty, ErrorState, Loading, Stat } from "@/components/ui";
import { useProps } from "@/lib/api";
import { dateTimeET, stampToIso, todayET } from "@/lib/format";
import { isPlay, MAX_PRICE, PROP_LABEL, playStake } from "@/lib/props";
import type { PropType } from "@/lib/types";
import { cn } from "@/lib/utils";

const VIEWS = [
  { key: "plays", label: "Plays" },
  { key: "positive", label: `Positive Edge (≤ +${MAX_PRICE})` },
  { key: "all", label: "All" },
] as const;
type View = (typeof VIEWS)[number]["key"];
const STATS: (PropType | "all")[] = ["all", "goals", "assists", "points", "shots", "blocks", "saves"];

function Toggle<T extends string>({ options, value, onChange }: { options: { key: T; label: string }[]; value: T; onChange: (v: T) => void }) {
  return (
    <div className="inline-flex rounded-md border border-border bg-card p-0.5">
      {options.map((o) => (
        <button
          key={o.key}
          onClick={() => onChange(o.key)}
          aria-pressed={value === o.key}
          className={cn(
            "rounded px-2.5 py-1 text-xs font-medium transition-colors",
            value === o.key ? "bg-accent/15 text-foreground" : "text-muted-foreground hover:text-foreground"
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

function Props() {
  const date = useSearchParams().get("date") ?? todayET();
  const [view, setView] = useState<View>("plays");
  const [stat, setStat] = useState<PropType | "all">("all");
  const [book, setBook] = useState("all");
  const [query, setQuery] = useState("");
  const { data, isLoading, error } = useProps(date);

  const all = useMemo(() => data?.props ?? [], [data]);
  const books = useMemo(() => [...new Set(all.map((p) => p.book))].sort(), [all]);
  const plays = all.filter(isPlay);
  const q = query.trim().toLowerCase();
  const shown = all.filter(
    (p) =>
      (view === "plays" ? isPlay(p) : view === "positive" ? p.edge > 0 && p.price <= MAX_PRICE : true) &&
      (stat === "all" || p.prop_type === stat) &&
      (book === "all" || p.book === book) &&
      (!q || `${p.player_name} ${p.team} ${p.away_abbr} ${p.home_abbr}`.toLowerCase().includes(q))
  );

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-xl font-semibold">Props</h1>
        <DateNav date={date} />
      </div>

      {isLoading && <Loading />}
      {error && <ErrorState error={error} />}
      {data && all.length === 0 && <Empty>No prop prices for {date} yet. Props are priced after the morning pregame run.</Empty>}

      {data && all.length > 0 && (
        <>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Stat label="Plays" value={plays.length} />
            <Stat label="Units at risk" value={`${plays.reduce((s, p) => s + playStake(p), 0).toFixed(2)}u`} />
            <Stat label="Players priced" value={new Set(all.map((p) => p.player_id)).size} />
            <Stat label="Last update (ET)" value={<span className="text-base">{dateTimeET(data.stamp ? stampToIso(data.stamp) : null)}</span>} />
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Toggle options={[...VIEWS]} value={view} onChange={setView} />
            <Toggle options={STATS.map((s) => ({ key: s, label: s === "all" ? "All Stats" : PROP_LABEL[s] }))} value={stat} onChange={setStat} />
            <select
              value={book}
              onChange={(e) => setBook(e.target.value)}
              className="rounded-md border border-border bg-card px-2 py-1 text-xs text-foreground"
              aria-label="Book"
            >
              <option value="all">All books</option>
              {books.map((b) => <option key={b} value={b}>{b}</option>)}
            </select>
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Player or team"
              className="w-44 rounded-md border border-border bg-card px-2 py-1 text-xs text-foreground placeholder:text-muted-foreground"
            />
            <span className="ml-auto text-xs text-muted-foreground">
              Edge uses a 50/50 blend of model and market; market = median of books quoting both sides; plays need ≥ 5% edge, ≥ 2 books quoting both sides and a confirmed lineup spot
              (goalies: ≥ 90% to start).
            </span>
          </div>

          {shown.length ? (
            <Card className="overflow-hidden">
              <PropTable rows={shown} />
            </Card>
          ) : (
            <Empty>Nothing matches these filters.</Empty>
          )}
        </>
      )}
    </div>
  );
}

export default function PropsPage() {
  return (
    <Suspense fallback={null}>
      <Props />
    </Suspense>
  );
}
