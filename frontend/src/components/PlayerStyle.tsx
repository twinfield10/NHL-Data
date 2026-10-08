"use client";

import { useState } from "react";
import ArchetypeBadge from "@/components/ArchetypeBadge";
import { ErrorState, Loading, Pills } from "@/components/ui";
import { usePlayerStyle } from "@/lib/api";
import { archetypeInfo } from "@/lib/archetypes";
import { seasonLabel } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { StyleAxis, StyleView } from "@/lib/types";

const ordinal = (n: number) => {
  const r = Math.round(n);
  const s = r % 100 >= 11 && r % 100 <= 13 ? "th" : ["th", "st", "nd", "rd"][r % 10] ?? "th";
  return `${r}${s}`;
};

/** One axis as a bar diverging from the median (50th percentile). */
function AxisBar({ axis, group }: { axis: StyleAxis; group: "F" | "D" }) {
  const p = axis.pct;
  const left = p == null ? 50 : Math.min(p, 50);
  const width = p == null ? 0 : Math.abs(p - 50);
  return (
    <div title={`${axis.name}: ${axis.label}. Percentile among ${group === "D" ? "defensemen" : "forwards"} with 500+ 5v5 minutes.`}>
      <div className="flex justify-between text-xs">
        <span className="text-muted-foreground">{axis.name}</span>
        <span className="tabular font-medium">{p == null ? "–" : ordinal(p)}</span>
      </div>
      <div className="relative mt-1 h-1.5 rounded-full bg-muted">
        <div className="absolute top-[-2px] h-[10px] w-px bg-border" style={{ left: "50%" }} />
        {p != null && (
          <div className={cn("absolute h-1.5 rounded-full", p >= 50 ? "bg-accent" : "bg-accent/50")}
            style={{ left: `${left}%`, width: `${Math.max(width, 1)}%` }} />
        )}
      </div>
    </div>
  );
}

function View({ view }: { view: StyleView }) {
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)_minmax(0,1fr)]">
      <div className="space-y-2.5">
        <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Style Axes</div>
        {view.axes.map((a) => <AxisBar key={a.key} axis={a} group={view.group} />)}
        {!view.reliable && (
          <div className="text-[11px] text-amber-600 dark:text-amber-400">
            Only {Math.round(view.toi_5v5_min)} 5v5 minutes behind this view: features are shrunk toward the
            position average, so expect it to move.
          </div>
        )}
      </div>
      <div className="space-y-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Archetype</div>
        {view.group === "F" && view.archetype ? (
          <>
            <ArchetypeBadge name={view.archetype} conf={view.confidence} full />
            <div className="text-xs text-muted-foreground">{archetypeInfo(view.archetype)?.blurb}</div>
            <ul className="space-y-1 pt-1 text-xs">
              {view.probs.map((x) => (
                <li key={x.name} className="grid grid-cols-[7.5rem_1fr_2.5rem] items-center gap-2">
                  <span className="truncate text-muted-foreground">{x.name}</span>
                  <div className="h-1.5 rounded-full bg-muted">
                    <div className="h-1.5 rounded-full bg-accent" style={{ width: `${Math.max(1, x.p * 100)}%` }} />
                  </div>
                  <span className="tabular text-right">{Math.round(x.p * 100)}%</span>
                </li>
              ))}
            </ul>
            <div className="text-[11px] text-muted-foreground">A summary of the axes; most forwards are blends.</div>
          </>
        ) : (
          <div className="text-xs text-muted-foreground">
            No archetype for defensemen: their styles form a continuum with no stable groups, so the axes are the description.
          </div>
        )}
      </div>
      <div>
        <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Plays Like</div>
        {view.comps.length ? (
          <ul className="space-y-1 text-sm">
            {view.comps.map((c) => (
              <li key={c.player_id} className="grid grid-cols-[1fr_auto] gap-3" title={`Style distance ${c.distance.toFixed(2)} (lower = closer)`}>
                <span className="truncate">{c.player_name}</span>
                <span className="tabular text-muted-foreground">{seasonLabel(c.season)}</span>
              </li>
            ))}
          </ul>
        ) : (
          <div className="text-xs text-muted-foreground">No comps.</div>
        )}
        <div className="mt-2 text-[11px] text-muted-foreground">
          Closest seasons by style alone (500+ 5v5 minutes, since 2010-11), not by quality.
        </div>
      </div>
    </div>
  );
}

/** The style panel under a skater row: axes, forward archetype, comps and archetype history. */
export default function PlayerStyle({ playerId }: { playerId: number }) {
  const { data, isLoading, error } = usePlayerStyle(playerId);
  const [pick, setPick] = useState<string | null>(null);
  if (isLoading) return <Loading />;
  if (error) return <ErrorState error={error} />;
  if (!data || !data.views.length) return <div className="text-sm text-muted-foreground">No style data this season or last.</div>;

  const key = (v: StyleView) => `${v.season}-${v.window}`;
  const view = data.views.find((v) => key(v) === pick) ?? data.views[0];
  const history = data.history.filter((h) => h.archetype);
  return (
    <div className="space-y-4">
      {data.views.length > 1 && (
        <Pills options={data.views.map((v) => ({ key: key(v), label: v.label }))} value={key(view)} onChange={setPick} />
      )}
      <View view={view} />
      {history.length > 1 && (
        <div>
          <div className="mb-1 text-xs text-muted-foreground">Archetype by Season</div>
          <div className="flex flex-wrap gap-1.5">
            {history.map((h) => (
              <span key={h.season} className={cn("inline-flex items-center gap-1 text-[11px]", h.toi_5v5_min < 500 && "opacity-50")}
                title={`${Math.round(h.toi_5v5_min)} 5v5 minutes`}>
                <span className="tabular text-muted-foreground">{seasonLabel(h.season)}</span>
                <ArchetypeBadge name={h.archetype} conf={h.confidence} />
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
