"use client";

import { useState } from "react";
import { ErrorState, Loading, Pills } from "@/components/ui";
import { useTeamMatchups } from "@/lib/api";
import { pct, seasonLabel } from "@/lib/format";
import type { MatchupCell } from "@/lib/types";

const F_TIERS = ["F1", "F2", "F3", "F4"];
const VENUES = [
  { key: "all", label: "All" },
  { key: "home", label: "Home" },
  { key: "away", label: "Away" },
] as const;

/** Cell fill for a matching ratio: accent for "faced more than ice time explains", neutral gray
 *  for "less" (matching is neither good nor bad, so not green/red). Saturates at ±60%. */
function fill(ratio: number) {
  const d = Math.max(-1, Math.min(1, (ratio - 1) / 0.6));
  const mix = Math.round(Math.abs(d) * 45);
  if (!mix) return undefined;
  return { background: `color-mix(in srgb, var(${d > 0 ? "--accent" : "--muted-foreground"}) ${mix}%, transparent)` };
}

function Grid({ cells, own, title }: { cells: MatchupCell[]; own: string[]; title: string }) {
  const at = (o: string, t: string) => cells.find((c) => c.own_tier === o && c.opp_tier === t);
  return (
    <div>
      <div className="mb-1.5 text-xs text-muted-foreground">{title}</div>
      <table className="tabular text-sm">
        <thead>
          <tr className="text-xs text-muted-foreground">
            <th className="pr-2 text-left font-medium" />
            {F_TIERS.map((t) => <th key={t} className="w-14 px-1 pb-1 text-center font-medium">vs {t}</th>)}
          </tr>
        </thead>
        <tbody>
          {own.map((o) => (
            <tr key={o}>
              <td className="pr-2 text-xs text-muted-foreground">{o}</td>
              {F_TIERS.map((t) => {
                const c = at(o, t);
                return (
                  <td key={t} className="p-0.5">
                    <div
                      className="rounded px-1 py-1.5 text-center"
                      style={c ? fill(c.ratio) : undefined}
                      title={c ? `Own ${o} vs opponent ${t}: ${pct(c.share, 0)} of his time against forwards, ${c.ratio.toFixed(2)}× what ice time alone gives (${Math.round(c.seconds / 60)} min)` : "No time"}
                    >
                      {c ? `${c.ratio.toFixed(2)}×` : "–"}
                    </div>
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Line matchups for a team: own tier vs opponent forward tier, the coach matching index and the
 *  top pair's assignment at home (last change) vs away. */
export default function MatchupGrid({ teamId }: { teamId: number }) {
  const [season, setSeason] = useState<number | null>(null);
  const [venue, setVenue] = useState<(typeof VENUES)[number]["key"]>("all");
  const { data, isLoading, error } = useTeamMatchups(teamId, season);
  if (isLoading) return <Loading />;
  if (error) return <ErrorState error={error} />;
  if (!data) return null;
  const cells = data.cells.filter((c) => c.venue === venue);
  const ix = data.index;
  const seasons = data.seasons.map((s) => ({ key: String(s), label: seasonLabel(s) }));

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Line matchups · 5v5 · {data.coaches.join(", ") || "–"}
        </div>
        <div className="flex gap-3">
          <Pills options={VENUES} value={venue} onChange={setVenue} />
          <Pills options={seasons} value={String(data.season)} onChange={(s) => setSeason(Number(s))} />
        </div>
      </div>
      {cells.length ? (
        <div className="flex flex-wrap gap-8">
          <Grid cells={cells} own={F_TIERS} title="Forward tiers vs opponent forward tiers" />
          <Grid cells={cells} own={["D1", "D2", "D3"]} title="D pairs vs opponent forward tiers" />
          {ix && (
            <div className="min-w-56 space-y-2 text-sm">
              <div title="How strongly own forward tiers line up with opponent tiers (mutual information), as a percentile of teams this season">
                <div className="text-xs text-muted-foreground">Coach matching index</div>
                <div className="font-semibold">{Math.round(ix.pct * 100)}th <span className="text-xs font-normal text-muted-foreground">of {ix.teams} teams</span></div>
              </div>
              <div title="Top line's time against the opponent's top line, relative to no matching">
                <div className="text-xs text-muted-foreground">F1 vs opponent F1</div>
                <div className="tabular">{ix.f1_vs_f1 == null ? "–" : `${ix.f1_vs_f1.toFixed(2)}×`}</div>
              </div>
              <div title="Top pair vs the opponent's top line. Home teams have the last change, so a higher home number means the coach uses it to get that matchup">
                <div className="text-xs text-muted-foreground">D1 vs opponent F1, home / away</div>
                <div className="tabular">
                  {ix.d1_f1_home == null ? "–" : `${ix.d1_f1_home.toFixed(2)}×`} / {ix.d1_f1_away == null ? "–" : `${ix.d1_f1_away.toFixed(2)}×`}
                </div>
              </div>
            </div>
          )}
        </div>
      ) : (
        <div className="text-xs text-muted-foreground">No 5v5 matchups for this season yet.</div>
      )}
      <div className="text-[11px] text-muted-foreground">
        Each cell is the share of a tier&apos;s 5v5 time against an opponent tier, divided by that tier&apos;s overall share:
        1.00× is no matching, blue is faced more often, gray less. Tiers are ice-time ranks each game. League-wide, top
        lines face top lines about 1.18× and fourth lines fourth lines about 1.58×, partly because both benches roll lines in order.
      </div>
    </div>
  );
}
