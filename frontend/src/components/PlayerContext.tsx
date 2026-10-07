"use client";

import { useState } from "react";
import Breakdown from "@/components/Breakdown";
import { ErrorState, Loading, Pills, Signed } from "@/components/ui";
import { usePlayerContext } from "@/lib/api";
import { pct, seasonLabel, signed } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { PlayerContextRow } from "@/lib/types";

const F_TIERS = ["F1", "F2", "F3", "F4"];
const D_TIERS = ["D1", "D2", "D3"];

function Percentile({ label, value, title }: { label: string; value: number | null; title: string }) {
  return (
    <div title={title}>
      <div className="flex justify-between text-xs">
        <span className="text-muted-foreground">{label}</span>
        <span className="tabular font-medium">{value == null ? "–" : `${Math.round(value * 100)}th`}</span>
      </div>
      <div className="mt-1 h-1.5 rounded-full bg-muted">
        {value != null && <div className="h-1.5 rounded-full bg-accent" style={{ width: `${Math.max(2, value * 100)}%` }} />}
      </div>
    </div>
  );
}

function Deployment({ row }: { row: PlayerContextRow }) {
  const u = row.usage;
  const tiers = row.group === "D" ? D_TIERS : F_TIERS;
  const counts = tiers.map((t) => Number(u[`games_${t}`] ?? 0));
  const n = counts.reduce((a, b) => a + b, 0);
  const fmt = (m: number | null | undefined) => (m == null ? "–" : m.toFixed(1));
  return (
    <div className="space-y-3">
      <div>
        <div className="mb-1 text-xs text-muted-foreground" title="Games in each ice-time tier (rank in the team's 5v5 TOI that game; linemates share a tier)">
          Tier by game
        </div>
        <div className="flex h-5 gap-0.5 overflow-hidden rounded">
          {tiers.map((t, i) =>
            counts[i] ? (
              <div
                key={t}
                title={`${t}: ${counts[i]} of ${n} games`}
                className={cn("flex items-center justify-center text-[10px] font-medium text-white", ["bg-accent", "bg-accent/75", "bg-accent/50", "bg-accent/30"][i])}
                style={{ width: `${(counts[i] / n) * 100}%` }}
              >
                {counts[i] / n >= 0.12 ? t : ""}
              </div>
            ) : null
          )}
        </div>
        <div className="tabular mt-1 text-[11px] text-muted-foreground">
          {tiers.map((t, i) => `${t} ${counts[i]}`).join(" · ")}
        </div>
      </div>
      <div className="tabular grid grid-cols-3 gap-2 text-sm">
        <div><div className="text-xs text-muted-foreground">5v5 / GP</div>{fmt(u.toi_5v5_pg)}</div>
        <div><div className="text-xs text-muted-foreground">PP / GP</div>{fmt(u.toi_pp_pg)}</div>
        <div><div className="text-xs text-muted-foreground">PK / GP</div>{fmt(u.toi_pk_pg)}</div>
        <div title="Games on the first / second power-play unit"><div className="text-xs text-muted-foreground">PP1 / PP2</div>{u.games_pp1 ?? 0} / {u.games_pp2 ?? 0}</div>
        <div title="Games on the first / second penalty-kill unit"><div className="text-xs text-muted-foreground">PK1 / PK2</div>{u.games_pk1 ?? 0} / {u.games_pk2 ?? 0}</div>
        <div title="Offensive-zone share of his 5v5 offensive + defensive zone faceoff starts"><div className="text-xs text-muted-foreground">OZ starts</div>{pct(u.oz_start_share, 0)}</div>
      </div>
    </div>
  );
}

function Season({ row }: { row: PlayerContextRow }) {
  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)_minmax(0,1fr)]">
      <Breakdown parts={row.parts} toiS={row.toi_s} />
      <div className="space-y-4">
        <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Deployment</div>
        <Deployment row={row} />
        <div className="space-y-2 pt-1">
          <Percentile label="Quality of competition" value={row.qoc_net_pct}
            title={`Opponents' mean net rating ${signed(row.qoc_net, 3)}; percentile among ${row.group === "D" ? "defensemen" : "forwards"} with 200+ 5v5 minutes (higher = tougher)`} />
          <Percentile label="Quality of teammates" value={row.qot_net_pct}
            title={`Teammates' mean net rating ${signed(row.qot_net, 3)}; percentile among ${row.group === "D" ? "defensemen" : "forwards"} with 200+ 5v5 minutes (higher = better help)`} />
        </div>
      </div>
      <div>
        <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Most common linemates</div>
        {row.linemates.length ? (
          <ul className="space-y-1 text-sm">
            {row.linemates.map((m) => (
              <li key={m.player_id} className="grid grid-cols-[1fr_auto_auto] gap-3" title={`${m.games} games together`}>
                <span className="truncate">{m.player_name}</span>
                <span className="tabular text-muted-foreground">{pct(m.share, 0)}</span>
                <span className="tabular w-12 text-right"><Signed value={m.ev_net}>{signed(m.ev_net)}</Signed></span>
              </li>
            ))}
          </ul>
        ) : (
          <div className="text-xs text-muted-foreground">No 5v5 time yet.</div>
        )}
        <div className="mt-2 text-[11px] text-muted-foreground">Share of his 5v5 time together · teammate&apos;s 5v5 net rating.</div>
      </div>
    </div>
  );
}

/** The expanded panel under a skater row: on-ice decomposition, deployment and linemates. */
export default function PlayerContext({ playerId }: { playerId: number }) {
  const { data, isLoading, error } = usePlayerContext(playerId);
  const [pick, setPick] = useState<string | null>(null);
  if (isLoading) return <Loading />;
  if (error) return <ErrorState error={error} />;
  if (!data || !data.rows.length) return <div className="text-sm text-muted-foreground">No 5v5 on-ice data this season or last.</div>;

  const key = (r: PlayerContextRow) => `${r.season}-${r.team_id}`;
  // Default to this season once he has ~100 5v5 minutes, else last season.
  const firstGood = data.rows.find((r) => r.season === data.season && r.toi_s >= 6000) ?? data.rows.find((r) => r.season !== data.season) ?? data.rows[0];
  const current = data.rows.find((r) => key(r) === pick) ?? firstGood;
  const options = data.rows.map((r) => ({ key: key(r), label: `${seasonLabel(r.season)}${r.team_abbr ? ` ${r.team_abbr}` : ""} · ${r.games} GP` }));

  return (
    <div className="space-y-4">
      <Pills options={options} value={key(current)} onChange={setPick} />
      <Season row={current} />
    </div>
  );
}
