"use client";

import type { LineupChange, LineupChangeKind } from "@/lib/types";
import { dateTimeET, pct } from "@/lib/format";
import TeamLogo from "./TeamLogo";
import { Badge, Card, Signed } from "./ui";

const KIND: Record<LineupChangeKind, { label: string; tone: "muted" | "accent" | "pos" | "neg" | "warn" }> = {
  starter: { label: "Starter", tone: "warn" },
  goalie_status: { label: "Goalie Status", tone: "accent" },
  goalie_p: { label: "Goalie Odds", tone: "muted" },
  out: { label: "Out", tone: "neg" },
  in: { label: "In", tone: "pos" },
  gtd: { label: "Game-Time", tone: "warn" },
  line: { label: "Line", tone: "muted" },
  pp: { label: "PP", tone: "muted" },
  price: { label: "Model Inputs", tone: "muted" },
};

const signedPts = (v: number) => `${v > 0 ? "+" : v < 0 ? "−" : "±"}${Math.abs(100 * v).toFixed(1)}`;
const signedGoals = (v: number) => `${v > 0 ? "+" : v < 0 ? "−" : "±"}${Math.abs(v).toFixed(2)}`;

/** Every lineup / goalie change between pregame runs, newest first, one block per run with the
 *  price move it caused. A run with several changes shares one move between them. */
export default function LineupChanges({ changes, away, home }: { changes: LineupChange[]; away: string; home: string }) {
  if (!changes.length) {
    return <Card className="p-6 text-center text-sm text-muted-foreground">No lineup or goalie changes between pricing runs yet.</Card>;
  }
  const steps = [...new Set(changes.map((c) => c.stamp))].map((st) => changes.filter((c) => c.stamp === st));
  return (
    <Card className="divide-y divide-border">
      {steps.map((rows) => {
        const r = rows[0];
        return (
          <div key={r.stamp} className="px-4 py-3">
            <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 text-sm">
              <span className="font-medium">{dateTimeET(r.as_of)} ET</span>
              <span className="tabular text-xs text-muted-foreground">
                {away} {pct(1 - r.p_home_win_before)} → <span className="text-foreground">{pct(1 - r.p_home_win)}</span>{" "}
                (<Signed value={-r.dp_home_win}>{signedPts(-r.dp_home_win)}</Signed>)
                <span className="mx-2">·</span>
                Proj Goals {away} <Signed value={r.d_away_goals}>{signedGoals(r.d_away_goals)}</Signed>, {home}{" "}
                <Signed value={r.d_home_goals}>{signedGoals(r.d_home_goals)}</Signed>
                {r.step_changes > 1 && <span className="ml-2 italic">shared by {r.step_changes} changes</span>}
              </span>
            </div>
            <ul className="mt-2 space-y-1">
              {rows.map((c, i) => (
                <li key={i} className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-sm">
                  <Badge tone={KIND[c.kind].tone}>{KIND[c.kind].label}</Badge>
                  {c.team && <TeamLogo abbr={c.team} className="h-4 w-4" />}
                  {c.kind === "price" ? (
                    <span className="text-muted-foreground">Price moved with no lineup change (ratings, coaches, referees or other inputs)</span>
                  ) : c.kind === "starter" ? (
                    <span>{c.before} <span className="text-muted-foreground">→</span> <span className="font-medium">{c.after}</span></span>
                  ) : (
                    <>
                      <span className="font-medium">{c.player_name ?? "Unknown"}</span>
                      <span className="text-muted-foreground">{c.before} → {c.after}</span>
                    </>
                  )}
                </li>
              ))}
            </ul>
          </div>
        );
      })}
    </Card>
  );
}
