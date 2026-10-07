import type { LineupPlayer } from "@/lib/types";
import { pct } from "@/lib/format";
import { cn } from "@/lib/utils";
import { team } from "@/lib/teams";
import TeamLogo from "./TeamLogo";
import { Badge, Card } from "./ui";

const SLOT_LABEL: Record<string, string> = {
  f1: "L1", f2: "L2", f3: "L3", f4: "L4", d1: "D1", d2: "D2", d3: "D3",
};

/** Lines and pairs as rows of names; 5v5 share of ice and special-teams units under each. */
export default function Lineup({ abbr, players }: { abbr: string; players: LineupPlayer[] }) {
  const groups = new Map<string, LineupPlayer[]>();
  for (const p of players) {
    const key = p.slot && SLOT_LABEL[p.slot] ? p.slot : "other";
    groups.set(key, [...(groups.get(key) ?? []), p]);
  }
  const source = players[0];

  return (
    <Card className="p-4">
      <div className="mb-3 flex items-center justify-between">
        <span className="flex items-center gap-2 text-sm font-semibold">
          <TeamLogo abbr={abbr} className="h-6 w-6" /> {team(abbr).name || abbr}
        </span>
        {source && (
          <span className="flex gap-1">
            <Badge>{source.source}</Badge>
            <Badge tone={source.confidence === "high" ? "pos" : "warn"}>{source.confidence}</Badge>
          </span>
        )}
      </div>
      {players.length === 0 ? (
        <div className="text-xs text-muted-foreground">No projection.</div>
      ) : (
        <div className="space-y-1.5">
          {[...groups.entries()].map(([slot, ps]) => (
            <div key={slot} className={cn("grid grid-cols-[2.25rem_1fr] items-start gap-2", slot === "d1" && "mt-3")}>
              <span className="pt-0.5 text-xs text-muted-foreground">{SLOT_LABEL[slot] ?? "Other"}</span>
              <div className={cn("grid gap-2", ps.length === 2 ? "grid-cols-2" : "grid-cols-3")}>
                {ps.map((p) => (
                  <div key={p.player_id} className="min-w-0" title={p.issues || undefined}>
                    <div className={cn("truncate text-sm", p.p_dressed < 1 && "text-amber-600 dark:text-amber-400")}>
                      {p.player_name}
                    </div>
                    <div className="tabular text-[11px] text-muted-foreground">
                      {pct(p.s5, 0)} 5v5
                      {p.pp_unit ? ` · PP${p.pp_unit}` : ""}
                      {p.pk_unit ? ` · PK${p.pk_unit}` : ""}
                      {p.p_dressed < 1 ? ` · ${pct(p.p_dressed, 0)} dress` : ""}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
