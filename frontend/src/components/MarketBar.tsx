import { american, pct, signedPct } from "@/lib/format";
import { textOn } from "@/lib/teams";
import { cn } from "@/lib/utils";

/** One side of a market bar. ``p`` sizes the segment (the model where there is one, else the market). */
export interface BarSide {
  key: string;
  /** "TOR +1.5", "Over 6.5", "OT" */
  label: string;
  p: number | null;
  pMarket?: number | null;
  price?: number | null;
  book?: string | null;
  edge?: number | null;
  /** Flagged by the edge pipeline: the side to bet. */
  play?: boolean;
  /** An edge this large is more likely a bad quote than a real price. */
  warn?: boolean;
  /** Units staked on this side (ledger), shown with the PLAY label. */
  stake?: number | null;
  color: string;
}

interface MarketBarProps {
  title: string;
  /** Left to right: away / over first. A three-way has the draw in the middle. */
  sides: BarSide[];
  /** Model-sized bar with market ticks ("Model"), or market only ("Market"). */
  basis?: "model" | "market";
  /** Prices at the close: nothing can be bet, so nothing is a play. */
  closed?: boolean;
  /** Edges are model-only (no blend, never staked): shown without color. */
  neutralEdges?: boolean;
}

function EdgeChip({ s, closed, align }: { s?: BarSide; closed?: boolean; align: "left" | "right" | "center" }) {
  if (!s || s.edge == null) return <span className="w-14" />;
  const tone = closed ? "muted" : s.warn ? "warn" : s.play ? "play" : s.edge > 0 ? "pos" : "muted";
  return (
    <span
      title={`${s.label}: ${signedPct(s.edge, 2)} edge${s.book ? ` at ${s.book}` : ""}`}
      className={cn(
        "w-14 shrink-0 rounded px-1 py-0.5 text-center text-[11px] font-semibold tabular",
        align === "left" ? "justify-self-start" : align === "right" ? "justify-self-end" : "justify-self-center",
        tone === "play" && "bg-emerald-600 text-white",
        tone === "warn" && "bg-red-600 text-white",
        tone === "pos" && "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400",
        tone === "muted" && "bg-muted text-muted-foreground"
      )}
    >
      {signedPct(s.edge, 1)}
    </span>
  );
}

function SideLabel({ s, align, closed }: { s: BarSide; align: "left" | "right" | "center"; closed?: boolean }) {
  const flag = !closed && (s.warn ? "WARN" : s.play ? "PLAY" : null);
  return (
    <div className={cn("min-w-0", align === "right" ? "text-right" : align === "center" ? "text-center" : "text-left")}>
      <div className={cn("flex h-4 items-center gap-1", align === "right" ? "justify-end" : align === "center" ? "justify-center" : "justify-start")}>
        {flag && (
          <span className={cn("rounded px-1.5 text-[10px] font-bold leading-4 tracking-wide text-white", flag === "WARN" ? "bg-red-600" : "bg-emerald-600")}>
            {flag}
            {s.stake ? ` · ${s.stake.toFixed(2)}u` : ""}
          </span>
        )}
      </div>
      <div className="truncate text-xs">
        <span className="font-semibold">{s.label}</span>
        {s.price != null && <span className="ml-1 tabular text-muted-foreground">{american(s.price)}</span>}
      </div>
    </div>
  );
}

/**
 * One market as a thick split bar: each side's probability inside its segment, the market's split
 * as ticks, side labels with the best price above (PLAY over the side to bet), and each side's edge
 * at its end of the bar.
 */
export default function MarketBar({ title, sides, basis = "model", closed, neutralEdges }: MarketBarProps) {
  const total = sides.reduce((a, s) => a + (s.p ?? 0), 0) || 1;
  const three = sides.length === 3;
  const [first, last] = [sides[0], sides[sides.length - 1]];
  // Market split as cumulative ticks (only where the bar is the model's).
  const ticks: number[] = [];
  if (basis === "model" && sides.every((s) => s.pMarket != null)) {
    let acc = 0;
    for (const s of sides.slice(0, -1)) ticks.push((acc += s.pMarket!));
  }

  return (
    <div>
      <div className={cn("grid items-end gap-2", three ? "grid-cols-3" : "grid-cols-2")}>
        <SideLabel s={first} align="left" closed={closed} />
        {three && <SideLabel s={sides[1]} align="center" closed={closed} />}
        <SideLabel s={last} align="right" closed={closed} />
      </div>
      <div className="mt-1 grid grid-cols-[3.5rem_1fr_3.5rem] items-center gap-2">
        <EdgeChip s={first} closed={closed || neutralEdges} align="left" />
        <div className="relative flex h-6 overflow-hidden rounded-md bg-muted" title={`${title} (${basis === "model" ? "model; ticks = market" : "market, no vig"})`}>
          {sides.map((s) => {
            const w = ((s.p ?? 0) / total) * 100;
            return (
              <div
                key={s.key}
                className="flex items-center justify-center overflow-hidden text-[11px] font-semibold tabular"
                style={{ width: `${w}%`, backgroundColor: s.color, color: textOn(s.color) }}
              >
                {w >= 12 && pct(s.p, 1)}
              </div>
            );
          })}
          {ticks.map((t, i) => (
            <div key={i} className="absolute top-0 h-full w-0.5 bg-foreground/80" style={{ left: `calc(${t * 100}% - 1px)` }} />
          ))}
        </div>
        <EdgeChip s={last} closed={closed || neutralEdges} align="right" />
      </div>
      <div className="mt-0.5 flex justify-between text-[10px] text-muted-foreground tabular">
        <span>{first.pMarket != null && basis === "model" ? `Mkt ${pct(first.pMarket, 1)}` : ""}</span>
        <span className="font-semibold uppercase tracking-wider">
          {title}
          {three && sides[1].edge != null && <> · OT Edge {signedPct(sides[1].edge, 1)}</>}
        </span>
        <span>{last.pMarket != null && basis === "model" ? `Mkt ${pct(last.pMarket, 1)}` : ""}</span>
      </div>
    </div>
  );
}
