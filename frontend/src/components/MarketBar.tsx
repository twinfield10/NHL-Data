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
        "w-14 shrink-0 rounded px-1 py-0.5 text-center text-xs font-semibold tabular",
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

const sideFlag = (s: BarSide, closed?: boolean) => (!closed && (s.warn ? "WARN" : s.play ? "PLAY" : null)) || null;

/** PLAY (with the units to risk) or WARN under the side's edge chip. */
function FlagBadge({ s, closed, align }: { s: BarSide; closed?: boolean; align: "left" | "right" }) {
  const flag = sideFlag(s, closed);
  if (!flag) return <span />;
  return (
    <span className={cn("flex", align === "right" ? "justify-end" : "justify-start")}>
      <span className={cn("whitespace-nowrap rounded px-1.5 text-[11px] font-bold leading-4 tracking-wide text-white", flag === "WARN" ? "bg-red-600" : "bg-emerald-600")}>
        {flag}
        {flag === "PLAY" && s.stake ? ` · RISK ${s.stake.toFixed(2)}u` : ""}
      </span>
    </span>
  );
}

function SideLabel({ s, align }: { s: BarSide; align: "left" | "right" | "center" }) {
  return (
    <div className={cn("min-w-0", align === "right" ? "text-right" : align === "center" ? "text-center" : "text-left")}>
      <div className="truncate text-sm">
        <span className="font-semibold">{s.label}</span>
        {s.price != null && <span className="ml-1 tabular text-muted-foreground">{american(s.price)}</span>}
      </div>
    </div>
  );
}

/**
 * One market as a thick split bar: each side's probability inside its segment, the market's split
 * as ticks, side labels with the best price above, each side's edge at its end of the bar, and
 * PLAY (units to risk) or WARN under the side it applies to.
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

  // The bar itself carries the signal: outlined green when a side is the play, red when a
  // side's edge is large enough to be a bad quote.
  const tone = closed ? null : sides.some((s) => s.warn) ? "warn" : sides.some((s) => s.play) ? "play" : null;
  const draw = three ? sides[1] : null;
  // The row under the bar holds PLAY / WARN badges and the 3-way's OT line; bars without either skip it.
  const footer = draw != null || sideFlag(first, closed) != null || sideFlag(last, closed) != null;

  return (
    <div
      className={cn(
        "rounded-lg border px-2 pb-1.5 pt-1",
        tone === "warn" ? "border-red-600 bg-red-500/10" : tone === "play" ? "border-emerald-600 bg-emerald-500/10" : "border-transparent"
      )}
    >
      <div className="grid grid-cols-[1fr_auto_1fr] items-end gap-2">
        <SideLabel s={first} align="left" />
        <div className="text-center text-xs font-bold uppercase leading-5 tracking-wider text-foreground">{title}</div>
        <SideLabel s={last} align="right" />
      </div>
      <div className="mt-1 grid grid-cols-[3.5rem_1fr_3.5rem] items-center gap-2">
        <EdgeChip s={first} closed={closed || neutralEdges} align="left" />
        <div className="relative flex h-6 overflow-hidden rounded-md bg-muted" title={`${title} (${basis === "model" ? "model; ticks = market" : "market, no vig"})`}>
          {sides.map((s) => {
            const w = ((s.p ?? 0) / total) * 100;
            return (
              <div
                key={s.key}
                className="flex items-center justify-center overflow-hidden text-xs font-semibold tabular"
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
      {footer && <div className="mt-1 grid grid-cols-[1fr_auto_1fr] items-center gap-2 text-[11px] text-muted-foreground tabular">
        <FlagBadge s={first} closed={closed} align="left" />
        <span className="text-center text-xs font-semibold text-foreground">
          {draw && (
            <>
              OT{draw.price != null && ` ${american(draw.price)}`}
              {draw.edge != null && <span className="font-normal text-muted-foreground"> ({signedPct(draw.edge, 1)} Edge)</span>}
            </>
          )}
        </span>
        <FlagBadge s={last} closed={closed} align="right" />
      </div>}
    </div>
  );
}
