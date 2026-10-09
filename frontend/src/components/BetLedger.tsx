import type { ReactNode } from "react";
import Link from "next/link";
import { TeamTag } from "@/components/TeamLogo";
import { Badge, Card, Empty, Signed } from "@/components/ui";
import { american, dateTimeET, fairAmerican, pct, signedPct, units } from "@/lib/format";
import type { BetInfo, BetInfoBreakdown, BetStatus, BetTotals, BetWindow } from "@/lib/types";
import { cn } from "@/lib/utils";

/** One ledger bet, game market or prop, in the shape the shared table shows. */
export interface LedgerRow {
  id: string;
  placedAt: string;
  window: BetWindow | null;
  leadMinutes: number | null;
  gameId: number;
  gameHref: string;
  away: string | null;
  home: string | null;
  bet: ReactNode;
  book: string;
  price: number;
  stake: number;
  edge: number | null;
  nowPrice: number | null;
  nowBook: string | null;
  /** Graded: the close (a price, or the fair consensus when only that is known). */
  close: ReactNode;
  clv: number | null;
  clvNow: number | null;
  status: BetStatus;
  result: string | null;
  pnl: number | null;
  info: BetInfo;
}

export type LedgerView = "pending" | "graded";

const RESULT_TONE: Record<string, "pos" | "neg" | "muted"> = { win: "pos", loss: "neg", push: "muted", void: "muted" };
const STATUS: Record<Exclude<BetStatus, "graded">, { label: string; tone: "pos" | "warn" | "muted" | "accent"; title: string }> = {
  value: { label: "Still +EV", tone: "pos", title: "Still flagged at the best price now" },
  faded: { label: "Faded", tone: "warn", title: "Still quoted, but no longer a play at the best price now" },
  gone: { label: "Pulled", tone: "muted", title: "No book quotes this line now" },
  closed: { label: "Closed", tone: "accent", title: "Game started; graded once the game is final" },
};
const GRADE_TONE = { A: "pos", B: "warn", C: "neg" } as const;
export const WINDOW_LABEL: Record<BetWindow, string> = { open: "Open", pre: "Pre", post: "Post" };

/** Minutes before puck drop -> "45m" / "6.5h". */
export const lead = (m: number | null) => (m == null ? null : m < 60 ? `${Math.round(m)}m` : `${(m / 60).toFixed(1)}h`);

/** Open (the day's first run) / Post (last 30 minutes) / "4.2h pre". */
export function TimingCell({ window, leadMinutes }: { window: BetWindow | null; leadMinutes: number | null }) {
  const w = window ?? (leadMinutes != null && leadMinutes < 30 ? "post" : null);
  const ahead = lead(leadMinutes);
  if (w === "open" || w === "post")
    return (
      <span className="inline-flex items-center gap-1.5">
        <Badge tone={w === "open" ? "accent" : "warn"}>{WINDOW_LABEL[w]}</Badge>
        {ahead && <span className="text-xs text-muted-foreground">{ahead}</span>}
      </span>
    );
  return <span className="text-xs text-muted-foreground">{ahead ? `${ahead} pre` : "–"}</span>;
}

const goalieText = (side: string, s: BetInfo["home_goalie"], p: number | null) =>
  `${side} goalie ${s ?? "?"}${p != null ? ` (${pct(p, 0)} to start)` : ""}`;

/** What the model knew at placement, as one sentence per line for the tooltip. */
export function infoDetail(i: BetInfo): string {
  if (!i.info_grade) return "No snapshot (real bet, or placed before snapshots were recorded)";
  return [
    goalieText("Away", i.away_goalie, i.away_goalie_p),
    goalieText("Home", i.home_goalie, i.home_goalie_p),
    `Lineups projected: away ${pct(i.away_dfo_share, 0)}, home ${pct(i.home_dfo_share, 0)}`,
    `Lineup flags: ${i.lineup_flags ?? 0}`,
    i.stale_inputs ? `Stale: ${i.stale_inputs}` : null,
  ].filter(Boolean).join("\n");
}

const dot = (s: BetInfo["home_goalie"]) =>
  s === "Confirmed" ? "bg-emerald-500" : s === "Likely" ? "bg-yellow-400" : s === "Model" ? "bg-red-400" : "bg-muted-foreground/40";

/** Info grade, with each team's goalie status as dots (away, home); details on hover. */
export function InfoCell({ info }: { info: BetInfo }) {
  if (!info.info_grade) return <span className="text-muted-foreground" title={infoDetail(info)}>–</span>;
  return (
    <span className="inline-flex items-center gap-1.5" title={infoDetail(info)}>
      <Badge tone={GRADE_TONE[info.info_grade]}>{info.info_grade}</Badge>
      <span className={cn("h-1.5 w-1.5 rounded-full", dot(info.away_goalie))} />
      <span className={cn("h-1.5 w-1.5 rounded-full", dot(info.home_goalie))} />
      {(info.lineup_flags ?? 0) > 0 && <span className="text-[11px] text-amber-600 dark:text-amber-400">{info.lineup_flags}⚑</span>}
    </span>
  );
}

function PriceNow({ r }: { r: LedgerRow }) {
  if (r.nowPrice == null) return <span className="text-muted-foreground">–</span>;
  const tone = r.nowPrice < r.price ? "text-positive" : r.nowPrice > r.price ? "text-negative" : "";
  return (
    <span className={tone} title={`best now at ${r.nowBook}`}>
      {american(r.nowPrice)} <span className="text-xs text-muted-foreground">{r.nowBook}</span>
    </span>
  );
}

/** A close we only know as a devigged probability, shown as its fair American price. */
export const fairClose = (p: number | null) =>
  p == null ? <span className="text-muted-foreground">–</span> : <span title={`fair ${pct(p)}`}>{fairAmerican(p)}</span>;

const COLS: Record<LedgerView, { h: string; right?: boolean; title?: string }[]> = {
  pending: [
    { h: "Placed (ET)" }, { h: "Timing", title: "Open = the day's first edges run; Post = the last 30 minutes before puck drop" },
    { h: "Game" }, { h: "Bet" }, { h: "Book" }, { h: "Price", right: true }, { h: "Stake", right: true },
    { h: "Edge", right: true }, { h: "Now", right: true }, { h: "CLV", right: true, title: "Live: vs. the consensus now" },
    { h: "Info", title: "What the model knew at placement: A = starters confirmed and lineups projected; C = a starter or lineup from the model alone, or a stale input. Dots: away, home goalie." },
    { h: "Status" },
  ],
  graded: [
    { h: "Placed (ET)" }, { h: "Timing", title: "Open = the day's first edges run; Post = the last 30 minutes before puck drop" },
    { h: "Game" }, { h: "Bet" }, { h: "Book" }, { h: "Price", right: true }, { h: "Stake", right: true },
    { h: "Edge", right: true }, { h: "Close", right: true }, { h: "CLV", right: true, title: "vs. the devigged closing consensus" },
    { h: "Info", title: "What the model knew at placement: A = starters confirmed and lineups projected; C = a starter or lineup from the model alone, or a stale input. Dots: away, home goalie." },
    { h: "Result" }, { h: "Units", right: true },
  ],
};

/** The standard ledger: when, where and at what price each bet was taken, and how it's doing. */
export function LedgerTable({ rows, view }: { rows: LedgerRow[]; view: LedgerView }) {
  if (!rows.length) return <Empty>{view === "pending" ? "No pending bets." : "No graded bets yet."}</Empty>;
  const cols = COLS[view];
  return (
    <Card className="overflow-x-auto">
      <table className="w-full text-sm tabular">
        <thead className="text-left text-xs text-muted-foreground">
          <tr className="border-b border-border">
            {cols.map((c) => (
              <th key={c.h} title={c.title} className={cn("whitespace-nowrap px-3 py-2 font-medium", c.right && "text-right")}>{c.h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.id} className="border-b border-border last:border-0">
              <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{dateTimeET(r.placedAt)}</td>
              <td className="whitespace-nowrap px-3 py-2"><TimingCell window={r.window} leadMinutes={r.leadMinutes} /></td>
              <td className="whitespace-nowrap px-3 py-2">
                <Link href={r.gameHref} className="hover:text-accent">
                  {r.away && r.home ? (
                    <span className="inline-flex items-center gap-1.5"><TeamTag abbr={r.away} /> @ <TeamTag abbr={r.home} /></span>
                  ) : r.gameId}
                </Link>
              </td>
              <td className="whitespace-nowrap px-3 py-2">{r.bet}</td>
              <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{r.book}</td>
              <td className="px-3 py-2 text-right font-medium">{american(r.price)}</td>
              <td className="px-3 py-2 text-right">{r.stake.toFixed(2)}u</td>
              <td className="px-3 py-2 text-right">{signedPct(r.edge)}</td>
              {view === "pending" ? (
                <>
                  <td className="whitespace-nowrap px-3 py-2 text-right"><PriceNow r={r} /></td>
                  <td className="px-3 py-2 text-right italic"><Signed value={r.clvNow}>{signedPct(r.clvNow, 2)}</Signed></td>
                  <td className="whitespace-nowrap px-3 py-2"><InfoCell info={r.info} /></td>
                  <td className="whitespace-nowrap px-3 py-2">
                    {r.status !== "graded" && (
                      <span title={STATUS[r.status].title}><Badge tone={STATUS[r.status].tone}>{STATUS[r.status].label}</Badge></span>
                    )}
                  </td>
                </>
              ) : (
                <>
                  <td className="px-3 py-2 text-right text-muted-foreground">{r.close}</td>
                  <td className="px-3 py-2 text-right"><Signed value={r.clv}>{signedPct(r.clv, 2)}</Signed></td>
                  <td className="whitespace-nowrap px-3 py-2"><InfoCell info={r.info} /></td>
                  <td className="px-3 py-2 capitalize">{r.result && <Badge tone={RESULT_TONE[r.result] ?? "muted"}>{r.result}</Badge>}</td>
                  <td className="px-3 py-2 text-right"><Signed value={r.pnl}>{units(r.pnl)}</Signed></td>
                </>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}

type Metrics = Omit<BetTotals, "bets"> & { bets: number };

/** A breakdown of graded bets: label columns, then count, staked, units, ROI, CLV, beat close. */
export function BreakdownTable<T extends Metrics>({ rows, labels }: { rows: T[]; labels: { h: string; cell: (r: T) => ReactNode }[] }) {
  return (
    <Card className="overflow-x-auto">
      <table className="w-full text-sm tabular">
        <thead className="text-left text-xs text-muted-foreground">
          <tr className="border-b border-border">
            {[...labels.map((l) => l.h), "Bets", "Staked", "Units", "ROI", "CLV", "Beat close"].map((h, i) => (
              <th key={h} className={cn("px-3 py-2 font-medium", i >= labels.length && "text-right")}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-b border-border last:border-0">
              {labels.map((l) => <td key={l.h} className="px-3 py-2">{l.cell(r)}</td>)}
              <td className="px-3 py-2 text-right">{r.bets}</td>
              <td className="px-3 py-2 text-right">{r.staked.toFixed(2)}u</td>
              <td className="px-3 py-2 text-right"><Signed value={r.pnl}>{units(r.pnl)}</Signed></td>
              <td className="px-3 py-2 text-right"><Signed value={r.roi}>{signedPct(r.roi)}</Signed></td>
              <td className="px-3 py-2 text-right"><Signed value={r.mean_clv}>{signedPct(r.mean_clv, 2)}</Signed></td>
              <td className="px-3 py-2 text-right">{pct(r.beat_close, 0)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}

/** Graded bets by when they were placed and how settled the game was. */
export function InfoBreakdown({ rows }: { rows: BetInfoBreakdown[] }) {
  return (
    <BreakdownTable
      rows={rows}
      labels={[
        { h: "Timing", cell: (r) => (r.window ? WINDOW_LABEL[r.window] : <span className="text-muted-foreground">–</span>) },
        { h: "Info", cell: (r) => (r.info_grade ? <Badge tone={GRADE_TONE[r.info_grade]}>{r.info_grade}</Badge>
          : <span className="text-muted-foreground">No snapshot</span>) },
      ]}
    />
  );
}
