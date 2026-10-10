import type { ReactNode } from "react";
import Link from "next/link";
import SortTable, { type Column } from "@/components/SortTable";
import { TeamTag } from "@/components/TeamLogo";
import { Badge, Card, Empty, Signed } from "@/components/ui";
import { american, dateTimeET, pct, signedPct, timeET, units } from "@/lib/format";
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
  /** What the Bet column sorts by. */
  betSort: string;
  book: string;
  price: number;
  stake: number;
  edge: number | null;
  nowPrice: number | null;
  nowBook: string | null;
  /** Graded: the same book's closing price. */
  closePrice: number | null;
  clv: number | null;
  clvNow: number | null;
  /** vs. the same book later: its close once graded, else its price now. */
  priceClv: number | null;
  bookNow: number | null;
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
const WINDOW_RANK: Record<BetWindow, number> = { open: 0, pre: 1, post: 2 };

/** A CLV value in green / red, or a dash. */
export function ClvCell({ v, title }: { v: number | null | undefined; title?: string }) {
  if (v == null) return <span className="text-muted-foreground">–</span>;
  return (
    <span title={title} className={cn("font-semibold", v > 0 ? "text-positive" : v < 0 ? "text-negative" : "text-muted-foreground")}>
      {signedPct(v, 1)}
    </span>
  );
}

/** A paper bet already placed: price taken, book and time (ET). */
export function PlacedBet({ price, book, stake, at }: { price: number | null | undefined; book: string | null | undefined;
  stake: number | null | undefined; at: string | null | undefined }) {
  if (price == null) return null;
  return (
    <span className="inline-flex items-center gap-1.5 whitespace-nowrap text-xs" title={`placed ${american(price)} at ${book}, ${stake?.toFixed(2)}u`}>
      <span className="font-medium">{american(price)}</span>
      <span className="text-muted-foreground">{book} · {timeET(at)}</span>
    </span>
  );
}

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

const TIMING_TITLE = "Open = the day's first edges run; Post = the last 30 minutes before puck drop";
const INFO_TITLE = "What the model knew at placement: A = starters confirmed and lineups projected; C = a starter or lineup from the model alone, or a stale input. Dots: away, home goalie.";
const GRADE_RANK = { A: 0, B: 1, C: 2 } as const;
const STATUS_RANK: Record<BetStatus, number> = { value: 0, faded: 1, gone: 2, closed: 3, graded: 4 };

/** Columns both views share, through Edge. */
const LEAD_COLS: Column<LedgerRow>[] = [
  { key: "placed", label: "Placed (ET)", sort: (r) => r.placedAt, className: "text-muted-foreground", render: (r) => dateTimeET(r.placedAt) },
  {
    key: "timing", label: "Timing", title: TIMING_TITLE, sort: (r) => r.leadMinutes,
    render: (r) => <TimingCell window={r.window} leadMinutes={r.leadMinutes} />,
  },
  {
    key: "game", label: "Game", sort: (r) => (r.away && r.home ? `${r.away}@${r.home}` : String(r.gameId)),
    render: (r) => (
      <Link href={r.gameHref} className="hover:text-accent">
        {r.away && r.home ? (
          <span className="inline-flex items-center gap-1.5"><TeamTag abbr={r.away} /> @ <TeamTag abbr={r.home} /></span>
        ) : r.gameId}
      </Link>
    ),
  },
  { key: "bet", label: "Bet", sort: (r) => r.betSort, render: (r) => r.bet },
  { key: "book", label: "Book", sort: (r) => r.book, className: "text-muted-foreground", render: (r) => r.book },
  { key: "price", label: "Price", align: "right", sort: (r) => r.price, className: "font-medium", render: (r) => american(r.price) },
  { key: "stake", label: "Stake", align: "right", sort: (r) => r.stake, render: (r) => `${r.stake.toFixed(2)}u` },
  { key: "edge", label: "Edge", align: "right", sort: (r) => r.edge, render: (r) => signedPct(r.edge) },
];

const INFO_COL: Column<LedgerRow> = {
  key: "info", label: "Info", title: INFO_TITLE, sort: (r) => (r.info.info_grade ? GRADE_RANK[r.info.info_grade] : null),
  render: (r) => <InfoCell info={r.info} />,
};

const COLS: Record<LedgerView, Column<LedgerRow>[]> = {
  pending: [
    ...LEAD_COLS,
    { key: "now", label: "Now", align: "right", sort: (r) => r.nowPrice, render: (r) => <PriceNow r={r} /> },
    {
      key: "clv", label: "Fair CLV", title: "vs. the devigged market consensus now", align: "right", sort: (r) => r.clvNow, className: "italic",
      render: (r) => <Signed value={r.clvNow}>{signedPct(r.clvNow, 2)}</Signed>,
    },
    {
      key: "price_clv", label: "Price CLV", title: "vs. the same book's price now (positive = it shortened since the bet)", align: "right",
      sort: (r) => r.priceClv, className: "italic",
      render: (r) => (
        <span title={r.bookNow != null ? `${r.book} now ${american(r.bookNow)}` : undefined}>
          <Signed value={r.priceClv}>{signedPct(r.priceClv, 2)}</Signed>
        </span>
      ),
    },
    INFO_COL,
    {
      key: "status", label: "Status", sort: (r) => STATUS_RANK[r.status],
      render: (r) => r.status !== "graded" && (
        <span title={STATUS[r.status].title}><Badge tone={STATUS[r.status].tone}>{STATUS[r.status].label}</Badge></span>
      ),
    },
  ],
  graded: [
    ...LEAD_COLS,
    {
      key: "close", label: "Close", title: "The same book's last price before puck drop", align: "right", sort: (r) => r.closePrice,
      className: "text-muted-foreground", render: (r) => american(r.closePrice),
    },
    {
      key: "clv", label: "Fair CLV", title: "vs. the devigged closing consensus", align: "right", sort: (r) => r.clv,
      render: (r) => <Signed value={r.clv}>{signedPct(r.clv, 2)}</Signed>,
    },
    {
      key: "price_clv", label: "Price CLV", title: "vs. the same book's closing price", align: "right", sort: (r) => r.priceClv,
      render: (r) => <Signed value={r.priceClv}>{signedPct(r.priceClv, 2)}</Signed>,
    },
    INFO_COL,
    {
      key: "result", label: "Result", sort: (r) => r.result, className: "capitalize",
      render: (r) => r.result && <Badge tone={RESULT_TONE[r.result] ?? "muted"}>{r.result}</Badge>,
    },
    { key: "pnl", label: "Units", align: "right", sort: (r) => r.pnl, render: (r) => <Signed value={r.pnl}>{units(r.pnl)}</Signed> },
  ],
};

/** The standard ledger: when, where and at what price each bet was taken, and how it's doing.
 *  Every column sorts; newest first by default. */
export function LedgerTable({ rows, view }: { rows: LedgerRow[]; view: LedgerView }) {
  if (!rows.length) return <Empty>{view === "pending" ? "No pending bets." : "No graded bets yet."}</Empty>;
  return (
    <Card className="overflow-hidden">
      <SortTable rows={rows} columns={COLS[view]} rowKey={(r) => r.id} initialSort={{ key: "placed", desc: true }} rank={false} />
    </Card>
  );
}

type Metrics = Omit<BetTotals, "bets"> & { bets: number };

/** A label column of a breakdown: header, cell and what it sorts by. */
export interface BreakdownLabel<T> {
  h: string;
  cell: (r: T) => ReactNode;
  sort: (r: T) => number | string | null | undefined;
}

/** A breakdown of graded bets: label columns, then count, staked, units, ROI, CLV, beat close.
 *  Every column sorts; rows start in the order given. */
export function BreakdownTable<T extends Metrics>({ rows, labels }: { rows: T[]; labels: BreakdownLabel<T>[] }) {
  const columns: Column<T>[] = [
    ...labels.map((l) => ({ key: `label-${l.h}`, label: l.h, render: l.cell, sort: l.sort })),
    { key: "bets", label: "Bets", align: "right", sort: (r) => r.bets, render: (r) => r.bets },
    { key: "staked", label: "Staked", align: "right", sort: (r) => r.staked, render: (r) => `${r.staked.toFixed(2)}u` },
    { key: "pnl", label: "Units", align: "right", sort: (r) => r.pnl, render: (r) => <Signed value={r.pnl}>{units(r.pnl)}</Signed> },
    { key: "roi", label: "ROI", align: "right", sort: (r) => r.roi, render: (r) => <Signed value={r.roi}>{signedPct(r.roi)}</Signed> },
    {
      key: "clv", label: "Fair CLV", align: "right", sort: (r) => r.mean_clv,
      render: (r) => <Signed value={r.mean_clv}>{signedPct(r.mean_clv, 2)}</Signed>,
    },
    {
      key: "price_clv", label: "Price CLV", align: "right", sort: (r) => r.mean_price_clv,
      render: (r) => <Signed value={r.mean_price_clv}>{signedPct(r.mean_price_clv, 2)}</Signed>,
    },
    { key: "beat", label: "Beat fair close", align: "right", sort: (r) => r.beat_close, render: (r) => pct(r.beat_close, 0) },
  ];
  return (
    <Card className="overflow-hidden">
      <SortTable rows={rows} columns={columns} rowKey={(r) => rows.indexOf(r)} rank={false} />
    </Card>
  );
}

/** Graded bets by when they were placed and how settled the game was. */
export function InfoBreakdown({ rows }: { rows: BetInfoBreakdown[] }) {
  return (
    <BreakdownTable
      rows={rows}
      labels={[
        {
          h: "Timing", sort: (r) => (r.window ? WINDOW_RANK[r.window] : null),
          cell: (r) => (r.window ? WINDOW_LABEL[r.window] : <span className="text-muted-foreground">–</span>),
        },
        {
          h: "Info", sort: (r) => (r.info_grade ? GRADE_RANK[r.info_grade] : null),
          cell: (r) => (r.info_grade ? <Badge tone={GRADE_TONE[r.info_grade]}>{r.info_grade}</Badge>
            : <span className="text-muted-foreground">No snapshot</span>),
        },
      ]}
    />
  );
}
