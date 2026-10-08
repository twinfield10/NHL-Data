"use client";

import Link from "next/link";
import type { Bet, Edge, MarketLine, SlateGame, TeamInfo, ThreeWay, UnpricedGame } from "@/lib/types";
import { pct, timeET } from "@/lib/format";
import { DRAW_COLOR, matchupColors, OVER_COLOR, UNDER_COLOR } from "@/lib/teams";
import { useDark } from "@/lib/useDark";
import { cn } from "@/lib/utils";
import GameBanner, { type Tone } from "./GameBanner";
import MarketBar, { type BarSide } from "./MarketBar";

/** An edge this large is more likely a bad quote or a stale input than a real price. */
export const WARN_EDGE = 0.09;

/** "+1.5" / "-1.5" */
const handicap = (v: number) => `${v > 0 ? "+" : ""}${v.toFixed(1)}`;

/** Biggest edge still bettable (−∞ without one, e.g. after the close); used for sorting the page. */
export const maxEdge = (edges: Edge[]) => Math.max(...edges.filter((e) => e.point !== "close").map((e) => e.edge), -Infinity);

/** Anything with a side label and a price; edges add the model's view. */
type Priced = Pick<Edge, "selection" | "price"> & Partial<Pick<Edge, "edge" | "flagged" | "point">>;

function edgeTone(e: Priced): Tone {
  if (e.edge == null || e.point === "close") return null;
  if (e.edge >= WARN_EDGE) return "warn";
  return e.flagged ? "play" : null;
}

/** The strongest signal among a set of edges. */
function worst(edges: Edge[]): Tone {
  const tones = edges.map(edgeTone);
  return tones.includes("warn") ? "warn" : tones.includes("play") ? "play" : null;
}

interface SideSpec {
  key: string;
  label: string;
  color: string;
  /** Fallback probability when the side has no edge row (e.g. the complement of the other side). */
  p?: number | null;
  pMarket?: number | null;
}

/** A bar side from the side's best edge row and any bets on it. */
function fromEdge(spec: SideSpec, e: Edge | undefined, bets: Bet[], closing: boolean): BarSide {
  const stake = e ? bets.filter((b) => b.market === e.market && b.side === e.side && (b.line ?? null) === (e.line ?? null))
    .reduce((a, b) => a + b.stake_units, 0) : 0;
  return {
    key: spec.key, label: spec.label, color: spec.color,
    p: spec.p ?? e?.p_model_side ?? null,
    pMarket: spec.pMarket ?? e?.p_market_side ?? null,
    price: e?.price, book: e?.book, edge: e?.edge,
    play: !closing && !!e?.flagged, warn: !closing && e != null && e.edge >= WARN_EDGE,
    stake: stake || e?.stake_units || null,
  };
}

/** Fill a missing side's probabilities with the complement of the other side. */
function complete(a: BarSide, b: BarSide): [BarSide, BarSide] {
  const fill = (x: BarSide, y: BarSide) => ({ ...x, p: x.p ?? (y.p != null ? 1 - y.p : null), pMarket: x.pMarket ?? (y.pMarket != null ? 1 - y.pMarket : null) });
  return [fill(a, b), fill(b, a)];
}

/** The regulation three-way as bar sides (away, draw, home). */
function threeWaySides(tw: ThreeWay, away: string, home: string, colors: { away: string; home: string }, basis: "model" | "market"): BarSide[] {
  const spec = { away: { label: away, color: colors.away }, draw: { label: "OT", color: DRAW_COLOR }, home: { label: home, color: colors.home } };
  return (["away", "draw", "home"] as const).map((k) => {
    const s = tw.sides.find((x) => x.side === k)!;
    return { key: k, ...spec[k], p: basis === "model" ? s.p_model : s.p_market, pMarket: s.p_market, price: s.price, book: s.book,
      edge: basis === "model" ? s.edge : null };
  });
}

/** Middle line of the banner: final score, live, or start time. */
export function gameStatus(g: { is_final: boolean | null; home_score: number | null; away_score: number | null;
  last_period: number | null }, start: string | null) {
  if (g.is_final && g.home_score != null) {
    const extra = g.last_period && g.last_period > 3 ? (g.last_period === 5 ? "/SO" : "/OT") : "";
    return <span className="tabular">{g.away_score}–{g.home_score} <span className="text-xs font-normal text-muted-foreground">Final{extra}</span></span>;
  }
  if (start && new Date(start) < new Date()) return <span className="text-amber-600 dark:text-amber-400">Live</span>;
  return start ? `${timeET(start)} ET` : "TBD";
}

/** One game: teams and starters, the model's win split in team colors, best prices and edges, bets. */
export default function GameCard({ g, edges, bets, teams, threeWay }: {
  g: SlateGame; edges: Edge[]; bets: Bet[]; teams: Record<string, TeamInfo>; threeWay?: ThreeWay;
}) {
  const dark = useDark();
  const colors = matchupColors(g.away_team_abbr, g.home_team_abbr, dark);

  const find = (market: Edge["market"], side: number, line?: number | null) =>
    edges.find((e) => e.market === market && e.side === side && (line === undefined || e.line === line));
  // Closing prices can't be bet any more, so they never light up a team or a bar.
  const closing = edges.some((e) => e.point === "close");
  const signal = closing ? [] : edges;
  const teamEdges = (side: number) => signal.filter((e) => e.market !== "total" && e.side === side);
  const awayTone = worst(teamEdges(2));
  const homeTone = worst(teamEdges(1));

  // Main puck line and the total at the market's consensus line (else the first one captured).
  const plLine = edges.find((e) => e.market === "puckline")?.line;
  const totalLine = edges.some((e) => e.market === "total" && e.line === g.mkt_total_line)
    ? g.mkt_total_line
    : edges.find((e) => e.market === "total")?.line;

  const [away, home] = [g.away_team_abbr, g.home_team_abbr];
  const ml = complete(
    fromEdge({ key: "away", label: away, color: colors.away, p: 1 - g.p_home_win, pMarket: g.mkt_p_home_win != null ? 1 - g.mkt_p_home_win : null }, find("moneyline", 2), bets, closing),
    fromEdge({ key: "home", label: home, color: colors.home, p: g.p_home_win, pMarket: g.mkt_p_home_win }, find("moneyline", 1), bets, closing),
  );
  const pl = plLine != null ? complete(
    fromEdge({ key: "away", label: `${away} ${handicap(-plLine)}`, color: colors.away }, find("puckline", 2, plLine), bets, closing),
    fromEdge({ key: "home", label: `${home} ${handicap(plLine)}`, color: colors.home }, find("puckline", 1, plLine), bets, closing),
  ) : null;
  const ou = totalLine != null ? complete(
    fromEdge({ key: "over", label: `Over ${totalLine}`, color: OVER_COLOR }, find("total", 1, totalLine), bets, closing),
    fromEdge({ key: "under", label: `Under ${totalLine}`, color: UNDER_COLOR }, find("total", 2, totalLine), bets, closing),
  ) : null;
  const tw = threeWay && threeWay.sides.some((s) => s.p_model != null) ? threeWaySides(threeWay, away, home, colors, "model") : null;
  const issues = [g.away_lineup_issues, g.home_lineup_issues].filter(Boolean).join("; ");

  return (
    <Link
      href={`/games/${g.game_id}`}
      className={cn(
        "block overflow-hidden rounded-lg border border-border bg-card transition-colors hover:border-muted-foreground"
      )}
    >
      <GameBanner
        away={{ abbr: g.away_team_abbr, info: teams[g.away_team_abbr], color: colors.away, price: find("moneyline", 2)?.price,
          tone: awayTone, goalie: { name: g.away_starter, p: g.away_starter_p, status: g.away_starter_dfo } }}
        home={{ abbr: g.home_team_abbr, info: teams[g.home_team_abbr], color: colors.home, price: find("moneyline", 1)?.price,
          tone: homeTone, goalie: { name: g.home_starter, p: g.home_starter_p, status: g.home_starter_dfo } }}
        status={gameStatus(g, g.start_time)}
        venue={g.venue_name}
        location={g.venue_location}
        chips={[
          { label: "Proj", value: `${g.mean_away_goals.toFixed(1)}–${g.mean_home_goals.toFixed(1)}` },
          { label: "Total", value: g.mkt_total_line ?? "–" },
          { label: "OT", value: pct(g.p_overtime, 0) },
        ]}
        className="border-b border-border"
      />

      <div className="space-y-1.5 px-3 pb-3 pt-2">
        {closing && <div className="-mb-2 text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">Closing Prices</div>}
        <MarketBar title="Moneyline" sides={ml} closed={closing} />
        {tw && <MarketBar title="3-Way (Regulation)" sides={tw} closed={closing} neutralEdges />}
        {pl && <MarketBar title="Puck Line" sides={pl} closed={closing} />}
        {ou && <MarketBar title="Total" sides={ou} closed={closing} />}
        {edges.length === 0 && <div className="text-xs italic text-muted-foreground">No lines captured</div>}
        {(issues || g.stale_inputs) && (
          <div className="flex justify-end gap-2 border-t border-border pt-2 text-[11px]">
            {issues && <span title={issues} className="text-amber-600 dark:text-amber-400">Lineup Flag</span>}
            {g.stale_inputs && <span title={g.stale_inputs} className="text-red-600 dark:text-red-400">Stale Inputs</span>}
          </div>
        )}
      </div>
    </Link>
  );
}

/** A game with no pregame run yet: the same banner, and the market's best prices without the model. */
export function UnpricedGameCard({ g, teams, lines, threeWay }: {
  g: UnpricedGame; teams: Record<string, TeamInfo>; lines: MarketLine[]; threeWay?: ThreeWay;
}) {
  const dark = useDark();
  const colors = matchupColors(g.away_abbr, g.home_abbr, dark);
  const find = (market: MarketLine["market"], side: number, line?: number | null) =>
    lines.find((l) => l.market === market && l.side === side && (line === undefined || l.line === line));
  const homeMl = find("moneyline", 1);
  const plLine = find("puckline", 1)?.line;
  const totalLine = lines.find((l) => l.market === "total")?.cons_line ?? lines.find((l) => l.market === "total")?.line;
  const side = (key: string, label: string, color: string, l?: MarketLine): BarSide =>
    ({ key, label, color, p: l?.p_market_side ?? null, price: l?.price, book: l?.book });
  const pair = (a: BarSide, b: BarSide) => complete(a, b);
  const ml = homeMl ? pair(side("away", g.away_abbr, colors.away, find("moneyline", 2)), side("home", g.home_abbr, colors.home, homeMl)) : null;
  const pl = plLine != null ? pair(side("away", `${g.away_abbr} ${handicap(-plLine)}`, colors.away, find("puckline", 2, plLine)),
    side("home", `${g.home_abbr} ${handicap(plLine)}`, colors.home, find("puckline", 1, plLine))) : null;
  const ou = totalLine != null ? pair(side("over", `Over ${totalLine}`, OVER_COLOR, find("total", 1, totalLine)),
    side("under", `Under ${totalLine}`, UNDER_COLOR, find("total", 2, totalLine))) : null;
  const tw = threeWay && threeWay.sides.some((x) => x.p_market != null) ? threeWaySides(threeWay, g.away_abbr, g.home_abbr, colors, "market") : null;
  return (
    <Link href={`/games/${g.game_id}`} className="block overflow-hidden rounded-lg border border-border bg-card transition-colors hover:border-muted-foreground">
      <GameBanner
        away={{ abbr: g.away_abbr, info: teams[g.away_abbr], color: colors.away, price: find("moneyline", 2)?.price }}
        home={{ abbr: g.home_abbr, info: teams[g.home_abbr], color: colors.home, price: homeMl?.price }}
        status={gameStatus(g, g.start_time)}
        venue={g.venue_name}
        location={g.venue_location}
        chips={totalLine != null ? [{ label: "Total", value: totalLine }] : undefined}
        className="border-b border-border"
      />
      <div className="space-y-1.5 px-3 pb-3 pt-2">
        {ml && <MarketBar title="Moneyline · Market" sides={ml} basis="market" />}
        {tw && <MarketBar title="3-Way (Regulation) · Market" sides={tw} basis="market" />}
        {pl && <MarketBar title="Puck Line · Market" sides={pl} basis="market" />}
        {ou && <MarketBar title="Total · Market" sides={ou} basis="market" />}
        <div className={cn("text-xs italic text-muted-foreground", lines.length > 0 && "border-t border-border pt-2")}>
          {g.is_final
            ? "No pregame price was recorded for this game."
            : `${lines.length ? "Market prices only. " : "No lines yet. "}The model prices the day's games at 9:14 AM ET.`}
        </div>
      </div>
    </Link>
  );
}
