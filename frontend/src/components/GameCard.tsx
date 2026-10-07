"use client";

import Link from "next/link";
import type { Bet, Edge, MarketLine, SlateGame, TeamInfo, UnpricedGame } from "@/lib/types";
import { american, pct, timeET, units } from "@/lib/format";
import { matchupColors } from "@/lib/teams";
import { useDark } from "@/lib/useDark";
import { cn } from "@/lib/utils";
import GameBanner, { toneText, type Tone } from "./GameBanner";
import ProbBar from "./ProbBar";

/** An edge this large is more likely a bad quote or a stale input than a real price. */
export const WARN_EDGE = 0.09;

const MARKET_SHORT = { moneyline: "ML", puckline: "PL", total: "" } as const;

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

/** "away side price   home side price" for one market line. */
function MarketRow({ label, away, home }: { label: string; away?: Priced; home?: Priced }) {
  const cell = (e?: Priced) =>
    e ? (
      <span className={cn("whitespace-nowrap text-right", edgeTone(e) ? toneText(edgeTone(e)) : "text-muted-foreground")}>
        {e.selection} {american(e.price)}
      </span>
    ) : (
      <span className="text-muted-foreground">–</span>
    );
  return (
    <div className="flex justify-between text-xs">
      <span className="w-8 text-muted-foreground">{label}</span>
      <div className="flex gap-3">
        {cell(away)}
        {cell(home)}
      </div>
    </div>
  );
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
export default function GameCard({ g, edges, bets, teams }: { g: SlateGame; edges: Edge[]; bets: Bet[]; teams: Record<string, TeamInfo> }) {
  const dark = useDark();
  const colors = matchupColors(g.away_team_abbr, g.home_team_abbr, dark);

  const find = (market: Edge["market"], side: number, line?: number | null) =>
    edges.find((e) => e.market === market && e.side === side && (line === undefined || e.line === line));
  // Closing prices can't be bet any more, so they never light the card up.
  const closing = edges.some((e) => e.point === "close");
  const signal = closing ? [] : edges;
  const teamEdges = (side: number) => signal.filter((e) => e.market !== "total" && e.side === side);
  const awayTone = worst(teamEdges(2));
  const homeTone = worst(teamEdges(1));
  const cardTone = worst(signal);

  // Main puck line and the total at the market's consensus line (else the first one captured).
  const plLine = edges.find((e) => e.market === "puckline")?.line;
  const totalLine = edges.some((e) => e.market === "total" && e.line === g.mkt_total_line)
    ? g.mkt_total_line
    : edges.find((e) => e.market === "total")?.line;

  const topEdges = [...edges].filter((e) => e.edge > 0).sort((a, b) => b.edge - a.edge).slice(0, 2);
  const issues = [g.away_lineup_issues, g.home_lineup_issues].filter(Boolean).join("; ");

  return (
    <Link
      href={`/games/${g.game_id}`}
      className={cn(
        "block overflow-hidden rounded-lg border transition-colors hover:border-muted-foreground",
        cardTone === "warn" ? "border-red-600 bg-red-500/10" : cardTone === "play" ? "border-emerald-600 bg-emerald-500/10" : "border-border bg-card"
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

      <div className="px-4 pb-4 pt-3">
      <ProbBar pHome={g.p_home_win} pMarket={g.mkt_p_home_win} awayColor={colors.away} homeColor={colors.home} />

      {edges.length === 0 ? (
        <div className="mt-3 border-t border-border pt-2 text-xs italic text-muted-foreground">No lines captured</div>
      ) : (
        <>
          <div className="mt-3 border-t border-border pt-2">
            {closing && (
              <div className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">Closing prices</div>
            )}
            {topEdges.length === 0 ? (
              <div className="text-xs italic text-muted-foreground">No positive edges</div>
            ) : (
              topEdges.map((e) => (
                <div key={`${e.market}-${e.side}-${e.line}`} className={cn("mt-1 flex justify-between text-xs first:mt-0", toneText(edgeTone(e)) || "text-muted-foreground")}>
                  <span>
                    {e.selection} {MARKET_SHORT[e.market]}
                    <span className="ml-1 text-muted-foreground">{american(e.price)}</span>
                  </span>
                  <span className="tabular">
                    {(e.edge * 100).toFixed(2)}% edge @ {e.book}
                    {e.stake_units > 0 && ` · ${e.stake_units.toFixed(2)}u`}
                  </span>
                </div>
              ))
            )}
          </div>
          <div className="mt-2 space-y-1 border-t border-border pt-2">
            {plLine != null && <MarketRow label="PL" away={find("puckline", 2, plLine)} home={find("puckline", 1, plLine)} />}
            {totalLine != null && <MarketRow label="O/U" away={find("total", 1, totalLine)} home={find("total", 2, totalLine)} />}
          </div>
        </>
      )}

      {(issues || g.stale_inputs) && (
        <div className="mt-2 flex justify-end gap-2 border-t border-border pt-2 text-[11px]">
          {issues && <span title={issues} className="text-amber-600 dark:text-amber-400">Lineup flag</span>}
          {g.stale_inputs && <span title={g.stale_inputs} className="text-red-600 dark:text-red-400">Stale inputs</span>}
        </div>
      )}

      {bets.length > 0 && (
        <div className="mt-3 space-y-0.5 rounded-md bg-emerald-700 px-3 py-1.5">
          {bets.map((b) => (
            <div key={b.bet_id} className="text-[11px] font-medium leading-5 text-white">
              {b.kind === "real" ? "Placed" : "Paper"} ({b.selection}
              {b.market === "moneyline" && " ML"} {american(b.price)}, {b.stake_units.toFixed(2)}u)
              {b.result && ` · ${b.result} ${units(b.pnl_units)}`}
            </div>
          ))}
        </div>
      )}
      </div>
    </Link>
  );
}

/** A game with no pregame run yet: the same banner, and the market's best prices without the model. */
export function UnpricedGameCard({ g, teams, lines }: { g: UnpricedGame; teams: Record<string, TeamInfo>; lines: MarketLine[] }) {
  const dark = useDark();
  const colors = matchupColors(g.away_abbr, g.home_abbr, dark);
  const find = (market: MarketLine["market"], side: number, line?: number | null) =>
    lines.find((l) => l.market === market && l.side === side && (line === undefined || l.line === line));
  const homeMl = find("moneyline", 1);
  const plLine = find("puckline", 1)?.line;
  const totalLine = lines.find((l) => l.market === "total")?.cons_line ?? lines.find((l) => l.market === "total")?.line;
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
      <div className="px-4 pb-4 pt-3">
        {homeMl && (
          <ProbBar pHome={homeMl.p_market_side} awayColor={colors.away} homeColor={colors.home} label="market, no vig" />
        )}
        {lines.length > 0 && (
          <div className={cn("space-y-1", homeMl && "mt-3 border-t border-border pt-2")}>
            {plLine != null && <MarketRow label="PL" away={find("puckline", 2, plLine)} home={find("puckline", 1, plLine)} />}
            {totalLine != null && <MarketRow label="O/U" away={find("total", 1, totalLine)} home={find("total", 2, totalLine)} />}
          </div>
        )}
        <div className={cn("text-xs italic text-muted-foreground", lines.length > 0 && "mt-2 border-t border-border pt-2")}>
          {g.is_final
            ? "No pregame price was recorded for this game."
            : `${lines.length ? "Market prices only. " : "No lines yet. "}The model prices the day's games at 9:14 AM ET.`}
        </div>
      </div>
    </Link>
  );
}
