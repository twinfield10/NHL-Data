"use client";

import { Fragment, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import PropTable, { RiskBadge } from "@/components/PropTable";
import { TeamTag } from "@/components/TeamLogo";
import { Card, Empty, ErrorState, Loading, SectionTitle } from "@/components/ui";
import { useGameProps } from "@/lib/api";
import { american, pct } from "@/lib/format";
import { bookAbbr, isPlay, MAX_PRICE, modelOver, PROP_LABEL, playStake, rung, SLOT_LABEL } from "@/lib/props";
import type { PropEdge, PropPlayer, PropQuote, PropType } from "@/lib/types";
import { cn } from "@/lib/utils";

/** The market columns on each team's board: stat and line (over). */
const MARKETS: { prop: PropType; line: number }[] = [
  { prop: "goals", line: 0.5 },
  { prop: "assists", line: 0.5 },
  { prop: "points", line: 0.5 },
  { prop: "points", line: 1.5 },
  { prop: "shots", line: 1.5 },
  { prop: "shots", line: 2.5 },
  { prop: "blocks", line: 1.5 },
];

const fix = (v: number | null | undefined, d = 2) => (v == null ? "–" : v.toFixed(d));

const key = (pid: number, prop: string, line: number) => `${pid}|${prop}|${line}`;

/** Model %, then the best over price across books and the consensus; green when a side is a play. */
function MarketCell({ model, quotes, play }: { model: number | null; quotes: PropQuote[]; play?: PropEdge }) {
  const overs = quotes.filter((q) => q.price_over != null);
  const best = overs.length ? overs.reduce((a, b) => (b.price_over! > a.price_over! ? b : a)) : null;
  return (
    <div className={cn("rounded px-1.5 py-0.5", play && "bg-emerald-500/15 ring-1 ring-emerald-600")}>
      <div className="font-semibold">{pct(model)}</div>
      <div className="text-[11px] text-muted-foreground">
        {best ? `${american(best.price_over)} ${bookAbbr(best.book)} · mkt ${pct(best.p_market, 0)}` : "no line"}
      </div>
      {play && (
        <div className="mt-0.5 text-[11px] font-semibold text-positive">
          {play.side === "over" ? "Over" : "Under"} {american(play.price)} {bookAbbr(play.book)}
        </div>
      )}
    </div>
  );
}

/** Every book's prices for one player, by stat and line. */
function QuoteGrid({ player, quotes }: { player: PropPlayer; quotes: PropQuote[] }) {
  const books = [...new Set(quotes.map((q) => q.book))].sort();
  const lines = [...new Set(quotes.map((q) => `${q.prop_type}|${q.line}`))].sort();
  if (!lines.length) return <div className="text-xs text-muted-foreground">No book quotes for this player.</div>;
  return (
    <table className="text-xs tabular">
      <thead className="text-muted-foreground">
        <tr>
          <th className="px-2 py-1 text-left font-medium">Line</th>
          <th className="px-2 py-1 text-right font-medium">Model</th>
          <th className="px-2 py-1 text-right font-medium">Market</th>
          {books.map((b) => <th key={b} className="px-2 py-1 text-right font-medium">{b}</th>)}
        </tr>
      </thead>
      <tbody>
        {lines.map((l) => {
          const [prop, lineStr] = l.split("|");
          const line = Number(lineStr);
          const row = quotes.filter((q) => q.prop_type === prop && q.line === line);
          return (
            <tr key={l} className="border-t border-border">
              <td className="whitespace-nowrap px-2 py-1">{PROP_LABEL[prop as PropType]} {line} <span className="text-muted-foreground">({rung(prop as PropType, line)})</span></td>
              <td className="px-2 py-1 text-right">{pct(modelOver(player as unknown as Record<string, unknown>, prop as PropType, line))}</td>
              <td className="px-2 py-1 text-right">{pct(row[0]?.p_market)}</td>
              {books.map((b) => {
                const q = row.find((r) => r.book === b);
                return (
                  <td key={b} className="whitespace-nowrap px-2 py-1 text-right">
                    {q ? <>{american(q.price_over)}{q.price_under != null && <span className="text-muted-foreground"> / {american(q.price_under)}</span>}</> : "–"}
                  </td>
                );
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function TeamBoard({ team, players, quotes, plays }: { team: string; players: PropPlayer[]; quotes: Map<string, PropQuote[]>; plays: Map<string, PropEdge> }) {
  const [open, setOpen] = useState<number | null>(null);
  return (
    <Card className="overflow-x-auto">
      <div className="border-b border-border px-3 py-2"><TeamTag abbr={team} /></div>
      <table className="w-full text-sm tabular">
        <thead className="text-xs text-muted-foreground">
          <tr className="border-b border-border">
            <th className="px-3 py-2 text-left font-medium">Player</th>
            <th className="px-2 py-2 text-right font-medium" title="Expected goals / assists / points / shots on goal">xG / xA / xPts / xSOG</th>
            {MARKETS.map((m) => (
              <th key={`${m.prop}${m.line}`} className="px-2 py-2 text-left font-medium">{rung(m.prop, m.line)}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {players.map((p) => {
            const isOpen = open === p.player_id;
            const all = MARKETS.flatMap((m) => quotes.get(key(p.player_id, m.prop, m.line)) ?? []);
            const playerQuotes = [...quotes.entries()].filter(([k]) => k.startsWith(`${p.player_id}|`)).flatMap(([, v]) => v);
            const playerPlays = [...plays.values()].filter((e) => e.player_id === p.player_id);
            return (
              <Fragment key={p.player_id}>
                <tr onClick={() => setOpen(isOpen ? null : p.player_id)} className="cursor-pointer border-b border-border align-top last:border-0 hover:bg-muted/60">
                  <td className="whitespace-nowrap px-3 py-2">
                    <span className="inline-flex items-center gap-1">
                      {isOpen ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />}
                      <span className="font-medium">{p.player_name}</span>
                    </span>
                    <div className="ml-4.5 text-xs text-muted-foreground">
                      {p.position}{p.slot ? ` · ${SLOT_LABEL[p.slot] ?? p.slot}` : ""}{p.pp_unit ? ` · PP${p.pp_unit}` : ""}
                      {p.p_dressed != null && p.p_dressed < 0.999 && <span className="text-amber-600"> · GTD {pct(p.p_dressed, 0)}</span>}
                      {!all.length && " · no lines"}
                    </div>
                    {playerPlays.length > 0 && (
                      <div className="ml-4.5 mt-0.5"><RiskBadge stake={playerPlays.reduce((s, e) => s + playStake(e), 0)} /></div>
                    )}
                  </td>
                  <td className="whitespace-nowrap px-2 py-2 text-right text-muted-foreground">
                    {fix(p.exp_goals)} / {fix(p.exp_ast)} / <span className="font-medium text-foreground">{fix(p.exp_points)}</span>
                    {" "}/ {fix(p.exp_shots, 1)}
                  </td>
                  {MARKETS.map((m) => (
                    <td key={`${m.prop}${m.line}`} className="px-2 py-1.5">
                      <MarketCell
                        model={modelOver(p as unknown as Record<string, unknown>, m.prop, m.line)}
                        quotes={quotes.get(key(p.player_id, m.prop, m.line)) ?? []}
                        play={plays.get(key(p.player_id, m.prop, m.line))}
                      />
                    </td>
                  ))}
                </tr>
                {isOpen && (
                  <tr className="border-b border-border bg-muted/40">
                    <td colSpan={MARKETS.length + 2} className="px-3 py-3">
                      <QuoteGrid player={p} quotes={playerQuotes} />
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </Card>
  );
}

/** The saves line most books hang for a goalie (two-way lines first). */
function mainLine(quotes: PropQuote[]): number | null {
  if (!quotes.length) return null;
  const count = new Map<number, number>();
  for (const q of quotes) count.set(q.line, (count.get(q.line) ?? 0) + (q.price_under != null ? 2 : 1));
  return [...count.entries()].sort((a, b) => b[1] - a[1] || a[0] - b[0])[0][0];
}

function GoalieBoard({ goalies, quotes, plays }: { goalies: PropPlayer[]; quotes: Map<string, PropQuote[]>; plays: Map<string, PropEdge> }) {
  const [open, setOpen] = useState<number | null>(null);
  return (
    <Card className="overflow-x-auto">
      <table className="w-full text-sm tabular">
        <thead className="text-xs text-muted-foreground">
          <tr className="border-b border-border">
            <th className="px-3 py-2 text-left font-medium">Goalie</th>
            <th className="px-2 py-2 text-right font-medium" title="Chance he starts (saves props are void if he doesn't)">Start</th>
            <th className="px-2 py-2 text-right font-medium" title="Expected saves if he starts">xSaves</th>
            <th className="px-2 py-2 text-left font-medium">Main line (over)</th>
          </tr>
        </thead>
        <tbody>
          {goalies.map((g) => {
            const all = [...quotes.entries()].filter(([k]) => k.startsWith(`${g.player_id}|saves|`)).flatMap(([, v]) => v);
            const line = mainLine(all);
            const isOpen = open === g.player_id;
            return (
              <Fragment key={g.player_id}>
                <tr onClick={() => setOpen(isOpen ? null : g.player_id)} className="cursor-pointer border-b border-border align-top last:border-0 hover:bg-muted/60">
                  <td className="whitespace-nowrap px-3 py-2">
                    <span className="inline-flex items-center gap-1">
                      {isOpen ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />}
                      <span className="font-medium">{g.player_name}</span>
                      <span className="text-xs text-muted-foreground">{g.team}</span>
                    </span>
                  </td>
                  <td className="px-2 py-2 text-right">{pct(g.p_dressed, 0)}</td>
                  <td className="px-2 py-2 text-right font-medium">{fix(g.exp_saves, 1)}</td>
                  <td className="px-2 py-1.5">
                    {line == null ? <span className="text-xs text-muted-foreground">no line</span> : (
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-muted-foreground">O{line}</span>
                        <MarketCell model={modelOver(g as unknown as Record<string, unknown>, "saves", line)}
                          quotes={quotes.get(key(g.player_id, "saves", line)) ?? []} play={plays.get(key(g.player_id, "saves", line))} />
                      </div>
                    )}
                  </td>
                </tr>
                {isOpen && (
                  <tr className="border-b border-border bg-muted/40">
                    <td colSpan={4} className="px-3 py-3"><QuoteGrid player={g} quotes={all} /></td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </Card>
  );
}

/** A game's prop board: its plays and best edges, then each team's players with model vs market. */
export default function GameProps({ gameId, away, home }: { gameId: string; away: string; home: string }) {
  const { data, isLoading, error } = useGameProps(gameId);
  if (isLoading) return <Loading />;
  if (error) return <ErrorState error={error} />;
  if (!data || !data.players.length) return <Empty>No prop projections for this game yet (they follow the morning pregame run).</Empty>;

  const quotes = new Map<string, PropQuote[]>();
  for (const q of data.quotes) {
    const k = key(q.player_id, q.prop_type, q.line);
    quotes.set(k, [...(quotes.get(k) ?? []), q]);
  }
  const plays = new Map<string, PropEdge>();
  for (const e of data.edges) if (isPlay(e)) plays.set(key(e.player_id, e.prop_type, e.line), e);
  const best = [...data.edges]
    .filter((e) => isPlay(e) || (e.edge > 0 && e.price <= MAX_PRICE))
    .sort((a, b) => Number(isPlay(b)) - Number(isPlay(a)) || b.edge - a.edge)
    .slice(0, 15);

  return (
    <div className="space-y-6">
      <div>
        <SectionTitle right={<span className="text-xs text-muted-foreground">Plays, then positive edges at +{MAX_PRICE} or shorter</span>}>Best Edges</SectionTitle>
        {best.length ? <Card className="overflow-hidden"><PropTable rows={best} showGame={false} /></Card> : <Empty>No positive edges on this game.</Empty>}
      </div>
      <div>
        <SectionTitle right={<span className="text-xs text-muted-foreground">Click a player for every book&apos;s prices</span>}>
          Player Board
        </SectionTitle>
        <div className="grid grid-cols-1 gap-4">
          {[away, home].map((t) => (
            <TeamBoard key={t} team={t} players={data.players.filter((p) => p.team === t && p.position !== "G")} quotes={quotes} plays={plays} />
          ))}
        </div>
      </div>
      <div>
        <SectionTitle right={<span className="text-xs text-muted-foreground">Goalies at least 30% to start; saves assume he starts</span>}>
          Goalies
        </SectionTitle>
        <GoalieBoard
          goalies={data.players.filter((p) => p.position === "G" && (p.p_dressed ?? 0) >= 0.3).sort((a, b) => (b.p_dressed ?? 0) - (a.p_dressed ?? 0))}
          quotes={quotes}
          plays={plays}
        />
      </div>
    </div>
  );
}
