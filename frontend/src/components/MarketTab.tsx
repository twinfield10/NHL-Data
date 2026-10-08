"use client";

import { useState } from "react";
import type { Edge, GameResponse, MarketKey, SideKey } from "@/lib/types";
import { american, dateTimeET, fairAmerican, pct, signedPct, timeET } from "@/lib/format";
import { DRAW_COLOR, OVER_COLOR, UNDER_COLOR } from "@/lib/teams";
import { cn } from "@/lib/utils";
import EdgeTable from "./EdgeTable";
import LineHistoryChart, { type ChartSide } from "./LineHistoryChart";
import MarketBar, { type BarSide } from "./MarketBar";
import { WARN_EDGE } from "./GameCard";
import { Card, Pills, SectionTitle, Signed, Stat } from "./ui";

const MARKETS = [
  { key: "moneyline", label: "Moneyline" },
  { key: "moneyline_3way", label: "3-Way (Regulation)" },
  { key: "puckline", label: "Puck Line" },
  { key: "total", label: "Total" },
] as const satisfies readonly { key: MarketKey; label: string }[];

/** Edge-table side number for a market side (1 = home / over). */
const SIDE_NUM: Partial<Record<SideKey, number>> = { home: 1, over: 1, away: 2, under: 2 };

const handicap = (v: number) => `${v > 0 ? "+" : ""}${v.toFixed(1)}`;

/** Sides left to right (away / over first); puck-line labels carry each side's handicap. */
function sidesFor(market: MarketKey, line: number | null, away: string, home: string, colors: { away: string; home: string }): ChartSide[] {
  switch (market) {
    case "moneyline":
      return [{ key: "away", label: away, color: colors.away }, { key: "home", label: home, color: colors.home }];
    case "moneyline_3way":
      return [{ key: "away", label: away, color: colors.away }, { key: "draw", label: "OT", color: DRAW_COLOR },
        { key: "home", label: home, color: colors.home }];
    case "puckline":
      return [{ key: "away", label: line != null ? `${away} ${handicap(-line)}` : away, color: colors.away },
        { key: "home", label: line != null ? `${home} ${handicap(line)}` : home, color: colors.home }];
    case "total":
      return [{ key: "over", label: line != null ? `Over ${line}` : "Over", color: OVER_COLOR },
        { key: "under", label: line != null ? `Under ${line}` : "Under", color: UNDER_COLOR }];
  }
}

/** Odds with the implied probability, as in the books table. */
function Price({ price, best }: { price: number | null | undefined; best?: boolean }) {
  if (price == null) return <span className="text-muted-foreground">–</span>;
  const p = price < 0 ? -price / (-price + 100) : 100 / (price + 100);
  return (
    <span className={cn("rounded px-1.5 py-0.5 font-mono", best && "bg-emerald-500/15 font-bold text-emerald-700 dark:text-emerald-400")}>
      {american(price)} <span className="text-[11px] font-normal text-muted-foreground">({pct(p, 1)})</span>
    </span>
  );
}

export default function MarketTab({ data, colors }: { data: GameResponse; colors: { away: string; home: string } }) {
  const [market, setMarket] = useState<MarketKey>("moneyline");
  const { game, pregame: p, markets: mv } = data;
  const closing = data.edges.some((e) => e.point === "close");
  const cons = mv.consensus[market];
  const line = cons?.line ?? null;
  const sides = sidesFor(market, line, game.away_abbr, game.home_abbr, colors);
  const model = mv.model.filter((m) => m.market === market);
  const latestModel = model.length ? model[model.length - 1].p : null;
  // Edge rows carry the home handicap / the total on both sides, like the consensus line.
  const edgeFor = (s: SideKey): Edge | undefined =>
    data.edges.find((e) => e.market === market && e.side === SIDE_NUM[s] && (market === "moneyline" || e.line === line));
  const threeWay = market === "moneyline_3way" ? mv.three_way : null;

  const rows = sides.map((s) => {
    const e = market === "moneyline_3way" ? undefined : edgeFor(s.key);
    const tw = threeWay?.sides.find((x) => x.side === s.key);
    const best = cons?.best[s.key];
    return {
      ...s,
      pModel: latestModel?.[s.key] ?? e?.p_model_side ?? tw?.p_model ?? null,
      pMarket: cons?.fair[s.key] ?? e?.p_market_side ?? null,
      pBlend: e?.p ?? null,
      price: e?.price ?? tw?.price ?? best?.price ?? null,
      book: e?.book ?? tw?.book ?? best?.book ?? null,
      edge: e?.edge ?? tw?.edge ?? null,
      stake: e?.stake_units ?? null,
      play: !closing && !!e?.flagged,
      warn: !closing && e != null && e.edge >= WARN_EDGE,
    };
  });
  const bar: BarSide[] = rows.map((r) => ({ key: r.key, label: r.label, color: r.color, p: r.pModel ?? r.pMarket, pMarket: r.pMarket,
    price: r.price, book: r.book, edge: r.edge, play: r.play, warn: r.warn, stake: r.stake }));
  const books = mv.books[market] ?? [];
  const bestPrice = (k: SideKey) => Math.max(...books.filter((b) => b.line === line).map((b) => {
    const v = b.prices[k];
    return v == null ? -Infinity : v < 0 ? 100 / -v : v / 100;
  }));
  const decimalOf = (v: number) => (v < 0 ? 100 / -v : v / 100);
  const lineText = (l: number) => (market === "total" ? `O/U ${l}` : `${game.home_abbr} ${handicap(l)}`);

  return (
    <div className="space-y-6">
      {p && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <Stat label="Projected Score" value={`${game.away_abbr} ${p.mean_away_goals.toFixed(2)} – ${p.mean_home_goals.toFixed(2)} ${game.home_abbr}`} />
          <Stat label="Projected Total" value={(p.mean_away_goals + p.mean_home_goals).toFixed(2)} />
          <Stat label="Overtime" value={pct(p.p_overtime, 0)} />
          <Stat label="Last Priced" value={`${timeET(p.as_of)} ET`} />
        </div>
      )}

      <Pills options={MARKETS} value={market} onChange={setMarket} />

      <div>
        <SectionTitle right={closing && <span className="text-xs text-muted-foreground">Closing Prices</span>}>Model Projection</SectionTitle>
        <Card className="p-4">
          {rows.some((r) => r.pModel != null || r.pMarket != null) ? (
            <>
              <MarketBar title={MARKETS.find((m) => m.key === market)!.label} sides={bar} closed={closing} neutralEdges={market === "moneyline_3way"}
                basis={rows.some((r) => r.pModel != null) ? "model" : "market"} />
              <div className="mt-4 overflow-x-auto">
                <table className="w-full text-sm tabular">
                  <thead className="text-xs text-muted-foreground">
                    <tr className="border-b border-border">
                      <th className="py-2 text-left font-medium" />
                      {rows.map((r) => (
                        <th key={r.key} className="py-2 text-right font-semibold" style={{ color: r.color }}>{r.label}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      { label: "Model", cell: (r: (typeof rows)[number]) => r.pModel != null ? <>{pct(r.pModel)} <span className="text-muted-foreground">{fairAmerican(r.pModel)}</span></> : "–" },
                      { label: "Market (No Vig)", cell: (r: (typeof rows)[number]) => r.pMarket != null ? <>{pct(r.pMarket)} <span className="text-muted-foreground">{fairAmerican(r.pMarket)}</span></> : "–" },
                      ...(market === "moneyline_3way" ? [] : [{ label: "Blend", cell: (r: (typeof rows)[number]) => pct(r.pBlend) }]),
                      { label: "Model − Market", cell: (r: (typeof rows)[number]) => r.pModel != null && r.pMarket != null
                        ? <Signed value={r.pModel - r.pMarket}>{signedPct(r.pModel - r.pMarket)}</Signed> : "–" },
                      { label: "Best Price", cell: (r: (typeof rows)[number]) => r.price != null ? <>{american(r.price)} <span className="text-muted-foreground">{r.book}</span></> : "–" },
                      { label: "Edge", cell: (r: (typeof rows)[number]) => r.edge != null ? (
                        <span className="inline-flex items-center gap-1.5">
                          {r.play && <span className="rounded bg-emerald-600 px-1.5 text-[10px] font-bold text-white">PLAY</span>}
                          {r.warn && <span className="rounded bg-red-600 px-1.5 text-[10px] font-bold text-white">WARN</span>}
                          <Signed value={r.edge}>{signedPct(r.edge, 2)}</Signed>
                        </span>) : "–" },
                      ...(market === "moneyline_3way" ? [] : [{ label: "Stake", cell: (r: (typeof rows)[number]) => r.stake ? `${r.stake.toFixed(2)}u` : "–" }]),
                    ].map((row) => (
                      <tr key={row.label} className="border-b border-border last:border-0">
                        <td className="py-2 text-muted-foreground">{row.label}</td>
                        {rows.map((r) => <td key={r.key} className="py-2 text-right">{row.cell(r)}</td>)}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {market === "moneyline_3way" && (
                <p className="mt-3 text-xs text-muted-foreground">
                  Settles after 60 minutes: OT is any game that goes to overtime. The model&apos;s regulation split is shown without a
                  blend, so it is never flagged as a play.
                </p>
              )}
            </>
          ) : (
            <div className="py-6 text-center text-sm text-muted-foreground">No model or market price for this market yet</div>
          )}
        </Card>
      </div>

      <div>
        <SectionTitle right={cons && <span className="text-xs text-muted-foreground">{cons.books} book{cons.books !== 1 && "s"} at the main line</span>}>
          Current Lines
        </SectionTitle>
        <Card>
          {books.length === 0 ? (
            <div className="p-6 text-center text-sm text-muted-foreground">No lines captured</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="text-xs text-muted-foreground">
                  <tr className="border-b border-border">
                    <th className="px-3 py-2 text-left font-medium">Book</th>
                    {market !== "moneyline" && market !== "moneyline_3way" && <th className="px-3 py-2 text-left font-medium">Line</th>}
                    {sides.map((s) => <th key={s.key} className="px-3 py-2 text-right font-semibold" style={{ color: s.color }}>{s.label.split(" ")[0]}</th>)}
                    <th className="px-3 py-2 text-right font-medium">Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {books.map((b) => (
                    <tr key={b.book} className="border-b border-border last:border-0">
                      <td className="px-3 py-2 font-medium">{b.book}</td>
                      {market !== "moneyline" && market !== "moneyline_3way" && (
                        <td className={cn("px-3 py-2 tabular", b.line !== line && "text-amber-600 dark:text-amber-400")}>
                          {b.line != null ? lineText(b.line) : "–"}
                        </td>
                      )}
                      {sides.map((s) => {
                        const v = b.prices[s.key];
                        return (
                          <td key={s.key} className="px-3 py-2 text-right">
                            <Price price={v} best={v != null && b.line === line && decimalOf(v) === bestPrice(s.key)} />
                          </td>
                        );
                      })}
                      <td className="px-3 py-2 text-right text-xs text-muted-foreground">{dateTimeET(b.captured_at)}</td>
                    </tr>
                  ))}
                  {cons && (
                    <tr className="bg-muted/50">
                      <td className="px-3 py-2 font-semibold">Consensus (No Vig)</td>
                      {market !== "moneyline" && market !== "moneyline_3way" && <td className="px-3 py-2 tabular">{line != null ? lineText(line) : "–"}</td>}
                      {sides.map((s) => (
                        <td key={s.key} className="px-3 py-2 text-right font-mono">
                          {fairAmerican(cons.fair[s.key])} <span className="text-[11px] text-muted-foreground">({pct(cons.fair[s.key], 1)})</span>
                        </td>
                      ))}
                      <td className="px-3 py-2 text-right text-xs text-muted-foreground">{dateTimeET(cons.t)}</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      </div>

      <div>
        <SectionTitle>Line History</SectionTitle>
        <Card className="p-4">
          <LineHistoryChart
            history={mv.history.filter((h) => h.market === market)}
            model={model}
            sides={sides}
            start={mv.start}
            lineLabel={market === "puckline" || market === "total" ? lineText : undefined}
          />
          {(market === "puckline" || market === "total") && (
            <p className="mt-2 text-xs text-muted-foreground">
              The model is priced at the current main line ({line != null ? lineText(line) : "–"}); hover the chart for the line in effect at each moment.
            </p>
          )}
        </Card>
      </div>

      {data.edges.length > 0 && (
        <div>
          <SectionTitle>{closing ? "All Edges at the Close" : "All Edges"}</SectionTitle>
          <Card>
            <EdgeTable edges={data.edges} showGame={false} />
          </Card>
        </div>
      )}
    </div>
  );
}
