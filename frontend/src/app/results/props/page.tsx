"use client";

import Link from "next/link";
import { TeamTag } from "@/components/TeamLogo";
import { Badge, Card, Empty, ErrorState, Loading, SectionTitle, Signed, Stat } from "@/components/ui";
import { usePropBets } from "@/lib/api";
import { american, dateTimeET, pct, signedPct, units } from "@/lib/format";
import { PROP_LABEL, propBet } from "@/lib/props";
import type { PropBet, PropBetStatus } from "@/lib/types";
import { cn } from "@/lib/utils";

const tone = (v: number | null | undefined) => (v == null || v === 0 ? null : v > 0 ? "pos" : "neg");
const RESULT_TONE = { win: "pos", loss: "neg", void: "muted" } as const;
/** Ungraded bets judged against the market now. */
const STATUS: Record<Exclude<PropBetStatus, "graded">, { label: string; tone: "pos" | "warn" | "muted" | "accent"; title: string }> = {
  value: { label: "still +EV", tone: "pos", title: "Still flagged at the best price now" },
  faded: { label: "faded", tone: "warn", title: "Still quoted, but no longer a play at the best price now" },
  gone: { label: "pulled", tone: "muted", title: "No book quotes this line now" },
  closed: { label: "closed", tone: "accent", title: "Game started; graded once the game logs are in" },
};

/** Minutes before puck drop -> "45m" / "6.5h". */
const lead = (m: number | null) => (m == null ? null : m < 60 ? `${Math.round(m)}m` : `${(m / 60).toFixed(1)}h`);

/** Taken price -> price now: green when the market moved toward us (now shorter than we took). */
function PriceNow({ b }: { b: PropBet }) {
  if (b.now_price == null) return <span className="text-muted-foreground">–</span>;
  const tone = b.now_price < b.price ? "text-positive" : b.now_price > b.price ? "text-negative" : "";
  return (
    <span className={tone} title={`best now at ${b.now_book}`}>
      {american(b.now_price)} <span className="text-xs text-muted-foreground">{b.now_book}</span>
    </span>
  );
}

export default function PropBetsPage() {
  const { data, isLoading, error } = usePropBets();

  return (
    <div className="space-y-5">
      <h1 className="text-xl font-semibold">Prop Bets</h1>
      {isLoading && <Loading />}
      {error && <ErrorState error={error} />}
      {data && (
        <>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
            <Stat label="Graded bets" value={data.totals.bets} />
            <Stat label="Units won" value={units(data.totals.pnl)} tone={tone(data.totals.pnl)} />
            <Stat label="ROI" value={signedPct(data.totals.roi)} tone={tone(data.totals.roi)} />
            <Stat label="Mean CLV" value={signedPct(data.totals.mean_clv, 2)} tone={tone(data.totals.mean_clv)} />
            <Stat label="Beat the close" value={pct(data.totals.beat_close, 0)} />
          </div>

          {data.open.bets > 0 && (
            <div>
              <SectionTitle>Open Bets (vs. the market now)</SectionTitle>
              <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
                <Stat label="Open bets" value={`${data.open.bets} · ${data.open.staked.toFixed(2)}u`} />
                <Stat label="Live CLV" value={signedPct(data.open.mean_clv, 2)} tone={tone(data.open.mean_clv)} />
                <Stat label="Beating the market" value={pct(data.open.beating, 0)} />
                <Stat label="Still +EV" value={data.open.value} />
                <Stat label="Faded / pulled / closed" value={`${data.open.faded} / ${data.open.gone} / ${data.open.closed}`} />
              </div>
            </div>
          )}

          {data.timing.length > 0 && (
            <div>
              <SectionTitle>By Time Before Puck Drop</SectionTitle>
              <Card className="overflow-x-auto">
                <table className="w-full text-sm tabular">
                  <thead className="text-left text-xs text-muted-foreground">
                    <tr className="border-b border-border">
                      {["Placed", "Bets", "Staked", "Units", "ROI", "CLV", "Beat close"].map((h, i) => (
                        <th key={h} className={cn("px-3 py-2 font-medium", i >= 1 && "text-right")}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.timing.map((t) => (
                      <tr key={t.lead_bucket} className="border-b border-border last:border-0">
                        <td className="px-3 py-2">{t.lead_bucket} before</td>
                        <td className="px-3 py-2 text-right">{t.bets}</td>
                        <td className="px-3 py-2 text-right">{t.staked.toFixed(2)}u</td>
                        <td className="px-3 py-2 text-right"><Signed value={t.pnl}>{units(t.pnl)}</Signed></td>
                        <td className="px-3 py-2 text-right"><Signed value={t.roi}>{signedPct(t.roi)}</Signed></td>
                        <td className="px-3 py-2 text-right"><Signed value={t.mean_clv}>{signedPct(t.mean_clv, 2)}</Signed></td>
                        <td className="px-3 py-2 text-right">{pct(t.beat_close, 0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </Card>
            </div>
          )}

          {data.breakdown.length > 0 && (
            <div>
              <SectionTitle>By Stat and Side</SectionTitle>
              <Card className="overflow-x-auto">
                <table className="w-full text-sm tabular">
                  <thead className="text-left text-xs text-muted-foreground">
                    <tr className="border-b border-border">
                      {["Stat", "Side", "Bets", "Staked", "Units", "ROI", "CLV", "Beat close"].map((h, i) => (
                        <th key={h} className={cn("px-3 py-2 font-medium", i >= 2 && "text-right")}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.breakdown.map((b) => (
                      <tr key={`${b.prop_type}-${b.side}`} className="border-b border-border last:border-0">
                        <td className="px-3 py-2">{PROP_LABEL[b.prop_type]}</td>
                        <td className="px-3 py-2">{b.side}</td>
                        <td className="px-3 py-2 text-right">{b.bets}</td>
                        <td className="px-3 py-2 text-right">{b.staked.toFixed(2)}u</td>
                        <td className="px-3 py-2 text-right"><Signed value={b.pnl}>{units(b.pnl)}</Signed></td>
                        <td className="px-3 py-2 text-right"><Signed value={b.roi}>{signedPct(b.roi)}</Signed></td>
                        <td className="px-3 py-2 text-right"><Signed value={b.mean_clv}>{signedPct(b.mean_clv, 2)}</Signed></td>
                        <td className="px-3 py-2 text-right">{pct(b.beat_close, 0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </Card>
            </div>
          )}

          <div>
            <SectionTitle>Ledger</SectionTitle>
            {data.bets.length === 0 ? (
              <Empty>No prop bets yet.</Empty>
            ) : (
              <Card className="overflow-x-auto">
                <table className="w-full text-sm tabular">
                  <thead className="text-left text-xs text-muted-foreground">
                    <tr className="border-b border-border">
                      {["Placed", "Game", "Player", "Bet", "Taken", "Now", "Stake", "Edge", "Close", "CLV", "Stat", "Status", "Units"].map((h, i) => (
                        <th key={h} className={cn("px-3 py-2 font-medium", [4, 5, 6, 7, 8, 9, 10, 12].includes(i) && "text-right")}
                          title={h === "CLV" ? "Graded: vs. the closing consensus. Open (italic): vs. the consensus now." : undefined}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.bets.map((b) => (
                      <tr key={b.bet_id} className="border-b border-border last:border-0">
                        <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                          {dateTimeET(b.placed_at)}
                          {lead(b.lead_minutes) && <span className="ml-1.5 text-xs">({lead(b.lead_minutes)} pre)</span>}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2">
                          <Link href={`/games/${b.game_id}?tab=props`} className="hover:text-accent">
                            {b.away_abbr && b.home_abbr ? (
                              <span className="inline-flex items-center gap-1.5"><TeamTag abbr={b.away_abbr} /> @ <TeamTag abbr={b.home_abbr} /></span>
                            ) : b.game_id}
                          </Link>
                        </td>
                        <td className="whitespace-nowrap px-3 py-2">
                          <span className="font-medium">{b.player_name}</span>
                          <span className="ml-1.5 text-xs text-muted-foreground">{b.team}</span>
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 font-medium">{propBet(b.prop_type, b.line, b.side)}</td>
                        <td className="whitespace-nowrap px-3 py-2 text-right">
                          {american(b.price)} <span className="text-xs text-muted-foreground">{b.book}</span>
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-right">{b.status === "graded" ? <span className="text-muted-foreground">–</span> : <PriceNow b={b} />}</td>
                        <td className="px-3 py-2 text-right">{b.stake_units.toFixed(2)}u</td>
                        <td className="px-3 py-2 text-right">{signedPct(b.edge)}</td>
                        <td className="px-3 py-2 text-right text-muted-foreground">{american(b.close_price)}</td>
                        <td className="px-3 py-2 text-right">
                          {b.status === "graded" ? (
                            <Signed value={b.clv}>{signedPct(b.clv, 2)}</Signed>
                          ) : (
                            <span className="italic" title="Live: vs. the consensus now">
                              <Signed value={b.clv_now}>{signedPct(b.clv_now, 2)}</Signed>
                            </span>
                          )}
                        </td>
                        <td className="px-3 py-2 text-right">{b.stat ?? "–"}</td>
                        <td className="whitespace-nowrap px-3 py-2">
                          {b.result ? (
                            <Badge tone={RESULT_TONE[b.result]}>{b.result}</Badge>
                          ) : b.status !== "graded" ? (
                            <span title={STATUS[b.status].title}><Badge tone={STATUS[b.status].tone}>{STATUS[b.status].label}</Badge></span>
                          ) : (
                            <span className="text-muted-foreground">pending</span>
                          )}
                        </td>
                        <td className="px-3 py-2 text-right"><Signed value={b.pnl_units}>{units(b.pnl_units)}</Signed></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </Card>
            )}
          </div>
        </>
      )}
    </div>
  );
}
