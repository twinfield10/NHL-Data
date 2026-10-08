"use client";

import Link from "next/link";
import { TeamTag } from "@/components/TeamLogo";
import { Badge, Card, Empty, ErrorState, Loading, SectionTitle, Signed, Stat } from "@/components/ui";
import { usePropBets } from "@/lib/api";
import { american, dateTimeET, pct, signedPct, units } from "@/lib/format";
import { PROP_LABEL, propBet } from "@/lib/props";
import { cn } from "@/lib/utils";

const tone = (v: number | null | undefined) => (v == null || v === 0 ? null : v > 0 ? "pos" : "neg");
const RESULT_TONE = { win: "pos", loss: "neg", void: "muted" } as const;

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
                      {["Placed", "Game", "Player", "Bet", "Price", "Book", "Stake", "Edge", "Close", "CLV", "Stat", "Result", "Units"].map((h, i) => (
                        <th key={h} className={cn("px-3 py-2 font-medium", [4, 6, 7, 8, 9, 10, 12].includes(i) && "text-right")}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.bets.map((b) => (
                      <tr key={b.bet_id} className="border-b border-border last:border-0">
                        <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{dateTimeET(b.placed_at)}</td>
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
                        <td className="px-3 py-2 text-right">{american(b.price)}</td>
                        <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{b.book}</td>
                        <td className="px-3 py-2 text-right">{b.stake_units.toFixed(2)}u</td>
                        <td className="px-3 py-2 text-right">{signedPct(b.edge)}</td>
                        <td className="px-3 py-2 text-right text-muted-foreground">{american(b.close_price)}</td>
                        <td className="px-3 py-2 text-right"><Signed value={b.clv}>{signedPct(b.clv, 2)}</Signed></td>
                        <td className="px-3 py-2 text-right">{b.stat ?? "–"}</td>
                        <td className="px-3 py-2">
                          {b.result ? <Badge tone={RESULT_TONE[b.result]}>{b.result}</Badge> : <span className="text-muted-foreground">pending</span>}
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
