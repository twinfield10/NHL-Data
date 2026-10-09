import Link from "next/link";
import type { Edge } from "@/lib/types";
import { american, pct, signedPct, timeET } from "@/lib/format";
import { cn } from "@/lib/utils";
import TeamLogo from "./TeamLogo";
import { Signed } from "./ui";

const MARKET = { moneyline: "ML", puckline: "PL", total: "Total" } as const;

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

/** Best book per side; flagged rows are highlighted and carry a stake. With ledger data, a Placed
 *  column shows the paper bet already taken on each side. */
export default function EdgeTable({ edges, showGame = true }: { edges: Edge[]; showGame?: boolean }) {
  const withBets = edges.some((e) => e.bet_price !== undefined);
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="text-left text-xs text-muted-foreground">
          <tr className="border-b border-border">
            {showGame && <th className="px-3 py-2 font-medium">Game</th>}
            <th className="px-3 py-2 font-medium">Bet</th>
            <th className="px-3 py-2 text-right font-medium">Price</th>
            <th className="px-3 py-2 font-medium">Book</th>
            {withBets && <th className="px-3 py-2 text-right font-medium" title="Placed bets: the price taken vs. the devigged consensus now">CLV</th>}
            <th className="px-3 py-2 text-right font-medium">Model</th>
            <th className="px-3 py-2 text-right font-medium">Market</th>
            <th className="px-3 py-2 text-right font-medium">Blend</th>
            <th className="px-3 py-2 text-right font-medium">Edge</th>
            <th className="px-3 py-2 text-right font-medium">Stake</th>
            {withBets && <th className="px-3 py-2 font-medium" title="Paper bet already placed: price, book, time (ET)">Placed</th>}
          </tr>
        </thead>
        <tbody className="tabular">
          {edges.map((e) => (
            <tr
              key={`${e.game_id}-${e.market}-${e.side}-${e.line}`}
              className={cn("border-b border-border last:border-0", e.flagged && "bg-positive/10")}
            >
              {showGame && (
                <td className="whitespace-nowrap px-3 py-2">
                  <Link href={`/games/${e.game_id}`} className="inline-flex items-center gap-1.5 hover:text-accent">
                    <TeamLogo abbr={e.away_abbr} className="h-5 w-5" /> {e.away_abbr} @ {e.home_abbr}
                    <TeamLogo abbr={e.home_abbr} className="h-5 w-5" />
                  </Link>
                  <span className="ml-2 text-xs text-muted-foreground">{timeET(e.start_utc)}</span>
                </td>
              )}
              <td className="whitespace-nowrap px-3 py-2">
                <span className="mr-2 text-xs text-muted-foreground">{MARKET[e.market]}</span>
                <span className="font-medium">{e.selection}</span>
              </td>
              <td className="px-3 py-2 text-right">{american(e.price)}</td>
              <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{e.book}</td>
              {withBets && (
                <td className="px-3 py-2 text-right font-semibold">
                  {e.bet_clv == null ? <span className="font-normal text-muted-foreground">–</span> : <Signed value={e.bet_clv}>{signedPct(e.bet_clv, 1)}</Signed>}
                </td>
              )}
              <td className="px-3 py-2 text-right">{pct(e.p_model_side)}</td>
              <td className="px-3 py-2 text-right">{pct(e.p_market_side)}</td>
              <td className="px-3 py-2 text-right">{pct(e.p)}</td>
              <td className="px-3 py-2 text-right font-medium">
                <Signed value={e.edge}>{signedPct(e.edge)}</Signed>
              </td>
              <td className="px-3 py-2 text-right">{e.stake_units > 0 ? `${e.stake_units.toFixed(2)}u` : ""}</td>
              {withBets && <td className="px-3 py-2"><PlacedBet price={e.bet_price} book={e.bet_book} stake={e.bet_stake} at={e.placed_at} /></td>}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
