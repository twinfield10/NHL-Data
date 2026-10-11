"use client";

import { useState } from "react";
import Link from "next/link";
import { useQueries } from "@tanstack/react-query";
import { MarginChart, TeamGoalsChart, TotalGoalsChart } from "@/components/GoalDistributionChart";
import TeamLogo from "@/components/TeamLogo";
import { Badge, Card, Empty, ErrorState, Loading, Stat } from "@/components/ui";
import { useBets, useGame, useLiveBoxscore, usePropBets } from "@/lib/api";
import { american, timeET } from "@/lib/format";
import { gameBetStanding, propBetStanding, type BetStanding, type Standing, toWin } from "@/lib/live";
import { MARKET_LABEL } from "@/lib/markets";
import { propBet } from "@/lib/props";
import { matchupColors, OVER_COLOR, UNDER_COLOR } from "@/lib/teams";
import type { Bet, LiveGame, LiveResponse, PropBet } from "@/lib/types";
import { useDark } from "@/lib/useDark";
import { cn } from "@/lib/utils";

const STANDING: Record<Standing, { label: string; tone: "pos" | "neg" | "muted" | "warn" | "accent" }> = {
  won: { label: "Won", tone: "pos" },
  winning: { label: "Winning", tone: "pos" },
  even: { label: "Even", tone: "warn" },
  losing: { label: "Losing", tone: "neg" },
  lost: { label: "Lost", tone: "neg" },
  pending: { label: "Pending", tone: "muted" },
};
const ORDER: Standing[] = ["winning", "even", "losing", "won", "lost", "pending"];

interface Row {
  id: string;
  label: React.ReactNode;
  price: number;
  stake: number;
  s: BetStanding;
}

const started = (b: { status: string; start_utc: string | null }) =>
  b.status !== "graded" && (b.status === "closed" || (b.start_utc != null && Date.parse(b.start_utc) <= Date.now()));

/** The model's pregame distribution for the market the bets are in, loaded on demand. */
function Distribution({ g, total, puckLine }: { g: LiveGame; total: number | null; puckLine: number | null }) {
  const { data } = useGame(String(g.game_id));
  const dark = useDark();
  const goals = data?.markets.goals;
  if (!goals) return <div className="py-4 text-center text-xs text-muted-foreground">No pregame distribution for this game.</div>;
  const c = matchupColors(g.away_abbr, g.home_abbr, dark);
  const away = { label: g.away_abbr, color: c.away };
  const home = { label: g.home_abbr, color: c.home };
  if (total != null) return <TotalGoalsChart goals={goals} line={total} over={{ label: `Over ${total}`, color: OVER_COLOR }} under={{ label: `Under ${total}`, color: UNDER_COLOR }} />;
  if (puckLine != null) return <MarginChart goals={goals} line={puckLine} away={away} home={home} />;
  return <TeamGoalsChart goals={goals} away={away} home={home} />;
}

/** One started game: the score and clock, every bet on it with how it stands, and (on request)
 *  the model's pregame goal distribution. */
function LiveGameCard({ g, bets, props }: { g: LiveGame; bets: Bet[]; props: PropBet[] }) {
  const [open, setOpen] = useState(false);
  const box = useLiveBoxscore(g.game_id, props.length > 0);
  const players = new Map((box.data?.players ?? []).map((p) => [p.player_id, p]));
  const rows: Row[] = [
    ...bets.map((b) => ({
      id: b.bet_id, price: b.price, stake: b.stake_units, s: gameBetStanding(b, g),
      label: <><span className="text-xs text-muted-foreground">{MARKET_LABEL[b.market] ?? b.market}</span> <span className="font-medium">{b.selection}</span></>,
    })),
    ...props.map((b) => ({
      id: b.bet_id, price: b.price, stake: b.stake_units, s: propBetStanding(b, players.get(b.player_id), g),
      label: <><span className="font-medium">{b.player_name}</span> <span className="text-muted-foreground">{propBet(b.prop_type, b.line, b.side)}</span></>,
    })),
  ].sort((x, y) => ORDER.indexOf(x.s.standing) - ORDER.indexOf(y.s.standing));
  const totalBet = bets.find((b) => b.market === "total");
  const plBet = bets.find((b) => b.market === "puckline");
  const atRisk = rows.reduce((a, r) => a + r.stake, 0);
  const live = g.state === "live";

  return (
    <Card className="overflow-hidden">
      <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 border-b border-border px-4 py-3">
        <Link href={`/games/${g.game_id}`} className="flex items-center gap-3 hover:text-accent">
          <span className="inline-flex items-center gap-1.5 font-semibold"><TeamLogo abbr={g.away_abbr} className="h-6 w-6" />{g.away_abbr}</span>
          <span className="tabular text-xl font-bold">{g.away_score ?? "–"}–{g.home_score ?? "–"}</span>
          <span className="inline-flex items-center gap-1.5 font-semibold">{g.home_abbr}<TeamLogo abbr={g.home_abbr} className="h-6 w-6" /></span>
        </Link>
        <div className="flex items-center gap-3 text-xs">
          <span className={cn("font-semibold", live ? "text-amber-600 dark:text-amber-400" : "text-muted-foreground")}>
            {g.detail ?? (g.start_utc ? `${timeET(g.start_utc)} ET` : "")}
          </span>
          {g.home_sog != null && <span className="tabular text-muted-foreground">SOG {g.away_sog}–{g.home_sog}</span>}
          {g.gamecenter_url && <a href={g.gamecenter_url} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-foreground hover:underline">GameCenter</a>}
          {g.espn_url && <a href={g.espn_url} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-foreground hover:underline">Watch</a>}
        </div>
      </div>
      <ul className="divide-y divide-border">
        {rows.map((r) => (
          <li key={r.id} className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1 px-4 py-2 text-sm">
            <span className="min-w-0">{r.label}</span>
            <span className="flex items-center gap-3">
              <span className="tabular text-xs text-muted-foreground">{american(r.price)} · {r.stake.toFixed(2)}u to win {toWin(r.stake, r.price).toFixed(2)}u</span>
              <span className="text-xs text-muted-foreground">{r.s.note}</span>
              <Badge tone={STANDING[r.s.standing].tone}>{STANDING[r.s.standing].label}</Badge>
            </span>
          </li>
        ))}
      </ul>
      <div className="border-t border-border px-4 py-2">
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>{rows.length} bet{rows.length !== 1 && "s"} · {atRisk.toFixed(2)}u at risk</span>
          <button onClick={() => setOpen(!open)} className="hover:text-foreground">{open ? "Hide" : "Show"} Model Distribution</button>
        </div>
        {open && (
          <div className="mt-2">
            <Distribution g={g} total={totalBet?.line ?? null} puckLine={totalBet ? null : plBet?.line ?? null} />
          </div>
        )}
      </div>
    </Card>
  );
}

/** Bets on games that have started and aren't graded yet, game by game, with live scores. */
export default function LiveBetsPage() {
  const bets = useBets(null);
  const props = usePropBets();
  const pending = (bets.data?.bets ?? []).filter(started);
  const pendingProps = (props.data?.bets ?? []).filter(started);
  const dates = [...new Set([...pending, ...pendingProps].map((b) => b.game_date))].sort();
  // Usually one date; two when a late game runs past midnight ET.
  const lives = useQueries({ queries: dates.map(liveQuery) });

  if (bets.isLoading || props.isLoading) return <Loading />;
  if (bets.error) return <ErrorState error={bets.error} />;

  const games = lives.flatMap((q) => (q.data as LiveResponse | undefined)?.games ?? []);
  const byGame = new Map<number, { g: LiveGame; bets: Bet[]; props: PropBet[] }>();
  for (const g of games) byGame.set(g.game_id, { g, bets: [], props: [] });
  for (const b of pending) byGame.get(b.game_id)?.bets.push(b);
  for (const b of pendingProps) byGame.get(b.game_id)?.props.push(b);
  const shown = [...byGame.values()].filter((x) => (x.bets.length || x.props.length) && x.g.state !== "pre")
    .sort((a, b) => Number(b.g.state === "live") - Number(a.g.state === "live") || (a.g.start_utc ?? "").localeCompare(b.g.start_utc ?? ""));

  const lines = shown.flatMap((x) => x.bets.map((b) => gameBetStanding(b, x.g).standing));
  const nBets = shown.reduce((a, x) => a + x.bets.length + x.props.length, 0);
  const risk = shown.reduce((a, x) => a + [...x.bets, ...x.props].reduce((s, b) => s + b.stake_units, 0), 0);

  return (
    <div className="space-y-5">
      <h1 className="text-xl font-semibold">Live</h1>
      {shown.length > 0 && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <Stat label="Games On" value={shown.filter((x) => x.g.state === "live").length} />
          <Stat label="Bets" value={nBets} />
          <Stat label="Game Lines Up / Down" value={`${lines.filter((v) => v === "winning" || v === "won").length} / ${lines.filter((v) => v === "losing" || v === "lost").length}`} />
          <Stat label="Units at Risk" value={`${risk.toFixed(2)}u`} />
        </div>
      )}
      {lives.some((q) => q.error) && <ErrorState error={lives.find((q) => q.error)!.error} />}
      {shown.length === 0 && !lives.some((q) => q.isLoading) && (
        <Empty>No started games with pending bets. Bets show here from puck drop until they are graded.</Empty>
      )}
      <div className="grid items-start gap-4 xl:grid-cols-2">
        {shown.map((x) => <LiveGameCard key={x.g.game_id} g={x.g} bets={x.bets} props={x.props} />)}
      </div>
      <p className="text-xs text-muted-foreground">
        Scores from the NHL every 30 seconds while games are on; props from the box score. Won / Lost is settled by the score
        already (an over that has gone over); the nightly grade stays the record.
      </p>
    </div>
  );
}

function liveQuery(date: string) {
  return {
    queryKey: ["live", date],
    queryFn: async (): Promise<LiveResponse> => {
      const res = await fetch(`/api/live?date=${date}`);
      if (!res.ok) throw new Error(`API ${res.status}: live scores unavailable`);
      return res.json();
    },
    retry: false,
    refetchInterval: 30_000,
  };
}
