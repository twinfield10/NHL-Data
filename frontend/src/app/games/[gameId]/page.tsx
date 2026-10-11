"use client";

import { Suspense, use } from "react";
import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { ArrowLeft } from "lucide-react";
import GameBanner from "@/components/GameBanner";
import { gameStatus } from "@/components/GameCard";
import GameLineups from "@/components/GameLineups";
import GameProps from "@/components/GameProps";
import MarketTab from "@/components/MarketTab";
import { Card, ErrorState, Loading } from "@/components/ui";
import { useGame, useLive } from "@/lib/api";
import { longDate, timeET } from "@/lib/format";
import { gameCenterUrl, matchupColors } from "@/lib/teams";
import { useDark } from "@/lib/useDark";
import { cn } from "@/lib/utils";

const TABS = [
  { key: "market", label: "Market" },
  { key: "lineups", label: "Lineups" },
  { key: "props", label: "Props" },
] as const;
type Tab = (typeof TABS)[number]["key"];

function Game({ gameId }: { gameId: string }) {
  const { data, isLoading, error } = useGame(gameId);
  const live = useLive(data?.game.game_date, !!data);
  const search = useSearchParams();
  const router = useRouter();
  const pathname = usePathname();
  const tab: Tab = TABS.find((t) => t.key === search.get("tab"))?.key ?? "market";
  const setTab = (t: Tab) => router.replace(t === "market" ? pathname : `${pathname}?tab=${t}`, { scroll: false });
  const dark = useDark();

  if (isLoading) return <Loading />;
  if (error) return <ErrorState error={error} />;
  if (!data) return null;

  const { game, pregame: p } = data;
  const colors = matchupColors(game.away_abbr, game.home_abbr, dark);
  const now = live.data?.games.find((g) => g.game_id === game.game_id);
  const link = "underline-offset-2 hover:underline";
  const links = (
    <>
      <a href={now?.gamecenter_url ?? gameCenterUrl(game.game_id, game.game_date, game.away_abbr, game.home_abbr)}
        target="_blank" rel="noopener noreferrer" className={link}>GameCenter</a>
      {now?.espn_url && (
        <>
          <span className="mx-1.5 text-white/50">|</span>
          <a href={now.espn_url} target="_blank" rel="noopener noreferrer" className={link}
            title="ESPN game page (ESPN+ games stream from here)">Watch</a>
        </>
      )}
    </>
  );

  return (
    <div className="space-y-6">
      <Link href={`/games?date=${game.game_date}`} className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
        <ArrowLeft className="h-4 w-4" /> {longDate(game.game_date)}
      </Link>

      <Card className="overflow-hidden">
        <GameBanner
          large
          away={{ abbr: game.away_abbr, info: data.teams[game.away_abbr],
            goalie: p ? { name: p.away_starter, p: p.away_starter_p, status: p.away_starter_dfo } : undefined,
            lineup: p ? { dfoShare: p.away_dfo_share, issues: p.away_lineup_issues, gtd: p.away_game_time_decisions } : undefined }}
          home={{ abbr: game.home_abbr, info: data.teams[game.home_abbr],
            goalie: p ? { name: p.home_starter, p: p.home_starter_p, status: p.home_starter_dfo } : undefined,
            lineup: p ? { dfoShare: p.home_dfo_share, issues: p.home_lineup_issues, gtd: p.home_game_time_decisions } : undefined }}
          status={now?.state === "live" && now.home_score != null ? (
            <span className="tabular">{now.away_score}–{now.home_score}{" "}
              <span className="text-xs font-normal text-amber-300">{now.detail}</span></span>
          ) : gameStatus(game, p?.start_time ?? data.markets.start)}
          venue={game.venue_name}
          location={game.venue_location}
          links={links}
          chips={p ? [{ label: "Priced", value: timeET(p.as_of) }] : undefined}
        />
      </Card>

      <nav className="flex gap-1 border-b border-border">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={cn(
              "-mb-px border-b-2 px-4 py-2 text-sm font-medium transition-colors",
              tab === t.key ? "border-accent text-foreground" : "border-transparent text-muted-foreground hover:text-foreground"
            )}
          >
            {t.label}
          </button>
        ))}
      </nav>

      {tab === "market" && <MarketTab data={data} colors={colors} />}
      {tab === "lineups" && <GameLineups gameId={gameId} away={game.away_abbr} home={game.home_abbr} />}
      {tab === "props" && <GameProps gameId={gameId} away={game.away_abbr} home={game.home_abbr} />}
    </div>
  );
}

export default function GamePage({ params }: { params: Promise<{ gameId: string }> }) {
  const { gameId } = use(params);
  return (
    <Suspense fallback={null}>
      <Game gameId={gameId} />
    </Suspense>
  );
}
