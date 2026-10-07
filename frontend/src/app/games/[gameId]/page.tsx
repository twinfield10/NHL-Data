"use client";

import { use } from "react";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import EdgeTable from "@/components/EdgeTable";
import Lineup from "@/components/Lineup";
import PriceHistory from "@/components/PriceHistory";
import ProbBar from "@/components/ProbBar";
import TeamLogo from "@/components/TeamLogo";
import GameBanner from "@/components/GameBanner";
import { gameStatus } from "@/components/GameCard";
import { Badge, Card, Empty, ErrorState, Loading, SectionTitle, Signed, Stat } from "@/components/ui";
import { useGame } from "@/lib/api";
import { fairAmerican, longDate, pct, signedPct, timeET } from "@/lib/format";
import { matchupColors, team } from "@/lib/teams";
import type { GoalieProb } from "@/lib/types";
import { useDark } from "@/lib/useDark";

function Goalies({ abbr, rows }: { abbr: string; rows: GoalieProb[] }) {
  return (
    <Card className="p-4">
      <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
        <TeamLogo abbr={abbr} className="h-6 w-6" /> {team(abbr).name || abbr}
      </div>
      {rows.length === 0 ? (
        <div className="text-xs text-muted-foreground">No projection.</div>
      ) : (
        <table className="w-full text-sm tabular">
          <thead className="text-left text-xs text-muted-foreground">
            <tr>
              <th className="py-1 font-medium">Goalie</th>
              <th className="py-1 text-right font-medium">Start</th>
              <th className="py-1 text-right font-medium">Model only</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.player_id ?? "other"}>
                <td className="py-1">
                  {r.player_name} {r.dfo_status && <Badge tone={r.dfo_status === "Confirmed" ? "pos" : "muted"}>{r.dfo_status}</Badge>}
                </td>
                <td className="py-1 text-right font-medium">{pct(r.p_start, 0)}</td>
                <td className="py-1 text-right text-muted-foreground">{pct(r.p_model, 0)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

export default function GamePage({ params }: { params: Promise<{ gameId: string }> }) {
  const { gameId } = use(params);
  const { data, isLoading, error } = useGame(gameId);
  const dark = useDark();

  if (isLoading) return <Loading />;
  if (error) return <ErrorState error={error} />;
  if (!data) return null;

  const { game, pregame: p } = data;
  const colors = matchupColors(game.away_abbr, game.home_abbr, dark);

  return (
    <div className="space-y-6">
      <Link href={`/games?date=${game.game_date}`} className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
        <ArrowLeft className="h-4 w-4" /> {longDate(game.game_date)}
      </Link>

      <Card className="overflow-hidden">
        <GameBanner
          large
          away={{ abbr: game.away_abbr, info: data.teams[game.away_abbr], color: colors.away,
            goalie: p ? { name: p.away_starter, p: p.away_starter_p, status: p.away_starter_dfo } : undefined }}
          home={{ abbr: game.home_abbr, info: data.teams[game.home_abbr], color: colors.home,
            goalie: p ? { name: p.home_starter, p: p.home_starter_p, status: p.home_starter_dfo } : undefined }}
          status={gameStatus(game, p?.start_time ?? null)}
          venue={game.venue_name}
          location={game.venue_location}
          chips={p ? [{ label: "Priced", value: timeET(p.as_of) }] : undefined}
        />
      </Card>

      {!p ? (
        <Empty>No pregame price for this game yet.</Empty>
      ) : (
        <>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
            <Stat label={`${game.home_abbr} win (model)`} value={<>{pct(p.p_home_win)} <span className="text-sm text-muted-foreground">{fairAmerican(p.p_home_win)}</span></>} />
            <Stat label={`${game.home_abbr} win (market)`} value={pct(p.mkt_p_home_win)} />
            <Stat label="Model − market" value={<Signed value={p.edge_home_win}>{signedPct(p.edge_home_win)}</Signed>} />
            <Stat label="Projected score" value={`${p.mean_away_goals.toFixed(2)}–${p.mean_home_goals.toFixed(2)}`} />
            <Stat label="Overtime" value={pct(p.p_overtime, 0)} />
          </div>

          <Card className="p-4">
            <ProbBar pHome={p.p_home_win} pMarket={p.mkt_p_home_win} awayColor={colors.away} homeColor={colors.home} label={`${game.away_abbr} · model (bar) vs market (tick) · ${game.home_abbr}`} />
            <div className="mt-3 grid grid-cols-2 gap-2 text-xs text-muted-foreground tabular sm:grid-cols-4">
              <span>{game.home_abbr} −1.5: {pct(p.p_home_minus_1_5)}</span>
              <span>{game.away_abbr} −1.5: {pct(p.p_away_minus_1_5)}</span>
              <span>Over 5.5: {pct(p["p_over_5.5"])}</span>
              <span>Over 6.5: {pct(p["p_over_6.5"])}</span>
            </div>
          </Card>
        </>
      )}

      {data.history.length > 1 && (
        <div>
          <SectionTitle>{game.home_abbr} win probability through the day</SectionTitle>
          <Card className="p-4">
            <PriceHistory points={data.history} color={colors.home} />
          </Card>
        </div>
      )}

      {data.edges.length > 0 && (
        <div>
          <SectionTitle>{data.edges[0].point === "close" ? "Edges at the close" : "Edges"}</SectionTitle>
          <Card>
            <EdgeTable edges={data.edges} showGame={false} />
          </Card>
        </div>
      )}

      <div>
        <SectionTitle>Starting goalies</SectionTitle>
        <div className="grid gap-4 md:grid-cols-2">
          <Goalies abbr={game.away_abbr} rows={data.goalies.away} />
          <Goalies abbr={game.home_abbr} rows={data.goalies.home} />
        </div>
      </div>

      <div>
        <SectionTitle>Projected lineups</SectionTitle>
        <div className="grid gap-4 md:grid-cols-2">
          <Lineup abbr={game.away_abbr} players={data.lineups.away} />
          <Lineup abbr={game.home_abbr} players={data.lineups.home} />
        </div>
      </div>
    </div>
  );
}
