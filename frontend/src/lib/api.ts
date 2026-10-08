import { useQuery } from "@tanstack/react-query";
import type {
  BetsResponse, EdgesResponse, GameResponse, LinesResponse, PlayerContextResponse, PlayersResponse, PlayerStyleResponse, SlateResponse, TeamMatchupsResponse, TeamsResponse,
} from "./types";

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) {
    let detail = res.statusText;
    try {
      detail = (await res.json()).detail ?? detail;
    } catch {}
    throw new Error(`API ${res.status}: ${detail}`);
  }
  return res.json();
}

const dateParam = (date?: string | null) => (date ? `?date=${date}` : "");

export const useSlate = (date?: string | null) =>
  useQuery({ queryKey: ["slate", date], queryFn: () => apiFetch<SlateResponse>(`/api/slate${dateParam(date)}`) });

export const useGame = (gameId: string) =>
  useQuery({ queryKey: ["game", gameId], queryFn: () => apiFetch<GameResponse>(`/api/games/${gameId}`) });

export const useEdges = (date?: string | null) =>
  useQuery({ queryKey: ["edges", date], queryFn: () => apiFetch<EdgesResponse>(`/api/edges${dateParam(date)}`) });

export const useBets = (kind?: string | null) =>
  useQuery({
    queryKey: ["bets", kind],
    queryFn: () => apiFetch<BetsResponse>(`/api/bets${kind ? `?kind=${kind}` : ""}`),
  });

export const usePlayerRatings = () =>
  useQuery({ queryKey: ["ratings", "players"], queryFn: () => apiFetch<PlayersResponse>("/api/ratings/players") });

export const useTeamRatings = () =>
  useQuery({ queryKey: ["ratings", "teams"], queryFn: () => apiFetch<TeamsResponse>("/api/ratings/teams") });

export const useLineRatings = (season?: number | null) =>
  useQuery({
    queryKey: ["ratings", "lines", season],
    queryFn: () => apiFetch<LinesResponse>(`/api/ratings/lines${season ? `?season=${season}` : ""}`),
  });

export const usePlayerContext = (playerId: number | null) =>
  useQuery({
    queryKey: ["ratings", "player-context", playerId],
    queryFn: () => apiFetch<PlayerContextResponse>(`/api/ratings/players/${playerId}/context`),
    enabled: playerId != null,
  });

export const usePlayerStyle = (playerId: number | null) =>
  useQuery({
    queryKey: ["ratings", "player-style", playerId],
    queryFn: () => apiFetch<PlayerStyleResponse>(`/api/ratings/players/${playerId}/style`),
    enabled: playerId != null,
  });

export const useTeamMatchups = (teamId: number | null, season?: number | null) =>
  useQuery({
    queryKey: ["ratings", "team-matchups", teamId, season],
    queryFn: () => apiFetch<TeamMatchupsResponse>(`/api/ratings/teams/${teamId}/matchups${season ? `?season=${season}` : ""}`),
    enabled: teamId != null,
  });
