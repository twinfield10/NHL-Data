"use client";

/* eslint-disable @next/next/no-img-element -- remote SVGs from the NHL CDN */
import type { ReactNode } from "react";
import { american, pct } from "@/lib/format";
import { fillColor, logoUrl, matchupColors } from "@/lib/teams";
import type { Record3, TeamInfo } from "@/lib/types";
import { useDark } from "@/lib/useDark";
import { cn } from "@/lib/utils";

export type Tone = "warn" | "play" | null;

/** Tone colors for text on the banner's dark team fill (same in both themes). */
export const toneText = (t: Tone) =>
  t === "warn" ? "text-red-300 font-bold" : t === "play" ? "text-emerald-300 font-bold" : "";

export interface BannerSide {
  abbr: string;
  info?: TeamInfo;
  price?: number | null;
  tone?: Tone;
  goalie?: { name: string | null; p: number | null; status: string | null };
  lineup?: LineupState;
}

/** How settled a team's skater lineup is: DailyFaceoff coverage, flags and game-time decisions. */
export interface LineupState {
  /** Share of the lineup taken from DailyFaceoff (the rest is filled from usage / last game). */
  dfoShare: number | null;
  issues: string | null;
  gtd: number | null;
}

export interface Chip {
  label: string;
  value: ReactNode;
  highlight?: boolean;
}

const wlo = (r: Record3) => `${r.w}-${r.l}-${r.otl}`;
const pctg = (v: number | null) => (v == null ? ".000" : v.toFixed(3).replace(/^0/, ""));

function GoalieDot({ status, p }: { status: string | null; p: number | null }) {
  const odds = p != null ? ` · ${pct(p, 0)} to start` : "";
  const [cls, label] =
    status === "Confirmed" ? ["bg-emerald-400", "Starter confirmed"]
    : status === "Likely" ? ["bg-yellow-400", "Starter likely"]
    : ["bg-red-400", "Starter unconfirmed (model)"];
  return <span className={cn("inline-block h-1.5 w-1.5 shrink-0 rounded-full", cls)} title={label + odds} />;
}

/** Green: a projected lineup with nothing flagged. Yellow: flagged or game-time decisions.
 *  Red: no projected lineup yet (built from usage and the last game). */
function lineupStatus(l: LineupState): { cls: string; label: string; detail: string } {
  const projected = (l.dfoShare ?? 0) >= 0.5;
  const notes = [l.issues, l.gtd ? `${l.gtd} game-time decision${l.gtd > 1 ? "s" : ""}` : null].filter(Boolean).join(" · ");
  if (!projected) return { cls: "bg-red-400", label: "Model Lines", detail: "No projected lineup yet (usage / last game)" + (notes ? ` · ${notes}` : "") };
  if (notes) return { cls: "bg-yellow-400", label: "Lineup Flag", detail: notes };
  return { cls: "bg-emerald-400", label: "Projected Lines", detail: "Projected lineup, nothing flagged" };
}

function Logo({ abbr, side, large }: { abbr: string; side: "left" | "right"; large: boolean }) {
  const cls = cn(
    "pointer-events-none absolute top-1/2 -translate-y-1/2 select-none opacity-45",
    large ? "h-56 w-56" : "h-32 w-32 sm:h-40 sm:w-40",
    side === "left" ? (large ? "-left-14" : "-left-10 sm:-left-12") : large ? "-right-14" : "-right-10 sm:-right-12"
  );
  return <img src={logoUrl(abbr, "dark")} alt="" className={cls} />;
}

function TeamBlock({ s, align, large }: { s: BannerSide; align: "left" | "right"; large: boolean }) {
  const right = align === "right";
  const rec = s.info?.record;
  const showPrev = rec && rec.gp === 0 && s.info?.prev_record;
  return (
    <div className={cn("min-w-0", right ? "text-right" : "text-left", large ? (right ? "pr-28" : "pl-28") : right ? "pr-14 sm:pr-20" : "pl-14 sm:pl-20")}>
      <div className={cn("flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-white/75 sm:text-[11px]", right && "justify-end")}>
        {s.tone && (
          <span className={cn("rounded-full px-1.5 py-px text-[10px] font-bold",
            s.tone === "warn" ? "bg-red-500/30 text-red-100" : "bg-emerald-500/30 text-emerald-100")}>
            {s.tone === "warn" ? "WARN" : "PLAY"}
          </span>
        )}
        <span className="truncate">{s.info?.place ?? s.abbr}</span>
      </div>
      <div className={cn("truncate font-extrabold uppercase leading-tight tracking-tight",
        large ? "text-3xl" : (s.info?.name ?? s.abbr).length > 11 ? "text-base sm:text-lg" : "text-lg sm:text-xl",
        s.tone ? toneText(s.tone) : "text-white")}>
        {s.info?.name ?? s.abbr}
      </div>
      <div className="mt-0.5 truncate text-xs tabular">
        {rec && (
          showPrev ? (
            <span className="text-white/75">Last season {wlo(s.info!.prev_record!)}</span>
          ) : (
            <>
              <span className="font-semibold text-white">{wlo(rec)}</span>
              <span className="text-white/75"> · {pctg(rec.pts_pct)}</span>
            </>
          )
        )}
        {s.price != null && (
          <span className={cn(s.tone ? toneText(s.tone) : "text-white/75")}> · ML {american(s.price)}</span>
        )}
      </div>
      {s.info && s.info.l10.gp > 0 && (
        <div className="truncate text-[11px] text-white/75 tabular">
          L{s.info.l10.gp} {wlo(s.info.l10)}
          {s.info.streak && ` · ${s.info.streak}`}
        </div>
      )}
      {s.goalie && (
        <div className={cn("mt-0.5 flex items-center gap-1.5 text-[11px] text-white", right && "flex-row-reverse")}>
          <GoalieDot status={s.goalie.status} p={s.goalie.p} />
          <span className="truncate">{s.goalie.name ?? "TBD"}</span>
        </div>
      )}
      {s.lineup && (() => {
        const l = lineupStatus(s.lineup);
        return (
          <div className={cn("flex items-center gap-1.5 text-[11px] text-white", right && "flex-row-reverse")} title={l.detail}>
            <span className={cn("inline-block h-1.5 w-1.5 shrink-0 rounded-full", l.cls)} />
            <span className="truncate">{l.label}</span>
          </div>
        );
      })()}
    </div>
  );
}

interface GameBannerProps {
  away: BannerSide;
  home: BannerSide;
  /** Top line in the middle: start time, "Live" or the final score. */
  status: ReactNode;
  venue?: string | null;
  location?: string | null;
  chips?: Chip[];
  large?: boolean;
  className?: string;
}

/** Matchup header: dark team fills meeting in a darker middle, logos bleeding off the edges, white text
 *  in either theme, records and form. */
export default function GameBanner({ away, home, status, venue, location, chips, large = false, className }: GameBannerProps) {
  const dark = useDark();
  // Light-mode picks are the true primaries (clashes resolved); fillColor makes them carry white text.
  const colors = matchupColors(away.abbr, home.abbr, false);
  const [a, h] = [fillColor(colors.away, dark), fillColor(colors.home, dark)];
  const mid = (c: string) => `color-mix(in srgb, ${c} 55%, black)`;
  const background = `linear-gradient(90deg, ${a} 0%, ${a} 30%, ${mid(a)} 46%, ${mid(h)} 54%, ${h} 70%, ${h} 100%)`;
  return (
    <div className={cn("relative overflow-hidden text-white", className)} style={{ background }}>
      <Logo abbr={away.abbr} side="left" large={large} />
      <Logo abbr={home.abbr} side="right" large={large} />
      <div className={cn("relative grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-2 sm:gap-3", large ? "px-6 py-7" : "px-3 py-4")}>
        <TeamBlock s={away} align="left" large={large} />
        <div className="flex flex-col items-center text-center">
          <div className={cn("font-semibold", large ? "text-base" : "text-sm")}>{status}</div>
          {venue && <div className="mt-0.5 max-w-40 truncate text-[11px] text-white/75">{venue}</div>}
          {location && <div className="text-[11px] text-white/75">{location}</div>}
          {chips && chips.length > 0 && (
            <div className="mt-2 flex flex-wrap justify-center gap-1">
              {chips.map((c) => (
                <div
                  key={c.label}
                  className={cn(
                    "min-w-12 rounded-md border bg-black/30 px-1.5 py-0.5 backdrop-blur-sm",
                    c.highlight ? "border-amber-400/70" : "border-white/25"
                  )}
                >
                  <div className={cn("text-[9px] font-semibold uppercase tracking-wider", c.highlight ? "text-amber-300" : "text-white/75")}>
                    {c.label}
                  </div>
                  <div className="text-xs font-semibold tabular">{c.value}</div>
                </div>
              ))}
            </div>
          )}
        </div>
        <TeamBlock s={home} align="right" large={large} />
      </div>
    </div>
  );
}
