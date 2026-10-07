/* eslint-disable @next/next/no-img-element -- remote SVGs from the NHL CDN */
import type { ReactNode } from "react";
import { american, pct } from "@/lib/format";
import { logoUrl } from "@/lib/teams";
import type { Record3, TeamInfo } from "@/lib/types";
import { cn } from "@/lib/utils";

export type Tone = "warn" | "play" | null;

export const toneText = (t: Tone) =>
  t === "warn" ? "text-red-600 dark:text-red-400 font-bold" : t === "play" ? "text-emerald-600 dark:text-emerald-400 font-bold" : "";

export interface BannerSide {
  abbr: string;
  info?: TeamInfo;
  color: string;
  price?: number | null;
  tone?: Tone;
  goalie?: { name: string | null; p: number | null; status: string | null };
}

export interface Chip {
  label: string;
  value: ReactNode;
  highlight?: boolean;
}

const wlo = (r: Record3) => `${r.w}-${r.l}-${r.otl}`;
const pctg = (v: number | null) => (v == null ? ".000" : v.toFixed(3).replace(/^0/, ""));

/** Hex color with an alpha channel, for the gradient. */
const alpha = (hex: string, a: number) =>
  `${hex}${Math.round(a * 255).toString(16).padStart(2, "0")}`;

function GoalieDot({ status, p }: { status: string | null; p: number | null }) {
  const odds = p != null ? ` · ${pct(p, 0)} to start` : "";
  const [cls, label] =
    status === "Confirmed" ? ["bg-emerald-400", "Starter confirmed"]
    : status === "Likely" ? ["bg-yellow-400", "Starter likely"]
    : ["bg-red-400", "Starter unconfirmed (model)"];
  return <span className={cn("inline-block h-1.5 w-1.5 shrink-0 rounded-full", cls)} title={label + odds} />;
}

function Logo({ abbr, side, large }: { abbr: string; side: "left" | "right"; large: boolean }) {
  const cls = cn(
    "pointer-events-none absolute top-1/2 -translate-y-1/2 select-none opacity-30",
    large ? "h-40 w-40" : "h-24 w-24 sm:h-28 sm:w-28",
    side === "left" ? (large ? "-left-8" : "-left-6") : large ? "-right-8" : "-right-6"
  );
  return (
    <>
      <img src={logoUrl(abbr, "light")} alt="" className={cn(cls, "dark:hidden")} />
      <img src={logoUrl(abbr, "dark")} alt="" className={cn(cls, "hidden dark:block")} />
    </>
  );
}

function TeamBlock({ s, align, large }: { s: BannerSide; align: "left" | "right"; large: boolean }) {
  const right = align === "right";
  const rec = s.info?.record;
  const showPrev = rec && rec.gp === 0 && s.info?.prev_record;
  return (
    <div className={cn("min-w-0", right ? "text-right" : "text-left", large ? (right ? "pr-28" : "pl-28") : right ? "pr-14 sm:pr-20" : "pl-14 sm:pl-20")}>
      <div className={cn("flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground sm:text-[11px]", right && "justify-end")}>
        {s.tone && (
          <span className={cn("rounded-full px-1.5 py-px text-[10px] font-bold",
            s.tone === "warn" ? "bg-red-500/15 text-red-700 dark:text-red-400" : "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400")}>
            {s.tone === "warn" ? "WARN" : "PLAY"}
          </span>
        )}
        <span className="truncate">{s.info?.place ?? s.abbr}</span>
      </div>
      <div className={cn("truncate font-extrabold uppercase leading-tight tracking-tight",
        large ? "text-3xl" : (s.info?.name ?? s.abbr).length > 11 ? "text-base sm:text-lg" : "text-lg sm:text-xl",
        s.tone ? toneText(s.tone) : "text-foreground")}>
        {s.info?.name ?? s.abbr}
      </div>
      <div className="mt-0.5 truncate text-xs tabular">
        {rec && (
          showPrev ? (
            <span className="text-muted-foreground">Last season {wlo(s.info!.prev_record!)}</span>
          ) : (
            <>
              <span className="font-semibold text-foreground">{wlo(rec)}</span>
              <span className="text-muted-foreground"> · {pctg(rec.pts_pct)}</span>
            </>
          )
        )}
        {s.price != null && (
          <span className={cn(s.tone ? toneText(s.tone) : "text-muted-foreground")}> · ML {american(s.price)}</span>
        )}
      </div>
      {s.info && s.info.l10.gp > 0 && (
        <div className="truncate text-[11px] text-muted-foreground tabular">
          L{s.info.l10.gp} {wlo(s.info.l10)}
          {s.info.streak && ` · ${s.info.streak}`}
        </div>
      )}
      {s.goalie && (
        <div className={cn("mt-0.5 flex items-center gap-1.5 text-[11px] text-foreground", right && "flex-row-reverse")}>
          <GoalieDot status={s.goalie.status} p={s.goalie.p} />
          <span className="truncate">{s.goalie.name ?? "TBD"}</span>
        </div>
      )}
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

/** Matchup header: team colors fading into the middle, logos bleeding off the edges, records and form. */
export default function GameBanner({ away, home, status, venue, location, chips, large = false, className }: GameBannerProps) {
  const background = `linear-gradient(90deg, ${alpha(away.color, 0.5)} 0%, ${alpha(away.color, 0.14)} 28%, transparent 48%, transparent 52%, ${alpha(home.color, 0.14)} 72%, ${alpha(home.color, 0.5)} 100%)`;
  return (
    <div className={cn("relative overflow-hidden", className)} style={{ background }}>
      <Logo abbr={away.abbr} side="left" large={large} />
      <Logo abbr={home.abbr} side="right" large={large} />
      <div className={cn("relative grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-2 sm:gap-3", large ? "px-6 py-7" : "px-3 py-4")}>
        <TeamBlock s={away} align="left" large={large} />
        <div className="flex flex-col items-center text-center">
          <div className={cn("font-semibold", large ? "text-base" : "text-sm")}>{status}</div>
          {venue && <div className="mt-0.5 max-w-40 truncate text-[11px] text-muted-foreground">{venue}</div>}
          {location && <div className="text-[11px] text-muted-foreground">{location}</div>}
          {chips && chips.length > 0 && (
            <div className="mt-2 flex flex-wrap justify-center gap-1">
              {chips.map((c) => (
                <div
                  key={c.label}
                  className={cn(
                    "min-w-12 rounded-md border bg-card/70 px-1.5 py-0.5 backdrop-blur-sm",
                    c.highlight ? "border-amber-500/60" : "border-border"
                  )}
                >
                  <div className={cn("text-[9px] font-semibold uppercase tracking-wider", c.highlight ? "text-amber-600 dark:text-amber-400" : "text-muted-foreground")}>
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
