/* eslint-disable @next/next/no-img-element -- remote SVGs from the NHL CDN, no optimisation needed */
import { logoUrl, team } from "@/lib/teams";
import { cn } from "@/lib/utils";

/** Team logo, with the NHL's dark-background variant in dark mode. */
export default function TeamLogo({ abbr, className }: { abbr: string; className?: string }) {
  const alt = team(abbr).name || abbr;
  return (
    <>
      <img src={logoUrl(abbr, "light")} alt={alt} className={cn("h-8 w-8 shrink-0 dark:hidden", className)} />
      <img src={logoUrl(abbr, "dark")} alt={alt} className={cn("hidden h-8 w-8 shrink-0 dark:block", className)} />
    </>
  );
}

/** Small logo + abbreviation, for tables. */
export function TeamTag({ abbr }: { abbr: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <TeamLogo abbr={abbr} className="h-5 w-5" />
      {abbr}
    </span>
  );
}
