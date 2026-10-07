"use client";

/* eslint-disable @next/next/no-img-element -- remote SVGs from the NHL CDN */
import type { ReactNode } from "react";
import { alpha, logoUrl, teamColor } from "@/lib/teams";
import { useDark } from "@/lib/useDark";

/** Table name cell in the game-banner style: team color fading right, logo bleeding off the left edge.
 *  Use in a column with ``className="h-px p-0"`` so it fills the cell. */
export default function IdentityCell({ abbr, children }: { abbr: string | null | undefined; children: ReactNode }) {
  const dark = useDark();
  const color = abbr ? teamColor(abbr, dark) : null;
  const background = color
    ? `linear-gradient(90deg, ${alpha(color, 0.5)} 0%, ${alpha(color, 0.16)} 40%, transparent 85%)`
    : undefined;
  const logo = "pointer-events-none absolute -left-3 top-1/2 h-14 w-14 -translate-y-1/2 select-none opacity-30";
  return (
    <div className="relative flex h-full min-h-10 items-center overflow-hidden py-2 pl-12 pr-4" style={{ background }}>
      {abbr && (
        <>
          <img src={logoUrl(abbr, "light")} alt="" className={`${logo} dark:hidden`} />
          <img src={logoUrl(abbr, "dark")} alt="" className={`${logo} hidden dark:block`} />
        </>
      )}
      <div className="relative min-w-0">{children}</div>
    </div>
  );
}
