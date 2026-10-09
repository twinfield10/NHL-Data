"use client";

/* eslint-disable @next/next/no-img-element -- remote SVGs from the NHL CDN */
import type { ReactNode } from "react";
import { fillColor, logoUrl, team } from "@/lib/teams";
import { useDark } from "@/lib/useDark";

/** Table name cell in the game-banner style: solid team color easing darker to the right, logo
 *  bleeding off the left edge, white text in either theme.
 *  Use in a column with ``className="h-px p-0"`` so it fills the cell. */
export default function IdentityCell({ abbr, children }: { abbr: string | null | undefined; children: ReactNode }) {
  const dark = useDark();
  const color = abbr ? fillColor(team(abbr).primary, dark) : null;
  const background = color ? `linear-gradient(90deg, ${color} 0%, ${color} 35%, color-mix(in srgb, ${color} 70%, black) 100%)` : undefined;
  return (
    <div
      className={`relative flex h-full min-h-10 items-center overflow-hidden py-2 pl-12 pr-4 ${color ? "text-white" : ""}`}
      style={{ background }}
    >
      {abbr && (
        <img
          src={logoUrl(abbr, "dark")}
          alt=""
          className="pointer-events-none absolute -left-3 top-1/2 h-14 w-14 -translate-y-1/2 select-none opacity-30"
        />
      )}
      <div className="relative min-w-0">{children}</div>
    </div>
  );
}
