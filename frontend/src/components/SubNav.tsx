"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";

export type SubNavItem = { href: string; label: string };

/** Sub-tab bar for a top-level section (Ratings, Bets, Results). */
export default function SubNav({ items }: { items: readonly SubNavItem[] }) {
  const pathname = usePathname();
  return (
    <nav className="mb-6 flex gap-1 border-b border-border">
      {items.map(({ href, label }) => {
        const active = pathname.startsWith(href);
        return (
          <Link
            key={href}
            href={href}
            className={cn(
              "-mb-px border-b-2 px-3 py-2 text-sm transition-colors",
              active ? "border-accent font-medium text-foreground" : "border-transparent text-muted-foreground hover:text-foreground"
            )}
          >
            {label}
          </Link>
        );
      })}
    </nav>
  );
}
