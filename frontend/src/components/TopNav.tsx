"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { Moon, Sun } from "lucide-react";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/games", label: "Games" },
  { href: "/ratings", label: "Ratings" },
  { href: "/bets", label: "Bets" },
  { href: "/results", label: "Results" },
];

export default function TopNav() {
  const pathname = usePathname();
  const { resolvedTheme, setTheme } = useTheme();

  return (
    <header className="sticky top-0 z-40 border-b border-border bg-card/90 backdrop-blur">
      <div className="mx-auto flex h-12 max-w-[1800px] items-center gap-3 px-4 sm:gap-6 md:px-8">
        <Link href="/games" className="shrink-0 font-semibold tracking-tight">
          NHL<span className="text-accent">·</span>Data
        </Link>
        <nav className="flex min-w-0 gap-0.5 overflow-x-auto sm:gap-1">
          {NAV.map(({ href, label }) => {
            const active = pathname.startsWith(href);
            return (
              <Link
                key={href}
                href={href}
                className={cn(
                  "shrink-0 rounded-md px-2 py-1.5 text-sm transition-colors sm:px-3",
                  active ? "bg-muted text-foreground font-medium" : "text-muted-foreground hover:text-foreground"
                )}
              >
                {label}
              </Link>
            );
          })}
        </nav>
        <button
          onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
          className="ml-auto shrink-0 rounded-md p-1.5 text-muted-foreground hover:bg-muted hover:text-foreground"
          aria-label="Toggle theme"
        >
          <Sun className="hidden h-4 w-4 dark:block" />
          <Moon className="block h-4 w-4 dark:hidden" />
        </button>
      </div>
    </header>
  );
}
