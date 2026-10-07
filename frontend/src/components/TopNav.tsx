"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { Moon, Sun } from "lucide-react";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/games", label: "Games" },
  { href: "/teams", label: "Teams" },
  { href: "/lines", label: "Lines" },
  { href: "/players", label: "Players" },
  { href: "/edges", label: "Edges" },
  { href: "/bets", label: "Bets" },
];

export default function TopNav() {
  const pathname = usePathname();
  const { resolvedTheme, setTheme } = useTheme();

  return (
    <header className="sticky top-0 z-40 border-b border-border bg-card/90 backdrop-blur">
      <div className="mx-auto flex h-12 max-w-[1800px] items-center gap-6 px-4 md:px-8">
        <Link href="/games" className="font-semibold tracking-tight">
          NHL<span className="text-accent">·</span>Data
        </Link>
        <nav className="flex gap-1">
          {NAV.map(({ href, label }) => {
            const active = pathname.startsWith(href);
            return (
              <Link
                key={href}
                href={href}
                className={cn(
                  "rounded-md px-3 py-1.5 text-sm transition-colors",
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
          className="ml-auto rounded-md p-1.5 text-muted-foreground hover:bg-muted hover:text-foreground"
          aria-label="Toggle theme"
        >
          <Sun className="hidden h-4 w-4 dark:block" />
          <Moon className="block h-4 w-4 dark:hidden" />
        </button>
      </div>
    </header>
  );
}
