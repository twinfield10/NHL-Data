"use client";

import { ChevronLeft, ChevronRight } from "lucide-react";
import { usePathname, useRouter } from "next/navigation";
import { shiftDate, todayET } from "@/lib/format";

/** Previous / next arrows around a date picker; sets ?date= on the current page. */
export default function DateNav({ date }: { date: string }) {
  const router = useRouter();
  const pathname = usePathname();
  const go = (d: string) => d && router.push(d === todayET() ? pathname : `${pathname}?date=${d}`);
  const btn = "rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground";

  return (
    <div className="flex items-center gap-2">
      <button className={btn} onClick={() => go(shiftDate(date, -1))} aria-label="Previous day">
        <ChevronLeft className="h-5 w-5" />
      </button>
      <input
        type="date"
        value={date}
        onChange={(e) => go(e.target.value)}
        className="rounded-md border border-border bg-card px-3 py-1.5 text-sm text-foreground [color-scheme:light] dark:[color-scheme:dark]"
      />
      <button className={btn} onClick={() => go(shiftDate(date, 1))} aria-label="Next day">
        <ChevronRight className="h-5 w-5" />
      </button>
    </div>
  );
}
