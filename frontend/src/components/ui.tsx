import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function Card({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={cn("rounded-lg border border-border bg-card", className)}>{children}</div>;
}

export function SectionTitle({ children, right }: { children: ReactNode; right?: ReactNode }) {
  return (
    <div className="mb-3 flex items-baseline justify-between gap-4">
      <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">{children}</h2>
      {right}
    </div>
  );
}

export function Stat({ label, value, tone }: { label: string; value: ReactNode; tone?: "pos" | "neg" | null }) {
  return (
    <Card className="px-4 py-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={cn("tabular mt-1 text-xl font-semibold", tone === "pos" && "text-positive", tone === "neg" && "text-negative")}>
        {value}
      </div>
    </Card>
  );
}

export function Badge({ children, tone = "muted" }: { children: ReactNode; tone?: "muted" | "accent" | "pos" | "neg" | "warn" }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded px-1.5 py-0.5 text-[11px] font-medium",
        tone === "muted" && "bg-muted text-muted-foreground",
        tone === "accent" && "bg-accent/15 text-accent",
        tone === "pos" && "bg-positive/15 text-positive",
        tone === "neg" && "bg-negative/15 text-negative",
        tone === "warn" && "bg-amber-500/15 text-amber-600 dark:text-amber-400"
      )}
    >
      {children}
    </span>
  );
}

export function Loading() {
  return <div className="py-16 text-center text-sm text-muted-foreground">Loading…</div>;
}

export function ErrorState({ error }: { error: unknown }) {
  return (
    <Card className="border-negative/40 p-4 text-sm text-negative">
      {error instanceof Error ? error.message : "Something went wrong."} Is the API running (<code>nhl serve</code>)?
    </Card>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <Card className="p-8 text-center text-sm text-muted-foreground">{children}</Card>;
}

/** Signed value coloured green/red. */
export function Signed({ value, children }: { value: number | null | undefined; children: ReactNode }) {
  return (
    <span className={cn("tabular", value != null && value > 0 && "text-positive", value != null && value < 0 && "text-negative")}>
      {children}
    </span>
  );
}
