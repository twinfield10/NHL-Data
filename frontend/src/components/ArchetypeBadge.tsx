import { archetypeInfo } from "@/lib/archetypes";
import { cn } from "@/lib/utils";

/** A forward archetype as a colored chip: the short code, or the full name with ``full``. */
export default function ArchetypeBadge({ name, conf, full = false }: { name: string | null | undefined; conf?: number | null; full?: boolean }) {
  const info = archetypeInfo(name);
  if (!name || !info) return <span className="text-muted-foreground">–</span>;
  const title = `${name[0].toUpperCase()}${name.slice(1)}${conf == null ? "" : ` (${Math.round(conf * 100)}%)`}: ${info.blurb}`;
  return (
    <span title={title} className={cn("inline-flex items-center rounded px-1.5 py-0.5 text-[11px] font-semibold", info.className)}>
      {full ? name : info.code}
    </span>
  );
}
