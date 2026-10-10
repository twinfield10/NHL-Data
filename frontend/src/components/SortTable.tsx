"use client";

import { useState, type CSSProperties, type ReactNode } from "react";
import { ArrowDown, ArrowUp } from "lucide-react";
import { cn } from "@/lib/utils";

export interface Column<T> {
  key: string;
  label: ReactNode;
  /** Header tooltip: what the number means. */
  title?: string;
  render: (row: T) => ReactNode;
  /** Sort key; columns without one aren't sortable. */
  sort?: (row: T) => number | string | null | undefined;
  align?: "left" | "right";
  className?: string;
  /** Per-cell style, e.g. a heat-scale background. */
  style?: (row: T) => CSSProperties | undefined;
}

/** Click a header to sort (numbers start descending); unless ``rank`` is false, the first column is
 *  a rank in the current order. */
export default function SortTable<T>({
  rows,
  columns,
  rowKey,
  initialSort,
  rowClassName,
  onRowClick,
  expanded,
  rank = true,
}: {
  rows: T[];
  columns: Column<T>[];
  rowKey: (row: T) => string | number;
  initialSort?: { key: string; desc: boolean };
  rowClassName?: (row: T) => string | undefined;
  onRowClick?: (row: T) => void;
  /** Content shown under a row (e.g. details for the clicked one). */
  expanded?: (row: T) => ReactNode;
  rank?: boolean;
}) {
  const [sort, setSort] = useState(initialSort);
  const col = columns.find((c) => c.key === sort?.key);
  const sorted = col?.sort
    ? [...rows].sort((a, b) => {
        const x = col.sort!(a);
        const y = col.sort!(b);
        if (x == null) return 1;
        if (y == null) return -1;
        const cmp = x < y ? -1 : x > y ? 1 : 0;
        return sort!.desc ? -cmp : cmp;
      })
    : rows;

  const toggle = (c: Column<T>) => {
    if (!c.sort) return;
    if (sort?.key === c.key) setSort({ key: c.key, desc: !sort.desc });
    else {
      const sample = rows.map(c.sort).find((v) => v != null);
      setSort({ key: c.key, desc: typeof sample === "number" });
    }
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="text-xs text-muted-foreground">
          <tr className="border-b border-border">
            {rank && <th className="w-10 px-3 py-2 text-right font-medium">#</th>}
            {columns.map((c) => (
              <th
                key={c.key}
                title={c.title}
                onClick={() => toggle(c)}
                className={cn(
                  "whitespace-nowrap px-3 py-2 font-medium",
                  c.align === "right" ? "text-right" : "text-left",
                  c.sort && "cursor-pointer select-none hover:text-foreground",
                  sort?.key === c.key && "text-foreground"
                )}
              >
                <span className="inline-flex items-center gap-0.5">
                  {c.label}
                  {sort?.key === c.key && (sort.desc ? <ArrowDown className="h-3 w-3" /> : <ArrowUp className="h-3 w-3" />)}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="tabular">
          {sorted.map((r, i) => {
            const extra = expanded?.(r);
            return [
              <tr
                key={rowKey(r)}
                onClick={onRowClick ? () => onRowClick(r) : undefined}
                className={cn("border-b border-border last:border-0", onRowClick && "cursor-pointer hover:bg-muted/60", rowClassName?.(r))}
              >
                {rank && <td className="px-3 py-2 text-right text-muted-foreground">{i + 1}</td>}
                {columns.map((c) => (
                  <td key={c.key} style={c.style?.(r)} className={cn("whitespace-nowrap px-3 py-2", c.align === "right" && "text-right", c.className)}>
                    {c.render(r)}
                  </td>
                ))}
              </tr>,
              extra ? (
                <tr key={`${rowKey(r)}-x`} className="border-b border-border bg-muted/40">
                  <td colSpan={columns.length + (rank ? 1 : 0)} className="px-3 py-3">
                    {extra}
                  </td>
                </tr>
              ) : null,
            ];
          })}
        </tbody>
      </table>
    </div>
  );
}
