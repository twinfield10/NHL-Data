"use client";

import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { PricePoint } from "@/lib/types";
import { pct, timeET } from "@/lib/format";

/** Model vs market home win probability across the day's pregame runs. */
export default function PriceHistory({ points, color = "var(--accent)" }: { points: PricePoint[]; color?: string }) {
  const rows = points.map((p) => ({ t: p.as_of, model: p.p_home_win, market: p.mkt_p_home_win }));
  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={rows} margin={{ top: 8, right: 8, bottom: 0, left: -12 }}>
        <CartesianGrid stroke="var(--border)" vertical={false} />
        <XAxis dataKey="t" tickFormatter={timeET} tick={{ fontSize: 11, fill: "var(--muted-foreground)" }} stroke="var(--border)" minTickGap={30} />
        <YAxis
          domain={["auto", "auto"]}
          tickFormatter={(v: number) => pct(v, 0)}
          tick={{ fontSize: 11, fill: "var(--muted-foreground)" }}
          stroke="var(--border)"
        />
        <Tooltip
          contentStyle={{ background: "var(--card)", border: "1px solid var(--border)", borderRadius: 6, fontSize: 12 }}
          labelFormatter={(l) => `${timeET(String(l))} ET`}
          formatter={(v) => pct(Number(v))}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        <Line type="stepAfter" dataKey="model" name="Model" stroke={color} strokeWidth={2} dot={false} isAnimationActive={false} />
        <Line type="stepAfter" dataKey="market" name="Market" stroke="var(--muted-foreground)" strokeWidth={2} strokeDasharray="4 3" dot={false} connectNulls isAnimationActive={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}
