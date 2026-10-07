"use client";

import { useTheme } from "next-themes";

/** True when the resolved theme is dark (false during SSR). */
export function useDark(): boolean {
  return useTheme().resolvedTheme === "dark";
}
