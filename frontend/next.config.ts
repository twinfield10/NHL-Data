import type { NextConfig } from "next";

// The browser calls /api/*; Next proxies it to the FastAPI app (`nhl serve`, port 8010).
const apiUrl = process.env.INTERNAL_API_URL || "http://127.0.0.1:8010";

const nextConfig: NextConfig = {
  // Old tabs: Ratings sub-tabs (2026-10-07); live edges and props moved under Bets, the ledgers
  // under Results (2026-10-09). Not permanent, so routes can keep moving.
  async redirects() {
    const moved: [string, string][] = [
      ["/edges", "/bets/markets"], ["/results/edges", "/bets/markets"], ["/props", "/bets/props"],
      ["/results/bets", "/results/markets"],
    ];
    return [
      ...["teams", "lines", "players"].map((t) => ({ source: `/${t}`, destination: `/ratings/${t}`, permanent: true })),
      ...moved.map(([source, destination]) => ({ source, destination, permanent: false })),
    ];
  },
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${apiUrl}/api/:path*` }];
  },
};

export default nextConfig;
