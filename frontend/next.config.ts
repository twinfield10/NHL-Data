import type { NextConfig } from "next";

// The browser calls /api/*; Next proxies it to the FastAPI app (`nhl serve`, port 8010).
const apiUrl = process.env.INTERNAL_API_URL || "http://127.0.0.1:8010";

const nextConfig: NextConfig = {
  // Old top-level tabs, now sub-tabs of Ratings and Model Results (2026-10-07).
  async redirects() {
    return [
      ...["teams", "lines", "players"].map((t) => ({ source: `/${t}`, destination: `/ratings/${t}`, permanent: true })),
      ...["edges", "bets"].map((t) => ({ source: `/${t}`, destination: `/results/${t}`, permanent: true })),
    ];
  },
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${apiUrl}/api/:path*` }];
  },
};

export default nextConfig;
