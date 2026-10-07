import type { NextConfig } from "next";

// The browser calls /api/*; Next proxies it to the FastAPI app (`nhl serve`, port 8010).
const apiUrl = process.env.INTERNAL_API_URL || "http://127.0.0.1:8010";

const nextConfig: NextConfig = {
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${apiUrl}/api/:path*` }];
  },
};

export default nextConfig;
