"""FastAPI app for the site. Run with ``nhl serve`` (or ``uvicorn nhl.api.main:app``)."""

from __future__ import annotations

import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from nhl import config
from nhl.api.deps import get_data
from nhl.api.routers import betting, context, games, ratings, slate, style
from nhl.api.serialize import today_et


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Warm the expensive caches in the background so the first ratings request doesn't pay."""
    threading.Thread(target=lambda: get_data().warm(today_et()), daemon=True).start()
    yield


app = FastAPI(title="NHL-Data API", description="Pregame prices, edges and bets", version="0.1.0", lifespan=lifespan)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("NHL_CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(slate.router)
app.include_router(games.router)
app.include_router(betting.router)
app.include_router(ratings.router)
app.include_router(context.router)
app.include_router(style.router)


@app.get("/api/health")
def health() -> dict:
    """Liveness plus the bucket and today's (Eastern) date."""
    return {"status": "ok", "bucket": config.S3_BUCKET, "today": today_et().isoformat()}
