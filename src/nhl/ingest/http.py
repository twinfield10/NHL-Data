"""HTTP client for the NHL APIs: retries with backoff, timeouts and a global rate limit."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nhl import config

logger = logging.getLogger(__name__)

WEB_API = "https://api-web.nhle.com/v1"
STATS_API = "https://api.nhle.com/stats/rest/en"


class RateLimiter:
    """Thread-safe limiter that spaces calls at least ``1 / rps`` seconds apart."""

    def __init__(self, rps: float) -> None:
        self.interval = 1.0 / rps if rps > 0 else 0.0
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        """Block until the next request slot."""
        with self._lock:
            now = time.monotonic()
            sleep_for = self._next - now
            self._next = max(now, self._next) + self.interval
        if sleep_for > 0:
            time.sleep(sleep_for)


class SourceUnavailable(RuntimeError):
    """A third-party source refused the request (401/403/429 after retries).

    Treated as a skip, not a failure: edge refusals are intermittent and nothing here can
    act on them. A poll that is skipped is simply a gap in the archive.
    """


BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0 Safari/537.36"
)


class WebClient:
    """Rate-limited JSON/HTML client for third-party sources (books, DailyFaceoff, ESPN).

    Header names are sent exactly as given. Some edges (BetOnline/LowVig) refuse
    lowercase header names, so callers pass canonically cased headers.

    Args:
        name: Source name, used in logs and errors.
        rps: Maximum requests per second for this source.
        headers: Default headers.
        timeout: Per-request timeout in seconds.
    """

    def __init__(self, name: str, rps: float = 1.0, headers: dict[str, str] | None = None, timeout: float = 30.0) -> None:
        self.name = name
        self.timeout = timeout
        self.limiter = RateLimiter(rps)
        retry = Retry(
            total=4,
            backoff_factor=2.0,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.headers.update({"User-Agent": BROWSER_UA, **(headers or {})})

    def _send(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self.limiter.wait()
        resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
        if resp.status_code in (401, 403, 429):
            raise SourceUnavailable(f"{self.name}: {method} {url} refused with {resp.status_code}")
        resp.raise_for_status()
        return resp

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        """GET and decode JSON. Raises :class:`SourceUnavailable` on refusal."""
        return self._send("GET", url, params=params).json()

    def get_text(self, url: str, params: dict[str, Any] | None = None) -> str:
        """GET and return the body as text. Raises :class:`SourceUnavailable` on refusal."""
        return self._send("GET", url, params=params).text

    def post_json(self, url: str, payload: dict[str, Any]) -> Any:
        """POST a JSON body and decode the JSON reply. Raises :class:`SourceUnavailable` on refusal."""
        return self._send("POST", url, json=payload).json()


class NHLClient:
    """Thin JSON client shared by all ingest modules.

    Args:
        rps: Maximum requests per second across threads.
        timeout: Per-request timeout in seconds.
    """

    def __init__(self, rps: float | None = None, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self.limiter = RateLimiter(rps if rps is not None else config.API_RPS)
        retry = Retry(
            total=6,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
        self.session.mount("https://", adapter)
        self.session.headers["User-Agent"] = "nhl-xg-research/0.2 (+github.com/twinfield10/NHL-Data)"

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        """GET a URL and decode JSON.

        Raises:
            requests.HTTPError: For non-retryable or exhausted HTTP failures.
        """
        self.limiter.wait()
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # Endpoint helpers ----------------------------------------------------
    def play_by_play(self, game_id: int) -> dict[str, Any]:
        """Gamecenter play-by-play for one game."""
        return self.get_json(f"{WEB_API}/gamecenter/{game_id}/play-by-play")

    def shift_chart(self, game_id: int) -> dict[str, Any]:
        """Shift chart for one game."""
        return self.get_json(f"{STATS_API}/shiftcharts", params={"cayenneExp": f"gameId={game_id}"})

    def all_games(self) -> list[dict[str, Any]]:
        """Every game the stats API knows about (all seasons, ~75k rows, one request)."""
        return self.get_json(f"{STATS_API}/game")["data"]

    def teams(self) -> list[dict[str, Any]]:
        """Every franchise/team id with abbreviation and name."""
        return self.get_json(f"{STATS_API}/team")["data"]

    def player_bios(self, kind: str, season: int) -> list[dict[str, Any]]:
        """Skater or goalie bios (handedness, position, size) for everyone who played a season.

        Args:
            kind: ``"skater"`` or ``"goalie"``.
            season: 8-digit season id.
        """
        params = {
            "isAggregate": "false",
            "isGame": "false",
            "limit": -1,
            "cayenneExp": f"seasonId={season}",
        }
        return self.get_json(f"{STATS_API}/{kind}/bios", params=params)["data"]
