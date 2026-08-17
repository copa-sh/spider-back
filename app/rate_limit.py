"""Proactive + reactive rate limiting for the GitHub REST API.

GitHub documents two ceilings that matter here:

* primary: 5.000 requests/hour for an authenticated PAT;
* secondary: 80 *content-generating* requests/minute and 500/hour, with at
  most 100 concurrent requests.

The scarce resource is therefore requests, not bytes. Colliding with the
secondary limit is what puts an account at risk, so this limiter is primarily
**proactive**: it holds a token bucket below the documented ceiling and blocks
before issuing a request that would exceed it. The reactive half (``Retry-After``,
``x-ratelimit-remaining: 0``, ``403 secondary rate limit``) is the safety net for
budget drift, not the main mechanism.

The budget is configurable because the documentation does not state whether
uploads to ``uploads.github.com`` consume the content-generating budget; that has
to be established from measurement, which is what :meth:`RateLimiter.summary`
feeds.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import Any, Callable


LOGGER = logging.getLogger("spider-back")

# GitHub counts POST/PUT/PATCH/DELETE against the content-generating budget.
CONTENT_GENERATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

# Documented ceilings, kept here so the defaults below can be read against them.
DOCUMENTED_CONTENT_PER_MINUTE = 80
DOCUMENTED_CONTENT_PER_HOUR = 500

# Defaults sit under the documented ceilings on purpose, to leave headroom for
# requests issued outside this limiter (other processes sharing the token).
DEFAULT_CONTENT_PER_HOUR = 450
DEFAULT_CONTENT_PER_MINUTE = 70
DEFAULT_MAX_CONCURRENCY = 3

# Cap on how long a single reactive wait may block, so a bogus reset header
# cannot park a sync for hours.
MAX_REACTIVE_WAIT_SECONDS = 15 * 60

_SECONDARY_LIMIT_MARKERS = ("secondary rate limit", "abuse detection")


class ConcurrencyGate:
    """A semaphore whose limit can be lowered while callers are waiting."""

    def __init__(self, limit: int):
        self._limit = max(1, int(limit))
        self._active = 0
        self._condition = threading.Condition()

    @property
    def limit(self) -> int:
        with self._condition:
            return self._limit

    def set_limit(self, limit: int) -> None:
        with self._condition:
            self._limit = max(1, int(limit))
            self._condition.notify_all()

    @contextmanager
    def slot(self):
        with self._condition:
            while self._active >= self._limit:
                self._condition.wait()
            self._active += 1
        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify()


class RateLimiter:
    def __init__(
        self,
        *,
        content_per_hour: int = DEFAULT_CONTENT_PER_HOUR,
        content_per_minute: int = DEFAULT_CONTENT_PER_MINUTE,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
        label: str = "github",
    ):
        self.content_per_hour = max(1, int(content_per_hour))
        self.content_per_minute = max(1, int(content_per_minute))
        self.label = label
        self._clock = clock or time.monotonic
        self._sleeper = sleeper or time.sleep
        self._lock = threading.RLock()
        self._content_events: deque[float] = deque()
        self.gate = ConcurrencyGate(max_concurrency)
        self._initial_concurrency = max(1, int(max_concurrency))
        self._stats: dict[str, Any] = {
            "requests": 0,
            "content_requests": 0,
            "proactive_waits": 0,
            "proactive_wait_seconds": 0.0,
            "reactive_waits": 0,
            "reactive_wait_seconds": 0.0,
            "secondary_limit_hits": 0,
            "primary_limit_hits": 0,
            "last_remaining": None,
            "last_limit": None,
            "last_used": None,
            "last_resource": None,
        }

    # ── proactive budget ────────────────────────────────────────────────────

    def before_request(self, *, content_generating: bool) -> None:
        """Block until issuing this request stays inside the configured budget."""

        with self._lock:
            self._stats["requests"] += 1
        if not content_generating:
            return

        while True:
            with self._lock:
                now = self._clock()
                self._trim(now)
                wait = self._wait_needed(now)
                if wait <= 0:
                    self._content_events.append(now)
                    self._stats["content_requests"] += 1
                    return
                self._stats["proactive_waits"] += 1
                self._stats["proactive_wait_seconds"] += wait
            LOGGER.info(
                "github rate-limit espera preventiva %s: %.1fs (presupuesto %s/min, %s/h)",
                self.label,
                wait,
                self.content_per_minute,
                self.content_per_hour,
            )
            self._sleeper(wait)

    def _trim(self, now: float) -> None:
        horizon = now - 3600.0
        while self._content_events and self._content_events[0] <= horizon:
            self._content_events.popleft()

    def _wait_needed(self, now: float) -> float:
        wait = 0.0
        if len(self._content_events) >= self.content_per_hour:
            wait = max(wait, self._content_events[0] + 3600.0 - now)

        minute_horizon = now - 60.0
        in_minute = 0
        for stamp in reversed(self._content_events):
            if stamp <= minute_horizon:
                break
            in_minute += 1
        if in_minute >= self.content_per_minute:
            oldest_in_minute = self._content_events[len(self._content_events) - in_minute]
            wait = max(wait, oldest_in_minute + 60.0 - now)
        return wait

    @contextmanager
    def slot(self):
        with self.gate.slot():
            yield

    # ── reactive handling ───────────────────────────────────────────────────

    def observe(self, response: Any) -> None:
        """Record the rate-limit headers of a response.

        Every response is logged: the budget can only be tuned against measured
        consumption, and this is where that measurement comes from.
        """

        headers = getattr(response, "headers", None) or {}
        remaining = _as_int(headers.get("x-ratelimit-remaining"))
        limit = _as_int(headers.get("x-ratelimit-limit"))
        used = _as_int(headers.get("x-ratelimit-used"))
        resource = headers.get("x-ratelimit-resource")
        with self._lock:
            if remaining is not None:
                self._stats["last_remaining"] = remaining
            if limit is not None:
                self._stats["last_limit"] = limit
            if used is not None:
                self._stats["last_used"] = used
            if resource:
                self._stats["last_resource"] = resource
        LOGGER.debug(
            "github rate-limit %s status=%s resource=%s remaining=%s/%s used=%s reset=%s retry_after=%s",
            self.label,
            getattr(response, "status_code", None),
            resource,
            remaining,
            limit,
            used,
            headers.get("x-ratelimit-reset"),
            headers.get("retry-after"),
        )

    def penalty_for(self, response: Any) -> float | None:
        """Seconds to wait before retrying, or ``None`` if not rate limited.

        Honours ``Retry-After`` first (GitHub sends it on secondary limits),
        then ``x-ratelimit-remaining: 0`` plus ``x-ratelimit-reset``.
        """

        status = getattr(response, "status_code", None)
        headers = getattr(response, "headers", None) or {}
        body = _response_text(response)
        is_secondary = status in (403, 429) and any(
            marker in body.lower() for marker in _SECONDARY_LIMIT_MARKERS
        )

        retry_after = _as_float(headers.get("retry-after"))
        remaining = _as_int(headers.get("x-ratelimit-remaining"))
        reset_at = _as_float(headers.get("x-ratelimit-reset"))
        is_primary = status in (403, 429) and remaining == 0

        if not (is_secondary or is_primary or (status == 429 and retry_after is not None)):
            return None

        wait = 0.0
        if retry_after is not None:
            wait = retry_after
        elif reset_at is not None:
            # x-ratelimit-reset is wall-clock epoch seconds, not monotonic.
            wait = reset_at - time.time()
        if wait <= 0:
            wait = 60.0
        wait = min(wait, MAX_REACTIVE_WAIT_SECONDS)

        with self._lock:
            self._stats["reactive_waits"] += 1
            self._stats["reactive_wait_seconds"] += wait
            if is_secondary:
                self._stats["secondary_limit_hits"] += 1
            if is_primary:
                self._stats["primary_limit_hits"] += 1

        if is_secondary and self.gate.limit > 1:
            # One trip is enough: back down to serial and stay there for the run.
            LOGGER.warning(
                "github rate-limit secundario %s: concurrencia %s -> 1", self.label, self.gate.limit
            )
            self.gate.set_limit(1)

        LOGGER.warning(
            "github rate-limit %s alcanzado (status=%s secundario=%s): esperando %.1fs",
            self.label,
            status,
            is_secondary,
            wait,
        )
        return wait

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            self._sleeper(seconds)

    # ── measurement ─────────────────────────────────────────────────────────

    def summary(self) -> dict[str, Any]:
        with self._lock:
            snapshot = dict(self._stats)
        snapshot["label"] = self.label
        snapshot["content_per_hour_budget"] = self.content_per_hour
        snapshot["content_per_minute_budget"] = self.content_per_minute
        snapshot["concurrency"] = self.gate.limit
        snapshot["initial_concurrency"] = self._initial_concurrency
        snapshot["proactive_wait_seconds"] = round(snapshot["proactive_wait_seconds"], 1)
        snapshot["reactive_wait_seconds"] = round(snapshot["reactive_wait_seconds"], 1)
        return snapshot

    def reset_stats(self) -> None:
        with self._lock:
            for key in (
                "requests",
                "content_requests",
                "proactive_waits",
                "reactive_waits",
                "secondary_limit_hits",
                "primary_limit_hits",
            ):
                self._stats[key] = 0
            self._stats["proactive_wait_seconds"] = 0.0
            self._stats["reactive_wait_seconds"] = 0.0


def _as_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _response_text(response: Any) -> str:
    text = getattr(response, "text", "")
    if isinstance(text, bytes):  # pragma: no cover - requests always decodes
        return text.decode("utf-8", errors="replace")
    return text or ""
