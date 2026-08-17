from __future__ import annotations

import pytest

from app.github_api import RETRYABLE_HTTP_STATUS_CODES, GitHubClient, GitHubError, GitHubSettings
from app.rate_limit import RateLimiter


class DummyResponse:
    def __init__(self, status_code: int, payload: dict | None = None, headers: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.headers = headers or {}
        self.content = b""

    def json(self):
        return self._payload


class RecordingSession:
    """Stands in for requests.Session, returning a scripted response queue."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[dict] = []
        self.headers: dict[str, str] = {}

    def request(self, method, url, headers=None, timeout=None, **kwargs):
        self.calls.append({"method": method, "url": url, "headers": headers, "timeout": timeout, **kwargs})
        if not self._responses:
            raise AssertionError("Se hicieron mas peticiones de las previstas")
        return self._responses.pop(0)

    def close(self):
        pass


def make_client(responses, *, max_retry: int = 3, sleeps: list[float] | None = None) -> GitHubClient:
    limiter = RateLimiter(
        content_per_hour=450,
        content_per_minute=70,
        max_concurrency=3,
        clock=_FakeClock(),
        sleeper=(sleeps.append if sleeps is not None else (lambda _: None)),
    )
    client = GitHubClient(
        GitHubSettings(token="token", owner="owner-a", timeout_s=30, max_retry=max_retry, backoff_s=0),
        rate_limiter=limiter,
    )
    client._session = RecordingSession(responses)
    client._authenticated_login = "owner-a"
    return client


class _FakeClock:
    """Monotonic clock that only advances when told to."""

    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_create_repository_forces_private_and_auto_init():
    client = make_client([DummyResponse(201, {"name": "repo-test", "size": 0, "private": True})])

    info = client.create_repository("owner-a", "repo-test", private=False)

    call = client._session.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "https://api.github.com/user/repos"
    assert call["json"] == {"name": "repo-test", "private": True, "auto_init": True}
    assert info.private is True


def test_client_reuses_a_single_session_for_keep_alive():
    client = GitHubClient(GitHubSettings(token="t", owner="o", timeout_s=1, max_retry=1, backoff_s=0))
    try:
        assert client._session.headers["Authorization"] == "token t"
    finally:
        client.close()


def test_permanent_errors_are_not_retried():
    # 401 and 422 used to be in RETRYABLE_HTTP_STATUS_CODES: retrying a
    # permanent error only burns the request budget.
    assert 401 not in RETRYABLE_HTTP_STATUS_CODES
    assert 422 not in RETRYABLE_HTTP_STATUS_CODES
    assert RETRYABLE_HTTP_STATUS_CODES == {429, 500, 502, 503, 504}

    client = make_client([DummyResponse(401, text="Bad credentials")], max_retry=3)
    with pytest.raises(GitHubError, match="HTTP 401"):
        client.create_blob("owner-a", "repo", b"data")
    assert len(client._session.calls) == 1


def test_transient_errors_are_retried():
    client = make_client(
        [DummyResponse(502, text="bad gateway"), DummyResponse(201, {"sha": "abc"})],
        max_retry=3,
    )
    assert client.create_blob("owner-a", "repo", b"data") == "abc"
    assert len(client._session.calls) == 2


def test_secondary_rate_limit_is_honoured_and_drops_concurrency():
    sleeps: list[float] = []
    client = make_client(
        [
            DummyResponse(
                403,
                headers={"retry-after": "37"},
                text='{"message":"You have exceeded a secondary rate limit"}',
            ),
            DummyResponse(201, {"sha": "abc"}),
        ],
        sleeps=sleeps,
    )
    assert client.rate_limiter.gate.limit == 3

    assert client.create_blob("owner-a", "repo", b"data") == "abc"

    assert 37.0 in sleeps
    assert client.rate_limiter.gate.limit == 1
    assert client.rate_limiter.summary()["secondary_limit_hits"] == 1


def test_primary_rate_limit_sleeps_until_reset(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("app.rate_limit.time.time", lambda: 1_000.0)
    client = make_client(
        [
            DummyResponse(
                403,
                headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1120"},
                text="API rate limit exceeded",
            ),
            DummyResponse(201, {"sha": "abc"}),
        ],
        sleeps=sleeps,
    )

    assert client.create_blob("owner-a", "repo", b"data") == "abc"
    assert sleeps == [120.0]
    assert client.rate_limiter.summary()["primary_limit_hits"] == 1


def test_proactive_budget_never_exceeds_the_per_minute_ceiling():
    clock = _FakeClock()
    slept: list[float] = []

    def sleeper(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(seconds)

    limiter = RateLimiter(
        content_per_hour=450, content_per_minute=5, max_concurrency=1, clock=clock, sleeper=sleeper
    )

    for _ in range(5):
        limiter.before_request(content_generating=True)
    assert slept == []

    # The 6th content request in the same minute must block until the window
    # rolls, never exceeding the configured ceiling.
    limiter.before_request(content_generating=True)
    assert slept == [60.0]
    assert limiter.summary()["content_requests"] == 6


def test_reads_do_not_consume_the_content_budget():
    limiter = RateLimiter(content_per_hour=1, content_per_minute=1, max_concurrency=1, sleeper=lambda _: None)
    for _ in range(10):
        limiter.before_request(content_generating=False)
    summary = limiter.summary()
    assert summary["content_requests"] == 0
    assert summary["requests"] == 10
    assert summary["proactive_waits"] == 0


def test_summary_reports_observed_headers():
    limiter = RateLimiter(sleeper=lambda _: None)
    limiter.observe(
        DummyResponse(
            200,
            headers={
                "x-ratelimit-remaining": "4321",
                "x-ratelimit-limit": "5000",
                "x-ratelimit-used": "679",
                "x-ratelimit-resource": "core",
            },
        )
    )
    summary = limiter.summary()
    assert summary["last_remaining"] == 4321
    assert summary["last_limit"] == 5000
    assert summary["last_used"] == 679
    assert summary["last_resource"] == "core"


def test_ok_response_is_not_treated_as_rate_limited():
    limiter = RateLimiter(sleeper=lambda _: None)
    assert limiter.penalty_for(DummyResponse(200, headers={"x-ratelimit-remaining": "0"})) is None
