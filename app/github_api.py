from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from .rate_limit import (
    CONTENT_GENERATING_METHODS,
    DEFAULT_CONTENT_PER_HOUR,
    DEFAULT_CONTENT_PER_MINUTE,
    DEFAULT_MAX_CONCURRENCY,
    RateLimiter,
)


LOGGER = logging.getLogger("spider-back")

API_BASE_URL = "https://api.github.com"
# Asset bodies go to a different host, and as raw binary rather than base64:
# 2 GiB per asset against the Blob API's 100 MB (+33% for base64).
UPLOADS_BASE_URL = "https://uploads.github.com"

# Only transient failures belong here. 401 (bad credentials) and 422
# (unprocessable) are permanent: retrying them with a 2s->4s backoff just burns
# the request budget and delays the real error.
RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}

# A rate-limit wait is not a failed attempt, so it gets its own budget instead
# of consuming `max_retry`.
MAX_RATE_LIMIT_WAITS = 5


class GitHubError(Exception):
    pass


@dataclass(frozen=True)
class GitHubSettings:
    token: str
    owner: str
    timeout_s: int
    max_retry: int
    backoff_s: int
    content_requests_per_hour: int = DEFAULT_CONTENT_PER_HOUR
    content_requests_per_minute: int = DEFAULT_CONTENT_PER_MINUTE
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY


@dataclass(frozen=True)
class RepositoryInfo:
    owner: str
    name: str
    size_kb: int
    private: bool


class GitHubClient:
    def __init__(self, settings: GitHubSettings, rate_limiter: RateLimiter | None = None):
        self.settings = settings
        self.headers = {
            "Authorization": f"token {settings.token}",
            "Accept": "application/vnd.github+json",
        }
        # One Session per client: without it every request pays a fresh TLS
        # handshake, which on thousands of small uploads dominates the wall clock.
        self._session = requests.Session()
        self._session.headers.update(self.headers)
        self.rate_limiter = rate_limiter or RateLimiter(
            content_per_hour=settings.content_requests_per_hour,
            content_per_minute=settings.content_requests_per_minute,
            max_concurrency=settings.max_concurrency,
            label=settings.owner or "github",
        )
        self._authenticated_login: str | None = None

    def close(self) -> None:
        self._session.close()

    def _repo_url(self, owner: str, repo: str, path: str) -> str:
        return f"{API_BASE_URL}/repos/{owner}/{repo}/{path.lstrip('/')}"

    def _owner_url(self, path: str) -> str:
        return f"{API_BASE_URL}/{path.lstrip('/')}"

    def _request(
        self,
        method: str,
        url: str,
        *,
        expected_status: tuple[int, ...] = (200, 201),
        content_generating: bool | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        if content_generating is None:
            content_generating = method.upper() in CONTENT_GENERATING_METHODS

        attempt = 0
        rate_limit_waits = 0
        last_exc: Exception | None = None

        while attempt < self.settings.max_retry:
            self.rate_limiter.before_request(content_generating=content_generating)
            request_kwargs = dict(kwargs)
            # A retried upload must resend from the start of the body.
            body = request_kwargs.get("data")
            if hasattr(body, "seek"):
                body.seek(0)
            try:
                with self.rate_limiter.slot():
                    response = self._session.request(
                        method=method,
                        url=url,
                        headers=request_kwargs.pop("headers", None),
                        timeout=request_kwargs.pop("timeout", self.settings.timeout_s),
                        **request_kwargs,
                    )
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exc = exc
                attempt += 1
                if attempt >= self.settings.max_retry:
                    raise GitHubError(f"Fallo de red tras reintentos: {exc}") from exc
                time.sleep(self.settings.backoff_s * (2 ** (attempt - 1)))
                continue

            self.rate_limiter.observe(response)
            if response.status_code in expected_status:
                return response

            penalty = self.rate_limiter.penalty_for(response)
            if penalty is not None and rate_limit_waits < MAX_RATE_LIMIT_WAITS:
                rate_limit_waits += 1
                self.rate_limiter.sleep(penalty)
                continue

            attempt += 1
            if response.status_code in RETRYABLE_HTTP_STATUS_CODES and attempt < self.settings.max_retry:
                time.sleep(self.settings.backoff_s * (2 ** (attempt - 1)))
                continue
            raise GitHubError(f"HTTP {response.status_code}: {response.text[:500]}")

        raise GitHubError(f"Fallo de red: {last_exc}")

    def authenticated_login(self) -> str:
        if self._authenticated_login:
            return self._authenticated_login
        response = self._request("GET", self._owner_url("user"))
        self._authenticated_login = response.json()["login"]
        return self._authenticated_login

    def get_repository(self, owner: str, repo: str) -> RepositoryInfo:
        response = self._request("GET", self._owner_url(f"repos/{owner}/{repo}"))
        payload = response.json()
        return RepositoryInfo(
            owner=owner,
            name=repo,
            size_kb=int(payload.get("size", 0)),
            private=bool(payload.get("private", True)),
        )

    def list_managed_repositories(self, owner: str, prefix: str) -> list[RepositoryInfo]:
        repositories: list[RepositoryInfo] = []
        base_paths = []
        try:
            if owner == self.authenticated_login():
                base_paths.append("user/repos")
        except GitHubError:
            pass
        base_paths.extend([f"orgs/{owner}/repos", f"users/{owner}/repos"])
        for base_path in base_paths:
            try:
                page = 1
                while True:
                    response = self._request(
                        "GET",
                        self._owner_url(base_path),
                        params={"per_page": 100, "page": page, "type": "owner", "sort": "created"},
                    )
                    items = response.json()
                    if not items:
                        break
                    for item in items:
                        if item.get("name", "").startswith(prefix):
                            repositories.append(
                                RepositoryInfo(
                                    owner=owner,
                                    name=item["name"],
                                    size_kb=int(item.get("size", 0)),
                                    private=bool(item.get("private", True)),
                                )
                            )
                    if len(items) < 100:
                        break
                    page += 1
                if repositories:
                    break
            except GitHubError:
                continue
        repositories.sort(key=lambda item: item.name)
        return repositories

    def create_repository(self, owner: str, name: str, private: bool) -> RepositoryInfo:
        payload = {"name": name, "private": True, "auto_init": True}
        if owner == self.authenticated_login():
            response = self._request("POST", self._owner_url("user/repos"), json=payload)
        else:
            response = self._request("POST", self._owner_url(f"orgs/{owner}/repos"), json=payload)
        body = response.json()
        return RepositoryInfo(
            owner=owner,
            name=body["name"],
            size_kb=int(body.get("size", 0)),
            private=bool(body.get("private", True)),
        )

    def ensure_branch_initialized(self, owner: str, repo: str, branch: str) -> None:
        response = self._request(
            "GET",
            self._repo_url(owner, repo, f"git/ref/heads/{branch}"),
            expected_status=(200, 404, 409),
        )
        if response.status_code == 200:
            return

        init_path = ".spider-back-init"
        init_content = f"Initialized at {datetime.now(timezone.utc).isoformat()}\n".encode()
        self._request(
            "PUT",
            self._repo_url(owner, repo, f"contents/{init_path}"),
            json={
                "message": f"Initialize branch '{branch}' for spider-back",
                "content": base64.b64encode(init_content).decode(),
                "branch": branch,
            },
        )

    def create_blob(self, owner: str, repo: str, payload: bytes) -> str:
        response = self._request(
            "POST",
            self._repo_url(owner, repo, "git/blobs"),
            json={"content": base64.b64encode(payload).decode(), "encoding": "base64"},
        )
        return response.json()["sha"]

    def branch_info(self, owner: str, repo: str, branch: str) -> tuple[str | None, str | None]:
        response = self._request(
            "GET",
            self._repo_url(owner, repo, f"git/ref/heads/{branch}"),
            expected_status=(200, 404, 409),
        )
        if response.status_code != 200:
            return None, None

        commit_sha = response.json()["object"]["sha"]
        commit_response = self._request("GET", self._repo_url(owner, repo, f"git/commits/{commit_sha}"))
        return commit_sha, commit_response.json()["tree"]["sha"]

    def create_tree(self, owner: str, repo: str, entries: list[dict[str, Any]], base_tree: str | None) -> str:
        payload: dict[str, Any] = {"tree": entries}
        if base_tree:
            payload["base_tree"] = base_tree
        response = self._request("POST", self._repo_url(owner, repo, "git/trees"), json=payload)
        return response.json()["sha"]

    def create_commit(self, owner: str, repo: str, tree_sha: str, parent_sha: str | None, message: str) -> str:
        payload = {"message": message, "tree": tree_sha, "parents": [parent_sha] if parent_sha else []}
        response = self._request("POST", self._repo_url(owner, repo, "git/commits"), json=payload)
        return response.json()["sha"]

    def update_ref(self, owner: str, repo: str, branch: str, commit_sha: str) -> None:
        ref_path = f"git/refs/heads/{branch}"
        response = self._request(
            "GET", self._repo_url(owner, repo, ref_path), expected_status=(200, 404, 409)
        )
        if response.status_code == 200:
            self._request("PATCH", self._repo_url(owner, repo, ref_path), json={"sha": commit_sha, "force": False})
            return
        self._request(
            "POST",
            self._repo_url(owner, repo, "git/refs"),
            json={"ref": f"refs/heads/{branch}", "sha": commit_sha},
        )

    def commit_tree(self, owner: str, repo: str, branch: str, entries: list[dict[str, Any]], message: str) -> str | None:
        if not entries:
            return None
        self.ensure_branch_initialized(owner, repo, branch)
        head_commit_sha, base_tree_sha = self.branch_info(owner, repo, branch)
        tree_sha = self.create_tree(owner, repo, entries, base_tree_sha)
        commit_sha = self.create_commit(owner, repo, tree_sha, head_commit_sha, message)
        self.update_ref(owner, repo, branch, commit_sha)
        return commit_sha

    # ── releases ────────────────────────────────────────────────────────────
    #
    # One asset per part costs exactly one content-generating request, against
    # five per file for the blob+commit path (blob, manifest blob, tree, commit,
    # ref). A release amortises its own creation across up to 1000 assets, and
    # an asset is reachable the moment the API returns 201 — unlike a blob,
    # which is collectable garbage until a commit references it.

    def get_release_by_tag(self, owner: str, repo: str, tag: str) -> dict[str, Any] | None:
        response = self._request(
            "GET",
            self._repo_url(owner, repo, f"releases/tags/{tag}"),
            expected_status=(200, 404),
        )
        return response.json() if response.status_code == 200 else None

    def create_release(self, owner: str, repo: str, tag: str, name: str | None = None) -> dict[str, Any]:
        response = self._request(
            "POST",
            self._repo_url(owner, repo, "releases"),
            json={
                "tag_name": tag,
                "name": name or tag,
                "draft": False,
                "prerelease": False,
            },
        )
        return response.json()

    def get_or_create_release(self, owner: str, repo: str, tag: str) -> dict[str, Any]:
        existing = self.get_release_by_tag(owner, repo, tag)
        if existing is not None:
            return existing
        return self.create_release(owner, repo, tag)

    def list_release_assets(self, owner: str, repo: str, release_id: int) -> list[dict[str, Any]]:
        assets: list[dict[str, Any]] = []
        page = 1
        while True:
            response = self._request(
                "GET",
                self._repo_url(owner, repo, f"releases/{release_id}/assets"),
                params={"per_page": 100, "page": page},
            )
            items = response.json()
            assets.extend(items)
            if len(items) < 100:
                break
            page += 1
        return assets

    def upload_release_asset(
        self,
        owner: str,
        repo: str,
        release_id: int,
        name: str,
        body: Any,
        size: int,
    ) -> dict[str, Any] | None:
        """Upload one asset. Returns ``None`` when the name is already taken.

        A duplicate name is a documented 422; the caller deletes the existing
        asset and retries rather than treating it as a hard failure.
        """
        response = self._request(
            "POST",
            f"{UPLOADS_BASE_URL}/repos/{owner}/{repo}/releases/{release_id}/assets",
            params={"name": name},
            data=body,
            headers={
                **self.headers,
                "Content-Type": "application/octet-stream",
                "Content-Length": str(size),
            },
            expected_status=(201, 422),
            content_generating=True,
        )
        if response.status_code == 422:
            return None
        return response.json()

    def delete_release_asset(self, owner: str, repo: str, asset_id: int) -> None:
        self._request(
            "DELETE",
            self._repo_url(owner, repo, f"releases/assets/{asset_id}"),
            expected_status=(204, 404),
        )

    def stream_release_asset(
        self, owner: str, repo: str, asset_id: int, chunk_size: int = 4 * 1024 * 1024
    ):
        """Download an asset without holding it in memory."""
        response = self._request(
            "GET",
            self._repo_url(owner, repo, f"releases/assets/{asset_id}"),
            headers={**self.headers, "Accept": "application/octet-stream"},
            stream=True,
        )
        return response.iter_content(chunk_size=chunk_size)

    @staticmethod
    def raw_url(owner: str, repo: str, branch: str, path: str) -> str:
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"

    def fetch_bytes(self, url: str) -> bytes:
        response = self._request("GET", url, headers={"Authorization": self.headers["Authorization"]})
        return response.content

    def rate_limit_summary(self) -> dict[str, Any]:
        return self.rate_limiter.summary()
