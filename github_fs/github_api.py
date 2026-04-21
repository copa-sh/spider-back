from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


API_BASE_URL = "https://api.github.com"
RETRYABLE_HTTP_STATUS_CODES = {401, 409, 422, 429, 500, 502, 503, 504}


class GitHubError(Exception):
    pass


@dataclass(frozen=True)
class GitHubSettings:
    token: str
    repo: str
    branch: str
    uploads_prefix: str
    timeout_s: int
    max_retry: int
    backoff_s: int


class GitHubClient:
    def __init__(self, settings: GitHubSettings):
        self.settings = settings
        self.headers = {
            "Authorization": f"token {settings.token}",
            "Accept": "application/vnd.github+json",
        }

    def _url(self, path: str) -> str:
        return f"{API_BASE_URL}/repos/{self.settings.repo}/{path.lstrip('/')}"

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(self.settings.max_retry):
            try:
                request_kwargs = dict(kwargs)
                response = requests.request(
                    method=method,
                    url=url,
                    headers=request_kwargs.pop("headers", self.headers),
                    timeout=request_kwargs.pop("timeout", self.settings.timeout_s),
                    **request_kwargs,
                )
                if response.status_code in (200, 201):
                    return response
                if (
                    response.status_code in RETRYABLE_HTTP_STATUS_CODES
                    and attempt < self.settings.max_retry - 1
                ):
                    time.sleep(self.settings.backoff_s * (2**attempt))
                    continue
                raise GitHubError(f"HTTP {response.status_code}: {response.text[:500]}")
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exc = exc
                if attempt == self.settings.max_retry - 1:
                    raise GitHubError(f"Fallo de red tras reintentos: {exc}") from exc
                time.sleep(self.settings.backoff_s * (2**attempt))
        raise GitHubError(f"Fallo de red: {last_exc}")

    def ensure_branch_initialized(self) -> None:
        ref_url = self._url(f"git/ref/heads/{self.settings.branch}")
        response = requests.get(ref_url, headers=self.headers, timeout=30)
        if response.status_code == 200:
            return
        if response.status_code not in (404, 409):
            raise GitHubError(
                f"No se pudo inspeccionar la rama '{self.settings.branch}' (HTTP {response.status_code})."
            )

        init_path = ".github-fs-init"
        init_content = f"Initialized at {datetime.now(timezone.utc).isoformat()}\n".encode()
        payload = {
            "message": f"Initialize branch '{self.settings.branch}' for github-fs",
            "content": base64.b64encode(init_content).decode(),
            "branch": self.settings.branch,
        }
        init_response = requests.put(
            self._url(f"contents/{init_path}"),
            headers=self.headers,
            timeout=30,
            json=payload,
        )
        if init_response.status_code not in (200, 201):
            raise GitHubError(
                f"No se pudo inicializar el repo. HTTP {init_response.status_code}: {init_response.text[:500]}"
            )

    def create_blob(self, payload: bytes) -> str:
        response = self._request(
            "POST",
            self._url("git/blobs"),
            json={"content": base64.b64encode(payload).decode(), "encoding": "base64"},
        )
        return response.json()["sha"]

    def branch_info(self) -> tuple[str | None, str | None]:
        ref_url = self._url(f"git/ref/heads/{self.settings.branch}")
        response = requests.get(ref_url, headers=self.headers, timeout=30)
        if response.status_code in (404, 409):
            return None, None
        if response.status_code != 200:
            raise GitHubError(f"No se pudo leer la rama: HTTP {response.status_code}")

        commit_sha = response.json()["object"]["sha"]
        commit_response = requests.get(
            self._url(f"git/commits/{commit_sha}"),
            headers=self.headers,
            timeout=30,
        )
        if commit_response.status_code != 200:
            raise GitHubError(f"No se pudo leer el commit: HTTP {commit_response.status_code}")
        return commit_sha, commit_response.json()["tree"]["sha"]

    def create_tree(self, entries: list[dict[str, Any]], base_tree: str | None) -> str:
        payload: dict[str, Any] = {"tree": entries}
        if base_tree:
            payload["base_tree"] = base_tree
        response = self._request("POST", self._url("git/trees"), json=payload)
        return response.json()["sha"]

    def create_commit(self, tree_sha: str, parent_sha: str | None, message: str) -> str:
        payload = {"message": message, "tree": tree_sha, "parents": [parent_sha] if parent_sha else []}
        response = self._request("POST", self._url("git/commits"), json=payload)
        return response.json()["sha"]

    def update_ref(self, commit_sha: str) -> None:
        ref_path = f"git/refs/heads/{self.settings.branch}"
        response = requests.get(self._url(ref_path), headers=self.headers, timeout=30)
        if response.status_code == 200:
            self._request("PATCH", self._url(ref_path), json={"sha": commit_sha, "force": False})
            return
        self._request(
            "POST",
            self._url("git/refs"),
            json={"ref": f"refs/heads/{self.settings.branch}", "sha": commit_sha},
        )

    def commit_tree(self, entries: list[dict[str, Any]], message: str) -> str | None:
        if not entries:
            return None
        self.ensure_branch_initialized()
        head_commit_sha, base_tree_sha = self.branch_info()
        tree_sha = self.create_tree(entries, base_tree_sha)
        commit_sha = self.create_commit(tree_sha, head_commit_sha, message)
        self.update_ref(commit_sha)
        return commit_sha

    def raw_url(self, path: str) -> str:
        return f"https://raw.githubusercontent.com/{self.settings.repo}/{self.settings.branch}/{path}"

    def fetch_bytes(self, url: str) -> bytes:
        response = self._request("GET", url, headers={"Authorization": self.headers["Authorization"]})
        return response.content
