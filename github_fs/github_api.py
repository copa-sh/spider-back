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
    owner: str
    timeout_s: int
    max_retry: int
    backoff_s: int


@dataclass(frozen=True)
class RepositoryInfo:
    owner: str
    name: str
    size_kb: int
    private: bool


class GitHubClient:
    def __init__(self, settings: GitHubSettings):
        self.settings = settings
        self.headers = {
            "Authorization": f"token {settings.token}",
            "Accept": "application/vnd.github+json",
        }
        self._authenticated_login: str | None = None

    def _repo_url(self, owner: str, repo: str, path: str) -> str:
        return f"{API_BASE_URL}/repos/{owner}/{repo}/{path.lstrip('/')}"

    def _owner_url(self, path: str) -> str:
        return f"{API_BASE_URL}/{path.lstrip('/')}"

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
                if response.status_code in RETRYABLE_HTTP_STATUS_CODES and attempt < self.settings.max_retry - 1:
                    time.sleep(self.settings.backoff_s * (2**attempt))
                    continue
                raise GitHubError(f"HTTP {response.status_code}: {response.text[:500]}")
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exc = exc
                if attempt == self.settings.max_retry - 1:
                    raise GitHubError(f"Fallo de red tras reintentos: {exc}") from exc
                time.sleep(self.settings.backoff_s * (2**attempt))
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
        ref_url = self._repo_url(owner, repo, f"git/ref/heads/{branch}")
        response = requests.get(ref_url, headers=self.headers, timeout=30)
        if response.status_code == 200:
            return
        if response.status_code not in (404, 409):
            raise GitHubError(
                f"No se pudo inspeccionar la rama '{branch}' de {owner}/{repo} (HTTP {response.status_code})."
            )

        init_path = ".github-fs-init"
        init_content = f"Initialized at {datetime.now(timezone.utc).isoformat()}\n".encode()
        payload = {
            "message": f"Initialize branch '{branch}' for github-fs",
            "content": base64.b64encode(init_content).decode(),
            "branch": branch,
        }
        init_response = requests.put(
            self._repo_url(owner, repo, f"contents/{init_path}"),
            headers=self.headers,
            timeout=30,
            json=payload,
        )
        if init_response.status_code not in (200, 201):
            raise GitHubError(
                f"No se pudo inicializar {owner}/{repo}. HTTP {init_response.status_code}: {init_response.text[:500]}"
            )

    def create_blob(self, owner: str, repo: str, payload: bytes) -> str:
        response = self._request(
            "POST",
            self._repo_url(owner, repo, "git/blobs"),
            json={"content": base64.b64encode(payload).decode(), "encoding": "base64"},
        )
        return response.json()["sha"]

    def branch_info(self, owner: str, repo: str, branch: str) -> tuple[str | None, str | None]:
        response = requests.get(self._repo_url(owner, repo, f"git/ref/heads/{branch}"), headers=self.headers, timeout=30)
        if response.status_code in (404, 409):
            return None, None
        if response.status_code != 200:
            raise GitHubError(f"No se pudo leer la rama {owner}/{repo}@{branch}: HTTP {response.status_code}")

        commit_sha = response.json()["object"]["sha"]
        commit_response = requests.get(
            self._repo_url(owner, repo, f"git/commits/{commit_sha}"),
            headers=self.headers,
            timeout=30,
        )
        if commit_response.status_code != 200:
            raise GitHubError(f"No se pudo leer el commit: HTTP {commit_response.status_code}")
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
        response = requests.get(self._repo_url(owner, repo, ref_path), headers=self.headers, timeout=30)
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

    @staticmethod
    def raw_url(owner: str, repo: str, branch: str, path: str) -> str:
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"

    def fetch_bytes(self, url: str) -> bytes:
        response = self._request("GET", url, headers={"Authorization": self.headers["Authorization"]})
        return response.content
