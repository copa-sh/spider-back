from __future__ import annotations

from github_fs.github_api import GitHubClient, GitHubSettings


class DummyResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = ""

    def json(self):
        return self._payload


def test_create_repository_forces_private_and_auto_init(monkeypatch):
    client = GitHubClient(
        GitHubSettings(
            token="token",
            owner="owner-a",
            timeout_s=30,
            max_retry=1,
            backoff_s=1,
        )
    )
    client._authenticated_login = "owner-a"
    captured: dict[str, object] = {}

    def fake_request(method, url, headers=None, timeout=None, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = kwargs["json"]
        return DummyResponse(201, {"name": "repo-test", "size": 0, "private": True})

    monkeypatch.setattr("github_fs.github_api.requests.request", fake_request)

    info = client.create_repository("owner-a", "repo-test", private=False)

    assert captured["method"] == "POST"
    assert captured["url"] == "https://api.github.com/user/repos"
    assert captured["json"] == {"name": "repo-test", "private": True, "auto_init": True}
    assert info.private is True
