from __future__ import annotations

from app.service import AppService


def _version(copies, requested):
    return {
        "copy_count_requested": requested,
        "copies": copies,
    }


def test_distinct_account_copy_count_dedupes_by_account():
    # Two copies under the SAME account count as one account.
    version = _version(
        [
            {"account_id": "account_1", "network": "github"},
            {"account_id": "account_1", "network": "github"},
            {"account_id": "account_2", "network": "github"},
        ],
        requested=3,
    )
    assert AppService.distinct_account_copy_count(version) == 2


def test_distinct_account_copy_count_separates_networks():
    # Same account_id on different networks are distinct accounts.
    version = _version(
        [
            {"account_id": "acct", "network": "github"},
            {"account_id": "acct", "network": "telegram"},
        ],
        requested=2,
    )
    assert AppService.distinct_account_copy_count(version) == 2


def test_distinct_account_copy_count_ignores_copies_without_account():
    version = _version([{"network": "github"}, {"account_id": None}], requested=1)
    assert AppService.distinct_account_copy_count(version) == 0


def test_replication_complete_requires_distinct_accounts():
    # Bound the staticmethod-style helper to a stub carrying a copy_count config.
    class _Cfg:
        copy_count = 2

    class _Stub:
        config = _Cfg()
        distinct_account_copy_count = staticmethod(AppService.distinct_account_copy_count)
        _is_version_replication_complete = AppService._is_version_replication_complete

    stub = _Stub()
    # Two copies but same account -> only 1 distinct account -> incomplete.
    same_account = _version(
        [{"account_id": "a", "network": "github"}, {"account_id": "a", "network": "github"}],
        requested=2,
    )
    assert stub._is_version_replication_complete(same_account) is False
    # Two distinct accounts -> complete.
    two_accounts = _version(
        [{"account_id": "a", "network": "github"}, {"account_id": "b", "network": "github"}],
        requested=2,
    )
    assert stub._is_version_replication_complete(two_accounts) is True
