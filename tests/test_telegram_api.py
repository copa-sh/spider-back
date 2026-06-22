from __future__ import annotations

import json

import pytest

from app.telegram_api import (
    ChannelInfo,
    TelegramClient,
    TelegramError,
    TelegramFloodWaitError,
    TelegramSettings,
    UploadedChunkMeta,
)


class FakeDocument:
    def __init__(self, file_unique_id: str):
        self.file_unique_id = file_unique_id


class FakeMessage:
    def __init__(self, message_id: int, file_unique_id: str, payload: bytes):
        self.id = message_id
        self.document = FakeDocument(file_unique_id)
        self.payload = payload


class FakeChat:
    def __init__(self, chat_id: int, title: str, chat_type: str = "channel", username: str | None = None):
        self.id = chat_id
        self.title = title
        self.type = chat_type
        self.username = username


class FakeDialog:
    def __init__(self, chat: FakeChat):
        self.chat = chat


class FakeUnderlyingClient:
    def __init__(self, dialogs: list[FakeChat] | None = None):
        self.is_connected = False
        self.sent: list[tuple[int, bytes, str]] = []
        self.messages: dict[tuple[int, int], FakeMessage] = {}
        self.dialogs = [FakeDialog(chat) for chat in (dialogs or [])]
        self._next_message_id = 100
        self._next_chat_id = -1001000000000

    def connect(self):
        self.is_connected = True

    def disconnect(self):
        self.is_connected = False

    def get_me(self):
        return FakeChat(999, "me", chat_type="private")

    def get_chat(self, chat_id: int):
        return FakeChat(chat_id, "spider-test")

    def get_dialogs(self):
        return list(self.dialogs)

    def create_channel(self, title: str, description: str = ""):
        chat = FakeChat(self._next_chat_id, title)
        self._next_chat_id -= 1
        self.dialogs.append(FakeDialog(chat))
        return chat

    def send_document(self, chat_id, document, file_name, disable_notification=True, force_document=True):
        self._next_message_id += 1
        message_id = self._next_message_id
        msg = FakeMessage(message_id, f"uid-{message_id}", bytes(document))
        self.messages[(chat_id, message_id)] = msg
        self.sent.append((chat_id, bytes(document), file_name))
        return msg

    def get_messages(self, chat_id, message_id):
        return self.messages[(chat_id, message_id)]

    def download_media(self, message, in_memory=True):
        return message.payload


def make_client(underlying: FakeUnderlyingClient, *, max_retry: int = 3):
    settings = TelegramSettings(
        api_id=123,
        api_hash="hash",
        phone_number="+34600000000",
        session_name="tg_account_1",
        timeout_s=30,
        max_retry=max_retry,
        backoff_s=1,
    )
    sleeps: list[float] = []
    client = TelegramClient(
        settings,
        client_factory=lambda: underlying,
        sleeper=lambda seconds: sleeps.append(seconds),
    )
    return client, sleeps


def test_upload_chunk_returns_metadata_and_connects():
    underlying = FakeUnderlyingClient()
    client, _ = make_client(underlying)

    meta = client.upload_chunk(-1001, b"hello world", "chunk_0000.bin")

    assert isinstance(meta, UploadedChunkMeta)
    assert meta.size == len(b"hello world")
    assert meta.message_id > 0
    assert underlying.is_connected is True
    assert underlying.sent[0][2] == "chunk_0000.bin"


def test_commit_copy_uploads_chunks_then_manifest():
    underlying = FakeUnderlyingClient()
    client, _ = make_client(underlying)

    copy = client.commit_copy(
        chat_id=-1001,
        version_id="v123",
        chunks_data=[b"aaaa", b"bbbbbb"],
        chunk_filenames=["chunk_0000.bin", "chunk_0001.bin"],
    )

    assert copy["network"] == "telegram"
    assert copy["channel_id"] == -1001
    assert len(copy["chunks"]) == 2
    assert [c["index"] for c in copy["chunks"]] == [0, 1]
    assert copy["uploaded_bytes"] == 4 + 6 + len(underlying.sent[-1][1])
    # last upload is the manifest, and it is valid JSON naming the version
    manifest = json.loads(underlying.sent[-1][1].decode("utf-8"))
    assert manifest["version_id"] == "v123"
    assert len(manifest["chunks"]) == 2


def test_fetch_bytes_round_trips_uploaded_chunk():
    underlying = FakeUnderlyingClient()
    client, _ = make_client(underlying)

    meta = client.upload_chunk(-1001, b"payload-bytes", "chunk.bin")
    fetched = client.fetch_bytes(-1001, meta.message_id)

    assert fetched == b"payload-bytes"


def test_list_managed_channels_filters_by_prefix_and_type():
    underlying = FakeUnderlyingClient(
        dialogs=[
            FakeChat(-1001, "spider-model-0001"),
            FakeChat(-1002, "spider-model-0002"),
            FakeChat(-1003, "unrelated-channel"),
            FakeChat(555, "spider-model-group", chat_type="group"),
        ]
    )
    client, _ = make_client(underlying)

    channels = client.list_managed_channels("spider-model")

    assert [c.title for c in channels] == ["spider-model-0001", "spider-model-0002"]
    assert all(isinstance(c, ChannelInfo) for c in channels)


def test_flood_wait_is_respected_then_retried():
    underlying = FakeUnderlyingClient()
    client, sleeps = make_client(underlying, max_retry=3)

    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise TelegramFloodWaitError("slow down", wait_seconds=7)
        return "ok"

    result = client._request("flaky", flaky)

    assert result == "ok"
    assert calls["n"] == 2
    assert sleeps == [8]  # wait_seconds + 1


def test_request_gives_up_after_max_retry_on_connection_error():
    underlying = FakeUnderlyingClient()
    client, _ = make_client(underlying, max_retry=2)

    def always_fail():
        raise ConnectionError("boom")

    with pytest.raises(TelegramError):
        client._request("always_fail", always_fail)


def test_other_errors_are_normalized_to_telegram_error():
    underlying = FakeUnderlyingClient()
    client, _ = make_client(underlying)

    def explode():
        raise ValueError("unexpected")

    with pytest.raises(TelegramError):
        client._request("explode", explode)
