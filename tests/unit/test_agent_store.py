"""Tests for the in-memory conversation store."""

from datetime import timedelta

from src.app.agent_api.store import InMemoryConversationStore


def test_create_and_get_conversation():
    store = InMemoryConversationStore(ttl_seconds=3 * 60 * 60)

    convo = store.create()
    cid = convo["conversation_id"]

    fetched = store.get(cid)
    assert fetched is not None
    assert fetched["conversation_id"] == cid
    assert fetched["state"]["messages"] == []
    assert fetched["state"]["format"] is None


def test_ttl_expiry_removes_conversation():
    store = InMemoryConversationStore(ttl_seconds=1)
    convo = store.create()
    cid = convo["conversation_id"]

    # Force expiration
    store._store[cid]["created_at"] -= timedelta(seconds=3600)

    assert store.get(cid) is None


def test_update_appends_messages_and_caps_history():
    store = InMemoryConversationStore(message_history_limit=3)
    convo = store.create()
    cid = convo["conversation_id"]

    for i in range(5):
        store.update(cid, messages=[{"role": "user", "content": f"msg-{i}"}])

    updated = store.get(cid)
    assert updated is not None
    assert len(updated["state"]["messages"]) == 3
    # Only most recent messages are retained
    assert updated["state"]["messages"][0]["content"] == "msg-2"

    store.update(cid, state_updates={"format": "Modern"})
    assert store.get(cid)["state"]["format"] == "Modern"

