"""In-memory conversation store with TTL and message history capping."""

from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List
from uuid import uuid4

from .state import ConversationState, create_initial_state


class InMemoryConversationStore:
    """Simple in-memory store. Suitable for MVP and tests."""

    def __init__(self, ttl_seconds: int = 3 * 60 * 60, message_history_limit: int = 10):
        self.ttl_seconds = ttl_seconds
        self.message_history_limit = message_history_limit
        self._store: Dict[str, Dict] = {}

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def create(
        self, conversation_id: Optional[str] = None, initial_state: Optional[ConversationState] = None
    ) -> Dict:
        cid = conversation_id or str(uuid4())
        state = initial_state or create_initial_state()
        entry = {
            "conversation_id": cid,
            "state": state,
            "created_at": self._now(),
            "updated_at": self._now(),
        }
        self._store[cid] = entry
        return entry

    def _is_expired(self, entry: Dict) -> bool:
        return (self._now() - entry["created_at"]) > timedelta(seconds=self.ttl_seconds)

    def get(self, conversation_id: str) -> Optional[Dict]:
        entry = self._store.get(conversation_id)
        if not entry:
            return None
        if self._is_expired(entry):
            self._store.pop(conversation_id, None)
            return None
        return entry

    def update(
        self,
        conversation_id: str,
        state_updates: Optional[Dict] = None,
        messages: Optional[List[Dict]] = None,
    ) -> Optional[Dict]:
        entry = self.get(conversation_id)
        if not entry:
            return None

        if state_updates:
            entry["state"].update(state_updates)

        if messages:
            entry["state"].setdefault("messages", [])
            entry["state"]["messages"].extend(messages)
            if len(entry["state"]["messages"]) > self.message_history_limit:
                entry["state"]["messages"] = entry["state"]["messages"][-self.message_history_limit :]

        entry["updated_at"] = self._now()
        return entry

    def exists(self, conversation_id: str) -> bool:
        return self.get(conversation_id) is not None

