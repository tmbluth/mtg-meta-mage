"""End-to-end integration tests for Agent API with real data and external APIs"""

import os
import pytest
import logging
import json
from typing import Dict, List
from dotenv import load_dotenv
import httpx
import re

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


def parse_sse_stream(response_text: str) -> List[Dict[str, str]]:
    """Parse SSE stream into list of events."""
    events = []
    current_event = {}
    for line in response_text.split("\n"):
        if line.startswith("event:"):
            current_event["event"] = line[6:].strip()
        elif line.startswith("data:"):
            current_event["data"] = line[5:].strip()
            events.append(current_event.copy())
            current_event = {}
    return events


@pytest.mark.integration
class TestAgentAPIFullConversationFlow:
    """E2E integration tests for full conversation flow"""

    @pytest.fixture
    def base_url(self):
        """Base URL for agent API. Defaults to port 8001 per README."""
        return os.getenv("AGENT_API_BASE_URL", "http://localhost:8001")

    @pytest.fixture
    async def client(self, base_url):
        """HTTP client for making requests."""
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
            yield client

    async def test_new_conversation_flow(self, client, load_test_data):
        """Test starting a new conversation with format and meta research query."""
        # Step 1: Get welcome information
        welcome_response = await client.get("/welcome")
        assert welcome_response.status_code == 200
        welcome_data = welcome_response.json()
        assert "available_formats" in welcome_data
        assert "workflows" in welcome_data
        assert len(welcome_data["available_formats"]) > 0

        # Step 2: Start conversation with format context
        chat_response = await client.post(
            "/chat",
            json={
                "message": "What are the top decks in Modern?",
                "conversation_id": None,
                "context": {"format": "Modern", "days": 30},
            },
        )
        assert chat_response.status_code == 200
        assert chat_response.headers["content-type"] == "text/event-stream; charset=utf-8"

        # Parse SSE stream
        events = parse_sse_stream(chat_response.text)
        assert len(events) > 0

        # Verify event types
        event_types = [e["event"] for e in events]
        assert "metadata" in event_types
        assert "content" in event_types
        assert "state" in event_types
        assert "done" in event_types

        # Extract conversation_id from metadata event
        metadata_event = next(e for e in events if e["event"] == "metadata")
        metadata_data = json.loads(metadata_event["data"])
        conversation_id = metadata_data.get("conversation_id")
        assert conversation_id is not None

        # Step 3: Retrieve conversation state
        convo_response = await client.get(f"/conversations/{conversation_id}")
        assert convo_response.status_code == 200
        convo_data = convo_response.json()
        assert convo_data["conversation_id"] == conversation_id
        assert convo_data["state"]["format"] == "Modern"
        assert convo_data["state"]["days"] == 30
        assert len(convo_data["messages"]) >= 2  # User message + assistant response

    async def test_conversation_continuation(self, client, load_test_data):
        """Test continuing an existing conversation."""
        # Start first conversation
        chat1_response = await client.post(
            "/chat",
            json={
                "message": "Show me Modern archetypes",
                "conversation_id": None,
                "context": {"format": "Modern", "days": 30},
            },
        )
        assert chat1_response.status_code == 200

        # Extract conversation_id
        events1 = parse_sse_stream(chat1_response.text)
        metadata1 = next(e for e in events1 if e["event"] == "metadata")
        conversation_id = json.loads(metadata1["data"])["conversation_id"]

        # Continue conversation
        chat2_response = await client.post(
            "/chat",
            json={
                "message": "What about Pioneer?",
                "conversation_id": conversation_id,
                "context": {"format": "Pioneer", "days": 30},
            },
        )
        assert chat2_response.status_code == 200

        # Verify conversation state updated
        convo_response = await client.get(f"/conversations/{conversation_id}")
        assert convo_response.status_code == 200
        convo_data = convo_response.json()
        assert len(convo_data["messages"]) >= 4  # 2 user + 2 assistant messages

    async def test_formats_endpoint(self, client, load_test_data):
        """Test GET /formats endpoint."""
        response = await client.get("/formats")
        assert response.status_code == 200
        data = response.json()
        assert "formats" in data
        assert isinstance(data["formats"], list)
        assert len(data["formats"]) > 0

    async def test_archetypes_endpoint(self, client, load_test_data):
        """Test GET /archetypes endpoint."""
        response = await client.get("/archetypes?format=Modern")
        assert response.status_code == 200
        data = response.json()
        assert "format" in data
        assert "archetypes" in data
        assert data["format"] == "Modern"
        assert isinstance(data["archetypes"], list)


@pytest.mark.integration
class TestAgentAPIWorkflowInterleaving:
    """E2E integration tests for workflow interleaving scenarios"""

    @pytest.fixture
    def base_url(self):
        """Base URL for agent API. Defaults to port 8001 per README."""
        return os.getenv("AGENT_API_BASE_URL", "http://localhost:8001")

    @pytest.fixture
    async def client(self, base_url):
        """HTTP client for making requests."""
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
            yield client

    async def test_meta_to_deck_coaching_transition(self, client, load_test_data):
        """Test switching from meta research to deck coaching workflow."""
        conversation_id = None

        # Step 1: Start with meta research query
        meta_response = await client.post(
            "/chat",
            json={
                "message": "What's the Modern meta?",
                "conversation_id": conversation_id,
                "context": {"format": "Modern", "days": 30},
            },
        )
        assert meta_response.status_code == 200

        # Extract conversation_id
        events = parse_sse_stream(meta_response.text)
        metadata = next(e for e in events if e["event"] == "metadata")
        conversation_id = json.loads(metadata["data"])["conversation_id"]

        # Step 2: Switch to deck coaching with deck text
        sample_deck = """4 Lightning Bolt
4 Ragavan, Nimble Pilferer
4 Dragon's Rage Channeler
4 Murktide Regent
4 Counterspell
4 Consider
4 Expressive Iteration
2 Spell Pierce
2 Unholy Heat
2 Subtlety
1 Brazen Borrower
1 Jace, the Mind Sculptor
4 Scalding Tarn
4 Flooded Strand
2 Steam Vents
2 Volcanic Island
1 Mountain
1 Island
4 Misty Rainforest
4 Polluted Delta

Sideboard:
2 Engineered Explosives
2 Relic of Progenitus
2 Blood Moon
2 Dress Down
2 Subtlety
2 Flusterstorm
1 Brazen Borrower
2 Surgical Extraction"""

        deck_response = await client.post(
            "/chat",
            json={
                "message": "How should I optimize this deck?",
                "conversation_id": conversation_id,
                "context": {
                    "format": "Modern",
                    "archetype": "Murktide",
                    "deck_text": sample_deck,
                },
            },
        )
        assert deck_response.status_code == 200

        # Verify conversation state includes deck
        convo_response = await client.get(f"/conversations/{conversation_id}")
        assert convo_response.status_code == 200
        convo_data = convo_response.json()
        assert convo_data["state"]["has_deck"] is True
        assert convo_data["state"]["archetype"] == "Murktide"

    async def test_deck_to_meta_transition(self, client, load_test_data):
        """Test switching from deck coaching back to meta research."""
        conversation_id = None

        # Step 1: Start with deck coaching
        sample_deck = """4 Lightning Bolt
4 Ragavan, Nimble Pilferer
4 Dragon's Rage Channeler
4 Murktide Regent
4 Counterspell
4 Consider
4 Expressive Iteration
2 Spell Pierce
2 Unholy Heat
2 Subtlety
1 Brazen Borrower
1 Jace, the Mind Sculptor
4 Scalding Tarn
4 Flooded Strand
2 Steam Vents
2 Volcanic Island
1 Mountain
1 Island
4 Misty Rainforest
4 Polluted Delta

Sideboard:
2 Engineered Explosives
2 Relic of Progenitus
2 Blood Moon
2 Dress Down
2 Subtlety
2 Flusterstorm
1 Brazen Borrower
2 Surgical Extraction"""

        deck_response = await client.post(
            "/chat",
            json={
                "message": "Analyze my deck",
                "conversation_id": conversation_id,
                "context": {
                    "format": "Modern",
                    "archetype": "Murktide",
                    "deck_text": sample_deck,
                },
            },
        )
        assert deck_response.status_code == 200

        # Extract conversation_id
        events = parse_sse_stream(deck_response.text)
        metadata = next(e for e in events if e["event"] == "metadata")
        conversation_id = json.loads(metadata["data"])["conversation_id"]

        # Step 2: Switch back to meta research
        meta_response = await client.post(
            "/chat",
            json={
                "message": "Now show me the Pioneer meta",
                "conversation_id": conversation_id,
                "context": {"format": "Pioneer", "days": 30},
            },
        )
        assert meta_response.status_code == 200

        # Verify conversation state preserved deck but switched format
        convo_response = await client.get(f"/conversations/{conversation_id}")
        assert convo_response.status_code == 200
        convo_data = convo_response.json()
        assert convo_data["state"]["format"] == "Pioneer"
        # Deck should still be present
        assert convo_data["state"]["has_deck"] is True


@pytest.mark.integration
class TestAgentAPIBlockingDependencies:
    """E2E integration tests for blocking dependency enforcement"""

    @pytest.fixture
    def base_url(self):
        """Base URL for agent API. Defaults to port 8001 per README."""
        return os.getenv("AGENT_API_BASE_URL", "http://localhost:8001")

    @pytest.fixture
    async def client(self, base_url):
        """HTTP client for making requests."""
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
            yield client

    async def test_format_required_blocking(self, client, load_test_data):
        """Test that format is required before proceeding."""
        response = await client.post(
            "/chat",
            json={
                "message": "What are the top decks?",
                "conversation_id": None,
                "context": {},  # No format provided
            },
        )
        assert response.status_code == 200

        # Parse SSE to get assistant response
        events = parse_sse_stream(response.text)
        content_events = [e for e in events if e["event"] == "content"]
        assert len(content_events) > 0

        # Check that response indicates format is required
        content_data = json.loads(content_events[0]["data"])
        assert "format" in content_data["text"].lower() or "required" in content_data["text"].lower()

    async def test_days_required_for_meta_research(self, client, load_test_data):
        """Test that days is required for meta research workflow."""
        response = await client.post(
            "/chat",
            json={
                "message": "Show me the Modern meta",
                "conversation_id": None,
                "context": {"format": "Modern"},  # No days provided
            },
        )
        assert response.status_code == 200

        # Parse SSE to get assistant response
        events = parse_sse_stream(response.text)
        content_events = [e for e in events if e["event"] == "content"]
        assert len(content_events) > 0

        # Check that response indicates days is required
        content_data = json.loads(content_events[0]["data"])
        assert "days" in content_data["text"].lower() or "required" in content_data["text"].lower()

    async def test_deck_required_for_deck_coaching(self, client, load_test_data):
        """Test that deck is required for deck coaching workflow."""
        response = await client.post(
            "/chat",
            json={
                "message": "Optimize my sideboard",
                "conversation_id": None,
                "context": {"format": "Modern", "archetype": "Murktide"},  # No deck_text
            },
        )
        assert response.status_code == 200

        # Parse SSE to get assistant response
        events = parse_sse_stream(response.text)
        content_events = [e for e in events if e["event"] == "content"]
        assert len(content_events) > 0

        # Check that response indicates deck is required
        content_data = json.loads(content_events[0]["data"])
        assert "deck" in content_data["text"].lower() or "required" in content_data["text"].lower()

    async def test_deck_enrichment_required_for_deck_coaching(self, client, load_test_data):
        """Test that deck enrichment (card_details) is required before deck coaching.
        
        When deck_text is provided but not enriched via get_enriched_deck,
        the system should block and request deck enrichment first.
        """
        sample_deck = """4 Lightning Bolt
4 Ragavan, Nimble Pilferer
4 Dragon's Rage Channeler
4 Murktide Regent
4 Counterspell
4 Consider
4 Expressive Iteration
2 Spell Pierce
2 Unholy Heat
2 Subtlety
1 Brazen Borrower
1 Jace, the Mind Sculptor
4 Scalding Tarn
4 Flooded Strand
2 Steam Vents
2 Volcanic Island
1 Mountain
1 Island
4 Misty Rainforest
4 Polluted Delta

Sideboard:
2 Engineered Explosives
2 Relic of Progenitus
2 Blood Moon
2 Dress Down
2 Subtlety
2 Flusterstorm
1 Brazen Borrower
2 Surgical Extraction"""

        response = await client.post(
            "/chat",
            json={
                "message": "Optimize my mainboard",
                "conversation_id": None,
                "context": {
                    "format": "Modern",
                    "deck_text": sample_deck,
                    # No archetype provided, but more importantly, no card_details (enriched deck)
                },
            },
        )
        assert response.status_code == 200

        # Parse SSE to get assistant response
        events = parse_sse_stream(response.text)
        content_events = [e for e in events if e["event"] == "content"]
        assert len(content_events) > 0

        # Check that response indicates deck enrichment is required
        # The blocking message says "Please provide your deck list so I can enrich it first."
        content_data = json.loads(content_events[0]["data"])
        assert "deck" in content_data["text"].lower() and "enrich" in content_data["text"].lower()

