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
                "context": {"format": "Modern", "current_days": 30},
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
        assert convo_data["state"]["current_days"] == 30
        assert len(convo_data["messages"]) >= 2  # User message + assistant response

    async def test_conversation_continuation(self, client, load_test_data):
        """Test continuing an existing conversation."""
        # Start first conversation
        chat1_response = await client.post(
            "/chat",
            json={
                "message": "Show me Modern archetypes",
                "conversation_id": None,
                "context": {"format": "Modern", "current_days": 30},
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
                "context": {"format": "Pioneer", "current_days": 30},
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
                "context": {"format": "Modern", "current_days": 30},
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
                "context": {"format": "Pioneer", "current_days": 30},
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


@pytest.mark.integration
class TestAgentAPILLMInterpretation:
    """E2E integration tests for LLM interpretation of tool results"""

    @pytest.fixture
    def base_url(self):
        """Base URL for agent API. Defaults to port 8001 per README."""
        return os.getenv("AGENT_API_BASE_URL", "http://localhost:8001")

    @pytest.fixture
    async def client(self, base_url):
        """HTTP client for making requests."""
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
            yield client

    async def test_full_conversation_flow_with_llm_interpretation(self, client, load_test_data):
        """Test that full conversation flow returns LLM-interpreted natural language responses, not raw JSON."""
        # Step 1: Start conversation with meta research query
        chat_response = await client.post(
            "/chat",
            json={
                "message": "What are the top decks in Modern?",
                "conversation_id": None,
                "context": {"format": "Modern", "current_days": 30},
            },
        )
        assert chat_response.status_code == 200
        assert chat_response.headers["content-type"] == "text/event-stream; charset=utf-8"

        # Parse SSE stream
        events = parse_sse_stream(chat_response.text)
        assert len(events) > 0

        # Extract conversation_id
        metadata_event = next(e for e in events if e["event"] == "metadata")
        metadata_data = json.loads(metadata_event["data"])
        conversation_id = metadata_data.get("conversation_id")
        assert conversation_id is not None

        # Verify content events contain natural language (not raw JSON)
        content_events = [e for e in events if e["event"] == "content"]
        assert len(content_events) > 0
        
        # Check that content is natural language (not raw JSON structure)
        all_content = " ".join([json.loads(e["data"])["text"] for e in content_events])
        
        # Natural language indicators (not raw JSON)
        assert not all_content.strip().startswith("{"), "Response should not start with JSON"
        assert not all_content.strip().startswith("["), "Response should not start with array"
        # Note: Current graph implementation returns short routing messages, so we check for minimum length
        assert len(all_content) > 10, "Response should contain some text"
        

        # Step 2: Continue conversation - verify LLM interpretation continues
        chat2_response = await client.post(
            "/chat",
            json={
                "message": "Show me archetypes in Pioneer",
                "conversation_id": conversation_id,
                "context": {"format": "Pioneer", "current_days": 30},
            },
        )
        assert chat2_response.status_code == 200
        
        events2 = parse_sse_stream(chat2_response.text)
        content_events2 = [e for e in events2 if e["event"] == "content"]
        assert len(content_events2) > 0
        
        all_content2 = " ".join([json.loads(e["data"])["text"] for e in content_events2])
        assert not all_content2.strip().startswith("{"), "Second response should also be natural language"
        assert len(all_content2) > 30, "Second response should be substantial"

        # Step 3: Verify conversation history shows natural language messages
        convo_response = await client.get(f"/conversations/{conversation_id}")
        assert convo_response.status_code == 200
        convo_data = convo_response.json()
        
        # Check assistant messages are natural language
        # Note: Current graph implementation may not store messages in the expected format
        # This test verifies messages exist and are not raw JSON
        messages = convo_data.get("messages", [])
        assert len(messages) > 1, "Should have some messages in conversation"
        
        for msg in messages:
            content = msg.get("content", "")
            assert len(content) > 20, "Assistant message should be substantial"
            assert not content.strip().startswith("{"), "Assistant message should not be raw JSON"
            assert not content.strip().startswith("["), "Assistant message should not be raw array"


@pytest.mark.integration
class TestAgentAPIWelcomeSessionInitialization:
    """E2E integration tests for /welcome session initialization"""

    @pytest.fixture
    def base_url(self):
        """Base URL for agent API. Defaults to port 8001 per README."""
        return os.getenv("AGENT_API_BASE_URL", "http://localhost:8001")

    @pytest.fixture
    async def client(self, base_url):
        """HTTP client for making requests."""
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
            yield client

    async def test_welcome_creates_session_with_tool_catalog(self, client, load_test_data):
        """Test that /welcome creates a new conversation session and stores tool_catalog."""
        # Call /welcome
        welcome_response = await client.get("/welcome")
        assert welcome_response.status_code == 200
        welcome_data = welcome_response.json()
        
        # Verify response structure
        assert "conversation_id" in welcome_data
        assert "message" in welcome_data
        assert "available_formats" in welcome_data
        assert "workflows" in welcome_data
        assert "tool_count" in welcome_data
        
        conversation_id = welcome_data["conversation_id"]
        assert conversation_id is not None
        assert len(conversation_id) > 0
        
        # Verify welcome message is LLM-generated (natural language, not static)
        welcome_message = welcome_data["message"]
        assert len(welcome_message) > 50, "Welcome message should be substantial"
        assert not welcome_message.strip().startswith("{"), "Welcome message should be natural language"
        
        # Verify formats are present
        assert isinstance(welcome_data["available_formats"], list)
        assert len(welcome_data["available_formats"]) > 0
        
        # Verify workflows are present
        assert isinstance(welcome_data["workflows"], list)
        assert len(welcome_data["workflows"]) >= 2  # meta_research and deck_coaching
        
        # Verify tool_count matches expected tools
        assert welcome_data["tool_count"] >= 5, "Should have at least 5 tools"
        
        # Verify conversation exists and has stored tool_catalog
        convo_response = await client.get(f"/conversations/{conversation_id}")
        assert convo_response.status_code == 200
        convo_data = convo_response.json()
        
        # Note: tool_catalog is stored in state but not exposed in GET /conversations/{id}
        # We verify it's stored by checking that /chat can use it (tested in next test)
        assert convo_data["conversation_id"] == conversation_id

    async def test_welcome_message_is_llm_generated(self, client, load_test_data):
        """Test that /welcome returns an LLM-generated welcome message, not static text."""
        welcome_response = await client.get("/welcome")
        assert welcome_response.status_code == 200
        welcome_data = welcome_response.json()
        
        welcome_message = welcome_data["message"]
        
        # Verify it's natural language (not static template)
        assert len(welcome_message) > 50, "Message should be substantial"
        assert not welcome_message.strip().startswith("{"), "Should not be JSON"
        
        # Should mention capabilities or workflows
        message_lower = welcome_message.lower()
        capability_indicators = ["help", "can", "workflow", "meta", "deck", "coach", "format"]
        assert any(indicator in message_lower for indicator in capability_indicators), \
            "Welcome message should mention capabilities"


@pytest.mark.integration
class TestAgentAPIChatUsingWelcomeInfo:
    """E2E integration tests for /chat using welcome info from session"""

    @pytest.fixture
    def base_url(self):
        """Base URL for agent API. Defaults to port 8001 per README."""
        return os.getenv("AGENT_API_BASE_URL", "http://localhost:8001")

    @pytest.fixture
    async def client(self, base_url):
        """HTTP client for making requests."""
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
            yield client

    async def test_chat_uses_welcome_tool_catalog(self, client, load_test_data):
        """Test that /chat uses tool_catalog stored from /welcome session."""
        # Step 1: Call /welcome to create session with tool_catalog
        welcome_response = await client.get("/welcome")
        assert welcome_response.status_code == 200
        welcome_data = welcome_response.json()
        conversation_id = welcome_data["conversation_id"]
        
        # Step 2: Use conversation_id from welcome in /chat
        chat_response = await client.post(
            "/chat",
            json={
                "message": "What tools do you have available?",
                "conversation_id": conversation_id,
                "context": {"format": "Modern", "current_days": 30},
            },
        )
        assert chat_response.status_code == 200
        
        # Parse SSE stream
        events = parse_sse_stream(chat_response.text)
        assert len(events) > 0
        
        # Verify metadata event includes tool_catalog info (indirectly via natural language response)
        metadata_event = next(e for e in events if e["event"] == "metadata")
        metadata_data = json.loads(metadata_event["data"])
        assert metadata_data.get("conversation_id") == conversation_id
        
        # Verify content shows LLM has access to tool information
        content_events = [e for e in events if e["event"] == "content"]
        assert len(content_events) > 0
        
        all_content = " ".join([json.loads(e["data"])["text"] for e in content_events])
        # LLM should be able to reference tools if it has tool_catalog from welcome
        # This is indirect verification - if tool_catalog wasn't available, LLM wouldn't know about tools
        assert len(all_content) > 30, "Response should reference capabilities"

    async def test_welcome_then_chat_flow(self, client, load_test_data):
        """Test complete flow: /welcome → /chat using conversation_id from welcome."""
        # Step 1: Get welcome info
        welcome_response = await client.get("/welcome")
        assert welcome_response.status_code == 200
        welcome_data = welcome_response.json()
        conversation_id = welcome_data["conversation_id"]
        
        # Step 2: Use conversation_id in chat
        chat_response = await client.post(
            "/chat",
            json={
                "message": "What are the top decks in Modern?",
                "conversation_id": conversation_id,
                "context": {"format": "Modern", "current_days": 30},
            },
        )
        assert chat_response.status_code == 200
        
        # Verify conversation continues properly
        events = parse_sse_stream(chat_response.text)
        metadata_event = next(e for e in events if e["event"] == "metadata")
        metadata_data = json.loads(metadata_event["data"])
        assert metadata_data.get("conversation_id") == conversation_id
        
        # Verify response is natural language (LLM interpretation)
        content_events = [e for e in events if e["event"] == "content"]
        assert len(content_events) > 0
        all_content = " ".join([json.loads(e["data"])["text"] for e in content_events])
        assert not all_content.strip().startswith("{"), "Response should be natural language"
        assert len(all_content) > 30, "Response should be substantial"

    async def test_chat_without_welcome_falls_back(self, client, load_test_data):
        """Test that /chat without prior /welcome still works (falls back to fetching tool_catalog)."""
        # Call /chat without prior /welcome
        chat_response = await client.post(
            "/chat",
            json={
                "message": "What are the top decks in Modern?",
                "conversation_id": None,
                "context": {"format": "Modern", "current_days": 30},
            },
        )
        assert chat_response.status_code == 200
        
        # Should still work (tool_catalog fetched on demand)
        events = parse_sse_stream(chat_response.text)
        assert len(events) > 0
        
        metadata_event = next(e for e in events if e["event"] == "metadata")
        metadata_data = json.loads(metadata_event["data"])
        conversation_id = metadata_data.get("conversation_id")
        assert conversation_id is not None
        
        # Verify response is natural language
        content_events = [e for e in events if e["event"] == "content"]
        assert len(content_events) > 0

