"""
Unit tests for MCP deck coaching tools.

Tests the deck_coaching_tools module which exposes deck-specific analysis
as MCP tools.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from src.app.mcp.tools import deck_coaching_tools

# Get the underlying functions from the MCP-wrapped versions
parse_and_validate_decklist = deck_coaching_tools.parse_and_validate_decklist.fn
get_deck_matchup_stats = deck_coaching_tools.get_deck_matchup_stats.fn
generate_matchup_strategy = deck_coaching_tools.generate_matchup_strategy.fn


class TestParseAndValidateDecklist:
    """Test the parse_and_validate_decklist MCP tool."""

    @patch("src.app.mcp.tools.deck_coaching_tools.DatabaseConnection")
    @patch("src.app.mcp.tools.deck_coaching_tools.parse_decklist")
    def test_parses_valid_decklist(self, mock_parse, mock_db):
        """Test parsing a valid decklist."""
        # Setup mocks
        mock_parse.return_value = [
            {"quantity": 4, "card_name": "Lightning Bolt", "section": "mainboard"},
            {"quantity": 4, "card_name": "Counterspell", "section": "mainboard"},
        ]
        
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("Lightning Bolt", "Deal 3 damage", "Instant", "{R}", 1.0, ["R"]),
            ("Counterspell", "Counter target spell", "Instant", "{U}{U}", 2.0, ["U"]),
        ]
        mock_db.get_cursor.return_value.__enter__.return_value = mock_cursor

        decklist_text = "4 Lightning Bolt\n4 Counterspell"
        result = parse_and_validate_decklist(decklist_text)

        assert "card_details" in result
        assert len(result["card_details"]) == 2
        assert result["mainboard_count"] == 8
        assert result["sideboard_count"] == 0

    @patch("src.app.mcp.tools.deck_coaching_tools.parse_decklist")
    def test_handles_empty_decklist(self, mock_parse):
        """Test handling an empty decklist."""
        mock_parse.return_value = []

        result = parse_and_validate_decklist("")

        assert result["card_details"] == []
        assert result["mainboard_count"] == 0
        assert result["sideboard_count"] == 0


class TestGetDeckMatchupStats:
    """Test the get_deck_matchup_stats MCP tool."""

    @patch("src.app.mcp.tools.meta_research_tools.get_format_matchup_stats")
    def test_retrieves_matchup_stats_for_archetype(self, mock_matchup_stats):
        """Test retrieving matchup stats for a specific archetype."""
        # Mock the MCP function's .fn property
        mock_matchup_stats.fn = MagicMock(return_value={
            "matrix": {
                "Murktide": {
                    "Rhinos": {"win_rate": 55.0, "match_count": 20},
                    "Hammer": {"win_rate": 45.0, "match_count": 15},
                }
            },
            "archetypes": ["Murktide", "Rhinos", "Hammer"],
            "metadata": {"format": "Modern", "days": 14},
        })
        mock_matchup_stats.return_value = mock_matchup_stats.fn.return_value

        result = get_deck_matchup_stats(
            archetype="Murktide",
            format="Modern",
            days=14
        )

        assert "matchup_stats" in result
        assert "metadata" in result
        assert len(result["matchup_stats"]) == 2
        assert result["matchup_stats"][0]["opponent_archetype"] in ["Rhinos", "Hammer"]
        assert result["matchup_stats"][0]["win_rate"] in [55.0, 45.0]

    @patch("src.app.mcp.tools.meta_research_tools.get_format_matchup_stats")
    def test_handles_missing_archetype(self, mock_matchup_stats):
        """Test handling when archetype is not in matrix."""
        # Mock the MCP function's .fn property
        mock_matchup_stats.fn = MagicMock(return_value={
            "matrix": {},
            "archetypes": [],
            "metadata": {"format": "Modern", "days": 14},
        })
        mock_matchup_stats.return_value = mock_matchup_stats.fn.return_value

        result = get_deck_matchup_stats(
            archetype="UnknownDeck",
            format="Modern",
            days=14
        )

        assert result["matchup_stats"] == []


class TestGenerateMatchupStrategy:
    """Test the generate_matchup_strategy MCP tool."""

    @patch("src.clients.llm_client.get_llm_client")
    def test_generates_strategy_with_llm(self, mock_get_llm_client):
        """Test generating matchup strategy using LLM."""
        mock_llm = MagicMock()
        mock_response = MagicMock()
        mock_response.text = "Strategic advice here"
        mock_llm.run.return_value = mock_response
        mock_get_llm_client.return_value = mock_llm

        deck_cards = [
            {"name": "Lightning Bolt", "oracle_text": "Deal 3 damage", "section": "mainboard"},
            {"name": "Counterspell", "oracle_text": "Counter target spell", "section": "mainboard"},
        ]
        
        matchup_stats = {
            "win_rate": 55.0,
            "match_count": 20,
        }

        result = generate_matchup_strategy(
            card_details=deck_cards,
            archetype="Murktide",
            opponent_archetype="Rhinos",
            matchup_stats=matchup_stats,
        )

        assert "advice" in result
        assert "archetype" in result
        assert "opponent_archetype" in result
        assert result["advice"] == "Strategic advice here"

    @patch("src.clients.llm_client.get_llm_client")
    def test_handles_llm_error(self, mock_get_llm_client):
        """Test handling when LLM call fails."""
        mock_llm = MagicMock()
        mock_llm.run.side_effect = Exception("LLM API error")
        mock_get_llm_client.return_value = mock_llm

        result = generate_matchup_strategy(
            card_details=[],
            archetype="Murktide",
            opponent_archetype="Rhinos",
            matchup_stats={},
        )
        
        assert "error" in result
        assert "LLM API error" in result["error"]

