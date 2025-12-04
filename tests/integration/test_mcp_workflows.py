"""
Integration tests for MCP tool workflows.

Tests end-to-end workflows that use multiple MCP tools together
to accomplish deck analysis tasks.
"""

import pytest
from unittest.mock import patch, MagicMock
import polars as pl
from datetime import datetime, timezone

from src.app.mcp.tools import meta_research_tools, deck_coaching_tools

# Get the underlying functions from the MCP-wrapped versions
get_format_meta_rankings = meta_research_tools.get_format_meta_rankings.fn
get_format_matchup_stats = meta_research_tools.get_format_matchup_stats.fn
parse_and_validate_decklist = deck_coaching_tools.parse_and_validate_decklist.fn
get_deck_matchup_stats = deck_coaching_tools.get_deck_matchup_stats.fn
generate_matchup_strategy = deck_coaching_tools.generate_matchup_strategy.fn


@pytest.mark.integration
class TestDeckAnalysisWorkflow:
    """Test complete deck analysis workflow using multiple MCP tools."""

    @patch("src.app.mcp.tools.meta_research_tools._fetch_archetype_data")
    @patch("src.app.mcp.tools.meta_research_tools._fetch_match_data")
    @patch("src.app.mcp.tools.deck_coaching_tools.DatabaseConnection")
    @patch("src.app.mcp.tools.deck_coaching_tools.parse_decklist")
    def test_complete_deck_analysis_workflow(
        self,
        mock_parse,
        mock_db,
        mock_match_data,
        mock_archetype_data,
    ):
        """
        Test complete workflow: parse decklist -> get meta rankings -> 
        get matchup stats -> analyze positioning.
        """
        # Setup: Parse a decklist
        mock_parse.return_value = [
            {"quantity": 4, "card_name": "Lightning Bolt", "section": "mainboard"},
            {"quantity": 4, "card_name": "Murktide Regent", "section": "mainboard"},
        ]
        
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("Lightning Bolt", "Deal 3 damage", "Instant", "{R}", 1.0, ["R"]),
            ("Murktide Regent", "Flying, delve", "Creature — Dragon", "{5}{U}{U}", 7.0, ["U"]),
        ]
        mock_db.get_cursor.return_value.__enter__.return_value = mock_cursor

        # Step 1: Parse decklist
        decklist_text = "4 Lightning Bolt\n4 Murktide Regent"
        parsed_deck = parse_and_validate_decklist(decklist_text)
        
        assert len(parsed_deck["card_details"]) == 2
        assert parsed_deck["mainboard_count"] == 8

        # Setup: Mock meta data
        mock_archetype_data.return_value = pl.DataFrame({
            "archetype_group_id": [1, 2, 3],
            "main_title": ["Murktide", "Rhinos", "Hammer"],
            "color_identity": ["UR", "WUBRG", "W"],
            "strategy": ["tempo", "cascade", "aggro"],
            "format": ["Modern"] * 3,
            "tournament_date": [datetime.now(timezone.utc)] * 3,
        })
        
        mock_match_data.return_value = pl.DataFrame({
            "player_archetype_id": [1, 1, 2, 2, 3, 3],
            "player_archetype": ["Murktide", "Murktide", "Rhinos", "Rhinos", "Hammer", "Hammer"],
            "opponent_archetype_id": [2, 3, 1, 3, 1, 2],
            "opponent_archetype": ["Rhinos", "Hammer", "Murktide", "Hammer", "Murktide", "Rhinos"],
            "player1_id": [100, 101, 102, 103, 104, 105],
            "player2_id": [200, 201, 202, 203, 204, 205],
            "winner_id": [100, 200, 102, 200, 104, 200],
            "tournament_date": [datetime.now(timezone.utc)] * 6,
        })

        # Step 2: Get current meta rankings
        rankings = get_format_meta_rankings(format="Modern", current_days=14)
        
        assert len(rankings["data"]) > 0
        assert rankings["metadata"]["format"] == "Modern"

        # Step 3: Get matchup statistics
        matchup_stats = get_format_matchup_stats(format="Modern", days=14)
        
        assert "matrix" in matchup_stats
        assert "Murktide" in matchup_stats["matrix"]

        # Step 4: Get specific deck matchup stats
        deck_matchups = get_deck_matchup_stats(
            archetype="Murktide",
            format="Modern",
            days=14
        )
        
        assert "matchup_stats" in deck_matchups
        # Should have matchups against Rhinos and Hammer
        assert len(deck_matchups["matchup_stats"]) >= 0

    @patch("src.clients.llm_client.get_llm_client")
    @patch("src.app.mcp.tools.meta_research_tools.get_format_matchup_stats")
    def test_matchup_strategy_generation_workflow(self, mock_matchup_stats, mock_get_llm_client):
        """
        Test workflow for generating matchup-specific strategy advice.
        """
        # Setup: Mock matchup stats
        mock_matchup_stats.fn = MagicMock(return_value={
            "matrix": {
                "Murktide": {
                    "Rhinos": {"win_rate": 55.0, "match_count": 20},
                }
            },
            "archetypes": ["Murktide", "Rhinos"],
            "metadata": {"format": "Modern", "days": 14},
        })
        mock_matchup_stats.return_value = mock_matchup_stats.fn.return_value

        # Setup: Mock LLM
        mock_llm = MagicMock()
        mock_response = MagicMock()
        mock_response.text = (
            "Focus on countering their cascade spells early. "
            "Murktide Regent is your best threat since it dodges their removal."
        )
        mock_llm.run.return_value = mock_response
        mock_get_llm_client.return_value = mock_llm

        # Step 1: Get matchup stats
        deck_matchups = get_deck_matchup_stats(
            archetype="Murktide",
            format="Modern",
            days=14
        )
        
        assert len(deck_matchups["matchup_stats"]) == 1
        rhinos_matchup = deck_matchups["matchup_stats"][0]
        assert rhinos_matchup["opponent_archetype"] == "Rhinos"

        # Step 2: Generate strategy for specific matchup
        deck_cards = [
            {"name": "Lightning Bolt", "oracle_text": "Deal 3 damage", "section": "mainboard"},
            {"name": "Murktide Regent", "oracle_text": "Flying, delve", "section": "mainboard"},
            {"name": "Counterspell", "oracle_text": "Counter target spell", "section": "mainboard"},
        ]

        strategy = generate_matchup_strategy(
            card_details=deck_cards,
            archetype="Murktide",
            opponent_archetype="Rhinos",
            matchup_stats=rhinos_matchup,
        )

        assert "advice" in strategy
        assert "Murktide" in strategy["advice"]
        assert strategy["archetype"] == "Murktide"
        assert strategy["opponent_archetype"] == "Rhinos"


@pytest.mark.integration
class TestMCPToolChaining:
    """Test chaining multiple MCP tools together."""

    @patch("src.app.mcp.tools.meta_research_tools._fetch_match_data")
    def test_meta_stats_to_deck_analysis_chain(self, mock_match_data):
        """
        Test using meta-level stats to inform deck-specific analysis.
        """
        # Setup format matchup stats
        mock_match_data.return_value = pl.DataFrame({
            "player_archetype": ["Murktide", "Murktide", "Rhinos"],
            "opponent_archetype": ["Rhinos", "Hammer", "Murktide"],
            "player1_id": [100, 101, 102],
            "player2_id": [200, 201, 202],
            "winner_id": [100, 200, 102],
            "tournament_date": [datetime.now(timezone.utc)] * 3,
        })

        # Call through the full chain
        format_stats = get_format_matchup_stats(format="Modern", days=14)

        deck_specific = get_deck_matchup_stats(
            archetype="Murktide",
            format="Modern",
            days=14
        )

        # Verify the chain worked
        assert "matchup_stats" in deck_specific
        assert deck_specific["metadata"]["format"] == "Modern"

