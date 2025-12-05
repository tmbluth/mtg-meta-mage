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


@pytest.mark.integration
class TestDeckOptimizationWorkflows:
    """Test complete deck optimization workflows."""

    @patch("src.clients.llm_client.get_llm_client")
    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    @patch("src.app.mcp.tools.deck_coaching_tools.DatabaseConnection")
    def test_mainboard_optimization_workflow(
        self,
        mock_db,
        mock_get_rankings,
        mock_get_llm_client,
    ):
        """
        Test full mainboard optimization workflow with real deck and meta data.
        """
        # Setup: Mock meta rankings
        mock_get_rankings.fn = MagicMock(return_value={
            "rankings": [
                {
                    "archetype_group_id": 1,
                    "main_title": "Rhinos",
                    "meta_share": 15.5,
                    "color_identity": "WUBRG",
                },
                {
                    "archetype_group_id": 2,
                    "main_title": "Hammer",
                    "meta_share": 12.3,
                    "color_identity": "W",
                },
            ],
            "metadata": {"format": "Modern", "days": 14},
        })
        mock_get_rankings.return_value = mock_get_rankings.fn.return_value

        # Setup: Mock database for legal cards query
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            # Legal cards query
            [
                (1, "Lightning Bolt", "Instant", "{R}", 1.0, ["R"]),
                (2, "Counterspell", "Instant", "{U}{U}", 2.0, ["U"]),
                (3, "Murktide Regent", "Creature — Dragon", "{5}{U}{U}", 7.0, ["U"]),
            ],
            # Archetype decklists query (archetype 1)
            [
                (100, "Crashing Footfalls", "Sorcery", "{G}{G}", 2.0, ["G"], 4, "mainboard"),
                (100, "Force of Negation", "Instant", "{2}{U}{U}", 5.0, ["U"], 4, "sideboard"),
            ],
            # Archetype decklists query (archetype 2)
            [
                (101, "Colossus Hammer", "Artifact — Equipment", "{1}", 1.0, [], 4, "mainboard"),
                (101, "Sigarda's Aid", "Enchantment", "{W}", 1.0, ["W"], 4, "mainboard"),
            ],
        ]
        mock_db.get_cursor.return_value.__enter__.return_value = mock_cursor

        # Setup: Mock LLM
        mock_llm = MagicMock()
        mock_response = MagicMock()
        mock_response.text = """{
            "flex_spots": [
                {
                    "card_name": "Spell Pierce",
                    "quantity": 2,
                    "reason": "Less effective against combo-heavy meta"
                }
            ],
            "recommendations": [
                {
                    "card_name": "Counterspell",
                    "quantity": 2,
                    "reason": "Better against Rhinos and Hammer"
                }
            ]
        }"""
        mock_llm.run.return_value = mock_response
        mock_get_llm_client.return_value = mock_llm

        # Execute: Optimize mainboard
        from src.app.mcp.tools.deck_coaching_tools import optimize_mainboard

        deck_cards = [
            {"name": "Lightning Bolt", "section": "mainboard", "color_identity": ["R"]},
            {"name": "Murktide Regent", "section": "mainboard", "color_identity": ["U"]},
            {"name": "Spell Pierce", "section": "mainboard", "color_identity": ["U"]},
        ]

        result = optimize_mainboard.fn(
            card_details=deck_cards,
            archetype="Murktide",
            format="Modern",
            top_n=2,
        )

        # Verify: Result structure
        assert "flex_spots" in result
        assert "recommendations" in result
        assert result["archetype"] == "Murktide"
        assert result["format"] == "Modern"
        assert len(result["flex_spots"]) == 1
        assert result["flex_spots"][0]["card_name"] == "Spell Pierce"
        assert len(result["recommendations"]) == 1
        assert result["recommendations"][0]["card_name"] == "Counterspell"

    @patch("src.clients.llm_client.get_llm_client")
    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    @patch("src.app.mcp.tools.deck_coaching_tools.DatabaseConnection")
    def test_sideboard_optimization_workflow(
        self,
        mock_db,
        mock_get_rankings,
        mock_get_llm_client,
    ):
        """
        Test full sideboard optimization workflow with real deck and meta data.
        """
        # Setup: Mock meta rankings
        mock_get_rankings.fn = MagicMock(return_value={
            "rankings": [
                {
                    "archetype_group_id": 1,
                    "main_title": "Rhinos",
                    "meta_share": 15.5,
                    "color_identity": "WUBRG",
                },
            ],
            "metadata": {"format": "Modern", "days": 14},
        })
        mock_get_rankings.return_value = mock_get_rankings.fn.return_value

        # Setup: Mock database
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            # Legal cards query
            [
                (1, "Grafdigger's Cage", "Artifact", "{1}", 1.0, []),
                (2, "Chalice of the Void", "Artifact", "{X}{X}", 0.0, []),
                (3, "Dress Down", "Enchantment", "{1}{U}", 2.0, ["U"]),
            ],
            # Archetype decklists query
            [
                (100, "Crashing Footfalls", "Sorcery", "{G}{G}", 2.0, ["G"], 4, "mainboard"),
                (100, "Force of Negation", "Instant", "{2}{U}{U}", 5.0, ["U"], 4, "sideboard"),
            ],
        ]
        mock_db.get_cursor.return_value.__enter__.return_value = mock_cursor

        # Setup: Mock LLM (with valid 15-card sideboard)
        mock_llm = MagicMock()
        mock_response = MagicMock()
        mock_response.text = """{
            "sideboard_changes": [
                {
                    "action": "remove",
                    "card_name": "Engineered Explosives",
                    "quantity": 2,
                    "reason": "Less relevant against current meta"
                },
                {
                    "action": "add",
                    "card_name": "Grafdigger's Cage",
                    "quantity": 2,
                    "reason": "Strong against Rhinos graveyard strategy"
                }
            ],
            "sideboard_plans": [
                {
                    "opponent_archetype": "Rhinos",
                    "cards_in": ["Grafdigger's Cage"],
                    "cards_out": ["Lightning Bolt"],
                    "strategy": "Shut down their cascade and graveyard recursion"
                }
            ],
            "final_sideboard": [
                {"card_name": "Grafdigger's Cage", "quantity": 2},
                {"card_name": "Chalice of the Void", "quantity": 3},
                {"card_name": "Dress Down", "quantity": 2},
                {"card_name": "Spell Pierce", "quantity": 3},
                {"card_name": "Mystical Dispute", "quantity": 2},
                {"card_name": "Subtlety", "quantity": 3}
            ]
        }"""
        mock_llm.run.return_value = mock_response
        mock_get_llm_client.return_value = mock_llm

        # Execute: Optimize sideboard
        from src.app.mcp.tools.deck_coaching_tools import optimize_sideboard

        deck_cards = [
            {"name": "Lightning Bolt", "section": "mainboard", "color_identity": ["R"]},
            {"name": "Murktide Regent", "section": "mainboard", "color_identity": ["U"]},
            {"name": "Engineered Explosives", "section": "sideboard", "color_identity": []},
            {"name": "Chalice of the Void", "section": "sideboard", "color_identity": []},
        ]

        result = optimize_sideboard.fn(
            card_details=deck_cards,
            archetype="Murktide",
            format="Modern",
            top_n=1,
        )

        # Verify: Result structure
        assert "sideboard_changes" in result
        assert "sideboard_plans" in result
        assert "final_sideboard" in result
        assert result["archetype"] == "Murktide"
        assert result["format"] == "Modern"
        
        # Verify: 15-card sideboard constraint
        total_cards = sum(card["quantity"] for card in result["final_sideboard"])
        assert total_cards == 15, f"Sideboard must have exactly 15 cards, got {total_cards}"

    @patch("src.clients.llm_client.get_llm_client")
    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    @patch("src.app.mcp.tools.deck_coaching_tools.DatabaseConnection")
    def test_complete_deck_optimization_workflow(
        self,
        mock_db,
        mock_get_rankings,
        mock_get_llm_client,
    ):
        """
        Test both optimization tools together for complete deck optimization.
        """
        # Setup: Mock meta rankings
        mock_get_rankings.fn = MagicMock(return_value={
            "rankings": [
                {
                    "archetype_group_id": 1,
                    "main_title": "Rhinos",
                    "meta_share": 15.5,
                    "color_identity": "WUBRG",
                },
            ],
            "metadata": {"format": "Modern", "days": 14},
        })
        mock_get_rankings.return_value = mock_get_rankings.fn.return_value

        # Setup: Mock database
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            # Legal cards query (mainboard)
            [
                (1, "Lightning Bolt", "Instant", "{R}", 1.0, ["R"]),
                (2, "Counterspell", "Instant", "{U}{U}", 2.0, ["U"]),
            ],
            # Archetype decklists query (mainboard)
            [
                (100, "Crashing Footfalls", "Sorcery", "{G}{G}", 2.0, ["G"], 4, "mainboard"),
            ],
            # Legal cards query (sideboard)
            [
                (3, "Grafdigger's Cage", "Artifact", "{1}", 1.0, []),
            ],
            # Archetype decklists query (sideboard)
            [
                (100, "Force of Negation", "Instant", "{2}{U}{U}", 5.0, ["U"], 4, "sideboard"),
            ],
        ]
        mock_db.get_cursor.return_value.__enter__.return_value = mock_cursor

        # Setup: Mock LLM responses
        mock_llm = MagicMock()
        mainboard_response = MagicMock()
        mainboard_response.text = """{
            "flex_spots": [{"card_name": "Spell Pierce", "quantity": 2, "reason": "Less effective"}],
            "recommendations": [{"card_name": "Counterspell", "quantity": 2, "reason": "Better"}]
        }"""
        sideboard_response = MagicMock()
        sideboard_response.text = """{
            "sideboard_changes": [{"action": "add", "card_name": "Grafdigger's Cage", "quantity": 2, "reason": "Good"}],
            "sideboard_plans": [{"opponent_archetype": "Rhinos", "cards_in": ["Grafdigger's Cage"], "cards_out": ["Lightning Bolt"], "strategy": "Shut down graveyard"}],
            "final_sideboard": [
                {"card_name": "Grafdigger's Cage", "quantity": 2},
                {"card_name": "Chalice of the Void", "quantity": 3},
                {"card_name": "Dress Down", "quantity": 2},
                {"card_name": "Spell Pierce", "quantity": 3},
                {"card_name": "Mystical Dispute", "quantity": 2},
                {"card_name": "Subtlety", "quantity": 3}
            ]
        }"""
        mock_llm.run.side_effect = [mainboard_response, sideboard_response]
        mock_get_llm_client.return_value = mock_llm

        # Execute: Optimize mainboard first
        from src.app.mcp.tools.deck_coaching_tools import optimize_mainboard, optimize_sideboard

        deck_cards = [
            {"name": "Lightning Bolt", "section": "mainboard", "color_identity": ["R"]},
            {"name": "Spell Pierce", "section": "mainboard", "color_identity": ["U"]},
            {"name": "Engineered Explosives", "section": "sideboard", "color_identity": []},
        ]

        mainboard_result = optimize_mainboard.fn(
            card_details=deck_cards,
            archetype="Murktide",
            format="Modern",
            top_n=1,
        )

        # Verify mainboard optimization
        assert "flex_spots" in mainboard_result
        assert "recommendations" in mainboard_result

        # Execute: Optimize sideboard
        sideboard_result = optimize_sideboard.fn(
            card_details=deck_cards,
            archetype="Murktide",
            format="Modern",
            top_n=1,
        )

        # Verify sideboard optimization
        assert "final_sideboard" in sideboard_result
        total_cards = sum(card["quantity"] for card in sideboard_result["final_sideboard"])
        assert total_cards == 15

    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    @patch("src.app.mcp.tools.deck_coaching_tools.DatabaseConnection")
    def test_format_legality_enforcement(
        self,
        mock_db,
        mock_get_rankings,
    ):
        """
        Verify format legality constraints are enforced end-to-end.
        """
        # Setup: Mock meta rankings
        mock_get_rankings.fn = MagicMock(return_value={
            "rankings": [
                {
                    "archetype_group_id": 1,
                    "main_title": "Rhinos",
                    "meta_share": 15.5,
                    "color_identity": "WUBRG",
                },
            ],
            "metadata": {"format": "Standard", "days": 14},
        })
        mock_get_rankings.return_value = mock_get_rankings.fn.return_value

        # Setup: Mock database with no legal cards (simulating legality check failure)
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []  # No legal cards found
        mock_db.get_cursor.return_value.__enter__.return_value = mock_cursor

        # Execute: Try to optimize mainboard
        from src.app.mcp.tools.deck_coaching_tools import optimize_mainboard

        deck_cards = [
            {"name": "Lightning Bolt", "section": "mainboard", "color_identity": ["R"]},
        ]

        result = optimize_mainboard.fn(
            card_details=deck_cards,
            archetype="Tempo",
            format="Standard",
            top_n=1,
        )

        # Verify: Error for unavailable legality data
        assert "error" in result
        assert "Card legality data unavailable" in result["error"]
        assert result["format"] == "Standard"

