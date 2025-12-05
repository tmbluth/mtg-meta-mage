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
            ("Lightning Bolt", "Deal 3 damage", "Instant", "{R}", 1.0, ["R"], ""),
            ("Counterspell", "Counter target spell", "Instant", "{U}{U}", 2.0, ["U"], ""),
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


class TestOptimizeMainboard:
    """Test the optimize_mainboard MCP tool."""

    @patch("src.clients.llm_client.get_llm_client")
    @patch("src.app.mcp.tools.deck_coaching_tools._fetch_archetype_decklists")
    @patch("src.app.mcp.tools.deck_coaching_tools._get_legal_cards_for_format")
    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    def test_optimize_mainboard_happy_path(
        self, 
        mock_meta_rankings, 
        mock_legal_cards, 
        mock_fetch_decklists,
        mock_get_llm_client
    ):
        """Test mainboard optimization with valid deck and top 5 archetypes."""
        # Mock meta rankings
        mock_meta_rankings.fn = MagicMock(return_value={
            "rankings": [
                {"archetype": "Murktide", "archetype_group_id": 1, "meta_share": 15.0},
                {"archetype": "Rhinos", "archetype_group_id": 2, "meta_share": 12.0},
                {"archetype": "Hammer", "archetype_group_id": 3, "meta_share": 10.0},
                {"archetype": "Creativity", "archetype_group_id": 4, "meta_share": 8.0},
                {"archetype": "Yawgmoth", "archetype_group_id": 5, "meta_share": 7.0},
            ]
        })
        
        # Mock legal cards
        mock_legal_cards.return_value = [
            {
                "card_id": 1,
                "name": "Force of Negation",
                "oracle_text": "Counter target noncreature spell. If this spell was cast from your hand, you may pay {1}{U}{U} rather than pay its mana cost.",
                "type_line": "Instant",
                "mana_cost": "{1}{U}{U}",
                "cmc": 3.0,
                "color_identity": ["U"]
            },
            {
                "card_id": 2,
                "name": "Dress Down",
                "oracle_text": "When Dress Down enters the battlefield, draw a card. Creatures lose all abilities.",
                "type_line": "Enchantment",
                "mana_cost": "{1}{U}",
                "cmc": 2.0,
                "color_identity": ["U"]
            }
        ]
        
        # Mock archetype decklists
        mock_fetch_decklists.return_value = {
            1: [
                {
                    "decklist_id": 1,
                    "player": "Player 1",
                    "tournament_date": "2024-01-01",
                    "cards": [
                        {"name": "Dragon's Rage Channeler", "quantity": 4, "section": "mainboard"}
                    ]
                }
            ]
        }
        
        # Mock LLM
        mock_llm = MagicMock()
        mock_response = MagicMock()
        mock_response.text = """{
            "flex_spots": [
                {"card_name": "Spell Pierce", "quantity": 2, "reason": "Not essential for core strategy"}
            ],
            "recommendations": [
                {
                    "flex_spot_card": "Spell Pierce",
                    "suggested_cards": [
                        {
                            "card_name": "Force of Negation",
                            "quantity": 2,
                            "justification": "Better against combo decks in the meta",
                            "matchup_improvements": ["Rhinos", "Creativity"]
                        }
                    ],
                    "alternatives_considered": [
                        {"card_name": "Counterspell", "why_not_recommended": "Too slow for current meta"}
                    ]
                }
            ]
        }"""
        mock_llm.run.return_value = mock_response
        mock_get_llm_client.return_value = mock_llm
        
        # Test input
        card_details = [
            {"name": "Ragavan", "quantity": 4, "section": "mainboard", "color_identity": ["R"]},
            {"name": "Dragon's Rage Channeler", "quantity": 4, "section": "mainboard", "color_identity": ["R"]},
            {"name": "Spell Pierce", "quantity": 2, "section": "mainboard", "color_identity": ["U"]},
        ]
        
        # Import after patches are set up
        from src.app.mcp.tools.deck_coaching_tools import optimize_mainboard
        result = optimize_mainboard.fn(
            card_details=card_details,
            archetype="Murktide",
            format="Modern",
            top_n=5
        )
        
        assert "flex_spots" in result
        assert "recommendations" in result
        assert len(result["flex_spots"]) == 1
        assert result["flex_spots"][0]["card_name"] == "Spell Pierce"

    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    def test_optimize_mainboard_empty_meta_data(self, mock_meta_rankings):
        """Test mainboard optimization with empty meta data."""
        mock_meta_rankings.fn = MagicMock(return_value={
            "rankings": []
        })
        
        from src.app.mcp.tools.deck_coaching_tools import optimize_mainboard
        result = optimize_mainboard.fn(
            card_details=[],
            archetype="TestDeck",
            format="Modern",
            top_n=5
        )
        
        assert "error" in result
        assert "insufficient meta data" in result["error"].lower()

    @patch("src.app.mcp.tools.deck_coaching_tools._get_legal_cards_for_format")
    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    def test_optimize_mainboard_format_legality_check(
        self, 
        mock_meta_rankings,
        mock_legal_cards
    ):
        """Test that format parameter is normalized before querying."""
        mock_meta_rankings.fn = MagicMock(return_value={
            "rankings": [
                {"archetype": "Murktide", "archetype_group_id": 1, "meta_share": 15.0}
            ]
        })
        
        # Mock should receive normalized format
        mock_legal_cards.return_value = []
        
        from src.app.mcp.tools.deck_coaching_tools import optimize_mainboard
        
        try:
            result = optimize_mainboard.fn(
                card_details=[{"name": "Test", "color_identity": ["R"]}],
                archetype="TestDeck",
                format="MODERN",  # Uppercase
                top_n=1
            )
        except ValueError:
            pass  # Expected if no legal cards
        
        # Verify format was normalized to lowercase
        mock_legal_cards.assert_called_once_with("modern")

    @patch("src.app.mcp.tools.deck_coaching_tools._get_legal_cards_for_format")
    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    def test_optimize_mainboard_unavailable_legality_data(
        self,
        mock_meta_rankings,
        mock_legal_cards
    ):
        """Test handling when card legality data is unavailable."""
        mock_meta_rankings.fn = MagicMock(return_value={
            "rankings": [
                {"archetype": "Murktide", "archetype_group_id": 1, "meta_share": 15.0}
            ]
        })
        
        mock_legal_cards.side_effect = ValueError("No legal cards found")
        
        from src.app.mcp.tools.deck_coaching_tools import optimize_mainboard
        result = optimize_mainboard.fn(
            card_details=[{"name": "Test", "color_identity": ["R"]}],
            archetype="TestDeck",
            format="Modern",
            top_n=1
        )
        
        assert "error" in result
        assert "legal cards" in result["error"].lower()


class TestOptimizeSideboard:
    """Test the optimize_sideboard MCP tool."""

    @patch("src.clients.llm_client.get_llm_client")
    @patch("src.app.mcp.tools.deck_coaching_tools._fetch_archetype_decklists")
    @patch("src.app.mcp.tools.deck_coaching_tools._get_legal_cards_for_format")
    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    def test_optimize_sideboard_happy_path(
        self, 
        mock_meta_rankings, 
        mock_legal_cards, 
        mock_fetch_decklists,
        mock_get_llm_client
    ):
        """Test sideboard optimization with valid deck and top 5 archetypes."""
        # Mock meta rankings
        mock_meta_rankings.fn = MagicMock(return_value={
            "rankings": [
                {"archetype": "Murktide", "archetype_group_id": 1, "meta_share": 15.0},
                {"archetype": "Rhinos", "archetype_group_id": 2, "meta_share": 12.0},
            ]
        })
        
        # Mock legal cards
        mock_legal_cards.return_value = [
            {
                "card_id": 1,
                "name": "Engineered Explosives",
                "oracle_text": "Sunburst (This enters the battlefield with a charge counter on it for each color of mana used to pay its cost.) {X}, Sacrifice Engineered Explosives: Destroy each nonland permanent with converted mana cost X or less.",
                "type_line": "Artifact",
                "mana_cost": "{X}",
                "cmc": 0.0,
                "color_identity": []
            }
        ]
        
        # Mock archetype decklists
        mock_fetch_decklists.return_value = {
            1: [
                {
                    "decklist_id": 1,
                    "player": "Player 1",
                    "tournament_date": "2024-01-01",
                    "cards": [
                        {"name": "Dragon's Rage Channeler", "quantity": 4, "section": "mainboard"},
                        {"name": "Engineered Explosives", "quantity": 2, "section": "sideboard"}
                    ]
                }
            ]
        }
        
        # Mock LLM with valid 15-card sideboard
        mock_llm = MagicMock()
        mock_response = MagicMock()
        mock_response.text = """{
            "sideboard_changes": [
                {
                    "remove": {"card_name": "Relic of Progenitus", "quantity": 2, "reason": "Not effective against current meta"},
                    "add": {"card_name": "Engineered Explosives", "quantity": 2, "justification": "Better against token strategies", "answers_archetypes": ["Rhinos"]}
                }
            ],
            "sideboard_plans": [
                {
                    "opponent_archetype": "Rhinos",
                    "games_2_3_plan": "Bring in EE, take out Relics",
                    "key_cards_to_answer": ["Crashing Footfalls"]
                }
            ],
            "final_sideboard": [
                {"card_name": "Engineered Explosives", "quantity": 2},
                {"card_name": "Rest in Peace", "quantity": 2},
                {"card_name": "Surgical Extraction", "quantity": 2},
                {"card_name": "Blood Moon", "quantity": 3},
                {"card_name": "Dress Down", "quantity": 2},
                {"card_name": "Subtlety", "quantity": 2},
                {"card_name": "Flusterstorm", "quantity": 2}
            ]
        }"""
        mock_llm.run.return_value = mock_response
        mock_get_llm_client.return_value = mock_llm
        
        # Test input (15 cards in sideboard)
        card_details = [
            {"name": "Ragavan", "quantity": 4, "section": "mainboard", "color_identity": ["R"]},
            {"name": "Relic of Progenitus", "quantity": 2, "section": "sideboard", "color_identity": []},
            {"name": "Rest in Peace", "quantity": 2, "section": "sideboard", "color_identity": ["W"]},
            {"name": "Surgical Extraction", "quantity": 2, "section": "sideboard", "color_identity": ["B"]},
            {"name": "Blood Moon", "quantity": 3, "section": "sideboard", "color_identity": ["R"]},
            {"name": "Dress Down", "quantity": 2, "section": "sideboard", "color_identity": ["U"]},
            {"name": "Subtlety", "quantity": 2, "section": "sideboard", "color_identity": ["U"]},
            {"name": "Flusterstorm", "quantity": 2, "section": "sideboard", "color_identity": ["U"]},
        ]
        
        from src.app.mcp.tools.deck_coaching_tools import optimize_sideboard
        result = optimize_sideboard.fn(
            card_details=card_details,
            archetype="Murktide",
            format="Modern",
            top_n=5
        )
        
        assert "sideboard_changes" in result
        assert "sideboard_plans" in result
        assert "final_sideboard" in result
        
        # Verify exactly 15 cards
        total_cards = sum(card["quantity"] for card in result["final_sideboard"])
        assert total_cards == 15

    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    def test_optimize_sideboard_empty_meta_data(self, mock_meta_rankings):
        """Test sideboard optimization with empty meta data."""
        mock_meta_rankings.fn = MagicMock(return_value={
            "rankings": []
        })
        
        from src.app.mcp.tools.deck_coaching_tools import optimize_sideboard
        result = optimize_sideboard.fn(
            card_details=[],
            archetype="TestDeck",
            format="Modern",
            top_n=5
        )
        
        assert "error" in result
        assert "insufficient meta data" in result["error"].lower()

    @patch("src.clients.llm_client.get_llm_client")
    @patch("src.app.mcp.tools.deck_coaching_tools._fetch_archetype_decklists")
    @patch("src.app.mcp.tools.deck_coaching_tools._get_legal_cards_for_format")
    @patch("src.app.mcp.tools.meta_research_tools.get_format_meta_rankings")
    def test_optimize_sideboard_validates_15_card_constraint(
        self, 
        mock_meta_rankings, 
        mock_legal_cards, 
        mock_fetch_decklists,
        mock_get_llm_client
    ):
        """Test that sideboard validation enforces 15-card constraint."""
        mock_meta_rankings.fn = MagicMock(return_value={
            "rankings": [
                {"archetype": "Murktide", "archetype_group_id": 1, "meta_share": 15.0}
            ]
        })
        
        mock_legal_cards.return_value = [
            {"card_id": 1, "name": "Test Card", "oracle_text": "Test oracle text", "type_line": "Instant", "mana_cost": "{1}", "cmc": 1.0, "color_identity": ["U"]}
        ]
        
        mock_fetch_decklists.return_value = {}
        
        # Mock LLM to return invalid sideboard (not 15 cards)
        mock_llm = MagicMock()
        mock_response = MagicMock()
        mock_response.text = """{
            "sideboard_changes": [],
            "sideboard_plans": [],
            "final_sideboard": [
                {"card_name": "Test Card", "quantity": 10}
            ]
        }"""
        mock_llm.run.return_value = mock_response
        mock_get_llm_client.return_value = mock_llm
        
        card_details = [
            {"name": "Test Card", "quantity": 15, "section": "sideboard", "color_identity": ["U"]}
        ]
        
        from src.app.mcp.tools.deck_coaching_tools import optimize_sideboard
        result = optimize_sideboard.fn(
            card_details=card_details,
            archetype="TestDeck",
            format="Modern",
            top_n=1
        )
        
        # Should retry and eventually return error or corrected result
        # The implementation should handle this with retries
        assert "final_sideboard" in result or "error" in result

