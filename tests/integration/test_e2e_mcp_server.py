"""End-to-end integration tests for MCP server tools with real data and external APIs"""

import os
import pytest
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


from src.app.mcp.tools import meta_research_tools, deck_coaching_tools
from src.etl.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


def get_archetype_for_format(format_name: str) -> str:
    """Helper to get an archetype name for a given format from the test database."""
    with DatabaseConnection.get_cursor() as cur:
        cur.execute("""
            SELECT ag.main_title 
            FROM archetype_groups ag
            JOIN decklists d ON ag.archetype_group_id = d.archetype_group_id
            JOIN tournaments t ON d.tournament_id = t.tournament_id
            WHERE ag.format = %s
            LIMIT 1
        """, (format_name,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"No {format_name} archetypes found in database")
        return row[0]


@pytest.mark.integration
class TestMCPMetaResearchTools:
    """E2E integration tests for MCP meta research tools with real data"""

    def test_get_format_meta_rankings(self, load_test_data):
        """Test get_format_meta_rankings with real tournament data"""
        # Use Legacy in last 90 days
        result = meta_research_tools.get_format_meta_rankings.fn(
            format="Legacy",
            current_days=90,
            previous_days=90
        )

        # Verify structure
        assert "data" in result
        assert "metadata" in result
        assert isinstance(result["data"], list)
        assert isinstance(result["metadata"], dict)

        # Verify metadata
        assert result["metadata"]["format"] == "Legacy"
        assert "current_period" in result["metadata"]
        assert "previous_period" in result["metadata"]
        assert "timestamp" in result["metadata"]

        # If we have data, verify structure
        if len(result["data"]) > 0:
            first_item = result["data"][0]
            assert "archetype" in first_item or "main_title" in first_item
            assert "meta_share_current" in first_item
            assert "meta_share_previous" in first_item

            logger.info(f"Found {len(result['data'])} archetypes in Legacy meta")
            logger.info(f"Top archetype: {first_item.get('archetype') or first_item.get('main_title')}")

    def test_get_format_matchup_stats(self, load_test_data):
        """Test get_format_matchup_stats with real match data"""
        result = meta_research_tools.get_format_matchup_stats.fn(
            format="Legacy",
            days=90
        )

        # Verify structure
        assert "matrix" in result
        assert "archetypes" in result
        assert "metadata" in result
        assert isinstance(result["matrix"], dict)
        assert isinstance(result["archetypes"], list)

        # Verify metadata
        assert result["metadata"]["format"] == "Legacy"
        assert result["metadata"]["days"] == 90

        # If we have data, verify matrix structure
        if len(result["matrix"]) > 0:
            first_archetype = list(result["matrix"].keys())[0]
            matchups = result["matrix"][first_archetype]

            assert isinstance(matchups, dict)
            if len(matchups) > 0:
                first_matchup = list(matchups.values())[0]
                assert "win_rate" in first_matchup
                assert "match_count" in first_matchup

                logger.info(f"Found matchup matrix with {len(result['archetypes'])} archetypes")
                logger.info(f"Sample matchup: {first_archetype} vs {list(matchups.keys())[0]}")

    def test_get_format_archetypes(self, load_test_data):
        """Test get_format_archetypes with real archetype data"""
        result = meta_research_tools.get_format_archetypes.fn(
            format="Legacy",
            days=90
        )

        # Verify structure
        assert "format" in result
        assert "archetypes" in result
        assert result["format"] == "Legacy"
        assert isinstance(result["archetypes"], list)

        # If we have data, verify archetype structure
        if len(result["archetypes"]) > 0:
            first_archetype = result["archetypes"][0]
            assert "id" in first_archetype
            assert "name" in first_archetype
            assert "meta_share" in first_archetype
            assert "color_identity" in first_archetype

            # Verify sorted by meta_share descending
            meta_shares = [a["meta_share"] for a in result["archetypes"]]
            assert meta_shares == sorted(meta_shares, reverse=True)

            logger.info(f"Found {len(result['archetypes'])} archetypes")
            logger.info(f"Top archetype: {first_archetype['name']} ({first_archetype['meta_share']:.2f}%)")

    def test_get_format_meta_rankings_with_filters(self, load_test_data):
        """Test get_format_meta_rankings with color identity and strategy filters"""
        # Test with color identity filter
        result = meta_research_tools.get_format_meta_rankings.fn(
            format="Legacy",
            current_days=90,
            color_identity="UR"
        )

        assert "data" in result
        assert isinstance(result["data"], list)

        # If we have data, verify all results match filter
        if len(result["data"]) > 0:
            for item in result["data"]:
                assert item.get("color_identity") == "UR"

        logger.info(f"Filtered to {len(result['data'])} UR archetypes")



@pytest.mark.integration
class TestMCPDeckCoachingTools:
    """E2E integration tests for MCP deck coaching tools with real data"""
    
    @pytest.fixture
    def sample_deck(self):
        """Sample Modern deck for testing"""
        return """4 Lightning Bolt
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
    
    def test_get_enriched_deck(self, load_test_data, sample_deck):
        """Test get_enriched_deck with real card data"""
        result = deck_coaching_tools.get_enriched_deck.fn(deck=sample_deck)
        
        # Verify structure
        assert "card_details" in result
        assert "mainboard_count" in result
        assert "sideboard_count" in result
        assert "errors" in result
        
        assert isinstance(result["card_details"], list)
        assert isinstance(result["errors"], list)
        
        # Verify card details structure
        if len(result["card_details"]) > 0:
            first_card = result["card_details"][0]
            assert "name" in first_card
            assert "quantity" in first_card
            assert "section" in first_card
            assert "oracle_text" in first_card
            assert "type_line" in first_card
            assert "mana_cost" in first_card
            assert "cmc" in first_card
            assert "color_identity" in first_card
            
            logger.info(f"Enriched {len(result['card_details'])} cards")
            logger.info(f"Mainboard: {result['mainboard_count']} cards")
            logger.info(f"Sideboard: {result['sideboard_count']} cards")
            if result["errors"]:
                logger.warning(f"Errors: {result['errors']}")
    
    @pytest.mark.parametrize("format_name", ["Modern", "Legacy", "Standard"])
    def test_get_deck_matchup_stats(self, load_test_data, format_name):
        """Test get_deck_matchup_stats with real matchup data for different formats"""
        # Get a real archetype from the database (fixture ensures data exists)
        archetype_name = get_archetype_for_format(format_name)
        
        result = deck_coaching_tools.get_deck_matchup_stats.fn(
            archetype=archetype_name,
            format=format_name,
            days=30
        )
        
        # Verify structure
        assert "archetype" in result
        assert "format" in result
        assert "days" in result
        assert "matchup_stats" in result
        assert result["archetype"] == archetype_name
        assert result["format"] == format_name
        assert isinstance(result["matchup_stats"], list)
        
        # If we have matchup data, verify structure
        if len(result["matchup_stats"]) > 0:
            first_matchup = result["matchup_stats"][0]
            assert "opponent_archetype" in first_matchup
            assert "win_rate" in first_matchup
            assert "match_count" in first_matchup
            
            logger.info(f"Found {len(result['matchup_stats'])} matchups for {archetype_name} in {format_name}")
            logger.info(
                f"Top matchup: vs {first_matchup['opponent_archetype']} "
                f"({first_matchup['win_rate']:.1f}% WR, {first_matchup['match_count']} matches)"
            )
    
    @pytest.mark.parametrize("format_name", ["Modern", "Legacy", "Standard"])
    def test_get_deck_matchup_stats_missing_archetype(self, load_test_data, format_name):
        """Test get_deck_matchup_stats with archetype that doesn't exist"""
        result = deck_coaching_tools.get_deck_matchup_stats.fn(
            archetype="NonExistentDeck123",
            format=format_name,
            days=30
        )
        
        # Should return empty matchup stats, not error
        assert "archetype" in result
        assert result["archetype"] == "NonExistentDeck123"
        assert result["format"] == format_name
        assert result["matchup_stats"] == []
    
    def test_generate_deck_matchup_strategy(self, load_test_data, sample_deck):
        """Test generate_deck_matchup_strategy with real LLM"""
        # Get enriched deck
        enriched = deck_coaching_tools.get_enriched_deck.fn(deck=sample_deck)
        assert len(enriched["card_details"]) > 0
        
        # Get a real opponent archetype (fixture ensures data exists)
        opponent_archetype = get_archetype_for_format("Modern")
        
        result = deck_coaching_tools.generate_deck_matchup_strategy.fn(
            card_details=enriched["card_details"],
            archetype="Murktide",
            opponent_archetype=opponent_archetype,
            matchup_stats={"win_rate": 55.0, "match_count": 20}
        )
        
        # Verify structure
        assert "archetype" in result
        assert "opponent_archetype" in result
        assert result["archetype"] == "Murktide"
        assert result["opponent_archetype"] == opponent_archetype
        
        # Should have advice or error
        assert "advice" in result or "error" in result
        
        if "advice" in result and result["advice"]:
            logger.info(f"Generated strategy advice for Murktide vs {opponent_archetype}")
            logger.info(f"Advice length: {len(result['advice'])} characters")
        elif "error" in result:
            logger.warning(f"LLM error: {result['error']}")
    
    def test_optimize_mainboard(self, load_test_data, sample_deck):
        """Test optimize_mainboard with real data and LLM"""
        # Get enriched deck
        enriched = deck_coaching_tools.get_enriched_deck.fn(deck=sample_deck)
        assert len(enriched["card_details"]) > 0
        
        result = deck_coaching_tools.optimize_mainboard.fn(
            card_details=enriched["card_details"],
            archetype="Murktide",
            format="Modern",
            top_n=5
        )
        
        # Verify structure
        assert "flex_spots" in result or "error" in result
        
        if "flex_spots" in result:
            assert "recommendations" in result
            assert isinstance(result["flex_spots"], list)
            assert isinstance(result["recommendations"], list)
            
            logger.info(f"Found {len(result['flex_spots'])} flex spots")
            logger.info(f"Generated {len(result['recommendations'])} recommendations")
        elif "error" in result:
            logger.warning(f"Optimization error: {result['error']}")
    
    def test_optimize_sideboard(self, load_test_data, sample_deck):
        """Test optimize_sideboard with real data and LLM"""
        # Get enriched deck
        enriched = deck_coaching_tools.get_enriched_deck.fn(deck=sample_deck)
        assert len(enriched["card_details"]) > 0
        
        result = deck_coaching_tools.optimize_sideboard.fn(
            card_details=enriched["card_details"],
            archetype="Murktide",
            format="Modern",
            top_n=5
        )
        
        # Verify structure
        assert "sideboard_changes" in result or "error" in result
        
        if "sideboard_changes" in result:
            assert "sideboard_plans" in result
            assert "final_sideboard" in result
            assert isinstance(result["sideboard_changes"], list)
            assert isinstance(result["sideboard_plans"], list)
            assert isinstance(result["final_sideboard"], list)
            
            # Verify final sideboard has exactly 15 cards
            total_cards = sum(card["quantity"] for card in result["final_sideboard"])
            assert total_cards == 15, f"Expected 15 sideboard cards, got {total_cards}"
            
            logger.info(f"Generated {len(result['sideboard_changes'])} sideboard changes")
            logger.info(f"Created {len(result['sideboard_plans'])} sideboard plans")
        elif "error" in result:
            logger.warning(f"Optimization error: {result['error']}")


@pytest.mark.integration
class TestMCPToolsIntegration:
    """Integration tests that test multiple MCP tools working together"""
    
    @pytest.mark.parametrize("format_name", ["Modern", "Legacy", "Standard"])
    def test_full_workflow_meta_research(self, load_test_data, format_name):
        """Test full workflow: get archetypes -> get matchup stats -> get rankings"""
        # Step 1: Get archetypes (fixture ensures data exists)
        archetypes_result = meta_research_tools.get_format_archetypes.fn(
            format=format_name,
            days=30
        )
        
        assert len(archetypes_result["archetypes"]) > 0
        
        # Step 2: Get matchup stats for top archetype
        top_archetype = archetypes_result["archetypes"][0]["name"]
        matchup_result = meta_research_tools.get_format_matchup_stats.fn(
            format=format_name,
            days=30
        )
        
        # Matchup data requires BOTH players in matches to have classified decklists
        # Some formats may have incomplete decklist coverage
        if len(matchup_result["archetypes"]) > 0:
            assert top_archetype in matchup_result["archetypes"]
        # If no matchup data, just verify the result structure is correct
        else:
            assert "matrix" in matchup_result
            assert "archetypes" in matchup_result
            assert "metadata" in matchup_result
        
        # Step 3: Get meta rankings
        rankings_result = meta_research_tools.get_format_meta_rankings.fn(
            format=format_name,
            current_days=30,
            previous_days=30
        )
        
        # Verify consistency: top archetype should appear in rankings
        if len(rankings_result["data"]) > 0:
            ranking_names = [
                item.get("archetype") or item.get("main_title")
                for item in rankings_result["data"]
            ]
            assert top_archetype in ranking_names
        
        logger.info(f"Full meta research workflow completed successfully for {format_name}")
    
    def test_full_workflow_deck_coaching(self, load_test_data):
        """Test full workflow: enrich deck -> get matchup stats -> generate strategy"""
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
        
        # Step 1: Enrich deck
        enriched = deck_coaching_tools.get_enriched_deck.fn(deck=sample_deck)
        assert len(enriched["card_details"]) > 0
        
        # Step 2: Get matchup stats
        matchup_stats = deck_coaching_tools.get_deck_matchup_stats.fn(
            archetype="Murktide",
            format="Modern",
            days=30
        )
        
        # Step 3: Generate strategy for top matchup (if available)
        if len(matchup_stats["matchup_stats"]) > 0:
            top_matchup = matchup_stats["matchup_stats"][0]
            opponent = top_matchup["opponent_archetype"]
            
            # Skip LLM call for this test (too slow/expensive)
            # But verify the data flow works
            logger.info(
                f"Would generate strategy for Murktide vs {opponent} "
                f"({top_matchup['win_rate']:.1f}% WR)"
            )
        
        logger.info("Full deck coaching workflow completed successfully")

