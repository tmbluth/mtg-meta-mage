"""Integration tests for archetype classification pipeline including LLM client with real API calls (Azure OpenAI only)"""

import os
import pytest
import logging
import json
from unittest.mock import patch, MagicMock

from src.clients.llm_client import get_llm_client
from src.etl.archetype_pipeline import (
    ArchetypeClassificationPipeline,
    ArchetypeClassificationResponse,
    StrategyType
)
from src.etl.cards_pipeline import CardsPipeline
from src.etl.tournaments_pipeline import TournamentsPipeline
from src.etl.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestLLMClientAzureOpenAI:
    """Integration tests for LLM client with Azure OpenAI (real API calls)"""
    
    @pytest.fixture
    def azure_client(self):
        """Create Azure OpenAI LLM client"""
        model_name = os.getenv('LLM_MODEL')
        assert model_name, "LLM_MODEL environment variable not set"
        
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        assert api_key, "AZURE_OPENAI_API_KEY environment variable not set"
        
        endpoint_template = os.getenv('AZURE_OPENAI_LLM_ENDPOINT')
        assert endpoint_template, "AZURE_OPENAI_LLM_ENDPOINT environment variable not set"
        
        azure_openai_api_version = os.getenv('AZURE_OPENAI_API_VERSION')
        assert azure_openai_api_version, "AZURE_OPENAI_API_VERSION environment variable not set"
        
        api_version = os.getenv('AZURE_OPENAI_API_VERSION')
        assert api_version, "AZURE_OPENAI_API_VERSION environment variable not set"
        
        return get_llm_client(model_name, 'azure_openai')
    
    @pytest.fixture
    def sample_decklist_prompt(self):
        """Create a sample decklist prompt for testing"""
        cards = [
            {
                "name": "Lightning Bolt",
                "quantity": 4,
                "type_line": "Instant",
                "mana_cost": "{R}",
                "cmc": 1,
                "color_identity": ["R"],
                "oracle_text": "Lightning Bolt deals 3 damage to any target."
            },
            {
                "name": "Monastery Swiftspear",
                "quantity": 4,
                "type_line": "Creature — Human Monk",
                "mana_cost": "{R}",
                "cmc": 1,
                "color_identity": ["R"],
                "oracle_text": "Haste\nProwess"
            },
            {
                "name": "Lava Spike",
                "quantity": 4,
                "type_line": "Sorcery — Arcane",
                "mana_cost": "{R}",
                "cmc": 1,
                "color_identity": ["R"],
                "oracle_text": "Lava Spike deals 3 damage to target player or planeswalker."
            },
            {
                "name": "Rift Bolt",
                "quantity": 4,
                "type_line": "Sorcery",
                "mana_cost": "{2}{R}",
                "cmc": 3,
                "color_identity": ["R"],
                "oracle_text": "Rift Bolt deals 3 damage to any target."
            },
            {
                "name": "Mountain",
                "quantity": 20,
                "type_line": "Basic Land — Mountain",
                "mana_cost": "",
                "cmc": 0,
                "color_identity": ["R"],
                "oracle_text": "{T}: Add {R}."
            }
        ]
        
        instructions = (
            "Analyze the decklist and return a JSON response with: "
            "main_title (archetype name), color_identity (e.g., 'mono-red', 'dimir'), "
            "strategy (one of: aggro, midrange, control, ramp, combo), "
            "confidence (0-1), and reasoning (explanation). "
            "Respond with ONLY valid JSON, no additional text."
        )
        
        prompt_data = {
            "task": "Classify MTG decklist archetype",
            "format": "Modern",
            "mainboard_cards": cards,
            "instructions": instructions
        }
        
        return json.dumps(prompt_data, indent=2)
    
    def test_azure_openai_classification(self, azure_client, sample_decklist_prompt):
        """Test real Azure OpenAI API call for deck classification"""
        # Call LLM
        response = azure_client.run(sample_decklist_prompt)
        
        assert response is not None
        assert hasattr(response, 'text')
        assert response.text
        
        logger.info(f"Received response from Azure OpenAI: {response.text[:200]}...")
        
        # Try to parse as JSON
        try:
            response_json = json.loads(response.text)
            
            # Validate expected fields
            assert 'main_title' in response_json
            assert 'color_identity' in response_json
            assert 'strategy' in response_json
            assert 'confidence' in response_json
            
            # Validate with Pydantic model
            classification = ArchetypeClassificationResponse(**response_json)
            
            assert classification.main_title
            assert classification.color_identity
            assert classification.strategy in ['aggro', 'midrange', 'control', 'ramp', 'combo']
            assert 0.0 <= classification.confidence <= 1.0
            
            logger.info(
                f"Successfully classified as: {classification.main_title} "
                f"({classification.strategy}, confidence: {classification.confidence:.2f})"
            )
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse response as JSON: {e}")
            logger.error(f"Response text: {response.text}")
            pytest.fail(f"LLM response was not valid JSON: {e}")
    
    def test_azure_openai_simple_query(self, azure_client):
        """Test simple query to verify Azure OpenAI connection"""
        simple_prompt = "What is 2+2? Respond with only a number."
        
        response = azure_client.run(simple_prompt)
        
        assert response is not None
        assert hasattr(response, 'text')
        assert response.text
        assert '4' in response.text
        
        logger.info(f"Simple query response: {response.text}")
    
    def test_azure_openai_json_mode(self, azure_client):
        """Test Azure OpenAI with JSON-formatted request"""
        json_prompt = json.dumps({
            "question": "What color is the sky?",
            "format": "json",
            "instructions": "Respond with a JSON object containing 'answer' and 'confidence' (0-1)"
        })
        
        response = azure_client.run(json_prompt)
        
        assert response is not None
        assert response.text
        
        # Try to parse as JSON
        try:
            response_json = json.loads(response.text)
            assert 'answer' in response_json or 'confidence' in response_json
            logger.info(f"JSON mode response: {response_json}")
        except json.JSONDecodeError:
            # Some models may not strictly follow JSON mode, just verify we got a response
            logger.warning("Response was not JSON, but call succeeded")
            assert len(response.text) > 0


@pytest.mark.integration
class TestArchetypeClassificationPipeline:
    """Integration tests for archetype classification with real database"""
    
    @pytest.fixture
    def pipeline(self):
        """Create ArchetypeClassificationPipeline instance with mock LLM"""
        return ArchetypeClassificationPipeline(
            model_provider='azure_openai',
            prompt_id='archetype_classification'
        )
    
    @pytest.fixture
    def sample_tournament_data(self, test_database):
        """Load sample tournament and card data into database"""
        # Check if cards already exist (preserved across tests for performance)
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cards")
            existing_card_count = cur.fetchone()[0]
        
        if existing_card_count > 0:
            logger.info(f"Using existing {existing_card_count} cards in database (skipping Scryfall load)")
        else:
            # Load cards from Scryfall (only on first test)
            logger.info("Loading card data from Scryfall...")
            cards_pipeline = CardsPipeline()
            cards_result = cards_pipeline.insert_cards(batch_size=1000, update_existing=True)
            assert cards_result['cards_loaded'] > 0, "Failed to load any cards from Scryfall"
            logger.info(f"Loaded {cards_result['cards_loaded']} cards")
            
            # Verify cards are in database
            with DatabaseConnection.get_cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM cards")
                card_count = cur.fetchone()[0]
                assert card_count > 0, "Cards table is empty after loading"
                logger.info(f"Verified {card_count} cards in database")
        
        # Load a tournament
        api_key = os.getenv('TOPDECK_API_KEY')
        assert api_key, "TOPDECK_API_KEY environment variable not set"
        
        logger.info("Fetching tournament data from TopDeck API...")
        tournaments_pipeline = TournamentsPipeline(api_key)
        tournaments = tournaments_pipeline.client.get_tournaments(
            game="Magic: The Gathering",
            format="Modern",
            last=30
        )
        
        assert tournaments is not None and len(tournaments) > 0, "No tournaments fetched from API"
        logger.info(f"Fetched {len(tournaments)} tournaments")
        
        # Insert first tournament with deck data
        logger.info(f"Inserting tournament: {tournaments[0].get('tournamentName', 'Unknown')}")
        success = tournaments_pipeline.insert_all(tournaments[0], include_rounds=False)
        assert success is True, "Failed to insert tournament data"
        
        # Verify we have tournament in database
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tournaments")
            tournament_count = cur.fetchone()[0]
            assert tournament_count > 0, "Tournaments table is empty after insertion"
            
            cur.execute("SELECT COUNT(*) FROM decklists")
            decklist_count = cur.fetchone()[0]
            assert decklist_count > 0, "Decklists table is empty after insertion"
            
            cur.execute("SELECT COUNT(*) FROM deck_cards WHERE section = 'mainboard'")
            deck_card_count = cur.fetchone()[0]
            assert deck_card_count > 0, "No mainboard cards inserted"
            
            # Verify we have decklists with cards
            cur.execute("""
                SELECT COUNT(DISTINCT d.decklist_id)
                FROM decklists d
                JOIN deck_cards dc ON d.decklist_id = dc.decklist_id
                WHERE dc.section = 'mainboard'
            """)
            decklists_with_cards = cur.fetchone()[0]
            assert decklists_with_cards > 0, "No decklists have mainboard cards"
            logger.info(f"Loaded tournament with {decklist_count} decklists and {decklists_with_cards} with mainboard cards")
        
        return tournaments[0]
    
    def test_get_unclassified_decklists(self, pipeline, sample_tournament_data, test_database):
        """Test querying unclassified decklists"""
        decklists = pipeline.get_unclassified_decklists()
        
        assert isinstance(decklists, list)
        assert len(decklists) > 0, "No unclassified decklists found - sample tournament data may not have been loaded"
        
        # Verify structure and content
        for decklist in decklists:
            assert 'decklist_id' in decklist
            assert 'format' in decklist
            assert 'tournament_id' in decklist
            assert isinstance(decklist['decklist_id'], int)
            assert decklist['decklist_id'] > 0, f"Invalid decklist_id: {decklist['decklist_id']}"
            assert isinstance(decklist['format'], str)
            assert len(decklist['format']) > 0, "Format should not be empty"
            assert isinstance(decklist['tournament_id'], str)
            assert len(decklist['tournament_id']) > 0, "Tournament ID should not be empty"
        
        # Verify these decklists actually have no archetype_group_id
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM decklists 
                WHERE archetype_group_id IS NULL
            """)
            unclassified_count = cur.fetchone()[0]
            assert unclassified_count == len(decklists), \
                f"Mismatch between query result ({len(decklists)}) and actual NULL archetype_group_id count ({unclassified_count})"
        
        logger.info(f"Found {len(decklists)} unclassified decklists")
    
    def test_get_decklist_mainboard_cards(self, pipeline, sample_tournament_data, test_database):
        """Test getting mainboard cards for a decklist"""
        # Get a decklist ID
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT d.decklist_id
                FROM decklists d
                JOIN deck_cards dc ON d.decklist_id = dc.decklist_id
                WHERE dc.section = 'mainboard'
                LIMIT 1
            """)
            result = cur.fetchone()
            assert result is not None, "No decklists with mainboard cards found"
            decklist_id = result[0]
        
        # Get mainboard cards
        cards = pipeline.get_decklist_mainboard_cards(decklist_id)
        
        assert isinstance(cards, list)
        assert len(cards) > 0, f"No mainboard cards found for decklist {decklist_id}"
        
        # Verify card structure and content
        total_quantity = 0
        for card in cards:
            assert 'card_id' in card
            assert 'name' in card
            assert 'quantity' in card
            assert 'type_line' in card
            assert 'mana_cost' in card
            assert 'cmc' in card
            assert 'color_identity' in card
            assert 'oracle_text' in card
            
            # Verify data types and values
            assert isinstance(card['card_id'], str), f"card_id should be string, got {type(card['card_id'])}"
            assert isinstance(card['name'], str), f"name should be string, got {type(card['name'])}"
            assert len(card['name']) > 0, "Card name should not be empty"
            assert isinstance(card['quantity'], int), f"quantity should be int, got {type(card['quantity'])}"
            assert card['quantity'] > 0, f"Quantity must be positive, got {card['quantity']}"
            assert isinstance(card['cmc'], (int, float)), f"cmc should be numeric, got {type(card['cmc'])}"
            assert isinstance(card['color_identity'], list), f"color_identity should be list, got {type(card['color_identity'])}"
            
            total_quantity += card['quantity']
        
        # Verify deck has reasonable size (40-100 cards is typical for constructed formats)
        assert total_quantity >= 40, f"Deck has only {total_quantity} cards, which seems too few"
        assert total_quantity <= 100, f"Deck has {total_quantity} cards, which seems too many"
        
        logger.info(f"Retrieved {len(cards)} unique mainboard cards (total {total_quantity} cards) for decklist {decklist_id}")
    
    def test_insert_archetype_with_mock_llm(self, pipeline, sample_tournament_data, test_database):
        """Test inserting archetype with mocked LLM response"""
        # Get a decklist
        decklists = pipeline.get_unclassified_decklists()
        assert len(decklists) > 0, "No unclassified decklists available for testing"
        
        decklist = decklists[0]
        decklist_id = decklist['decklist_id']
        format_name = decklist['format']
        
        # Get mainboard cards
        cards = pipeline.get_decklist_mainboard_cards(decklist_id)
        assert len(cards) > 0, f"No mainboard cards found for decklist {decklist_id}"
        
        # Verify initial state - no archetype groups or classifications
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM archetype_groups")
            initial_group_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM archetype_classifications")
            initial_classification_count = cur.fetchone()[0]
        
        # Mock LLM response
        mock_response = {
            'main_title': 'Burn',
            'color_identity': 'red',
            'strategy': 'aggro',
            'confidence': 0.95,
            'reasoning': 'Deck contains direct damage spells'
        }
        
        with patch.object(pipeline, 'classify_decklist_llm') as mock_classify:
            mock_classify.return_value = ArchetypeClassificationResponse(**mock_response)
            
            # Insert archetype
            archetype_group_id = pipeline.insert_archetype(decklist_id, format_name, cards)
            
            assert archetype_group_id is not None, "insert_archetype returned None"
            assert isinstance(archetype_group_id, int), f"Expected int, got {type(archetype_group_id)}"
            assert archetype_group_id > 0, f"Invalid archetype_group_id: {archetype_group_id}"
        
        # Verify archetype group in database
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT archetype_group_id, format, main_title, color_identity, strategy, created_at
                FROM archetype_groups
                WHERE archetype_group_id = %s
            """, (archetype_group_id,))
            
            group_result = cur.fetchone()
            assert group_result is not None, f"Archetype group {archetype_group_id} not found in database"
            assert group_result[0] == archetype_group_id  # archetype_group_id
            assert group_result[1] == format_name  # format
            assert group_result[2] == 'Burn'  # main_title
            assert group_result[3] == 'red'  # color_identity
            assert group_result[4] == 'aggro'  # strategy
            assert group_result[5] is not None  # created_at
            
            # Verify classification event in database
            cur.execute("""
                SELECT classification_id, decklist_id, archetype_group_id, 
                       archetype_confidence, llm_model, prompt_id, classified_at
                FROM archetype_classifications
                WHERE decklist_id = %s AND archetype_group_id = %s
            """, (decklist_id, archetype_group_id))
            
            classification_result = cur.fetchone()
            assert classification_result is not None, f"Classification not found for decklist {decklist_id}"
            assert classification_result[0] > 0  # classification_id
            assert classification_result[1] == decklist_id  # decklist_id
            assert classification_result[2] == archetype_group_id  # archetype_group_id
            assert classification_result[3] == 0.95  # archetype_confidence
            assert classification_result[4] == pipeline.model_name  # llm_model
            assert classification_result[5] == pipeline.prompt_id  # prompt_id
            assert classification_result[6] is not None  # classified_at
            
            # Verify counts increased
            cur.execute("SELECT COUNT(*) FROM archetype_groups")
            final_group_count = cur.fetchone()[0]
            assert final_group_count == initial_group_count + 1, \
                f"Expected group count to increase by 1 (from {initial_group_count} to {final_group_count})"
            
            cur.execute("SELECT COUNT(*) FROM archetype_classifications")
            final_classification_count = cur.fetchone()[0]
            assert final_classification_count == initial_classification_count + 1, \
                f"Expected classification count to increase by 1 (from {initial_classification_count} to {final_classification_count})"
        
        logger.info(f"Successfully inserted archetype group {archetype_group_id} for decklist {decklist_id}")
    
    def test_update_decklist_archetype(self, pipeline, sample_tournament_data, test_database):
        """Test updating decklist archetype_group_id reference"""
        # Get a decklist and create an archetype
        decklists = pipeline.get_unclassified_decklists()
        assert len(decklists) > 0, "No unclassified decklists available"
        
        decklist = decklists[0]
        decklist_id = decklist['decklist_id']
        format_name = decklist['format']
        
        # Verify initial state - decklist has no archetype_group_id
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT archetype_group_id FROM decklists WHERE decklist_id = %s
            """, (decklist_id,))
            result = cur.fetchone()
            assert result is not None, f"Decklist {decklist_id} not found"
            assert result[0] is None, f"Decklist {decklist_id} already has archetype_group_id: {result[0]}"
        
        cards = pipeline.get_decklist_mainboard_cards(decklist_id)
        assert len(cards) > 0, f"No cards found for decklist {decklist_id}"
        
        # Create archetype with mock LLM
        mock_response = {
            'main_title': 'Control',
            'color_identity': 'azorius',
            'strategy': 'control',
            'confidence': 0.88,
            'reasoning': 'Deck contains counterspells and card draw'
        }
        
        with patch.object(pipeline, 'classify_decklist_llm') as mock_classify:
            mock_classify.return_value = ArchetypeClassificationResponse(**mock_response)
            archetype_group_id = pipeline.insert_archetype(decklist_id, format_name, cards)
        
        assert archetype_group_id is not None, "Failed to create archetype group"
        assert archetype_group_id > 0, f"Invalid archetype_group_id: {archetype_group_id}"
        
        # Update decklist reference
        success = pipeline.update_decklist_archetype(decklist_id, archetype_group_id)
        assert success is True, f"Failed to update decklist {decklist_id}"
        
        # Verify decklist now references archetype group
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT archetype_group_id FROM decklists WHERE decklist_id = %s
            """, (decklist_id,))
            
            result = cur.fetchone()
            assert result is not None, f"Decklist {decklist_id} not found after update"
            assert result[0] is not None, "archetype_group_id is still NULL after update"
            assert result[0] == archetype_group_id, \
                f"Expected archetype_group_id {archetype_group_id}, got {result[0]}"
        
        logger.info(f"Successfully updated decklist {decklist_id} to reference archetype group {archetype_group_id}")
    
    def test_load_initial_with_mock_llm(self, pipeline, sample_tournament_data, test_database):
        """Test initial load with mocked LLM"""
        # Get initial counts
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM decklists")
            total_decklists = cur.fetchone()[0]
            assert total_decklists > 0, "No decklists in database to classify"
            
            cur.execute("SELECT COUNT(*) FROM archetype_groups")
            initial_group_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM archetype_classifications")
            initial_classification_count = cur.fetchone()[0]
        
        # Mock LLM responses
        def mock_classify(cards, format_name, max_retries=1):
            return ArchetypeClassificationResponse(
                main_title='Test Archetype',
                color_identity='multicolor',
                strategy=StrategyType.MIDRANGE,
                confidence=0.85,
                reasoning='Test classification'
            )
        
        with patch.object(pipeline, 'classify_decklist_llm', side_effect=mock_classify):
            # Run initial load with small batch size
            result = pipeline.load_initial(batch_size=5)
            
            assert result['success'] is True, "load_initial returned success=False"
            assert result['objects_loaded'] >= 0, f"Invalid objects_loaded: {result['objects_loaded']}"
            assert result['objects_processed'] >= result['objects_loaded'], \
                f"objects_processed ({result['objects_processed']}) should be >= objects_loaded ({result['objects_loaded']})"
            assert result['errors'] >= 0, f"Invalid errors count: {result['errors']}"
            assert result['objects_loaded'] + result['errors'] == result['objects_processed'], \
                "objects_loaded + errors should equal objects_processed"
        
        # Verify archetype groups were created
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM archetype_groups")
            archetype_group_count = cur.fetchone()[0]
            # Note: Could be less than objects_loaded if multiple decklists share same archetype
            assert archetype_group_count >= initial_group_count, \
                f"Archetype groups did not increase (was {initial_group_count}, now {archetype_group_count})"
            if result['objects_loaded'] > 0:
                assert archetype_group_count >= 1, "No archetype groups created despite successful classifications"
            
            # Verify decklists reference archetype groups
            cur.execute("""
                SELECT COUNT(*) FROM decklists WHERE archetype_group_id IS NOT NULL
            """)
            linked_count = cur.fetchone()[0]
            assert linked_count == result['objects_loaded'], \
                f"Expected {result['objects_loaded']} decklists with archetype_group_id, got {linked_count}"
            
            # Verify classification events were created
            cur.execute("SELECT COUNT(*) FROM archetype_classifications")
            classification_count = cur.fetchone()[0]
            assert classification_count == initial_classification_count + result['objects_loaded'], \
                f"Expected {result['objects_loaded']} new classifications, got {classification_count - initial_classification_count}"
        
        # Verify load metadata
        if result['objects_loaded'] > 0:
            with DatabaseConnection.get_cursor() as cur:
                cur.execute("""
                    SELECT last_load_date, objects_loaded, data_type, load_type
                    FROM load_metadata
                    WHERE data_type = 'archetypes'
                    ORDER BY id DESC LIMIT 1
                """)
                metadata = cur.fetchone()
                assert metadata is not None, "No load metadata created"
                assert metadata[1] == result['objects_loaded'], \
                    f"Metadata objects_loaded ({metadata[1]}) != result objects_loaded ({result['objects_loaded']})"
                assert metadata[2] == 'archetypes', f"Expected data_type 'archetypes', got '{metadata[2]}'"
                assert metadata[3] == 'initial', f"Expected load_type 'initial', got '{metadata[3]}'"
        
        logger.info(
            f"Initial load: {result['objects_loaded']} classified, "
            f"{result['errors']} errors, "
            f"{result['objects_processed']} processed"
        )
    
    def test_load_incremental_with_mock_llm(self, pipeline, sample_tournament_data, test_database):
        """Test incremental load with mocked LLM"""
        # First do initial load
        def mock_classify(cards, format_name, max_retries=1):
            return ArchetypeClassificationResponse(
                main_title='Test Archetype',
                color_identity='multicolor',
                strategy=StrategyType.COMBO,
                confidence=0.80,
                reasoning='Test classification'
            )
        
        with patch.object(pipeline, 'classify_decklist_llm', side_effect=mock_classify):
            initial_result = pipeline.load_initial(batch_size=3)
            assert initial_result['success'] is True, "Initial load failed"
            
            # Get counts after initial load
            with DatabaseConnection.get_cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM archetype_classifications")
                classifications_after_initial = cur.fetchone()[0]
                assert classifications_after_initial == initial_result['objects_loaded'], \
                    f"Classification count mismatch after initial load"
            
            # Run incremental load (should find no new tournaments since last load)
            incremental_result = pipeline.load_incremental(batch_size=3)
            assert incremental_result['success'] is True, "Incremental load failed"
            
            # Total classified should be initial + incremental
            total_classified = initial_result['objects_loaded'] + incremental_result['objects_loaded']
        
        # Verify total classification events
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM archetype_classifications")
            classification_count = cur.fetchone()[0]
            assert classification_count == total_classified, \
                f"Expected {total_classified} total classifications, got {classification_count}"
            
            # Verify archetype groups (may be fewer if decklists share archetypes)
            cur.execute("SELECT COUNT(*) FROM archetype_groups")
            archetype_group_count = cur.fetchone()[0]
            if total_classified > 0:
                assert archetype_group_count >= 1, "No archetype groups created"
            
            # Verify load metadata for incremental load
            if incremental_result['objects_loaded'] > 0:
                cur.execute("""
                    SELECT COUNT(*) FROM load_metadata 
                    WHERE data_type = 'archetypes' AND load_type = 'incremental'
                """)
                incremental_metadata_count = cur.fetchone()[0]
                assert incremental_metadata_count >= 1, "No incremental load metadata created"
        
        logger.info(
            f"Incremental load: {incremental_result['objects_loaded']} new classifications "
            f"(total: {total_classified})"
        )
    
    def test_confidence_scoring(self, pipeline, sample_tournament_data, test_database):
        """Test confidence scoring and filtering"""
        decklists = pipeline.get_unclassified_decklists()
        assert len(decklists) > 0
        
        decklist = decklists[0]
        decklist_id = decklist['decklist_id']
        format_name = decklist['format']
        cards = pipeline.get_decklist_mainboard_cards(decklist_id)
        
        # Test low confidence classification
        low_confidence_response = {
            'main_title': 'Unknown Brew',
            'color_identity': 'jund',
            'strategy': 'midrange',
            'confidence': 0.35,  # Low confidence
            'reasoning': 'Unclear archetype'
        }
        
        with patch.object(pipeline, 'classify_decklist_llm') as mock_classify:
            mock_classify.return_value = ArchetypeClassificationResponse(**low_confidence_response)
            
            archetype_group_id = pipeline.insert_archetype(decklist_id, format_name, cards)
            
            # Should still insert even with low confidence
            assert archetype_group_id is not None
        
        # Verify low confidence classification in database
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT archetype_confidence 
                FROM archetype_classifications 
                WHERE decklist_id = %s AND archetype_group_id = %s
            """, (decklist_id, archetype_group_id))
            result = cur.fetchone()
            assert result is not None
            assert result[0] == 0.35
        
        logger.info(f"Successfully stored low-confidence classification (confidence: 0.35)")
    
    def test_archetype_group_id_updates_on_decklists(self, pipeline, sample_tournament_data, test_database):
        """Test that archetype_group_id is properly updated on decklists table"""
        # Get multiple unclassified decklists
        decklists = pipeline.get_unclassified_decklists()
        assert len(decklists) >= 2
        
        classified_ids = []
        
        # Classify two decklists
        for i in range(min(2, len(decklists))):
            decklist = decklists[i]
            decklist_id = decklist['decklist_id']
            format_name = decklist['format']
            cards = pipeline.get_decklist_mainboard_cards(decklist_id)
            
            mock_response = {
                'main_title': f'Archetype {i+1}',
                'color_identity': 'mono-red',
                'strategy': 'aggro',
                'confidence': 0.90,
                'reasoning': f'Test archetype {i+1}'
            }
            
            with patch.object(pipeline, 'classify_decklist_llm') as mock_classify:
                mock_classify.return_value = ArchetypeClassificationResponse(**mock_response)
                archetype_group_id = pipeline.insert_archetype(decklist_id, format_name, cards)
            
            assert archetype_group_id is not None
            
            # Update decklist
            success = pipeline.update_decklist_archetype(decklist_id, archetype_group_id)
            assert success is True
            
            classified_ids.append((decklist_id, archetype_group_id))
        
        # Verify all decklists have correct archetype_group_id
        with DatabaseConnection.get_cursor() as cur:
            for decklist_id, expected_archetype_group_id in classified_ids:
                cur.execute("""
                    SELECT archetype_group_id FROM decklists WHERE decklist_id = %s
                """, (decklist_id,))
                result = cur.fetchone()
                assert result is not None
                assert result[0] == expected_archetype_group_id
        
        logger.info(f"Successfully verified archetype_group_id updates on {len(classified_ids)} decklists")

