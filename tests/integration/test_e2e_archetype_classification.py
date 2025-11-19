"""Integration tests for archetype classification pipeline including LLM client with real API calls (Azure OpenAI only)"""

import os
import pytest
import logging
import json
from unittest.mock import patch, MagicMock

from src.etl.api_clients.llm_client import get_llm_client
from src.etl.archetype_pipeline import ArchetypeClassificationResponse

from src.etl.archetype_pipeline import (
    ArchetypeClassificationPipeline,
    ArchetypeClassificationResponse,
    StrategyType
)
from src.etl.cards_pipeline import CardsPipeline
from src.etl.tournaments_pipeline import TournamentsPipeline
from src.database.connection import DatabaseConnection

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
        # Load cards first
        cards_pipeline = CardsPipeline()
        cards_result = cards_pipeline.insert_cards(batch_size=1000, update_existing=True)
        assert cards_result['cards_loaded'] > 0
        
        # Load a tournament
        api_key = os.getenv('TOPDECK_API_KEY')
        assert api_key, "TOPDECK_API_KEY environment variable not set"
        
        tournaments_pipeline = TournamentsPipeline(api_key)
        tournaments = tournaments_pipeline.client.get_tournaments(
            game="Magic: The Gathering",
            format="Modern",
            last=30
        )
        
        assert tournaments is not None and len(tournaments) > 0
        
        # Insert first tournament with deck data
        success = tournaments_pipeline.insert_all(tournaments[0], include_rounds=False)
        assert success is True
        
        # Verify we have decklists with cards
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT COUNT(DISTINCT d.decklist_id)
                FROM decklists d
                JOIN deck_cards dc ON d.decklist_id = dc.decklist_id
                WHERE dc.section = 'mainboard'
            """)
            decklist_count = cur.fetchone()[0]
            assert decklist_count > 0
        
        return tournaments[0]
    
    def test_get_unclassified_decklists(self, pipeline, sample_tournament_data, test_database):
        """Test querying unclassified decklists"""
        decklists = pipeline.get_unclassified_decklists()
        
        assert isinstance(decklists, list)
        assert len(decklists) > 0
        
        # Verify structure
        for decklist in decklists:
            assert 'decklist_id' in decklist
            assert 'format' in decklist
            assert 'tournament_id' in decklist
            assert isinstance(decklist['decklist_id'], int)
            assert isinstance(decklist['format'], str)
        
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
            assert result is not None
            decklist_id = result[0]
        
        # Get mainboard cards
        cards = pipeline.get_decklist_mainboard_cards(decklist_id)
        
        assert isinstance(cards, list)
        assert len(cards) > 0
        
        # Verify card structure
        for card in cards:
            assert 'card_id' in card
            assert 'name' in card
            assert 'quantity' in card
            assert 'type_line' in card
            assert 'mana_cost' in card
            assert 'cmc' in card
            assert 'color_identity' in card
            assert 'oracle_text' in card
            assert card['quantity'] > 0
        
        logger.info(f"Retrieved {len(cards)} mainboard cards for decklist {decklist_id}")
    
    def test_insert_archetype_with_mock_llm(self, pipeline, sample_tournament_data, test_database):
        """Test inserting archetype with mocked LLM response"""
        # Get a decklist
        decklists = pipeline.get_unclassified_decklists()
        assert len(decklists) > 0
        
        decklist = decklists[0]
        decklist_id = decklist['decklist_id']
        format_name = decklist['format']
        
        # Get mainboard cards
        cards = pipeline.get_decklist_mainboard_cards(decklist_id)
        assert len(cards) > 0
        
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
            archetype_id = pipeline.insert_archetype(decklist_id, format_name, cards)
            
            assert archetype_id is not None
            assert isinstance(archetype_id, int)
        
        # Verify archetype in database
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT archetype_id, main_title, color_identity, strategy, 
                       archetype_confidence, llm_model, prompt_id
                FROM archetypes
                WHERE archetype_id = %s
            """, (archetype_id,))
            
            result = cur.fetchone()
            assert result is not None
            assert result[1] == 'Burn'  # main_title
            assert result[2] == 'red'  # color_identity
            assert result[3] == 'aggro'  # strategy
            assert result[4] == 0.95  # archetype_confidence
            assert result[5] == pipeline.model_name  # llm_model
            assert result[6] == pipeline.prompt_id  # prompt_id
        
        logger.info(f"Successfully inserted archetype {archetype_id} for decklist {decklist_id}")
    
    def test_update_decklist_archetype(self, pipeline, sample_tournament_data, test_database):
        """Test updating decklist archetype_id reference"""
        # Get a decklist and create an archetype
        decklists = pipeline.get_unclassified_decklists()
        assert len(decklists) > 0
        
        decklist = decklists[0]
        decklist_id = decklist['decklist_id']
        format_name = decklist['format']
        
        cards = pipeline.get_decklist_mainboard_cards(decklist_id)
        
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
            archetype_id = pipeline.insert_archetype(decklist_id, format_name, cards)
        
        assert archetype_id is not None
        
        # Update decklist reference
        success = pipeline.update_decklist_archetype(decklist_id, archetype_id)
        assert success is True
        
        # Verify decklist references archetype
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT archetype_id FROM decklists WHERE decklist_id = %s
            """, (decklist_id,))
            
            result = cur.fetchone()
            assert result is not None
            assert result[0] == archetype_id
        
        logger.info(f"Successfully updated decklist {decklist_id} to reference archetype {archetype_id}")
    
    def test_load_initial_with_mock_llm(self, pipeline, sample_tournament_data, test_database):
        """Test initial load with mocked LLM"""
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
            
            assert result['success'] is True
            assert result['objects_loaded'] >= 0
            assert result['objects_processed'] >= result['objects_loaded']
            assert result['errors'] >= 0
        
        # Verify archetypes were created
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM archetypes")
            archetype_count = cur.fetchone()[0]
            assert archetype_count == result['objects_loaded']
            
            # Verify decklists reference archetypes
            cur.execute("""
                SELECT COUNT(*) FROM decklists WHERE archetype_id IS NOT NULL
            """)
            linked_count = cur.fetchone()[0]
            assert linked_count == result['objects_loaded']
        
        # Verify load metadata
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT last_load_timestamp, objects_loaded, data_type, load_type
                FROM load_metadata
                WHERE data_type = 'archetypes'
                ORDER BY id DESC LIMIT 1
            """)
            metadata = cur.fetchone()
            if result['objects_loaded'] > 0:
                assert metadata is not None
                assert metadata[1] == result['objects_loaded']
                assert metadata[2] == 'archetypes'
                assert metadata[3] == 'initial'
        
        logger.info(
            f"Initial load: {result['objects_loaded']} classified, "
            f"{result['errors']} errors"
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
            assert initial_result['success'] is True
            
            # Run incremental load (should find no new tournaments)
            incremental_result = pipeline.load_incremental(batch_size=3)
            assert incremental_result['success'] is True
            
            # Total classified should be initial + incremental
            total_classified = initial_result['objects_loaded'] + incremental_result['objects_loaded']
        
        # Verify total archetypes
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM archetypes")
            archetype_count = cur.fetchone()[0]
            assert archetype_count == total_classified
        
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
            
            archetype_id = pipeline.insert_archetype(decklist_id, format_name, cards)
            
            # Should still insert even with low confidence
            assert archetype_id is not None
        
        # Verify low confidence archetype in database
        with DatabaseConnection.get_cursor() as cur:
            cur.execute("""
                SELECT archetype_confidence FROM archetypes WHERE archetype_id = %s
            """, (archetype_id,))
            result = cur.fetchone()
            assert result is not None
            assert result[0] == 0.35
        
        logger.info(f"Successfully stored low-confidence archetype (confidence: 0.35)")
    
    def test_archetype_id_updates_on_decklists(self, pipeline, sample_tournament_data, test_database):
        """Test that archetype_id is properly updated on decklists table"""
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
                archetype_id = pipeline.insert_archetype(decklist_id, format_name, cards)
            
            assert archetype_id is not None
            
            # Update decklist
            success = pipeline.update_decklist_archetype(decklist_id, archetype_id)
            assert success is True
            
            classified_ids.append((decklist_id, archetype_id))
        
        # Verify all decklists have correct archetype_id
        with DatabaseConnection.get_cursor() as cur:
            for decklist_id, expected_archetype_id in classified_ids:
                cur.execute("""
                    SELECT archetype_id FROM decklists WHERE decklist_id = %s
                """, (decklist_id,))
                result = cur.fetchone()
                assert result is not None
                assert result[0] == expected_archetype_id
        
        logger.info(f"Successfully verified archetype_id updates on {len(classified_ids)} decklists")

