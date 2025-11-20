"""Unit tests for archetype classification pipeline"""

import json
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pydantic import ValidationError

from src.etl.archetype_pipeline import (
    ArchetypeClassificationPipeline,
    ArchetypeClassificationResponse
)
from src.database.connection import DatabaseConnection


@pytest.fixture
def db_cursor():
    """Provide a database cursor for testing"""
    with DatabaseConnection.get_cursor() as cur:
        yield cur


def test_strategy_check_constraint(db_cursor):
    """Test that strategy column has CHECK constraint for valid values"""
    # Test valid strategy
    db_cursor.execute("""
        INSERT INTO archetype_groups (format, main_title, strategy, color_identity)
        VALUES ('Modern', 'test_archetype', 'aggro', 'mono_red')
        RETURNING archetype_group_id
    """)
    archetype_group_id = db_cursor.fetchone()[0]
    assert archetype_group_id is not None
    
    # Test invalid strategy should fail
    with pytest.raises(Exception) as exc_info:
        db_cursor.execute("""
            INSERT INTO archetype_groups (format, main_title, strategy, color_identity)
            VALUES ('Modern', 'test_archetype', 'invalid_strategy', 'mono_red')
        """)
    
    assert 'check constraint' in str(exc_info.value).lower()


def test_confidence_check_constraint(db_cursor):
    """Test that archetype_confidence has CHECK constraint for 0-1 range"""
    db_cursor.execute("""
        INSERT INTO tournaments (tournament_id, tournament_name, format, start_date)
        VALUES ('test-002', 'Test Tournament 2', 'Modern', CURRENT_TIMESTAMP)
    """)
    db_cursor.execute("""
        INSERT INTO players (player_id, tournament_id, name)
        VALUES ('p2', 'test-002', 'Test Player 2')
    """)
    db_cursor.execute("""
        INSERT INTO decklists (player_id, tournament_id)
        VALUES ('p2', 'test-002')
        RETURNING decklist_id
    """)
    decklist_id = db_cursor.fetchone()[0]
    
    db_cursor.execute("""
        INSERT INTO archetype_groups (format, main_title, strategy, color_identity)
        VALUES ('Modern', 'test_archetype', 'aggro', 'mono_red')
        RETURNING archetype_group_id
    """)
    archetype_group_id = db_cursor.fetchone()[0]
    
    # Test invalid confidence < 0 using savepoint
    db_cursor.execute("SAVEPOINT sp1")
    try:
        db_cursor.execute("""
            INSERT INTO archetype_classifications 
            (decklist_id, archetype_group_id, archetype_confidence, llm_model, prompt_id)
            VALUES (%s, %s, -0.1, 'gpt-4o-mini', 'v1')
        """, (decklist_id, archetype_group_id))
        pytest.fail("Should have raised exception for invalid confidence")
    except Exception as e:
        db_cursor.execute("ROLLBACK TO SAVEPOINT sp1")
        assert 'check constraint' in str(e).lower()
    
    # Test invalid confidence > 1 using savepoint
    db_cursor.execute("SAVEPOINT sp2")
    try:
        db_cursor.execute("""
            INSERT INTO archetype_classifications 
            (decklist_id, archetype_group_id, archetype_confidence, llm_model, prompt_id)
            VALUES (%s, %s, 1.5, 'gpt-4o-mini', 'v1')
        """, (decklist_id, archetype_group_id))
        pytest.fail("Should have raised exception for invalid confidence")
    except Exception as e:
        db_cursor.execute("ROLLBACK TO SAVEPOINT sp2")
        assert 'check constraint' in str(e).lower()


def test_archetype_classification_foreign_key_cascade(db_cursor):
    """Test that deleting a decklist cascades to archetype_classifications"""
    db_cursor.execute("""
        INSERT INTO tournaments (tournament_id, tournament_name, format, start_date)
        VALUES ('test-003', 'Test Tournament 3', 'Modern', CURRENT_TIMESTAMP)
    """)
    db_cursor.execute("""
        INSERT INTO players (player_id, tournament_id, name)
        VALUES ('p3', 'test-003', 'Test Player 3')
    """)
    db_cursor.execute("""
        INSERT INTO decklists (player_id, tournament_id)
        VALUES ('p3', 'test-003')
        RETURNING decklist_id
    """)
    decklist_id = db_cursor.fetchone()[0]
    
    db_cursor.execute("""
        INSERT INTO archetype_groups (format, main_title, strategy, color_identity)
        VALUES ('Modern', 'test_archetype', 'combo', 'gruul')
        RETURNING archetype_group_id
    """)
    archetype_group_id = db_cursor.fetchone()[0]
    
    db_cursor.execute("""
        INSERT INTO archetype_classifications 
        (decklist_id, archetype_group_id, archetype_confidence, llm_model, prompt_id)
        VALUES (%s, %s, 0.85, 'gpt-4o-mini', 'v1')
        RETURNING classification_id
    """, (decklist_id, archetype_group_id))
    classification_id = db_cursor.fetchone()[0]
    
    # Delete the decklist
    db_cursor.execute("DELETE FROM decklists WHERE decklist_id = %s", (decklist_id,))
    
    # Verify classification was also deleted (CASCADE)
    db_cursor.execute(
        "SELECT COUNT(*) FROM archetype_classifications WHERE classification_id = %s",
        (classification_id,)
    )
    count = db_cursor.fetchone()[0]
    assert count == 0, "Classification should be deleted when decklist is deleted (CASCADE)"


def test_decklist_archetype_group_id_set_null(db_cursor):
    """Test that deleting an archetype_group sets decklists.archetype_group_id to NULL"""
    db_cursor.execute("""
        INSERT INTO tournaments (tournament_id, tournament_name, format, start_date)
        VALUES ('test-004', 'Test Tournament 4', 'Modern', CURRENT_TIMESTAMP)
    """)
    db_cursor.execute("""
        INSERT INTO players (player_id, tournament_id, name)
        VALUES ('p4', 'test-004', 'Test Player 4')
    """)
    db_cursor.execute("""
        INSERT INTO decklists (player_id, tournament_id)
        VALUES ('p4', 'test-004')
        RETURNING decklist_id
    """)
    decklist_id = db_cursor.fetchone()[0]
    
    db_cursor.execute("""
        INSERT INTO archetype_groups (format, main_title, strategy, color_identity)
        VALUES ('Modern', 'test_archetype', 'midrange', 'jeskai')
        RETURNING archetype_group_id
    """)
    archetype_group_id = db_cursor.fetchone()[0]
    
    # Update decklist to reference the archetype group
    db_cursor.execute(
        "UPDATE decklists SET archetype_group_id = %s WHERE decklist_id = %s",
        (archetype_group_id, decklist_id)
    )
    
    # Delete the archetype group (but not the decklist)
    db_cursor.execute("DELETE FROM archetype_groups WHERE archetype_group_id = %s", (archetype_group_id,))
    
    # Verify decklist still exists but archetype_group_id is NULL (SET NULL)
    db_cursor.execute(
        "SELECT decklist_id, archetype_group_id FROM decklists WHERE decklist_id = %s",
        (decklist_id,)
    )
    result = db_cursor.fetchone()
    assert result is not None, "Decklist should still exist"
    assert result[1] is None, "archetype_group_id should be NULL after archetype_group deletion (SET NULL)"


def test_archetype_indexes_exist(db_cursor):
    """Test that required indexes exist on archetype tables"""
    db_cursor.execute("""
        SELECT indexname FROM pg_indexes 
        WHERE tablename IN ('archetype_groups', 'archetype_classifications', 'decklists')
        AND indexname IN (
            'idx_archetype_groups_format',
            'idx_archetype_classifications_decklist',
            'idx_archetype_classifications_group',
            'idx_decklists_archetype_group_id'
        )
    """)
    indexes = {row[0] for row in db_cursor.fetchall()}
    
    expected_indexes = {
        'idx_archetype_groups_format',
        'idx_archetype_classifications_decklist',
        'idx_archetype_classifications_group',
        'idx_decklists_archetype_group_id'
    }
    
    assert indexes == expected_indexes, f"Expected indexes {expected_indexes}, got {indexes}"


@pytest.fixture
def pipeline():
    """Create pipeline instance for testing"""
    with patch('src.etl.archetype_pipeline.DatabaseConnection'):
        with patch.dict('os.environ', {'LLM_MODEL': 'gpt-4o-mini'}):
            return ArchetypeClassificationPipeline(
                model_provider='openai',
                prompt_id='test_v1'
            )


@pytest.fixture
def sample_cards():
    """Sample enriched mainboard cards"""
    return [
        {
            'card_id': 'card1',
            'name': 'Lightning Bolt',
            'quantity': 4,
            'type_line': 'Instant',
            'mana_cost': '{R}',
            'cmc': 1.0,
            'color_identity': ['R'],
            'oracle_text': 'Lightning Bolt deals 3 damage to any target.'
        },
        {
            'card_id': 'card2',
            'name': 'Goblin Guide',
            'quantity': 4,
            'type_line': 'Creature — Goblin Scout',
            'mana_cost': '{R}',
            'cmc': 1.0,
            'color_identity': ['R'],
            'oracle_text': 'Haste. Whenever Goblin Guide attacks...'
        }
    ]


@pytest.fixture
def sample_classification():
    """Sample LLM classification response"""
    return ArchetypeClassificationResponse(
        main_title='burn',
        color_identity='mono_red',
        strategy='aggro',
        confidence=0.92,
        reasoning='Fast red aggro deck'
    )


class TestPromptGeneration:
    """Tests for prompt formatting"""
    
    def test_format_prompt_basic(self, pipeline):
        """Test basic prompt formatting"""
        cards = [
            {
                'name': 'Lightning Bolt',
                'quantity': 4,
                'type_line': 'Instant',
                'mana_cost': '{R}',
                'cmc': 1,
                'color_identity': ['R'],
                'oracle_text': 'Lightning Bolt deals 3 damage to any target.'
            }
        ]
        
        prompt = pipeline.format_classification_prompt(
            cards=cards,
            format_name='Modern',
            instructions='Classify this deck'
        )
        
        assert isinstance(prompt, str)
        prompt_data = json.loads(prompt)
        assert 'mainboard_cards' in prompt_data
        assert len(prompt_data['mainboard_cards']) == 1
        assert prompt_data['mainboard_cards'][0]['name'] == 'Lightning Bolt'
        assert prompt_data['format'] == 'Modern'
    
    def test_format_prompt_empty_cards(self, pipeline):
        """Test prompt formatting with empty card list"""
        with pytest.raises((ValueError, AssertionError)):
            pipeline.format_classification_prompt(
                cards=[],
                format_name='Modern',
                instructions='Test'
            )


class TestResponseParsing:
    """Tests for LLM response parsing"""
    
    def test_parse_valid_response(self, pipeline):
        """Test parsing a valid LLM response"""
        response_text = json.dumps({
            'main_title': 'amulet_titan',
            'color_identity': 'gruul',
            'strategy': 'combo',
            'confidence': 0.95,
            'reasoning': 'Deck focuses on Amulet of Vigor combo'
        })
        
        result = pipeline.parse_classification_response(response_text)
        
        assert isinstance(result, ArchetypeClassificationResponse)
        assert result.main_title == 'amulet_titan'
        assert result.strategy == 'combo'
        assert result.confidence == 0.95
    
    def test_parse_response_invalid(self, pipeline):
        """Test parsing invalid responses"""
        # Missing required field
        with pytest.raises(ValidationError):
            pipeline.parse_classification_response(json.dumps({
                'main_title': 'burn',
                'color_identity': 'mono_red',
                'confidence': 0.90
            }))
        
        # Invalid strategy
        with pytest.raises(ValidationError):
            pipeline.parse_classification_response(json.dumps({
                'main_title': 'burn',
                'color_identity': 'mono_red',
                'strategy': 'invalid_strategy',
                'confidence': 0.90
            }))
        
        # Invalid confidence
        with pytest.raises(ValidationError):
            pipeline.parse_classification_response(json.dumps({
                'main_title': 'burn',
                'color_identity': 'mono_red',
                'strategy': 'aggro',
                'confidence': 1.5
            }))
        
        # Invalid JSON
        with pytest.raises((json.JSONDecodeError, ValueError)):
            pipeline.parse_classification_response('This is not valid JSON')


class TestLLMIntegration:
    """Tests for LLM API integration"""
    
    @patch('src.etl.archetype_pipeline.get_llm_client')
    def test_classify_decklist_success(self, mock_get_client, pipeline):
        """Test successful decklist classification"""
        # Mock LLM client
        mock_client = Mock()
        mock_response = Mock()
        mock_response.text = json.dumps({
            'main_title': 'burn',
            'color_identity': 'mono_red',
            'strategy': 'aggro',
            'confidence': 0.92,
            'reasoning': 'Aggressive red spells'
        })
        mock_client.run.return_value = mock_response
        mock_get_client.return_value = mock_client
        
        cards = [
            {
                'name': 'Lightning Bolt',
                'quantity': 4,
                'type_line': 'Instant',
                'mana_cost': '{R}',
                'cmc': 1,
                'color_identity': ['R'],
                'oracle_text': 'Lightning Bolt deals 3 damage to any target.'
            }
        ]
        
        result = pipeline.classify_decklist_llm(
            cards=cards,
            format_name='Modern'
        )
        
        assert result.main_title == 'burn'
        assert result.strategy == 'aggro'
        assert result.confidence == 0.92
    
    @patch('src.etl.archetype_pipeline.get_llm_client')
    def test_classify_decklist_api_error(self, mock_get_client, pipeline):
        """Test handling of LLM API errors"""
        mock_client = Mock()
        mock_client.run.side_effect = Exception('API Error')
        mock_get_client.return_value = mock_client
        
        cards = [
            {
                'name': 'Test Card',
                'quantity': 4,
                'type_line': 'Instant',
                'mana_cost': '{U}',
                'cmc': 1,
                'color_identity': ['U'],
                'oracle_text': 'Test text'
            }
        ]
        
        with pytest.raises(Exception):
            pipeline.classify_decklist_llm(
                cards=cards,
                format_name='Modern'
            )
    
    @patch('src.etl.archetype_pipeline.get_llm_client')
    def test_classify_decklist_invalid_response(self, mock_get_client, pipeline):
        """Test handling of invalid LLM response"""
        mock_client = Mock()
        mock_response = Mock()
        mock_response.text = 'Invalid JSON response'
        mock_client.run.return_value = mock_response
        mock_get_client.return_value = mock_client
        
        cards = [
            {
                'name': 'Test Card',
                'quantity': 4,
                'type_line': 'Instant',
                'mana_cost': '{U}',
                'cmc': 1,
                'color_identity': ['U'],
                'oracle_text': 'Test text'
            }
        ]
        
        with pytest.raises((json.JSONDecodeError, ValueError)):
            pipeline.classify_decklist_llm(
                cards=cards,
                format_name='Modern'
            )


class TestArchetypeClassificationResponse:
    """Tests for Pydantic response model"""
    
    def test_valid_response_model(self):
        """Test creating valid response model"""
        response = ArchetypeClassificationResponse(
            main_title='test_archetype',
            color_identity='dimir',
            strategy='control',
            confidence=0.88,
            reasoning='Test reasoning'
        )
        
        assert response.main_title == 'test_archetype'
        assert response.strategy == 'control'
        assert response.confidence == 0.88
    
    def test_response_model_validation(self):
        """Test response model validation"""
        # Invalid confidence
        with pytest.raises(ValidationError):
            ArchetypeClassificationResponse(
                main_title='test',
                color_identity='test',
                strategy='aggro',
                confidence=2.0
            )
        
        # Invalid strategy
        with pytest.raises(ValidationError):
            ArchetypeClassificationResponse(
                main_title='test',
                color_identity='test',
                strategy='invalid',
                confidence=0.8
            )


class TestGetUnclassifiedDecklists:
    """Tests for get_unclassified_decklists method"""
    
    def test_get_unclassified(self, pipeline):
        """Test retrieving unclassified decklists"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, 'Modern', 'tournament1'),
            (2, 'Modern', 'tournament2')
        ]
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_cursor
        mock_context.__exit__.return_value = None
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_context):
            decklists = pipeline.get_unclassified_decklists()
        
        assert len(decklists) == 2
        assert decklists[0]['decklist_id'] == 1
        assert decklists[0]['format'] == 'Modern'
        
        # Test empty case
        mock_cursor.fetchall.return_value = []
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_context):
            decklists = pipeline.get_unclassified_decklists()
        assert len(decklists) == 0


class TestGetDecklistMainboardCards:
    """Tests for get_decklist_mainboard_cards method"""
    
    def test_get_mainboard_cards(self, pipeline, sample_cards):
        """Test retrieving and enriching mainboard cards"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                sample_cards[0]['card_id'],
                sample_cards[0]['name'],
                sample_cards[0]['quantity'],
                sample_cards[0]['type_line'],
                sample_cards[0]['mana_cost'],
                sample_cards[0]['cmc'],
                sample_cards[0]['color_identity'],
                sample_cards[0]['oracle_text']
            )
        ]
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_cursor
        mock_context.__exit__.return_value = None
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_context):
            cards = pipeline.get_decklist_mainboard_cards(1)
        
        assert len(cards) == 1
        assert cards[0]['name'] == 'Lightning Bolt'
        
        # Verify SQL query filters by section='mainboard'
        call_args = mock_cursor.execute.call_args[0][0]
        assert "section = 'mainboard'" in call_args or "section='mainboard'" in call_args


class TestClassifyDecklist:
    """Tests for insert_archetype method"""
    
    @patch.object(ArchetypeClassificationPipeline, 'classify_decklist_llm')
    def test_classify_success(self, mock_classify, pipeline, sample_cards, sample_classification):
        """Test successful classification"""
        mock_classify.return_value = sample_classification
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [(100,), (200,)]  # archetype_group_id, classification_id
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_cursor
        mock_context.__exit__.return_value = None
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_context):
            archetype_group_id = pipeline.insert_archetype(
                decklist_id=1,
                format_name='Modern',
                mainboard_cards=sample_cards
            )
        
        assert archetype_group_id == 100
        mock_classify.assert_called_once()
        
        # Verify INSERTs were called
        insert_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any('INSERT INTO archetype_groups' in call for call in insert_calls)
        assert any('INSERT INTO archetype_classifications' in call for call in insert_calls)
    
    @patch.object(ArchetypeClassificationPipeline, 'classify_decklist_llm')
    def test_classify_failure(self, mock_classify, pipeline, sample_cards):
        """Test handling of classification failure"""
        mock_classify.return_value = None
        result = pipeline.insert_archetype(1, 'Modern', sample_cards)
        assert result is None
        
        # Test insufficient cards
        result = pipeline.insert_archetype(1, 'Modern', [])
        assert result is None


class TestLoadInitial:
    """Tests for load_initial method"""
    
    @patch.object(ArchetypeClassificationPipeline, 'get_unclassified_decklists')
    @patch.object(ArchetypeClassificationPipeline, 'get_decklist_mainboard_cards')
    @patch.object(ArchetypeClassificationPipeline, 'insert_archetype')
    @patch.object(ArchetypeClassificationPipeline, 'update_decklist_archetype')
    @patch('src.etl.archetype_pipeline.update_load_metadata')
    def test_load_initial_success(
        self, mock_update_metadata, mock_update_decklist,
        mock_classify, mock_get_cards, mock_get_decklists, pipeline, sample_cards
    ):
        """Test successful initial load"""
        mock_get_decklists.return_value = [
            {'decklist_id': 1, 'format': 'Modern', 'tournament_id': 't1'},
            {'decklist_id': 2, 'format': 'Modern', 'tournament_id': 't2'}
        ]
        mock_get_cards.return_value = sample_cards
        mock_classify.return_value = 100  # archetype_group_id
        
        result = pipeline.load_initial(batch_size=10)
        
        assert result['success'] is True
        assert result['objects_loaded'] == 2
        assert result['objects_processed'] == 2
        assert result['errors'] == 0
        
        # Verify metadata was updated
        mock_update_metadata.assert_called_once()
        # Verify update_decklist_archetype was called for each successful classification
        assert mock_update_decklist.call_count == 2
    
    @patch.object(ArchetypeClassificationPipeline, 'get_unclassified_decklists')
    def test_load_initial_no_decklists(self, mock_get_decklists, pipeline):
        """Test initial load when no decklists exist"""
        mock_get_decklists.return_value = []
        
        result = pipeline.load_initial()
        
        assert result['success'] is True
        assert result['objects_loaded'] == 0
    
    @patch.object(ArchetypeClassificationPipeline, 'get_unclassified_decklists')
    @patch.object(ArchetypeClassificationPipeline, 'get_decklist_mainboard_cards')
    @patch.object(ArchetypeClassificationPipeline, 'insert_archetype')
    @patch.object(ArchetypeClassificationPipeline, 'update_decklist_archetype')
    def test_load_initial_with_errors(
        self, mock_update_decklist, mock_classify, mock_get_cards, mock_get_decklists, pipeline, sample_cards
    ):
        """Test initial load with some classification errors"""
        mock_get_decklists.return_value = [
            {'decklist_id': 1, 'format': 'Modern', 'tournament_id': 't1'},
            {'decklist_id': 2, 'format': 'Modern', 'tournament_id': 't2'}
        ]
        mock_get_cards.return_value = sample_cards
        mock_classify.side_effect = [100, None]  # Second one fails (returns None)
        
        result = pipeline.load_initial()
        
        assert result['success'] is True
        assert result['objects_loaded'] == 1
        assert result['objects_processed'] == 2
        assert result['errors'] == 1
        # Only one successful classification should update decklist
        assert mock_update_decklist.call_count == 1


class TestLoadIncremental:
    """Tests for load_incremental method"""
    
    @patch('src.etl.archetype_pipeline.get_last_load_timestamp')
    @patch.object(ArchetypeClassificationPipeline, 'get_decklists_since_timestamp')
    @patch.object(ArchetypeClassificationPipeline, 'get_decklist_mainboard_cards')
    @patch.object(ArchetypeClassificationPipeline, 'insert_archetype')
    @patch.object(ArchetypeClassificationPipeline, 'update_decklist_archetype')
    @patch('src.etl.archetype_pipeline.update_load_metadata')
    def test_load_incremental_success(
        self, mock_update_metadata, mock_update_decklist,
        mock_classify, mock_get_cards, mock_get_since, mock_get_last, pipeline, sample_cards
    ):
        """Test successful incremental load"""
        mock_get_last.return_value = datetime.fromtimestamp(1699000000)
        mock_get_since.return_value = [
            {'decklist_id': 1, 'format': 'Modern', 'tournament_id': 't1', 'start_date': datetime.fromtimestamp(1699500000)}
        ]
        mock_get_cards.return_value = sample_cards
        mock_classify.return_value = 100  # archetype_group_id
        
        result = pipeline.load_incremental()
        
        assert result['success'] is True
        assert result['objects_loaded'] == 1
        mock_get_last.assert_called_with('archetypes')
        assert mock_update_decklist.call_count == 1
    
    @patch('src.etl.archetype_pipeline.get_last_load_timestamp')
    def test_load_incremental_no_previous_load(self, mock_get_last, pipeline):
        """Test incremental load with no previous load (falls back to initial)"""
        mock_get_last.return_value = None
        
        with patch.object(pipeline, 'load_initial') as mock_initial:
            mock_initial.return_value = {
                'success': True,
                'objects_loaded': 0,
                'objects_processed': 0,
                'errors': 0
            }
            result = pipeline.load_incremental()
        
        assert result['success'] is True
        mock_initial.assert_called_once()


class TestBatchProcessing:
    """Tests for batch processing logic"""
    
    @patch.object(ArchetypeClassificationPipeline, 'get_unclassified_decklists')
    @patch.object(ArchetypeClassificationPipeline, 'get_decklist_mainboard_cards')
    @patch.object(ArchetypeClassificationPipeline, 'insert_archetype')
    @patch.object(ArchetypeClassificationPipeline, 'update_decklist_archetype')
    @patch('src.etl.archetype_pipeline.update_load_metadata')
    def test_batch_size_respected(
        self, mock_update_metadata, mock_update_decklist, mock_classify,
        mock_get_cards, mock_get_decklists, pipeline, sample_cards
    ):
        """Test that batch_size parameter is respected"""
        decklists = [
            {'decklist_id': i, 'format': 'Modern', 'tournament_id': f't{i}'}
            for i in range(10)
        ]
        mock_get_decklists.return_value = decklists
        mock_get_cards.return_value = sample_cards
        mock_classify.return_value = 100
        
        result = pipeline.load_initial(batch_size=5)
        
        assert result['objects_processed'] == 10
        assert result['objects_loaded'] == 10
        assert mock_update_decklist.call_count == 10


class TestEdgeCases:
    """Tests for edge cases and error conditions"""
    
    @patch('src.etl.archetype_pipeline.get_llm_client')
    def test_missing_card_data_threshold(self, mock_get_client, pipeline):
        """Test handling when >10% of cards are missing"""
        # Mock LLM client to avoid needing actual API key
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.text = '{"main_title": "test", "color_identity": "test", "strategy": "aggro", "confidence": 0.8}'
        mock_client.run.return_value = mock_response
        mock_get_client.return_value = mock_client
        
        # 10 cards, 2 missing (20% > 10% threshold)
        cards_with_missing = [
            {'name': f'Card{i}', 'quantity': 4}
            for i in range(8)  # Only 8 cards have full data
        ]
        
        # Should not classify (not enough data - cards missing required fields)
        result = pipeline.insert_archetype(1, 'Modern', cards_with_missing)
        assert result is None

