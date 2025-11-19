"""Unit tests for archetype classification pipeline"""

import json
import pytest
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


def test_archetypes_table_exists(db_cursor):
    """Test that archetypes table exists"""
    db_cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'archetypes'
        )
    """)
    exists = db_cursor.fetchone()[0]
    assert exists, "archetypes table should exist"


def test_archetypes_columns_exist(db_cursor):
    """Test that archetypes table has all required columns"""
    db_cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'archetypes'
        ORDER BY ordinal_position
    """)
    columns = {row[0]: row[1] for row in db_cursor.fetchall()}
    
    expected_columns = {
        'archetype_id': 'integer',
        'decklist_id': 'integer',
        'format': 'text',
        'main_title': 'text',
        'color_identity': 'text',
        'strategy': 'text',
        'archetype_confidence': 'double precision',
        'llm_model': 'text',
        'prompt_id': 'text',
        'classified_at': 'timestamp without time zone'
    }
    
    for col_name, col_type in expected_columns.items():
        assert col_name in columns, f"Column {col_name} should exist"
        assert columns[col_name] == col_type, f"Column {col_name} should be type {col_type}"


def test_decklists_archetype_id_column_exists(db_cursor):
    """Test that decklists table has archetype_id column"""
    db_cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'decklists' AND column_name = 'archetype_id'
    """)
    result = db_cursor.fetchone()
    assert result is not None, "archetype_id column should exist in decklists table"
    assert result[1] == 'integer', "archetype_id should be integer type"


def test_strategy_check_constraint(db_cursor):
    """Test that strategy column has CHECK constraint for valid values"""
    # Insert valid strategy
    db_cursor.execute("""
        INSERT INTO tournaments (tournament_id, tournament_name, format, start_date)
        VALUES ('test-001', 'Test Tournament', 'Modern', 1700000000)
    """)
    db_cursor.execute("""
        INSERT INTO players (player_id, tournament_id, name)
        VALUES ('p1', 'test-001', 'Test Player')
    """)
    db_cursor.execute("""
        INSERT INTO decklists (player_id, tournament_id)
        VALUES ('p1', 'test-001')
        RETURNING decklist_id
    """)
    decklist_id = db_cursor.fetchone()[0]
    
    # Test valid strategy
    db_cursor.execute("""
        INSERT INTO archetypes 
        (decklist_id, format, main_title, strategy, archetype_confidence, llm_model, prompt_id)
        VALUES (%s, 'Modern', 'test_archetype', 'aggro', 0.95, 'gpt-4o-mini', 'v1')
    """, (decklist_id,))
    
    # Test invalid strategy should fail
    with pytest.raises(Exception) as exc_info:
        db_cursor.execute("""
            INSERT INTO archetypes 
            (decklist_id, format, main_title, strategy, archetype_confidence, llm_model, prompt_id)
            VALUES (%s, 'Modern', 'test_archetype', 'invalid_strategy', 0.95, 'gpt-4o-mini', 'v1')
        """, (decklist_id,))
    
    assert 'check constraint' in str(exc_info.value).lower()


def test_confidence_check_constraint(db_cursor):
    """Test that archetype_confidence has CHECK constraint for 0-1 range"""
    db_cursor.execute("""
        INSERT INTO tournaments (tournament_id, tournament_name, format, start_date)
        VALUES ('test-002', 'Test Tournament 2', 'Modern', 1700000000)
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
    
    # Test invalid confidence < 0
    with pytest.raises(Exception) as exc_info:
        db_cursor.execute("""
            INSERT INTO archetypes 
            (decklist_id, format, main_title, strategy, archetype_confidence, llm_model, prompt_id)
            VALUES (%s, 'Modern', 'test_archetype', 'aggro', -0.1, 'gpt-4o-mini', 'v1')
        """, (decklist_id,))
    
    assert 'check constraint' in str(exc_info.value).lower()
    
    # Test invalid confidence > 1
    with pytest.raises(Exception) as exc_info:
        db_cursor.execute("""
            INSERT INTO archetypes 
            (decklist_id, format, main_title, strategy, archetype_confidence, llm_model, prompt_id)
            VALUES (%s, 'Modern', 'test_archetype', 'aggro', 1.5, 'gpt-4o-mini', 'v1')
        """, (decklist_id,))
    
    assert 'check constraint' in str(exc_info.value).lower()


def test_archetype_foreign_key_cascade(db_cursor):
    """Test that deleting a decklist cascades to archetypes"""
    db_cursor.execute("""
        INSERT INTO tournaments (tournament_id, tournament_name, format, start_date)
        VALUES ('test-003', 'Test Tournament 3', 'Modern', 1700000000)
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
        INSERT INTO archetypes 
        (decklist_id, format, main_title, strategy, archetype_confidence, llm_model, prompt_id)
        VALUES (%s, 'Modern', 'test_archetype', 'combo', 0.85, 'gpt-4o-mini', 'v1')
        RETURNING archetype_id
    """, (decklist_id,))
    archetype_id = db_cursor.fetchone()[0]
    
    # Delete the decklist
    db_cursor.execute("DELETE FROM decklists WHERE decklist_id = %s", (decklist_id,))
    
    # Verify archetype was also deleted (CASCADE)
    db_cursor.execute("SELECT COUNT(*) FROM archetypes WHERE archetype_id = %s", (archetype_id,))
    count = db_cursor.fetchone()[0]
    assert count == 0, "Archetype should be deleted when decklist is deleted (CASCADE)"


def test_decklist_archetype_id_set_null(db_cursor):
    """Test that deleting an archetype sets decklists.archetype_id to NULL"""
    db_cursor.execute("""
        INSERT INTO tournaments (tournament_id, tournament_name, format, start_date)
        VALUES ('test-004', 'Test Tournament 4', 'Modern', 1700000000)
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
        INSERT INTO archetypes 
        (decklist_id, format, main_title, strategy, archetype_confidence, llm_model, prompt_id)
        VALUES (%s, 'Modern', 'test_archetype', 'midrange', 0.90, 'gpt-4o-mini', 'v1')
        RETURNING archetype_id
    """, (decklist_id,))
    archetype_id = db_cursor.fetchone()[0]
    
    # Update decklist to reference the archetype
    db_cursor.execute(
        "UPDATE decklists SET archetype_id = %s WHERE decklist_id = %s",
        (archetype_id, decklist_id)
    )
    
    # Delete the archetype (but not the decklist)
    db_cursor.execute("DELETE FROM archetypes WHERE archetype_id = %s", (archetype_id,))
    
    # Verify decklist still exists but archetype_id is NULL (SET NULL)
    db_cursor.execute(
        "SELECT decklist_id, archetype_id FROM decklists WHERE decklist_id = %s",
        (decklist_id,)
    )
    result = db_cursor.fetchone()
    assert result is not None, "Decklist should still exist"
    assert result[1] is None, "archetype_id should be NULL after archetype deletion (SET NULL)"


def test_archetype_indexes_exist(db_cursor):
    """Test that required indexes exist on archetypes and decklists"""
    db_cursor.execute("""
        SELECT indexname FROM pg_indexes 
        WHERE tablename IN ('archetypes', 'decklists')
        AND indexname IN (
            'idx_archetypes_decklist_id',
            'idx_archetypes_format',
            'idx_decklists_archetype_id'
        )
    """)
    indexes = {row[0] for row in db_cursor.fetchall()}
    
    expected_indexes = {
        'idx_archetypes_decklist_id',
        'idx_archetypes_format',
        'idx_decklists_archetype_id'
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
        """Test basic prompt formatting with minimal cards"""
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
        assert 'Lightning Bolt' in prompt
        assert 'Modern' in prompt
        assert 'Classify this deck' in prompt
        
        # Verify it's valid JSON
        prompt_data = json.loads(prompt)
        assert 'mainboard_cards' in prompt_data
        assert len(prompt_data['mainboard_cards']) == 1
        assert prompt_data['mainboard_cards'][0]['name'] == 'Lightning Bolt'
    
    def test_format_prompt_with_multiple_cards(self, pipeline):
        """Test prompt formatting with multiple cards"""
        cards = [
            {
                'name': 'Amulet of Vigor',
                'quantity': 4,
                'type_line': 'Artifact',
                'mana_cost': '{1}',
                'cmc': 1,
                'color_identity': [],
                'oracle_text': 'Whenever a permanent enters the battlefield tapped and under your control, untap it.'
            },
            {
                'name': 'Primeval Titan',
                'quantity': 4,
                'type_line': 'Creature — Giant',
                'mana_cost': '{4}{G}{G}',
                'cmc': 6,
                'color_identity': ['G'],
                'oracle_text': 'Trample\nWhenever Primeval Titan enters the battlefield or attacks, you may search your library for up to two land cards...'
            }
        ]
        
        prompt = pipeline.format_classification_prompt(
            cards=cards,
            format_name='Modern',
            instructions='Test instructions'
        )
        
        prompt_data = json.loads(prompt)
        assert len(prompt_data['mainboard_cards']) == 2
        assert prompt_data['format'] == 'Modern'
        assert prompt_data['instructions'] == 'Test instructions'
    
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
        assert result.color_identity == 'gruul'
        assert result.strategy == 'combo'
        assert result.confidence == 0.95
        assert 'Amulet of Vigor' in result.reasoning
    
    def test_parse_response_missing_field(self, pipeline):
        """Test parsing response with missing required field"""
        response_text = json.dumps({
            'main_title': 'burn',
            'color_identity': 'mono_red',
            # Missing strategy
            'confidence': 0.90
        })
        
        with pytest.raises(ValidationError):
            pipeline.parse_classification_response(response_text)
    
    def test_parse_response_invalid_strategy(self, pipeline):
        """Test parsing response with invalid strategy value"""
        response_text = json.dumps({
            'main_title': 'burn',
            'color_identity': 'mono_red',
            'strategy': 'invalid_strategy',  # Not in allowed list
            'confidence': 0.90
        })
        
        with pytest.raises(ValidationError):
            pipeline.parse_classification_response(response_text)
    
    def test_parse_response_invalid_confidence(self, pipeline):
        """Test parsing response with out-of-range confidence"""
        response_text = json.dumps({
            'main_title': 'burn',
            'color_identity': 'mono_red',
            'strategy': 'aggro',
            'confidence': 1.5  # > 1.0
        })
        
        with pytest.raises(ValidationError):
            pipeline.parse_classification_response(response_text)
    
    def test_parse_response_invalid_json(self, pipeline):
        """Test parsing invalid JSON response"""
        response_text = 'This is not valid JSON'
        
        with pytest.raises((json.JSONDecodeError, ValueError)):
            pipeline.parse_classification_response(response_text)
    
    def test_parse_response_all_strategies(self, pipeline):
        """Test parsing responses with all valid strategy values"""
        strategies = ['aggro', 'midrange', 'control', 'ramp', 'combo']
        
        for strategy in strategies:
            response_text = json.dumps({
                'main_title': 'test_deck',
                'color_identity': 'test',
                'strategy': strategy,
                'confidence': 0.80
            })
            
            result = pipeline.parse_classification_response(response_text)
            assert result.strategy == strategy


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
        assert response.color_identity == 'dimir'
        assert response.strategy == 'control'
        assert response.confidence == 0.88
    
    def test_response_model_without_reasoning(self):
        """Test response model with optional reasoning field"""
        response = ArchetypeClassificationResponse(
            main_title='test',
            color_identity='mono_blue',
            strategy='combo',
            confidence=0.75
        )
        
        assert response.reasoning is None or response.reasoning == ''
    
    def test_response_model_confidence_validation(self):
        """Test confidence field validation"""
        # Valid confidence
        response = ArchetypeClassificationResponse(
            main_title='test',
            color_identity='test',
            strategy='aggro',
            confidence=0.5
        )
        assert response.confidence == 0.5
        
        # Invalid confidence (will be caught by Pydantic)
        with pytest.raises(ValidationError):
            ArchetypeClassificationResponse(
                main_title='test',
                color_identity='test',
                strategy='aggro',
                confidence=2.0
            )
    
    def test_response_model_strategy_validation(self):
        """Test strategy field validation"""
        # Valid strategies
        valid_strategies = ['aggro', 'midrange', 'control', 'ramp', 'combo']
        
        for strategy in valid_strategies:
            response = ArchetypeClassificationResponse(
                main_title='test',
                color_identity='test',
                strategy=strategy,
                confidence=0.8
            )
            assert response.strategy == strategy
        
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
    
    def test_get_unclassified_basic(self, pipeline):
        """Test retrieving unclassified decklists"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, 'Modern', 'tournament1'),
            (2, 'Modern', 'tournament2')
        ]
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
            decklists = pipeline.get_unclassified_decklists()
        
        assert len(decklists) == 2
        assert decklists[0]['decklist_id'] == 1
        assert decklists[0]['format'] == 'Modern'
    
    def test_get_unclassified_empty(self, pipeline):
        """Test when no unclassified decklists exist"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
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
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
            cards = pipeline.get_decklist_mainboard_cards(1)
        
        assert len(cards) == 1
        assert cards[0]['name'] == 'Lightning Bolt'
        assert cards[0]['quantity'] == 4
    
    def test_get_mainboard_cards_empty(self, pipeline):
        """Test when decklist has no mainboard cards"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
            cards = pipeline.get_decklist_mainboard_cards(999)
        
        assert len(cards) == 0
    
    def test_get_mainboard_filters_sideboard(self, pipeline):
        """Test that only mainboard cards are retrieved"""
        mock_cursor = MagicMock()
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
            pipeline.get_decklist_mainboard_cards(1)
        
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
        mock_cursor.fetchone.return_value = (100,)  # archetype_id
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
            archetype_id = pipeline.insert_archetype(
                decklist_id=1,
                format_name='Modern',
                mainboard_cards=sample_cards
            )
        
        assert archetype_id == 100
        mock_classify.assert_called_once()
    
    @patch.object(ArchetypeClassificationPipeline, 'classify_decklist_llm')
    def test_classify_stores_metadata(self, mock_classify, pipeline, sample_cards, sample_classification):
        """Test that classification metadata is stored correctly"""
        mock_classify.return_value = sample_classification
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (100,)
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
            pipeline.insert_archetype(1, 'Modern', sample_cards)
        
        # Verify INSERT was called with correct parameters
        insert_call = mock_cursor.execute.call_args[0]
        assert 'INSERT INTO archetypes' in insert_call[0]
        assert sample_classification.main_title in insert_call[1]
        assert sample_classification.strategy.value in insert_call[1]
    
    @patch.object(ArchetypeClassificationPipeline, 'classify_decklist_llm')
    def test_classify_failure(self, mock_classify, pipeline, sample_cards):
        """Test handling of classification failure"""
        mock_classify.return_value = None  # Classification failed
        
        result = pipeline.insert_archetype(1, 'Modern', sample_cards)
        
        assert result is None
    
    def test_classify_insufficient_cards(self, pipeline):
        """Test handling of decklists with too few cards"""
        result = pipeline.insert_archetype(1, 'Modern', [])
        assert result is None


class TestUpdateDecklistArchetype:
    """Tests for update_decklist_archetype method"""
    
    def test_update_success(self, pipeline):
        """Test successful archetype_id update"""
        mock_cursor = MagicMock()
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
            pipeline.update_decklist_archetype(decklist_id=1, archetype_id=100)
        
        # Verify UPDATE was called
        update_call = mock_cursor.execute.call_args[0]
        assert 'UPDATE decklists' in update_call[0]
        assert 'SET archetype_id' in update_call[0]
    
    def test_update_null_archetype(self, pipeline):
        """Test updating with NULL archetype_id"""
        mock_cursor = MagicMock()
        
        with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
            pipeline.update_decklist_archetype(decklist_id=1, archetype_id=None)
        
        update_call = mock_cursor.execute.call_args[0]
        assert 'UPDATE decklists' in update_call[0]


class TestLoadInitial:
    """Tests for load_initial method"""
    
    @patch.object(ArchetypeClassificationPipeline, 'get_unclassified_decklists')
    @patch.object(ArchetypeClassificationPipeline, 'get_decklist_mainboard_cards')
    @patch.object(ArchetypeClassificationPipeline, 'insert_archetype')
    @patch.object(ArchetypeClassificationPipeline, 'update_decklist_archetype')
    @patch('src.etl.archetype_pipeline.update_load_metadata')
    @patch('src.etl.archetype_pipeline.time.time')
    def test_load_initial_success(
        self, mock_time, mock_update_metadata, mock_update_decklist,
        mock_classify, mock_get_cards, mock_get_decklists, pipeline, sample_cards
    ):
        """Test successful initial load"""
        mock_time.return_value = 1700000000
        mock_get_decklists.return_value = [
            {'decklist_id': 1, 'format': 'Modern', 'tournament_id': 't1'},
            {'decklist_id': 2, 'format': 'Modern', 'tournament_id': 't2'}
        ]
        mock_get_cards.return_value = sample_cards
        mock_classify.return_value = 100
        
        result = pipeline.load_initial(batch_size=10)
        
        assert result['success'] is True
        assert result['objects_loaded'] == 2
        assert result['objects_processed'] == 2
        assert result['errors'] == 0
        
        # Verify metadata was updated
        mock_update_metadata.assert_called_once()
    
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
    def test_load_initial_with_errors(
        self, mock_classify, mock_get_cards, mock_get_decklists, pipeline, sample_cards
    ):
        """Test initial load with some classification errors"""
        mock_get_decklists.return_value = [
            {'decklist_id': 1, 'format': 'Modern', 'tournament_id': 't1'},
            {'decklist_id': 2, 'format': 'Modern', 'tournament_id': 't2'}
        ]
        mock_get_cards.return_value = sample_cards
        mock_classify.side_effect = [100, None]  # Second one fails
        
        result = pipeline.load_initial()
        
        assert result['success'] is True
        assert result['objects_loaded'] == 1
        assert result['objects_processed'] == 2
        assert result['errors'] == 1


class TestLoadIncremental:
    """Tests for load_incremental method"""
    
    @patch('src.etl.archetype_pipeline.get_last_load_timestamp')
    @patch.object(ArchetypeClassificationPipeline, 'get_decklists_since_timestamp')
    @patch.object(ArchetypeClassificationPipeline, 'get_decklist_mainboard_cards')
    @patch.object(ArchetypeClassificationPipeline, 'insert_archetype')
    @patch.object(ArchetypeClassificationPipeline, 'update_decklist_archetype')
    @patch('src.etl.archetype_pipeline.update_load_metadata')
    @patch('src.etl.archetype_pipeline.time.time')
    def test_load_incremental_success(
        self, mock_time, mock_update_metadata, mock_update_decklist,
        mock_classify, mock_get_cards, mock_get_since, mock_get_last, pipeline, sample_cards
    ):
        """Test successful incremental load"""
        mock_time.return_value = 1700000000
        mock_get_last.return_value = 1699000000
        mock_get_since.return_value = [
            {'decklist_id': 1, 'format': 'Modern', 'tournament_id': 't1', 'start_date': 1699500000}
        ]
        mock_get_cards.return_value = sample_cards
        mock_classify.return_value = 100
        
        result = pipeline.load_incremental()
        
        assert result['success'] is True
        assert result['objects_loaded'] == 1
        assert mock_get_last.called_with('archetypes')
    
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
        # Create 100 decklists
        decklists = [
            {'decklist_id': i, 'format': 'Modern', 'tournament_id': f't{i}'}
            for i in range(100)
        ]
        mock_get_decklists.return_value = decklists
        mock_get_cards.return_value = sample_cards
        mock_classify.return_value = 100
        
        result = pipeline.load_initial(batch_size=25)
        
        # Should process all 100 in batches of 25
        assert result['objects_processed'] == 100


class TestEdgeCases:
    """Tests for edge cases and error conditions"""
    
    def test_missing_card_data_threshold(self, pipeline):
        """Test handling when >10% of cards are missing"""
        # 10 cards, 2 missing (20% > 10% threshold)
        cards_with_missing = [
            {'name': f'Card{i}', 'quantity': 4}
            for i in range(8)  # Only 8 cards have full data
        ]
        
        # Should not classify (not enough data)
        result = pipeline.insert_archetype(1, 'Modern', cards_with_missing)
        assert result is None
    
    @patch.object(ArchetypeClassificationPipeline, 'get_decklist_mainboard_cards')
    @patch.object(ArchetypeClassificationPipeline, 'insert_archetype')
    def test_confidence_threshold_logging(self, mock_classify, mock_get_cards, pipeline, sample_cards):
        """Test that low confidence classifications are logged"""
        mock_get_cards.return_value = sample_cards
        
        low_confidence = ArchetypeClassificationResponse(
            main_title='other',
            color_identity='unclear',
            strategy='midrange',
            confidence=0.3,  # Low confidence
            reasoning='Uncertain'
        )
        
        # Mock the classification to return low confidence
        with patch.object(pipeline, 'classify_decklist_llm', return_value=low_confidence):
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (100,)
            
            with patch.object(DatabaseConnection, 'get_cursor', return_value=mock_cursor):
                with patch('src.etl.archetype_pipeline.logger') as mock_logger:
                    pipeline.insert_archetype(1, 'Modern', sample_cards)
                    
                    # Verify warning was logged
                    assert any('confidence' in str(call).lower() for call in mock_logger.warning.call_args_list)

