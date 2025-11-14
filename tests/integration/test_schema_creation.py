"""Integration tests for schema creation"""

import pytest
from psycopg2.extensions import connection
from src.database.init_db import init_schema


@pytest.mark.integration
def test_schema_creation_creates_cards_table(test_db_connection: connection):
    """Test that schema creation creates the cards table"""
    # Drop tables if they exist to test fresh creation
    cursor = test_db_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS deck_cards CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS cards CASCADE;")
    cursor.close()
    
    # Initialize schema
    init_schema()
    
    # Verify cards table exists
    cursor = test_db_connection.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'cards'
        );
    """)
    exists = cursor.fetchone()[0]
    cursor.close()
    
    assert exists, "cards table should be created"


@pytest.mark.integration
def test_schema_creation_creates_deck_cards_table(test_db_connection: connection):
    """Test that schema creation creates the deck_cards table"""
    # Drop tables if they exist to test fresh creation
    cursor = test_db_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS deck_cards CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS cards CASCADE;")
    cursor.close()
    
    # Initialize schema
    init_schema()
    
    # Verify deck_cards table exists
    cursor = test_db_connection.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'deck_cards'
        );
    """)
    exists = cursor.fetchone()[0]
    cursor.close()
    
    assert exists, "deck_cards table should be created"


@pytest.mark.integration
def test_schema_creation_creates_indexes(test_db_connection: connection):
    """Test that schema creation creates performance indexes"""
    # Drop tables if they exist to test fresh creation
    cursor = test_db_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS deck_cards CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS cards CASCADE;")
    cursor.close()
    
    # Initialize schema
    init_schema()
    
    # Verify indexes exist
    cursor = test_db_connection.cursor()
    cursor.execute("""
        SELECT indexname
        FROM pg_indexes
        WHERE tablename IN ('cards', 'deck_cards')
        ORDER BY indexname;
    """)
    indexes = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    expected_indexes = [
        "idx_cards_name",
        "idx_cards_color_identity",
        "idx_deck_cards_card_id",
        "idx_deck_cards_decklist_id",
    ]
    
    for expected_index in expected_indexes:
        assert any(expected_index in idx for idx in indexes), f"Index {expected_index} should exist"


@pytest.mark.integration
def test_schema_creation_allows_card_insertion(test_db_connection: connection):
    """Test that schema allows inserting cards"""
    # Drop tables if they exist to test fresh creation
    cursor = test_db_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS deck_cards CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS cards CASCADE;")
    cursor.close()
    
    # Initialize schema
    init_schema()
    
    # Insert a test card
    cursor = test_db_connection.cursor()
    cursor.execute("""
        INSERT INTO cards (
            card_id, name, oracle_text, type_line, mana_cost, cmc, color_identity, scryfall_uri
        ) VALUES (
            'test-card-id-123',
            'Lightning Bolt',
            'Lightning Bolt deals 3 damage to any target.',
            'Instant',
            '{R}',
            1.0,
            ARRAY['R'],
            'https://scryfall.com/card/test'
        );
    """)
    
    # Verify card was inserted
    cursor.execute("SELECT name FROM cards WHERE card_id = 'test-card-id-123';")
    result = cursor.fetchone()
    cursor.close()
    
    assert result is not None, "Card should be inserted successfully"
    assert result[0] == "Lightning Bolt"


@pytest.mark.integration
def test_schema_creation_enforces_foreign_key_constraints(test_db_connection: connection):
    """Test that foreign key constraints are enforced"""
    # Drop tables if they exist to test fresh creation
    cursor = test_db_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS deck_cards CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS cards CASCADE;")
    cursor.close()
    
    # Initialize schema
    init_schema()
    
    # Create a test decklist
    cursor = test_db_connection.cursor()
    cursor.execute("""
        INSERT INTO decklists (decklist_text)
        VALUES ('4 Lightning Bolt')
        RETURNING decklist_id;
    """)
    decklist_id = cursor.fetchone()[0]
    
    # Try to insert deck_cards entry with invalid card_id (foreign key constraint violation)
    import psycopg2
    with pytest.raises(psycopg2.IntegrityError):
        cursor.execute("""
            INSERT INTO deck_cards (decklist_id, card_id, section, quantity)
            VALUES (%s, 'non-existent-card-id', 'mainboard', 4);
        """, (decklist_id,))
    
    cursor.close()


@pytest.mark.integration
def test_schema_creation_enforces_check_constraints(test_db_connection: connection):
    """Test that check constraints are enforced"""
    # Drop tables if they exist to test fresh creation
    cursor = test_db_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS deck_cards CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS cards CASCADE;")
    cursor.close()
    
    # Initialize schema
    init_schema()
    
    # Create a test decklist
    cursor = test_db_connection.cursor()
    cursor.execute("""
        INSERT INTO decklists (decklist_text)
        VALUES ('4 Lightning Bolt')
        RETURNING decklist_id;
    """)
    decklist_id = cursor.fetchone()[0]
    
    # Insert a test card
    cursor.execute("""
        INSERT INTO cards (card_id, name)
        VALUES ('test-card-id-456', 'Test Card');
    """)
    
    # Try to insert deck_cards entry with invalid section
    import psycopg2
    with pytest.raises(psycopg2.IntegrityError):
        cursor.execute("""
            INSERT INTO deck_cards (decklist_id, card_id, section, quantity)
            VALUES (%s, 'test-card-id-456', 'invalid-section', 4);
        """, (decklist_id,))
    
    # Try to insert deck_cards entry with invalid quantity
    with pytest.raises(psycopg2.IntegrityError):
        cursor.execute("""
            INSERT INTO deck_cards (decklist_id, card_id, section, quantity)
            VALUES (%s, 'test-card-id-456', 'mainboard', 0);
        """, (decklist_id,))
    
    cursor.close()

