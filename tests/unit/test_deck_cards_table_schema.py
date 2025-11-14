"""Unit tests for deck_cards table schema"""

import pytest
from psycopg2.extensions import cursor


@pytest.mark.unit
def test_deck_cards_table_exists(test_db_cursor: cursor):
    """Test that deck_cards table exists"""
    test_db_cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'deck_cards'
        );
    """)
    exists = test_db_cursor.fetchone()[0]
    assert exists, "deck_cards table should exist"


@pytest.mark.unit
def test_deck_cards_table_has_decklist_id_column(test_db_cursor: cursor):
    """Test that deck_cards table has decklist_id foreign key column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'deck_cards' AND column_name = 'decklist_id';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "decklist_id column should exist"
    column_name, data_type, is_nullable = result
    assert column_name == "decklist_id"
    assert data_type in ["integer", "bigint"], f"decklist_id should be INTEGER type, got {data_type}"
    assert is_nullable == "NO", "decklist_id should be NOT NULL"


@pytest.mark.unit
def test_deck_cards_table_has_card_id_column(test_db_cursor: cursor):
    """Test that deck_cards table has card_id foreign key column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'deck_cards' AND column_name = 'card_id';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "card_id column should exist"
    column_name, data_type, is_nullable = result
    assert column_name == "card_id"
    assert data_type == "text" or data_type == "character varying"
    assert is_nullable == "NO", "card_id should be NOT NULL"


@pytest.mark.unit
def test_deck_cards_table_has_section_column(test_db_cursor: cursor):
    """Test that deck_cards table has section column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'deck_cards' AND column_name = 'section';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "section column should exist"
    column_name, data_type, is_nullable = result
    assert column_name == "section"
    assert data_type == "text" or data_type == "character varying"
    assert is_nullable == "NO", "section should be NOT NULL"


@pytest.mark.unit
def test_deck_cards_table_has_quantity_column(test_db_cursor: cursor):
    """Test that deck_cards table has quantity column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'deck_cards' AND column_name = 'quantity';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "quantity column should exist"
    column_name, data_type, is_nullable = result
    assert column_name == "quantity"
    assert data_type in ["integer", "smallint"], f"quantity should be INTEGER type, got {data_type}"
    assert is_nullable == "NO", "quantity should be NOT NULL"


@pytest.mark.unit
def test_deck_cards_table_has_decklist_id_foreign_key(test_db_cursor: cursor):
    """Test that deck_cards table has foreign key constraint on decklist_id"""
    test_db_cursor.execute("""
        SELECT 
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name = 'deck_cards'
        AND kcu.column_name = 'decklist_id';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "decklist_id should have foreign key constraint"
    constraint_name, column_name, foreign_table, foreign_column = result
    assert column_name == "decklist_id"
    assert foreign_table == "decklists"
    assert foreign_column == "decklist_id"


@pytest.mark.unit
def test_deck_cards_table_has_card_id_foreign_key(test_db_cursor: cursor):
    """Test that deck_cards table has foreign key constraint on card_id"""
    test_db_cursor.execute("""
        SELECT 
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name = 'deck_cards'
        AND kcu.column_name = 'card_id';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "card_id should have foreign key constraint"
    constraint_name, column_name, foreign_table, foreign_column = result
    assert column_name == "card_id"
    assert foreign_table == "cards"
    assert foreign_column == "card_id"

