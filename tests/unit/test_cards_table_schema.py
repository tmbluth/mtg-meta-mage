"""Unit tests for cards table schema"""

import pytest
from psycopg2.extensions import cursor


@pytest.mark.unit
def test_cards_table_exists(test_db_cursor: cursor):
    """Test that cards table exists"""
    test_db_cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'cards'
        );
    """)
    exists = test_db_cursor.fetchone()[0]
    assert exists, "cards table should exist"


@pytest.mark.unit
def test_cards_table_has_card_id_primary_key(test_db_cursor: cursor):
    """Test that cards table has card_id as primary key"""
    test_db_cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'card_id';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "card_id column should exist"
    column_name, data_type, is_nullable = result
    assert column_name == "card_id"
    assert data_type == "text" or data_type == "character varying"
    assert is_nullable == "NO", "card_id should be NOT NULL"
    
    # Check primary key constraint
    test_db_cursor.execute("""
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_name = 'cards' 
        AND constraint_type = 'PRIMARY KEY'
        AND constraint_name LIKE '%card_id%';
    """)
    pk_constraint = test_db_cursor.fetchone()
    assert pk_constraint is not None, "card_id should have primary key constraint"


@pytest.mark.unit
def test_cards_table_has_set_column(test_db_cursor: cursor):
    """Test that cards table has set column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'set';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "set column should exist"
    column_name, data_type = result
    assert column_name == "set"
    assert data_type == "text" or data_type == "character varying"


@pytest.mark.unit
def test_cards_table_has_collector_num_column(test_db_cursor: cursor):
    """Test that cards table has collector_num column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'collector_num';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "collector_num column should exist"
    column_name, data_type = result
    assert column_name == "collector_num"
    assert data_type == "text" or data_type == "character varying"


@pytest.mark.unit
def test_cards_table_has_name_column(test_db_cursor: cursor):
    """Test that cards table has name column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'name';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "name column should exist"
    column_name, data_type = result
    assert column_name == "name"
    assert data_type == "text" or data_type == "character varying"


@pytest.mark.unit
def test_cards_table_has_oracle_text_column(test_db_cursor: cursor):
    """Test that cards table has oracle_text column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'oracle_text';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "oracle_text column should exist"
    column_name, data_type = result
    assert column_name == "oracle_text"
    assert data_type == "text" or data_type == "character varying"


@pytest.mark.unit
def test_cards_table_has_rulings_column(test_db_cursor: cursor):
    """Test that cards table has rulings column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'rulings';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "rulings column should exist"
    column_name, data_type = result
    assert column_name == "rulings"
    assert data_type == "text" or data_type == "character varying"


@pytest.mark.unit
def test_cards_table_has_type_line_column(test_db_cursor: cursor):
    """Test that cards table has type_line column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'type_line';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "type_line column should exist"
    column_name, data_type = result
    assert column_name == "type_line"
    assert data_type == "text" or data_type == "character varying"


@pytest.mark.unit
def test_cards_table_has_mana_cost_column(test_db_cursor: cursor):
    """Test that cards table has mana_cost column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'mana_cost';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "mana_cost column should exist"
    column_name, data_type = result
    assert column_name == "mana_cost"
    assert data_type == "text" or data_type == "character varying"


@pytest.mark.unit
def test_cards_table_has_cmc_column(test_db_cursor: cursor):
    """Test that cards table has cmc column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'cmc';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "cmc column should exist"
    column_name, data_type = result
    assert column_name == "cmc"
    assert data_type in ["double precision", "real", "numeric"], f"cmc should be FLOAT type, got {data_type}"


@pytest.mark.unit
def test_cards_table_has_color_identity_column(test_db_cursor: cursor):
    """Test that cards table has color_identity column as array"""
    test_db_cursor.execute("""
        SELECT column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'color_identity';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "color_identity column should exist"
    column_name, data_type, udt_name = result
    assert column_name == "color_identity"
    assert "array" in data_type.lower() or udt_name == "_text", f"color_identity should be array type, got {data_type}"


@pytest.mark.unit
def test_cards_table_has_scryfall_uri_column(test_db_cursor: cursor):
    """Test that cards table has scryfall_uri column"""
    test_db_cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'cards' AND column_name = 'scryfall_uri';
    """)
    result = test_db_cursor.fetchone()
    assert result is not None, "scryfall_uri column should exist"
    column_name, data_type = result
    assert column_name == "scryfall_uri"
    assert data_type == "text" or data_type == "character varying"

