"""Unit tests for etl utils module"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from src.etl.utils import parse_decklist, get_last_load_timestamp, update_load_metadata


def test_parse_decklist_basic_mainboard():
    """Test parsing basic mainboard cards"""
    decklist = """4 Lightning Bolt
2 Mountain
1 Plains"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
    assert result[1] == {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
    assert result[2] == {'quantity': 1, 'card_name': 'Plains', 'section': 'mainboard'}


def test_parse_decklist_with_sideboard_separator():
    """Test parsing decklist with Sideboard separator"""
    decklist = """4 Lightning Bolt
2 Mountain

Sideboard
2 Counterspell
1 Island"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 4
    assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
    assert result[1] == {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
    assert result[2] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}
    assert result[3] == {'quantity': 1, 'card_name': 'Island', 'section': 'sideboard'}


def test_parse_decklist_with_sideboard_colon():
    """Test parsing decklist with 'Sideboard:' separator"""
    decklist = """4 Lightning Bolt
Sideboard:
2 Counterspell"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['section'] == 'mainboard'
    assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}


def test_parse_decklist_with_sb_prefix():
    """Test parsing decklist with 'SB:' prefix"""
    decklist = """4 Lightning Bolt
SB: 2 Counterspell
SB: 1 Island"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    assert result[0]['section'] == 'mainboard'
    assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}
    assert result[2] == {'quantity': 1, 'card_name': 'Island', 'section': 'sideboard'}


def test_parse_decklist_with_sb_prefix_no_card():
    """Test parsing decklist with 'SB:' prefix on separate line"""
    decklist = """4 Lightning Bolt
SB:
2 Counterspell"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['section'] == 'mainboard'
    assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}


def test_parse_decklist_with_comment_separator():
    """Test parsing decklist with '// Sideboard' comment separator"""
    decklist = """4 Lightning Bolt
// Sideboard
2 Counterspell"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['section'] == 'mainboard'
    assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}


def test_parse_decklist_skips_comments():
    """Test that comment lines are skipped"""
    decklist = """4 Lightning Bolt
// This is a comment
# Another comment
2 Mountain"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'


def test_parse_decklist_handles_empty_lines():
    """Test that empty lines are skipped"""
    decklist = """4 Lightning Bolt


2 Mountain"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2


def test_parse_decklist_handles_empty_string():
    """Test that empty string returns empty list"""
    result = parse_decklist("")
    assert result == []


def test_parse_decklist_handles_whitespace_only():
    """Test that whitespace-only string returns empty list"""
    result = parse_decklist("   \n\t  \n  ")
    assert result == []


def test_parse_decklist_handles_none():
    """Test that None returns empty list"""
    result = parse_decklist(None)
    assert result == []


def test_parse_decklist_normalizes_split_card_separator():
    """Test that parse_decklist normalizes split card separator from / to //"""
    decklist = """4 Wear/Tear
2 Fire/Ice
1 Alive/Well"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    assert result[0]['card_name'] == 'Wear // Tear'
    assert result[0]['quantity'] == 4
    assert result[1]['card_name'] == 'Fire // Ice'
    assert result[1]['quantity'] == 2
    assert result[2]['card_name'] == 'Alive // Well'
    assert result[2]['quantity'] == 1


def test_parse_decklist_normalizes_unicode_apostrophes():
    """Test that parse_decklist normalizes Unicode apostrophes to ASCII"""
    # Use Unicode right single quotation mark (U+2019)
    unicode_apostrophe = '\u2019'
    ascii_apostrophe = "'"
    decklist = f"""4 Urza{unicode_apostrophe}s Mine
2 Vampire{unicode_apostrophe}s Kiss
1 Tormod{unicode_apostrophe}s Crypt"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    # All should have ASCII apostrophes (U+0027), not Unicode (U+2019)
    assert result[0]['card_name'] == f"Urza{ascii_apostrophe}s Mine"
    assert ascii_apostrophe in result[0]['card_name']  # Has ASCII apostrophe
    assert unicode_apostrophe not in result[0]['card_name']  # No Unicode apostrophe
    assert result[1]['card_name'] == f"Vampire{ascii_apostrophe}s Kiss"
    assert result[2]['card_name'] == f"Tormod{ascii_apostrophe}s Crypt"


def test_parse_decklist_smart_split_card_conversion():
    """Test that split card conversion is smart about when to convert / to //"""
    decklist = """2 Wear/Tear
1 Fire/Ice
1 Summon: Choco / Mog"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    # Regular split cards should be converted
    assert result[0]['card_name'] == 'Wear // Tear'
    assert result[1]['card_name'] == 'Fire // Ice'
    # Cards with colon prefix should NOT be converted
    assert result[2]['card_name'] == 'Summon: Choco / Mog'


def test_normalize_card_name():
    """Test the normalize_card_name function handles various formats"""
    from src.etl.utils import normalize_card_name
    
    # Unicode quotes
    assert normalize_card_name('Urza\u2019s Saga') == "Urza's Saga"
    assert normalize_card_name('Test\u201Cquoted\u201D') == 'Test"quoted"'
    
    # Multiple spaces
    assert normalize_card_name('Lightning  Bolt') == 'Lightning Bolt'
    assert normalize_card_name('Test   Card   Name') == 'Test Card Name'
    
    # Leading/trailing whitespace
    assert normalize_card_name('  Lightning Bolt  ') == 'Lightning Bolt'
    
    # Escaped characters
    assert normalize_card_name("Urza\\'s Saga") == "Urza's Saga"
    assert normalize_card_name("Test\\&More") == "Test&More"
    
    # Unicode dashes
    assert normalize_card_name('Test\u2013Card') == 'Test-Card'
    assert normalize_card_name('Test\u2014Card') == 'Test-Card'


def test_parse_decklist_skips_zero_quantity():
    """Test that zero quantity cards are skipped"""
    decklist = """4 Lightning Bolt
0 Mountain
2 Plains"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Plains'


def test_parse_decklist_skips_negative_quantity():
    """Test that negative quantity cards are skipped"""
    decklist = """4 Lightning Bolt
-1 Mountain
2 Plains"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Plains'


def test_parse_decklist_handles_malformed_lines():
    """Test that malformed lines are skipped"""
    decklist = """4 Lightning Bolt
This is not a card line
2 Mountain
Invalid line without quantity"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'


def test_parse_decklist_handles_whitespace_in_card_name():
    """Test that card names with extra whitespace are trimmed"""
    decklist = """4  Lightning Bolt  
2   Mountain"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'


def test_parse_decklist_case_insensitive_sideboard():
    """Test that sideboard separators are case-insensitive"""
    decklist = """4 Lightning Bolt
SIDEBOARD
2 Counterspell
sb:
1 Island"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    assert result[0]['section'] == 'mainboard'
    assert result[1]['section'] == 'sideboard'
    assert result[2]['section'] == 'sideboard'


def test_parse_decklist_complex_example():
    """Test parsing a complex real-world decklist"""
    decklist = """4 Lightning Bolt
2 Mountain
1 Plains

// Sideboard
2 Counterspell
SB: 1 Island
# Comment here
3 Forest"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 6
    mainboard = [c for c in result if c['section'] == 'mainboard']
    sideboard = [c for c in result if c['section'] == 'sideboard']
    
    assert len(mainboard) == 3
    assert len(sideboard) == 3


def test_parse_decklist_large_quantities():
    """Test parsing cards with large quantities"""
    decklist = """100 Lightning Bolt
999 Mountain"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['quantity'] == 100
    assert result[1]['quantity'] == 999


def test_get_last_load_timestamp_tournaments():
    """Test getting last load timestamp for tournaments"""
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = (1234567890,)
    
    with patch('src.etl.utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        result = get_last_load_timestamp('tournaments')
        
        assert result == 1234567890
        mock_cursor.execute.assert_called_once()
        # Verify it checks both 'tournaments' and 'incremental'
        call_args = mock_cursor.execute.call_args[0][0]
        assert 'tournaments' in call_args.lower()
        assert 'incremental' in call_args.lower()


def test_get_last_load_timestamp_cards():
    """Test getting last load timestamp for cards"""
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = (1234567890,)
    
    with patch('src.etl.utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        result = get_last_load_timestamp('cards')
        
        assert result == 1234567890
        mock_cursor.execute.assert_called_once()
        # Verify it uses the load_type parameter
        call_args = mock_cursor.execute.call_args
        assert call_args[0][1] == ('cards',)


def test_get_last_load_timestamp_no_previous_load():
    """Test getting last load timestamp when no previous load exists"""
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = None
    
    with patch('src.etl.utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        result = get_last_load_timestamp('cards')
        
        assert result is None


def test_get_last_load_timestamp_database_error():
    """Test that database errors are handled gracefully"""
    with patch('src.etl.utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.side_effect = Exception("Database error")
        
        result = get_last_load_timestamp('cards')
        
        assert result is None


def test_get_last_load_timestamp_default_parameter():
    """Test that default parameter works (backward compatibility)"""
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = (1234567890,)
    
    with patch('src.etl.utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        result = get_last_load_timestamp()
        
        assert result == 1234567890
        # Should use the default 'incremental' parameter
        call_args = mock_cursor.execute.call_args
        assert call_args[0][1] == ('incremental',)


def test_update_load_metadata_success():
    """Test successful update of load metadata"""
    mock_cursor = Mock()
    
    with patch('src.etl.utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        update_load_metadata(
            last_timestamp=1234567890,
            objects_loaded=100,
            data_type='tournaments',
            load_type='initial'
        )
        
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert 'INSERT INTO load_metadata' in call_args[0][0]
        # Verify parameters are passed correctly: (last_timestamp, objects_loaded, data_type, load_type)
        assert call_args[0][1] == (1234567890, 100, 'tournaments', 'initial')


def test_update_load_metadata_default_load_type():
    """Test update_load_metadata with default load_type"""
    mock_cursor = Mock()
    
    with patch('src.etl.utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        update_load_metadata(
            last_timestamp=1234567890,
            objects_loaded=50,
            data_type='cards'
        )
        
        call_args = mock_cursor.execute.call_args
        # Verify default load_type 'incremental' is used: (last_timestamp, objects_loaded, data_type, load_type)
        assert call_args[0][1] == (1234567890, 50, 'cards', 'incremental')


def test_update_load_metadata_uses_commit():
    """Test that update_load_metadata uses commit=True"""
    mock_cursor = Mock()
    
    with patch('src.etl.utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_cursor.return_value.__exit__.return_value = None
        
        update_load_metadata(
            last_timestamp=1234567890,
            objects_loaded=100,
            data_type='tournaments'
        )
        
        # Verify commit=True is passed
        mock_get_cursor.assert_called_once_with(commit=True)


def test_update_load_metadata_database_error():
    """Test that database errors are raised"""
    with patch('src.etl.utils.DatabaseConnection.get_cursor') as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.side_effect = Exception("Database error")
        
        with pytest.raises(Exception):
            update_load_metadata(
                last_timestamp=1234567890,
                objects_loaded=100,
                data_type='tournaments'
            )


def test_parse_decklist_multiple_sideboard_sections():
    """Test that multiple sideboard sections work correctly"""
    decklist = """4 Lightning Bolt
Sideboard
2 Counterspell
Sideboard
1 Island"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    assert result[0]['section'] == 'mainboard'
    assert result[1]['section'] == 'sideboard'
    assert result[2]['section'] == 'sideboard'


def test_parse_decklist_sb_prefix_with_whitespace():
    """Test SB: prefix with various whitespace patterns"""
    decklist = """4 Lightning Bolt
SB:2 Counterspell
sb: 1 Island
  SB  :  3 Forest"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 4
    assert result[0]['section'] == 'mainboard'
    assert all(c['section'] == 'sideboard' for c in result[1:])


def test_parse_decklist_card_name_with_numbers():
    """Test parsing cards with numbers in their names"""
    decklist = """4 Lightning Bolt
2 Mountain
1 Sol Ring"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'
    assert result[2]['card_name'] == 'Sol Ring'


def test_parse_decklist_card_name_with_special_chars():
    """Test parsing cards with special characters in names"""
    decklist = """4 Lightning Bolt
2 Jace, the Mind Sculptor
1 Oko, Thief of Crowns"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    assert result[1]['card_name'] == 'Jace, the Mind Sculptor'
    assert result[2]['card_name'] == 'Oko, Thief of Crowns'


def test_parse_decklist_handles_tabs():
    """Test that tabs are handled correctly"""
    decklist = "4\tLightning Bolt\n2\tMountain"
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'


def test_parse_decklist_escaped_apostrophe():
    """Test parsing cards with escaped apostrophes (from API responses)"""
    decklist = """4 Urza\\'s Saga
2 Dragon\\'s Rage Channeler
1 Tormod\\'s Crypt"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    assert result[0]['card_name'] == "Urza's Saga"
    assert result[1]['card_name'] == "Dragon's Rage Channeler"
    assert result[2]['card_name'] == "Tormod's Crypt"


def test_parse_decklist_escaped_double_quote():
    """Test parsing cards with escaped double quotes"""
    decklist = r"""1 \"Ach! Hans, Run!\"
2 \"Brims\" Barone, Midway Mobster"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == '"Ach! Hans, Run!"'
    assert result[1]['card_name'] == '"Brims" Barone, Midway Mobster'


def test_parse_decklist_escaped_ampersand():
    """Test parsing cards with escaped ampersands"""
    decklist = """4 Minsc \\& Boo, Timeless Heroes
1 Bebop \\& Rocksteady"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Minsc & Boo, Timeless Heroes'
    assert result[1]['card_name'] == 'Bebop & Rocksteady'


def test_parse_decklist_escaped_comma():
    """Test parsing cards with escaped commas"""
    decklist = """4 Teferi\\, Time Raveler
2 Jace\\, the Mind Sculptor"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Teferi, Time Raveler'
    assert result[1]['card_name'] == 'Jace, the Mind Sculptor'


def test_parse_decklist_multiple_escaped_chars():
    """Test parsing cards with multiple types of escaped characters"""
    decklist = r"""4 Urza\'s Saga
2 \"Ach! Hans\, Run!\"
1 Minsc \& Boo\, Timeless Heroes"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    assert result[0]['card_name'] == "Urza's Saga"
    assert result[1]['card_name'] == '"Ach! Hans, Run!"'
    assert result[2]['card_name'] == 'Minsc & Boo, Timeless Heroes'


def test_parse_decklist_topdeck_format():
    """Test parsing TopDeck format with ~~Mainboard~~ and ~~Sideboard~~"""
    decklist = """~~Mainboard~~
4 Lightning Bolt
4 Monastery Swiftspear
2 Mountain

~~Sideboard~~
3 Surgical Extraction
2 Spell Pierce"""
    
    result = parse_decklist(decklist)
    
    assert len(result) == 5
    # Check mainboard cards
    mainboard = [c for c in result if c['section'] == 'mainboard']
    assert len(mainboard) == 3
    assert mainboard[0]['card_name'] == 'Lightning Bolt'
    assert mainboard[0]['quantity'] == 4
    # Check sideboard cards
    sideboard = [c for c in result if c['section'] == 'sideboard']
    assert len(sideboard) == 2
    assert sideboard[0]['card_name'] == 'Surgical Extraction'
    assert sideboard[0]['quantity'] == 3


def test_parse_decklist_topdeck_format_with_escaped_newlines():
    """Test parsing TopDeck format with escaped newlines (as received from API)"""
    decklist = "~~Mainboard~~\\n4 Lightning Bolt\\n2 Mountain\\n\\n~~Sideboard~~\\n3 Surgical Extraction"
    
    result = parse_decklist(decklist)
    
    assert len(result) == 3
    mainboard = [c for c in result if c['section'] == 'mainboard']
    sideboard = [c for c in result if c['section'] == 'sideboard']
    assert len(mainboard) == 2
    assert len(sideboard) == 1
    assert mainboard[0]['card_name'] == 'Lightning Bolt'
    assert sideboard[0]['card_name'] == 'Surgical Extraction'


def test_parse_decklist_topdeck_format_with_escaped_chars():
    """Test realistic TopDeck API response with both escaped newlines and special chars"""
    decklist = r"~~Mainboard~~\n4 Urza\'s Saga\n2 Dragon\'s Rage Channeler\n1 \"Ach! Hans, Run!\"\n\n~~Sideboard~~\n3 Minsc \& Boo, Timeless Heroes"
    
    result = parse_decklist(decklist)
    
    assert len(result) == 4
    assert result[0]['card_name'] == "Urza's Saga"
    assert result[0]['section'] == 'mainboard'
    assert result[1]['card_name'] == "Dragon's Rage Channeler"
    assert result[2]['card_name'] == '"Ach! Hans, Run!"'
    assert result[3]['card_name'] == 'Minsc & Boo, Timeless Heroes'
    assert result[3]['section'] == 'sideboard'

