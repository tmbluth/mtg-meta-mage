"""Unit tests for core_utils module"""

import pytest

from src.core_utils import parse_deck, normalize_card_name, find_fuzzy_card_match


def test_parse_deck_basic_mainboard():
    """Test parsing basic mainboard cards"""
    decklist = """4 Lightning Bolt
2 Mountain
1 Plains"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
    assert result[1] == {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
    assert result[2] == {'quantity': 1, 'card_name': 'Plains', 'section': 'mainboard'}


def test_parse_deck_with_sideboard_separator():
    """Test parsing decklist with Sideboard separator"""
    decklist = """4 Lightning Bolt
2 Mountain

Sideboard
2 Counterspell
1 Island"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 4
    assert result[0] == {'quantity': 4, 'card_name': 'Lightning Bolt', 'section': 'mainboard'}
    assert result[1] == {'quantity': 2, 'card_name': 'Mountain', 'section': 'mainboard'}
    assert result[2] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}
    assert result[3] == {'quantity': 1, 'card_name': 'Island', 'section': 'sideboard'}


def test_parse_deck_with_sideboard_colon():
    """Test parsing decklist with 'Sideboard:' separator"""
    decklist = """4 Lightning Bolt
Sideboard:
2 Counterspell"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['section'] == 'mainboard'
    assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}


def test_parse_deck_with_sb_prefix():
    """Test parsing decklist with 'SB:' prefix"""
    decklist = """4 Lightning Bolt
SB: 2 Counterspell
SB: 1 Island"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    assert result[0]['section'] == 'mainboard'
    assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}
    assert result[2] == {'quantity': 1, 'card_name': 'Island', 'section': 'sideboard'}


def test_parse_deck_with_sb_prefix_no_card():
    """Test parsing decklist with 'SB:' prefix on separate line"""
    decklist = """4 Lightning Bolt
SB:
2 Counterspell"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['section'] == 'mainboard'
    assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}


def test_parse_deck_with_comment_separator():
    """Test parsing decklist with '// Sideboard' comment separator"""
    decklist = """4 Lightning Bolt
// Sideboard
2 Counterspell"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['section'] == 'mainboard'
    assert result[1] == {'quantity': 2, 'card_name': 'Counterspell', 'section': 'sideboard'}


def test_parse_deck_skips_comments():
    """Test that comment lines are skipped"""
    decklist = """4 Lightning Bolt
// This is a comment
# Another comment
2 Mountain"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'


def test_parse_deck_handles_empty_lines():
    """Test that empty lines are skipped"""
    decklist = """4 Lightning Bolt


2 Mountain"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2


def test_parse_deck_handles_empty_string():
    """Test that empty string returns empty list"""
    result = parse_deck("")
    assert result == []


def test_parse_deck_handles_whitespace_only():
    """Test that whitespace-only string returns empty list"""
    result = parse_deck("   \n\t  \n  ")
    assert result == []


def test_parse_deck_handles_none():
    """Test that None returns empty list"""
    result = parse_deck(None)
    assert result == []


def test_parse_deck_normalizes_split_card_separator():
    """Test that parse_deck normalizes split card separator from / to //"""
    decklist = """4 Wear/Tear
2 Fire/Ice
1 Alive/Well"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    assert result[0]['card_name'] == 'Wear // Tear'
    assert result[0]['quantity'] == 4
    assert result[1]['card_name'] == 'Fire // Ice'
    assert result[1]['quantity'] == 2
    assert result[2]['card_name'] == 'Alive // Well'
    assert result[2]['quantity'] == 1


def test_parse_deck_normalizes_unicode_apostrophes():
    """Test that parse_deck normalizes Unicode apostrophes to ASCII"""
    # Use Unicode right single quotation mark (U+2019)
    unicode_apostrophe = '\u2019'
    ascii_apostrophe = "'"
    decklist = f"""4 Urza{unicode_apostrophe}s Mine
2 Vampire{unicode_apostrophe}s Kiss
1 Tormod{unicode_apostrophe}s Crypt"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    # All should have ASCII apostrophes (U+0027), not Unicode (U+2019)
    assert result[0]['card_name'] == f"Urza{ascii_apostrophe}s Mine"
    assert ascii_apostrophe in result[0]['card_name']  # Has ASCII apostrophe
    assert unicode_apostrophe not in result[0]['card_name']  # No Unicode apostrophe
    assert result[1]['card_name'] == f"Vampire{ascii_apostrophe}s Kiss"
    assert result[2]['card_name'] == f"Tormod{ascii_apostrophe}s Crypt"


def test_parse_deck_smart_split_card_conversion():
    """Test that split card conversion is smart about when to convert / to //"""
    decklist = """2 Wear/Tear
1 Fire/Ice
1 Summon: Choco / Mog"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    # Regular split cards should be converted
    assert result[0]['card_name'] == 'Wear // Tear'
    assert result[1]['card_name'] == 'Fire // Ice'
    # Cards with colon prefix should NOT be converted
    assert result[2]['card_name'] == 'Summon: Choco / Mog'


def test_normalize_card_name():
    """Test the normalize_card_name function handles various formats"""
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


def test_parse_deck_skips_zero_quantity():
    """Test that zero quantity cards are skipped"""
    decklist = """4 Lightning Bolt
0 Mountain
2 Plains"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Plains'


def test_parse_deck_skips_negative_quantity():
    """Test that negative quantity cards are skipped"""
    decklist = """4 Lightning Bolt
-1 Mountain
2 Plains"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Plains'


def test_parse_deck_handles_malformed_lines():
    """Test that malformed lines are skipped"""
    decklist = """4 Lightning Bolt
This is not a card line
2 Mountain
Invalid line without quantity"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'


def test_parse_deck_handles_whitespace_in_card_name():
    """Test that card names with extra whitespace are trimmed"""
    decklist = """4  Lightning Bolt  
2   Mountain"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'


def test_parse_deck_case_insensitive_sideboard():
    """Test that sideboard separators are case-insensitive"""
    decklist = """4 Lightning Bolt
SIDEBOARD
2 Counterspell
sb:
1 Island"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    assert result[0]['section'] == 'mainboard'
    assert result[1]['section'] == 'sideboard'
    assert result[2]['section'] == 'sideboard'


def test_parse_deck_complex_example():
    """Test parsing a complex real-world decklist"""
    decklist = """4 Lightning Bolt
2 Mountain
1 Plains

// Sideboard
2 Counterspell
SB: 1 Island
# Comment here
3 Forest"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 6
    mainboard = [c for c in result if c['section'] == 'mainboard']
    sideboard = [c for c in result if c['section'] == 'sideboard']
    
    assert len(mainboard) == 3
    assert len(sideboard) == 3


def test_parse_deck_large_quantities():
    """Test parsing cards with large quantities"""
    decklist = """100 Lightning Bolt
999 Mountain"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['quantity'] == 100
    assert result[1]['quantity'] == 999


def test_parse_deck_multiple_sideboard_sections():
    """Test that multiple sideboard sections work correctly"""
    decklist = """4 Lightning Bolt
Sideboard
2 Counterspell
Sideboard
1 Island"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    assert result[0]['section'] == 'mainboard'
    assert result[1]['section'] == 'sideboard'
    assert result[2]['section'] == 'sideboard'


def test_parse_deck_sb_prefix_with_whitespace():
    """Test SB: prefix with various whitespace patterns"""
    decklist = """4 Lightning Bolt
SB:2 Counterspell
sb: 1 Island
  SB  :  3 Forest"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 4
    assert result[0]['section'] == 'mainboard'
    assert all(c['section'] == 'sideboard' for c in result[1:])


def test_parse_deck_card_name_with_numbers():
    """Test parsing cards with numbers in their names"""
    decklist = """4 Lightning Bolt
2 Mountain
1 Sol Ring"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'
    assert result[2]['card_name'] == 'Sol Ring'


def test_parse_deck_card_name_with_special_chars():
    """Test parsing cards with special characters in names"""
    decklist = """4 Lightning Bolt
2 Jace, the Mind Sculptor
1 Oko, Thief of Crowns"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    assert result[1]['card_name'] == 'Jace, the Mind Sculptor'
    assert result[2]['card_name'] == 'Oko, Thief of Crowns'


def test_parse_deck_handles_tabs():
    """Test that tabs are handled correctly"""
    decklist = "4\tLightning Bolt\n2\tMountain"
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Lightning Bolt'
    assert result[1]['card_name'] == 'Mountain'


def test_parse_deck_escaped_apostrophe():
    """Test parsing cards with escaped apostrophes (from API responses)"""
    decklist = """4 Urza\\'s Saga
2 Dragon\\'s Rage Channeler
1 Tormod\\'s Crypt"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    assert result[0]['card_name'] == "Urza's Saga"
    assert result[1]['card_name'] == "Dragon's Rage Channeler"
    assert result[2]['card_name'] == "Tormod's Crypt"


def test_parse_deck_escaped_double_quote():
    """Test parsing cards with escaped double quotes"""
    decklist = r"""1 \"Ach! Hans, Run!\"
2 \"Brims\" Barone, Midway Mobster"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == '"Ach! Hans, Run!"'
    assert result[1]['card_name'] == '"Brims" Barone, Midway Mobster'


def test_parse_deck_escaped_ampersand():
    """Test parsing cards with escaped ampersands"""
    decklist = """4 Minsc \\& Boo, Timeless Heroes
1 Bebop \\& Rocksteady"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Minsc & Boo, Timeless Heroes'
    assert result[1]['card_name'] == 'Bebop & Rocksteady'


def test_parse_deck_escaped_comma():
    """Test parsing cards with escaped commas"""
    decklist = """4 Teferi\\, Time Raveler
2 Jace\\, the Mind Sculptor"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 2
    assert result[0]['card_name'] == 'Teferi, Time Raveler'
    assert result[1]['card_name'] == 'Jace, the Mind Sculptor'


def test_parse_deck_multiple_escaped_chars():
    """Test parsing cards with multiple types of escaped characters"""
    decklist = r"""4 Urza\'s Saga
2 \"Ach! Hans\, Run!\"
1 Minsc \& Boo\, Timeless Heroes"""
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    assert result[0]['card_name'] == "Urza's Saga"
    assert result[1]['card_name'] == '"Ach! Hans, Run!"'
    assert result[2]['card_name'] == 'Minsc & Boo, Timeless Heroes'


def test_parse_deck_topdeck_format():
    """Test parsing TopDeck format with ~~Mainboard~~ and ~~Sideboard~~"""
    decklist = """~~Mainboard~~
4 Lightning Bolt
4 Monastery Swiftspear
2 Mountain

~~Sideboard~~
3 Surgical Extraction
2 Spell Pierce"""
    
    result = parse_deck(decklist)
    
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


def test_parse_deck_topdeck_format_with_escaped_newlines():
    """Test parsing TopDeck format with escaped newlines (as received from API)"""
    decklist = "~~Mainboard~~\\n4 Lightning Bolt\\n2 Mountain\\n\\n~~Sideboard~~\\n3 Surgical Extraction"
    
    result = parse_deck(decklist)
    
    assert len(result) == 3
    mainboard = [c for c in result if c['section'] == 'mainboard']
    sideboard = [c for c in result if c['section'] == 'sideboard']
    assert len(mainboard) == 2
    assert len(sideboard) == 1
    assert mainboard[0]['card_name'] == 'Lightning Bolt'
    assert sideboard[0]['card_name'] == 'Surgical Extraction'


def test_parse_deck_topdeck_format_with_escaped_chars():
    """Test realistic TopDeck API response with both escaped newlines and special chars"""
    decklist = r"~~Mainboard~~\n4 Urza\'s Saga\n2 Dragon\'s Rage Channeler\n1 \"Ach! Hans, Run!\"\n\n~~Sideboard~~\n3 Minsc \& Boo, Timeless Heroes"
    
    result = parse_deck(decklist)
    
    assert len(result) == 4
    assert result[0]['card_name'] == "Urza's Saga"
    assert result[0]['section'] == 'mainboard'
    assert result[1]['card_name'] == "Dragon's Rage Channeler"
    assert result[2]['card_name'] == '"Ach! Hans, Run!"'
    assert result[3]['card_name'] == 'Minsc & Boo, Timeless Heroes'
    assert result[3]['section'] == 'sideboard'


def test_find_fuzzy_card_match_exact_match():
    """Test fuzzy matching with exact match (should return immediately)"""
    available_cards = [
        ("card1", "Lightning Bolt"),
        ("card2", "Counterspell"),
        ("card3", "Mountain"),
    ]
    
    result = find_fuzzy_card_match("Lightning Bolt", available_cards, threshold=2)
    
    assert result is not None
    assert result[0] == "card1"
    assert result[1] == "Lightning Bolt"


def test_find_fuzzy_card_match_close_match():
    """Test fuzzy matching with a close match (1-2 character difference)"""
    available_cards = [
        ("card1", "Lightning Bolt"),
        ("card2", "Counterspell"),
        ("card3", "Snapcaster Mage"),
    ]
    
    # Test with 1 typo (should match with threshold=2)
    result = find_fuzzy_card_match("Lightening Bolt", available_cards, threshold=2)
    
    assert result is not None
    assert result[0] == "card1"
    assert result[1] == "Lightning Bolt"


def test_find_fuzzy_card_match_case_insensitive():
    """Test fuzzy matching is case-insensitive"""
    available_cards = [
        ("card1", "Lightning Bolt"),
        ("card2", "Counterspell"),
    ]
    
    result = find_fuzzy_card_match("lightning bolt", available_cards, threshold=2)
    
    assert result is not None
    assert result[0] == "card1"
    assert result[1] == "Lightning Bolt"


def test_find_fuzzy_card_match_no_match():
    """Test fuzzy matching when distance exceeds threshold"""
    available_cards = [
        ("card1", "Lightning Bolt"),
        ("card2", "Counterspell"),
    ]
    
    # "Mountain" is too different from any available card
    result = find_fuzzy_card_match("Mountain", available_cards, threshold=2)
    
    assert result is None


def test_find_fuzzy_card_match_empty_list():
    """Test fuzzy matching with empty card list"""
    available_cards = []
    
    result = find_fuzzy_card_match("Lightning Bolt", available_cards, threshold=2)
    
    assert result is None


def test_find_fuzzy_card_match_returns_best_match():
    """Test that fuzzy matching returns the closest match"""
    available_cards = [
        ("card1", "Lightning Bolt"),
        ("card2", "Lightning Strike"),  # Closer to "Lightning Blt"
        ("card3", "Chain Lightning"),
    ]
    
    # "Lightning Blt" (1 char off from "Bolt", but "Strike" is farther)
    result = find_fuzzy_card_match("Lightning Blt", available_cards, threshold=2)
    
    assert result is not None
    assert result[0] == "card1"  # Should match "Lightning Bolt" (closest)
    assert result[1] == "Lightning Bolt"


def test_find_fuzzy_card_match_with_special_characters():
    """Test fuzzy matching with special characters in card names"""
    available_cards = [
        ("card1", '"Ach! Hans, Run!"'),
        ("card2", "Minsc & Boo, Timeless Heroes"),
        ("card3", "Urza's Saga"),
    ]
    
    # Test with slight misspelling
    result = find_fuzzy_card_match("Urzas Saga", available_cards, threshold=2)
    
    assert result is not None
    assert result[0] == "card3"
    assert result[1] == "Urza's Saga"


def test_find_fuzzy_card_match_custom_threshold():
    """Test fuzzy matching with custom threshold"""
    available_cards = [
        ("card1", "Lightning Bolt"),
        ("card2", "Counterspell"),
    ]
    
    # "Lightnin Blt" has 2 differences from "Lightning Bolt"
    # Should match with threshold=2
    result = find_fuzzy_card_match("Lightnin Blt", available_cards, threshold=2)
    assert result is not None
    
    # Should NOT match with threshold=1
    result = find_fuzzy_card_match("Lightnin Blt", available_cards, threshold=1)
    assert result is None

