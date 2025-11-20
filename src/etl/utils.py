"""Utility functions for ETL pipelines"""

import re
import time
import logging
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from Levenshtein import distance as levenshtein_distance    
from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


def normalize_card_name(card_name: str) -> str:
    """
    Normalize card name for robust matching across different formats.
    
    Handles:
    - Unicode normalization (NFC)
    - Smart quotes to ASCII
    - Multiple spaces to single space
    - Special character variations
    - Escaped characters from API responses
    
    Args:
        card_name: Raw card name from decklist
        
    Returns:
        Normalized card name
    """
    if not card_name:
        return card_name
    
    # 1. Unicode normalization (NFC = composed form)
    # This handles accent variations: é vs e+combining_accent
    card_name = unicodedata.normalize('NFC', card_name)
    
    # 2. Normalize all quote-like characters to ASCII quotes
    quote_map = {
        '\u2018': "'",  # ' LEFT SINGLE QUOTATION MARK
        '\u2019': "'",  # ' RIGHT SINGLE QUOTATION MARK
        '\u201A': "'",  # ‚ SINGLE LOW-9 QUOTATION MARK
        '\u201B': "'",  # ‛ SINGLE HIGH-REVERSED-9 QUOTATION MARK
        '\u201C': '"',  # " LEFT DOUBLE QUOTATION MARK
        '\u201D': '"',  # " RIGHT DOUBLE QUOTATION MARK
        '\u201E': '"',  # „ DOUBLE LOW-9 QUOTATION MARK
        '\u201F': '"',  # ‟ DOUBLE HIGH-REVERSED-9 QUOTATION MARK
        '\u2032': "'",  # ′ PRIME
        '\u2033': '"',  # ″ DOUBLE PRIME
        '`': "'",       # ` GRAVE ACCENT
        '´': "'",       # ´ ACUTE ACCENT
    }
    for unicode_char, ascii_char in quote_map.items():
        card_name = card_name.replace(unicode_char, ascii_char)
    
    # 3. Normalize dash-like characters to ASCII hyphen
    dash_map = {
        '\u2010': '-',  # ‐ HYPHEN
        '\u2011': '-',  # ‑ NON-BREAKING HYPHEN
        '\u2012': '-',  # ‒ FIGURE DASH
        '\u2013': '-',  # – EN DASH
        '\u2014': '-',  # — EM DASH
        '\u2015': '-',  # ― HORIZONTAL BAR
        '\u2212': '-',  # − MINUS SIGN
    }
    for unicode_char, ascii_char in dash_map.items():
        card_name = card_name.replace(unicode_char, ascii_char)
    
    # 4. Normalize whitespace
    # Replace non-breaking spaces and other Unicode spaces with regular space
    card_name = card_name.replace('\u00A0', ' ')  # Non-breaking space
    card_name = card_name.replace('\u2009', ' ')  # Thin space
    card_name = card_name.replace('\u202F', ' ')  # Narrow no-break space
    card_name = card_name.replace('\t', ' ')      # Tab to space
    
    # Collapse multiple spaces to single space
    card_name = re.sub(r'\s+', ' ', card_name)
    
    # 5. Strip leading/trailing whitespace
    card_name = card_name.strip()
    
    # 6. Unescape common escape sequences (from API responses)
    card_name = card_name.replace("\\'", "'")
    card_name = card_name.replace('\\"', '"')
    card_name = card_name.replace('\\,', ',')
    card_name = card_name.replace('\\&', '&')
    
    return card_name


def parse_decklist(decklist_text: str) -> List[Dict[str, any]]:
    """
    Parse a standard MTG decklist text format to extract card quantities and names.
    
    Expected format: "4 Lightning Bolt" (quantity + card name)
    Supports mainboard/sideboard sections separated by:
    - "Sideboard" or "Sideboard:"
    - "SB:" prefix
    - "// Sideboard" comment
    
    Args:
        decklist_text: Raw decklist text string
    
    Returns:
        List of dictionaries with keys: quantity (int), card_name (str), section (str)
        Section is either "mainboard" or "sideboard"
    """
    if not decklist_text or not decklist_text.strip():
        logger.debug("Empty decklist text provided to parse_decklist")
        return []
    
    # Handle escaped newlines (common in API responses)
    # Replace literal \n with actual newlines
    if '\\n' in decklist_text and '\n' not in decklist_text:
        decklist_text = decklist_text.replace('\\n', '\n')
    
    cards = []
    current_section = "mainboard"
    lines = decklist_text.split('\n')
    logger.debug(f"Parsing decklist with {len(lines)} lines")
    
    # Patterns for sideboard detection
    sideboard_only_patterns = [
        re.compile(r'^\s*sideboard\s*:?\s*$', re.IGNORECASE),
        re.compile(r'^\s*//\s*sideboard\s*$', re.IGNORECASE),
        re.compile(r'^\s*~~\s*sideboard\s*~~\s*$', re.IGNORECASE),  # TopDeck format
    ]
    
    # Pattern for mainboard section (TopDeck format) - skip but don't change section
    mainboard_pattern = re.compile(r'^\s*~~\s*mainboard\s*~~\s*$', re.IGNORECASE)
    
    # Pattern for SB: prefix that may have card after it
    sb_prefix_pattern = re.compile(r'^\s*sb\s*:\s*(.*)$', re.IGNORECASE)
    
    # Pattern for card line: quantity (1+ digits) followed by whitespace and card name
    card_pattern = re.compile(r'^(\d+)\s+(.+)$')
    
    for line in lines:
        line = line.strip()
        
        # Skip empty lines
        if not line:
            continue
        
        # Skip mainboard marker (TopDeck format)
        if mainboard_pattern.match(line):
            current_section = "mainboard"
            continue
        
        # Check for SB: prefix (may have card on same line)
        sb_match = sb_prefix_pattern.match(line)
        if sb_match:
            current_section = "sideboard"
            # If there's content after SB:, process it as a card line
            remaining = sb_match.group(1).strip()
            if remaining:
                line = remaining
            else:
                continue
        
        # Check for sideboard-only separators (standalone)
        is_sideboard_separator = False
        for pattern in sideboard_only_patterns:
            if pattern.match(line):
                current_section = "sideboard"
                is_sideboard_separator = True
                break
        
        if is_sideboard_separator:
            continue
        
        # Skip comment lines (starting with // or #)
        if line.startswith('//') or line.startswith('#'):
            continue
        
        # Try to match card pattern: quantity + card name
        match = card_pattern.match(line)
        if match:
            quantity = int(match.group(1))
            card_name = match.group(2).strip()
            
            # Apply comprehensive normalization
            card_name = normalize_card_name(card_name)
            
            # Normalize split card separator: TopDeck uses "/" but Scryfall uses " // "
            # E.g., "Wear/Tear" -> "Wear // Tear"
            # BUT: Only for actual split cards (two capitalized words), not cards with / in the name
            # like "Summon: Choco/Mog" or "Rock/Paper/Scissors"
            if '/' in card_name and '//' not in card_name:
                parts = [part.strip() for part in card_name.split('/')]
                # Only convert if it's exactly 2 parts and both start with uppercase
                # (typical split card pattern)
                if len(parts) == 2 and parts[0] and parts[1] and parts[0][0].isupper() and parts[1][0].isupper():
                    # Also check that neither part contains a colon (would indicate special card type)
                    if ':' not in parts[0] and ':' not in parts[1]:
                        card_name = f"{parts[0]} // {parts[1]}"
            
            # Skip zero or negative quantities
            if quantity <= 0:
                logger.debug(f"Skipping card with invalid quantity: {line}")
                continue
            
            cards.append({
                'quantity': quantity,
                'card_name': card_name,
                'section': current_section
            })
        else:
            # Log malformed entries but continue processing
            logger.debug(f"Skipping malformed decklist line: {line}")
    
    mainboard_count = sum(1 for c in cards if c['section'] == 'mainboard')
    sideboard_count = sum(1 for c in cards if c['section'] == 'sideboard')
    logger.debug(f"Parsed decklist: {len(cards)} total cards ({mainboard_count} mainboard, {sideboard_count} sideboard)")
    
    return cards


def get_last_load_timestamp(data_type: str) -> Optional[datetime]:
    """
    Get the timestamp of the last successful load for a specific data type
    
    Args:
        data_type: Type of data ('tournaments', 'cards', 'archetypes')
    
    Returns:
        datetime of last load, or None if no previous load
    """
    try:
        with DatabaseConnection.get_cursor() as cur:
            cur.execute(
                """
                SELECT last_load_date FROM load_metadata 
                WHERE data_type = %s 
                ORDER BY id DESC LIMIT 1
                """,
                (data_type,)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            return None
    except Exception as e:
        logger.error(f"Error getting last load timestamp for {data_type}: {e}")
        return None


def update_load_metadata(
    last_timestamp: datetime,
    objects_loaded: int,
    data_type: str,
    load_type: str = 'incremental'
) -> None:
    """
    Update load metadata after successful load
    
    Args:
        last_timestamp: datetime of the latest item loaded
        objects_loaded: Number of items loaded in this batch
        data_type: Type of data ('tournaments', 'cards', 'archetypes')
        load_type: Type of load ('incremental', 'initial')
    """
    try:
        with DatabaseConnection.get_cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO load_metadata (last_load_date, objects_loaded, data_type, load_type)
                VALUES (%s, %s, %s, %s)
                """,
                (last_timestamp, objects_loaded, data_type, load_type)
            )
    except Exception as e:
        logger.error(f"Error updating load metadata for {load_type}, {data_type}: {e}")
        raise

def find_fuzzy_card_match(card_name: str, cur, threshold: int = 2) -> Optional[Tuple[str, str]]:
    """
    Find a fuzzy match for a card name using Levenshtein distance.
    
    Args:
        card_name: The card name to match
        cur: Database cursor
        threshold: Maximum edit distance to accept (default 2)
        
    Returns:
        Tuple of (card_id, matched_name) if found, None otherwise
    """    
    # Get all card names from database (could be optimized with caching)
    cur.execute("SELECT card_id, name FROM cards LIMIT 100000")  # Limit for safety
    all_cards = cur.fetchall()
    
    # Find closest match
    best_match = None
    best_distance = float('inf')
    card_name_lower = card_name.lower()
    
    for card_id, db_name in all_cards:
        dist = levenshtein_distance(card_name_lower, db_name.lower())
        if dist < best_distance and dist <= threshold:
            best_distance = dist
            best_match = (card_id, db_name)
    
    return best_match