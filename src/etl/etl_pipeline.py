"""ETL pipeline for loading TopDeck tournament data and Scryfall card data into PostgreSQL"""

import re
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch

from src.etl.api_clients.topdeck_client import TopDeckClient
from src.etl.api_clients.scryfall_client import ScryfallClient
from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


# Commander formats to exclude
COMMANDER_FORMATS = {
    'EDH',
    'Pauper EDH',
    'Duel Commander',
    'Tiny Leaders',
    'EDH Draft',
    'Oathbreaker'
}

# Limited formats to exclude (constructed-only database)
LIMITED_FORMATS = {
    'Draft',
    'Sealed',
    'Limited',
    'Booster Draft',
    'Sealed Deck',
    'Cube Draft',
    'Team Draft',
    'Team Sealed'
}


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
        return []
    
    cards = []
    current_section = "mainboard"
    lines = decklist_text.split('\n')
    
    # Patterns for sideboard detection
    sideboard_only_patterns = [
        re.compile(r'^\s*sideboard\s*:?\s*$', re.IGNORECASE),
        re.compile(r'^\s*//\s*sideboard\s*$', re.IGNORECASE),
    ]
    
    # Pattern for SB: prefix that may have card after it
    sb_prefix_pattern = re.compile(r'^\s*sb\s*:\s*(.*)$', re.IGNORECASE)
    
    # Pattern for card line: quantity (1+ digits) followed by whitespace and card name
    card_pattern = re.compile(r'^(\d+)\s+(.+)$')
    
    for line in lines:
        line = line.strip()
        
        # Skip empty lines
        if not line:
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
    
    return cards


def is_commander_format(format_name: str) -> bool:
    """
    Check if a format is a Commander format
    
    Args:
        format_name: Format name from API
    
    Returns:
        True if format is a Commander format, False otherwise
    """
    if not format_name:
        return False
    
    return format_name.strip() in COMMANDER_FORMATS


def is_limited_format(format_name: str) -> bool:
    """
    Check if a format is a Limited format (Draft, Sealed, etc.)
    
    Args:
        format_name: Format name from API
    
    Returns:
        True if format is a Limited format, False otherwise
    """
    if not format_name:
        return False
    
    return format_name.strip() in LIMITED_FORMATS


def should_include_tournament(tournament: Dict) -> bool:
    """
    Determine if a tournament should be included in the database
    
    Args:
        tournament: Tournament dictionary from API
    
    Returns:
        True if tournament should be included, False otherwise
    """
    format_name = tournament.get('format', '')
    
    # Exclude Commander formats
    if is_commander_format(format_name):
        logger.debug(f"Excluding Commander tournament: {tournament.get('TID')} - {format_name}")
        return False
    
    # Exclude Limited formats (constructed-only database)
    if is_limited_format(format_name):
        logger.debug(f"Excluding Limited tournament: {tournament.get('TID')} - {format_name}")
        return False
    
    # Must be Magic: The Gathering
    game = tournament.get('game', '')
    if game != 'Magic: The Gathering':
        logger.debug(f"Excluding non-MTG tournament: {tournament.get('TID')} - {game}")
        return False
    
    return True


def is_valid_match(table_data: Dict) -> bool:
    """
    Check if a match table is a valid 1v1 match
    
    Args:
        table_data: Table dictionary from API rounds endpoint
    
    Returns:
        True if valid 1v1 match, False otherwise
    """
    players = table_data.get('players', [])
    
    if len(players) > 2:
        logger.debug(f"Skipping match with {len(players)} players (not 1v1)")
        return False
    
    return True


def filter_tournaments(tournaments: List[Dict]) -> List[Dict]:
    """
    Filter tournaments to exclude Commander formats and Limited formats
    
    Args:
        tournaments: List of tournament dictionaries
    
    Returns:
        Filtered list of tournaments (constructed-only)
    """
    filtered = [t for t in tournaments if should_include_tournament(t)]
    excluded_count = len(tournaments) - len(filtered)
    if excluded_count > 0:
        logger.info(f"Filtered out {excluded_count} Commander/Limited/non-MTG tournaments")
    return filtered


def filter_rounds_data(rounds_data: List[Dict]) -> List[Dict]:
    """
    Filter rounds data to only include valid 1v1 matches
    
    Args:
        rounds_data: List of round dictionaries from API
    
    Returns:
        Filtered list of rounds with only valid 1v1 matches
    """
    filtered_rounds = []
    
    for round_data in rounds_data:
        round_number = round_data.get('round')
        tables = round_data.get('tables', [])
        
        # Filter tables to only include valid 1v1 matches
        valid_tables = [t for t in tables if is_valid_match(t)]
        
        if valid_tables:
            filtered_round = round_data.copy()
            filtered_round['tables'] = valid_tables
            filtered_rounds.append(filtered_round)
    
    return filtered_rounds


def load_cards_from_bulk_data(
    oracle_cards_url: Optional[str] = None,
    rulings_url: Optional[str] = None,
    batch_size: int = 1000
) -> Dict[str, int]:
    """
    Load cards from Scryfall bulk data into the database.
    
    Downloads oracle cards and rulings bulk data, joins them, transforms to database
    format, and inserts/updates cards in the database using batch insertion with
    upsert logic (ON CONFLICT DO UPDATE).
    
    Args:
        oracle_cards_url: Optional URL for oracle cards bulk data (fetches if None)
        rulings_url: Optional URL for rulings bulk data (fetches if None)
        batch_size: Number of cards to insert per batch (default: 1000)
    
    Returns:
        Dictionary with keys:
        - cards_loaded: Number of cards successfully loaded
        - cards_processed: Total number of cards processed
        - errors: Number of errors encountered
    """
    client = ScryfallClient()
    
    # Download oracle cards bulk data
    logger.info("Downloading oracle cards bulk data...")
    oracle_data = client.download_oracle_cards(oracle_cards_url)
    if not oracle_data or 'data' not in oracle_data:
        logger.error("Failed to download oracle cards bulk data")
        return {'cards_loaded': 0, 'cards_processed': 0, 'errors': 1}
    
    cards = oracle_data['data']
    logger.info(f"Downloaded {len(cards)} oracle cards")
    
    # Download rulings bulk data
    logger.info("Downloading rulings bulk data...")
    rulings_data = client.download_rulings(rulings_url)
    if not rulings_data or 'data' not in rulings_data:
        logger.warning("Failed to download rulings bulk data, continuing without rulings")
        rulings = []
    else:
        rulings = rulings_data['data']
        logger.info(f"Downloaded {len(rulings)} rulings")
    
    # Join cards with rulings
    logger.info("Joining cards with rulings...")
    cards_with_rulings = client.join_cards_with_rulings(cards, rulings)
    logger.info(f"Joined {len(cards_with_rulings)} cards with rulings")
    
    # Transform cards to database row format
    logger.info("Transforming cards to database format...")
    db_rows = []
    for card in cards_with_rulings:
        try:
            db_row = client.transform_card_to_db_row(card)
            db_rows.append(db_row)
        except Exception as e:
            logger.error(f"Error transforming card {card.get('id', 'unknown')}: {e}")
            continue
    
    logger.info(f"Transformed {len(db_rows)} cards to database format")
    
    # Batch insert into database
    logger.info(f"Inserting {len(db_rows)} cards into database (batch size: {batch_size})...")
    cards_loaded = 0
    errors = 0
    
    try:
        with DatabaseConnection.transaction() as conn:
            cur = conn.cursor()
            
            # Process in batches
            for i in range(0, len(db_rows), batch_size):
                batch = db_rows[i:i + batch_size]
                
                try:
                    # Prepare batch data as tuples
                    batch_data = [
                        (
                            row['card_id'],
                            row.get('set'),
                            row.get('collector_num'),
                            row['name'],
                            row.get('oracle_text'),
                            row.get('rulings', ''),
                            row.get('type_line'),
                            row.get('mana_cost'),
                            row.get('cmc'),
                            row.get('color_identity', []),
                            row.get('scryfall_uri')
                        )
                        for row in batch
                    ]
                    
                    # Execute batch insert with upsert logic
                    execute_batch(
                        cur,
                        """
                        INSERT INTO cards (
                            card_id, set, collector_num, name, oracle_text,
                            rulings, type_line, mana_cost, cmc, color_identity, scryfall_uri
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (card_id) DO UPDATE SET
                            set = EXCLUDED.set,
                            collector_num = EXCLUDED.collector_num,
                            name = EXCLUDED.name,
                            oracle_text = EXCLUDED.oracle_text,
                            rulings = EXCLUDED.rulings,
                            type_line = EXCLUDED.type_line,
                            mana_cost = EXCLUDED.mana_cost,
                            cmc = EXCLUDED.cmc,
                            color_identity = EXCLUDED.color_identity,
                            scryfall_uri = EXCLUDED.scryfall_uri
                        """,
                        batch_data
                    )
                    
                    cards_loaded += len(batch)
                    logger.debug(f"Inserted batch {i // batch_size + 1}: {len(batch)} cards")
                    
                except Exception as e:
                    logger.error(f"Error inserting batch {i // batch_size + 1}: {e}")
                    errors += len(batch)
                    # Continue with next batch
                    continue
            
            cur.close()
            logger.info(f"Successfully loaded {cards_loaded} cards into database")
            
    except Exception as e:
        logger.error(f"Database transaction failed: {e}")
        errors += len(db_rows) - cards_loaded
        raise
    
    return {
        'cards_loaded': cards_loaded,
        'cards_processed': len(db_rows),
        'errors': errors
    }


class ETLPipeline:
    """ETL pipeline for TopDeck tournament data"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize ETL pipeline
        
        Args:
            api_key: TopDeck API key (optional, uses env var if not provided)
        """
        self.client = TopDeckClient(api_key)
        DatabaseConnection.initialize_pool()
    
    def get_last_load_timestamp(self) -> Optional[int]:
        """
        Get the timestamp of the last successful load
        
        Returns:
            Unix timestamp of last load, or None if no previous load
        """
        try:
            with DatabaseConnection.get_cursor() as cur:
                cur.execute(
                    "SELECT last_load_timestamp FROM load_metadata ORDER BY id DESC LIMIT 1"
                )
                result = cur.fetchone()
                if result:
                    return result[0]
                return None
        except Exception as e:
            logger.error(f"Error getting last load timestamp: {e}")
            return None
    
    def update_load_metadata(self, last_timestamp: int, tournaments_loaded: int) -> None:
        """
        Update load metadata after successful load
        
        Args:
            last_timestamp: Unix timestamp of the latest tournament loaded
            tournaments_loaded: Number of tournaments loaded in this batch
        """
        try:
            with DatabaseConnection.get_cursor(commit=True) as cur:
                cur.execute(
                    """
                    INSERT INTO load_metadata (last_load_timestamp, tournaments_loaded, load_type)
                    VALUES (%s, %s, %s)
                    """,
                    (last_timestamp, tournaments_loaded, 'incremental')
                )
        except Exception as e:
            logger.error(f"Error updating load metadata: {e}")
            raise
    
    def insert_tournament(self, tournament: Dict, conn) -> None:
        """
        Insert a tournament into the database
        
        Args:
            tournament: Tournament dictionary from API
            conn: Database connection
        """
        tournament_id = tournament.get('TID')
        logger.debug(f"Inserting tournament {tournament_id}")
        
        cur = conn.cursor()
        try:
            event_data = tournament.get('eventData', {})
            
            cur.execute(
                """
                INSERT INTO tournaments (
                    tournament_id, tournament_name, format, start_date,
                    swiss_num, top_cut, city, state
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tournament_id) DO UPDATE SET
                    tournament_name = EXCLUDED.tournament_name,
                    format = EXCLUDED.format,
                    start_date = EXCLUDED.start_date,
                    swiss_num = EXCLUDED.swiss_num,
                    top_cut = EXCLUDED.top_cut,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    tournament.get('TID'),
                    tournament.get('tournamentName'),
                    tournament.get('format'),
                    tournament.get('startDate'),
                    tournament.get('swissNum'),
                    tournament.get('topCut'),
                    event_data.get('city'),
                    event_data.get('state')
                )
            )
            logger.debug(f"Successfully inserted tournament {tournament_id}")
        except Exception as e:
            logger.error(f"Error inserting tournament {tournament_id}: {e}")
            raise
        finally:
            cur.close()
    
    def insert_players(self, tournament_id: str, players: List[Dict], conn) -> None:
        """
        Insert players for a tournament
        
        Args:
            tournament_id: Tournament ID
            players: List of player dictionaries from API
            conn: Database connection
        """
        if not players:
            logger.debug(f"No players to insert for tournament {tournament_id}")
            return
        
        logger.debug(f"Inserting {len(players)} players for tournament {tournament_id}")
        
        cur = conn.cursor()
        try:
            player_data = []
            for player in players:
                player_data.append((
                    player.get('id'),
                    tournament_id,
                    player.get('name', ''),
                    player.get('wins', 0),
                    player.get('winsSwiss'),
                    player.get('winsBracket'),
                    player.get('winRate') or player.get('successRate'),
                    player.get('losses', 0),
                    player.get('lossesSwiss'),
                    player.get('lossesBracket'),
                    player.get('draws', 0),
                    player.get('points'),
                    player.get('standing')
                ))
            
            execute_batch(
                cur,
                """
                INSERT INTO players (
                    player_id, tournament_id, name, wins, wins_swiss, wins_bracket,
                    win_rate, losses, losses_swiss, losses_bracket, draws, points, standing
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, tournament_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    wins = EXCLUDED.wins,
                    wins_swiss = EXCLUDED.wins_swiss,
                    wins_bracket = EXCLUDED.wins_bracket,
                    win_rate = EXCLUDED.win_rate,
                    losses = EXCLUDED.losses,
                    losses_swiss = EXCLUDED.losses_swiss,
                    losses_bracket = EXCLUDED.losses_bracket,
                    draws = EXCLUDED.draws,
                    points = EXCLUDED.points,
                    standing = EXCLUDED.standing
                """,
                player_data
            )
            logger.info(f"Successfully inserted {len(player_data)} players for tournament {tournament_id}")
        except Exception as e:
            logger.error(f"Error inserting players for tournament {tournament_id}: {e}")
            raise
        finally:
            cur.close()
    
    def insert_decklists(self, tournament_id: str, players: List[Dict], conn) -> None:
        """
        Insert decklists for players
        
        Args:
            tournament_id: Tournament ID
            players: List of player dictionaries with decklist data
            conn: Database connection
        """
        if not players:
            logger.debug(f"No players provided for decklist insertion for tournament {tournament_id}")
            return
        
        cur = conn.cursor()
        try:
            decklist_data = []
            for player in players:
                decklist = player.get('decklist')
                if decklist:
                    decklist_data.append((
                        player.get('id'),
                        tournament_id,
                        decklist
                    ))
            
            if decklist_data:
                execute_batch(
                    cur,
                    """
                    INSERT INTO decklists (player_id, tournament_id, decklist_text)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (player_id, tournament_id) DO UPDATE SET
                        decklist_text = EXCLUDED.decklist_text
                    """,
                    decklist_data
                )
                logger.info(f"Successfully inserted {len(decklist_data)} decklists for tournament {tournament_id}")
        except Exception as e:
            logger.error(f"Error inserting decklists for tournament {tournament_id}: {e}")
            raise
        finally:
            cur.close()
    
    def parse_and_store_decklist_cards(
        self,
        player_id: str,
        tournament_id: str,
        decklist_text: str,
        conn
    ) -> None:
        """
        Parse decklist text and store individual cards in deck_cards table.
        
        Extracts cards from decklist_text, matches them to the cards table by name,
        and inserts entries into deck_cards table with quantities and sections.
        Cards not found in cards table are logged but processing continues.
        
        Args:
            player_id: Player ID
            tournament_id: Tournament ID
            decklist_text: Raw decklist text to parse
            conn: Database connection
        """
        if not decklist_text or not decklist_text.strip():
            logger.debug(f"Empty decklist for player {player_id}, tournament {tournament_id}")
            return
        
        cur = conn.cursor()
        try:
            # Get decklist_id from decklists table
            cur.execute(
                """
                SELECT decklist_id FROM decklists
                WHERE player_id = %s AND tournament_id = %s
                """,
                (player_id, tournament_id)
            )
            result = cur.fetchone()
            
            if not result:
                logger.warning(
                    f"Decklist not found for player {player_id}, tournament {tournament_id}. "
                    "Skipping card parsing."
                )
                return
            
            decklist_id = result[0]
            
            # Parse decklist to extract cards
            try:
                parsed_cards = parse_decklist(decklist_text)
            except Exception as e:
                logger.error(
                    f"Error parsing decklist for player {player_id}, tournament {tournament_id}: {e}"
                )
                return
            
            if not parsed_cards:
                logger.debug(f"No cards found in decklist for player {player_id}, tournament {tournament_id}")
                return
            
            # Match cards to cards table and prepare batch data
            deck_cards_data = []
            missing_cards = []
            
            for card in parsed_cards:
                card_name = card['card_name']
                quantity = card['quantity']
                section = card['section']
                
                # Look up card_id by name
                cur.execute(
                    """
                    SELECT card_id FROM cards
                    WHERE name = %s
                    LIMIT 1
                    """,
                    (card_name,)
                )
                card_result = cur.fetchone()
                
                if card_result:
                    card_id = card_result[0]
                    deck_cards_data.append((
                        decklist_id,
                        card_id,
                        section,
                        quantity
                    ))
                else:
                    missing_cards.append(card_name)
                    logger.warning(
                        f"Card '{card_name}' not found in cards table for player {player_id}, "
                        f"tournament {tournament_id}. Skipping."
                    )
            
            # Log summary of missing cards
            if missing_cards:
                logger.warning(
                    f"Found {len(missing_cards)} missing cards for player {player_id}, "
                    f"tournament {tournament_id}: {', '.join(missing_cards[:10])}"
                    + (f" and {len(missing_cards) - 10} more" if len(missing_cards) > 10 else "")
                )
            
            # Insert deck_cards entries
            if deck_cards_data:
                execute_batch(
                    cur,
                    """
                    INSERT INTO deck_cards (decklist_id, card_id, section, quantity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (decklist_id, card_id, section) DO UPDATE SET
                        quantity = EXCLUDED.quantity
                    """,
                    deck_cards_data
                )
                logger.info(
                    f"Successfully stored {len(deck_cards_data)} cards for decklist {decklist_id} "
                    f"(player {player_id}, tournament {tournament_id})"
                )
            else:
                logger.warning(
                    f"No valid cards found to store for player {player_id}, tournament {tournament_id}"
                )
                
        except Exception as e:
            logger.error(
                f"Error parsing and storing decklist cards for player {player_id}, "
                f"tournament {tournament_id}: {e}"
            )
            raise
        finally:
            cur.close()
    
    def insert_match_rounds(self, tournament_id: str, rounds_data: List[Dict], conn) -> None:
        """
        Insert match rounds and matches
        
        Args:
            tournament_id: Tournament ID
            rounds_data: List of round dictionaries from API
            conn: Database connection
        """
        if not rounds_data:
            logger.debug(f"No rounds data for tournament {tournament_id}")
            return
        
        logger.debug(f"Inserting match rounds for tournament {tournament_id}: {len(rounds_data)} rounds")
        
        cur = conn.cursor()
        try:
            # First, get all player IDs that exist for this tournament
            cur.execute(
                "SELECT player_id FROM players WHERE tournament_id = %s",
                (tournament_id,)
            )
            existing_player_ids = {row[0] for row in cur.fetchall()}
            logger.debug(f"Found {len(existing_player_ids)} existing players for tournament {tournament_id}")
            # Map string rounds to integers (e.g., "Top 8" -> 1000, "Top 4" -> 2000)
            # This ensures consistent round numbers for bracket rounds
            string_round_map = {
                'Top 8': 1000,
                'Top 4': 2000,
                'Top 16': 500,
                'Top 32': 250,
                'Semifinals': 2000,
                'Finals': 3000,
                'Quarterfinals': 1500
            }
            
            # Insert rounds
            round_data = []
            round_number_map = {}  # Map original round identifier to numeric round number
            
            for round_info in rounds_data:
                round_identifier = round_info.get('round')
                round_number = round_identifier
                
                # Handle string rounds like "Top 8"
                if isinstance(round_number, str):
                    # Check if we have a mapping
                    if round_number in string_round_map:
                        round_number = string_round_map[round_number]
                    else:
                        # Use a hash-based approach for unknown string rounds
                        # Add tournament_id to make it unique per tournament
                        round_number = abs(hash(f"{tournament_id}_{round_number}")) % 10000
                        # Ensure it's above 10000 to avoid conflicts with numeric rounds
                        round_number = round_number + 10000
                
                # Ensure round_number is an integer
                if not isinstance(round_number, int):
                    try:
                        round_number = int(round_number)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert round {round_identifier} to integer, skipping")
                        continue
                
                round_number_map[round_identifier] = round_number
                
                round_data.append((
                    round_number,
                    tournament_id,
                    round_info.get('round') if isinstance(round_info.get('round'), str) else None
                ))
            
            if round_data:
                execute_batch(
                    cur,
                    """
                    INSERT INTO match_rounds (round_number, tournament_id, round_description)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (round_number, tournament_id) DO NOTHING
                    """,
                    round_data
                )
            
            # Insert matches
            match_data = []
            skipped_matches = 0
            missing_players = set()
            
            for round_info in rounds_data:
                round_identifier = round_info.get('round')
                round_number = round_number_map.get(round_identifier)
                
                if round_number is None:
                    logger.debug(f"Skipping round {round_identifier} for tournament {tournament_id} (no round number)")
                    continue
                
                tables = round_info.get('tables', [])
                for table in tables:
                    if not is_valid_match(table):
                        continue
                    
                    players = table.get('players', [])
                    if len(players) > 2:
                        logger.debug(f"Skipping match with {len(players)} players (not 1v1)")
                        continue
                    
                    player1_id = players[0].get('id') if len(players) > 0 else None
                    player2_id = players[1].get('id') if len(players) > 1 else None
                    winner_id = table.get('winner_id')
                    
                    # Validate that players exist in the database
                    if player1_id and player1_id not in existing_player_ids:
                        logger.warning(
                            f"Skipping match in tournament {tournament_id}, round {round_number}: "
                            f"player1_id {player1_id} not found in players table"
                        )
                        missing_players.add(player1_id)
                        skipped_matches += 1
                        continue
                    
                    if player2_id and player2_id not in existing_player_ids:
                        logger.warning(
                            f"Skipping match in tournament {tournament_id}, round {round_number}: "
                            f"player2_id {player2_id} not found in players table"
                        )
                        missing_players.add(player2_id)
                        skipped_matches += 1
                        continue
                    
                    match_data.append((
                        round_number,
                        tournament_id,
                        table.get('table') if isinstance(table.get('table'), int) else None,
                        player1_id,
                        player2_id,
                        winner_id,
                        table.get('status', '')
                    ))
            
            if skipped_matches > 0:
                logger.warning(
                    f"Skipped {skipped_matches} matches for tournament {tournament_id} due to missing players. "
                    f"Missing player IDs: {missing_players}"
                )
            
            if match_data:
                logger.debug(f"Inserting {len(match_data)} matches for tournament {tournament_id}")
                execute_batch(
                    cur,
                    """
                    INSERT INTO matches (
                        round_number, tournament_id, match_num,
                        player1_id, player2_id, winner_id, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (round_number, tournament_id, match_num) DO UPDATE SET
                        player1_id = EXCLUDED.player1_id,
                        player2_id = EXCLUDED.player2_id,
                        winner_id = EXCLUDED.winner_id,
                        status = EXCLUDED.status
                    """,
                    match_data
                )
                logger.info(f"Successfully inserted {len(match_data)} matches for tournament {tournament_id}")
            else:
                logger.debug(f"No valid matches to insert for tournament {tournament_id}")
        except Exception as e:
            logger.error(f"Error inserting match rounds for tournament {tournament_id}: {e}")
            raise
        finally:
            cur.close()
    
    def load_tournament(self, tournament: Dict, include_rounds: bool = True) -> bool:
        """
        Load a single tournament into the database
        
        Args:
            tournament: Tournament dictionary from API
            include_rounds: Whether to fetch and load round/match data
        
        Returns:
            True if successful, False otherwise
        """
        tournament_id = tournament.get('TID')
        if not tournament_id:
            logger.warning("Tournament missing TID, skipping")
            return False
        
        try:
            with DatabaseConnection.transaction() as conn:
                # Insert tournament
                self.insert_tournament(tournament, conn)
                
                # Get full tournament details for players
                logger.debug(f"Fetching tournament details for {tournament_id}")
                tournament_details = self.client.get_tournament_details(tournament_id)
                if tournament_details:
                    players = tournament_details.get('players', [])
                    logger.debug(f"Tournament details returned {len(players)} players for {tournament_id}")
                    if players:
                        self.insert_players(tournament_id, players, conn)
                        self.insert_decklists(tournament_id, players, conn)
                        
                        # Parse and store decklist cards
                        for player in players:
                            decklist = player.get('decklist')
                            if decklist:
                                try:
                                    self.parse_and_store_decklist_cards(
                                        player.get('id'),
                                        tournament_id,
                                        decklist,
                                        conn
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"Error parsing decklist cards for player {player.get('id')}, "
                                        f"tournament {tournament_id}: {e}"
                                    )
                                    # Continue processing other players
                                    continue
                    else:
                        logger.warning(f"No players found in tournament details for {tournament_id}")
                else:
                    logger.warning(f"Failed to fetch tournament details for {tournament_id}")
                
                # Load rounds and matches if requested
                if include_rounds:
                    logger.debug(f"Fetching rounds data for tournament {tournament_id}")
                    rounds_data = self.client.get_tournament_rounds(tournament_id)
                    if rounds_data:
                        logger.debug(f"Fetched {len(rounds_data)} rounds for tournament {tournament_id}")
                        filtered_rounds = filter_rounds_data(rounds_data)
                        logger.debug(f"After filtering, {len(filtered_rounds)} rounds remain for tournament {tournament_id}")
                        if filtered_rounds:
                            self.insert_match_rounds(tournament_id, filtered_rounds, conn)
                        else:
                            logger.debug(f"No valid rounds after filtering for tournament {tournament_id}")
                    else:
                        logger.debug(f"No rounds data returned for tournament {tournament_id}")
                
                logger.info(f"Successfully loaded tournament {tournament_id}")
                return True
        except Exception as e:
            logger.error(f"Error loading tournament {tournament_id}: {e}")
            return False
    
    def load_initial(self, days_back: int = 90) -> int:
        """
        Perform initial load of tournaments from the past N days
        
        Args:
            days_back: Number of days back to load (default: 90)
        
        Returns:
            Number of tournaments loaded
        """
        logger.info(f"Starting initial load for past {days_back} days")
        
        # Common competitive constructed formats (excluding Commander and Limited formats)
        # The API requires both 'game' and 'format' parameters
        formats = [
            'Standard', 'Modern', 'Legacy', 'Vintage', 'Pioneer', 
            'Pauper', 'Constructed'
        ]
        
        # Fetch tournaments for each format and combine results
        all_tournaments = []
        tournament_ids = set()  # Track IDs to avoid duplicates
        
        for format_name in formats:
            try:
                tournaments = self.client.get_tournaments(
                    game="Magic: The Gathering",
                    format=format_name,
                    last=days_back
                )
                
                if tournaments:
                    # Deduplicate by TID
                    for tournament in tournaments:
                        tid = tournament.get('TID')
                        if tid and tid not in tournament_ids:
                            tournament_ids.add(tid)
                            all_tournaments.append(tournament)
                    
                    logger.debug(f"Found {len(tournaments)} tournaments for {format_name}")
            except Exception as e:
                logger.warning(f"Error fetching tournaments for format {format_name}: {e}")
                continue
        
        tournaments = all_tournaments
        
        if not tournaments:
            logger.warning("No tournaments found")
            return 0
        
        # Filter out Commander formats
        filtered_tournaments = filter_tournaments(tournaments)
        logger.info(f"Found {len(filtered_tournaments)} tournaments to load (after filtering)")
        
        # Load tournaments
        loaded_count = 0
        max_timestamp = 0
        
        for tournament in filtered_tournaments:
            if self.load_tournament(tournament, include_rounds=True):
                loaded_count += 1
                start_date = tournament.get('startDate', 0)
                if start_date > max_timestamp:
                    max_timestamp = start_date
        
        # Update load metadata
        if loaded_count > 0 and max_timestamp > 0:
            self.update_load_metadata(max_timestamp, loaded_count)
        
        logger.info(f"Initial load complete: {loaded_count} tournaments loaded")
        return loaded_count
    
    def load_incremental(self) -> int:
        """
        Perform incremental load of new tournaments since last load
        
        Returns:
            Number of tournaments loaded
        """
        logger.info("Starting incremental load")
        
        # Get last load timestamp
        last_timestamp = self.get_last_load_timestamp()
        if not last_timestamp:
            logger.info("No previous load found, performing initial load instead")
            return self.load_initial()
        
        # Fetch tournaments since last load
        # Calculate days since last load to use 'last' parameter
        days_since_last = int((time.time() - last_timestamp) / 86400) + 1  # Add 1 day buffer
        
        # Common competitive constructed formats (excluding Commander and Limited formats)
        # The API requires both 'game' and 'format' parameters
        formats = [
            'Standard', 'Modern', 'Legacy', 'Vintage', 'Pioneer', 
            'Pauper', 'Constructed'
        ]
        
        # Fetch tournaments for each format and combine results
        all_tournaments = []
        tournament_ids = set()  # Track IDs to avoid duplicates
        
        for format_name in formats:
            try:
                format_tournaments = self.client.get_tournaments(
                    game="Magic: The Gathering",
                    format=format_name,
                    last=days_since_last
                )
                
                if format_tournaments:
                    # Deduplicate by TID
                    for tournament in format_tournaments:
                        tid = tournament.get('TID')
                        if tid and tid not in tournament_ids:
                            tournament_ids.add(tid)
                            all_tournaments.append(tournament)
                    
                    logger.debug(f"Found {len(format_tournaments)} new tournaments for {format_name}")
            except Exception as e:
                logger.warning(f"Error fetching tournaments for format {format_name}: {e}")
                continue
        
        tournaments = all_tournaments
        
        if not tournaments:
            logger.info("No new tournaments found")
            return 0
        
        # Filter out Commander formats
        filtered_tournaments = filter_tournaments(tournaments)
        logger.info(f"Found {len(filtered_tournaments)} new tournaments to load (after filtering)")
        
        # Load tournaments
        loaded_count = 0
        max_timestamp = last_timestamp
        
        for tournament in filtered_tournaments:
            if self.load_tournament(tournament, include_rounds=True):
                loaded_count += 1
                start_date = tournament.get('startDate', 0)
                if start_date > max_timestamp:
                    max_timestamp = start_date
        
        # Update load metadata
        if loaded_count > 0 and max_timestamp > last_timestamp:
            self.update_load_metadata(max_timestamp, loaded_count)
        
        logger.info(f"Incremental load complete: {loaded_count} tournaments loaded")
        return loaded_count
