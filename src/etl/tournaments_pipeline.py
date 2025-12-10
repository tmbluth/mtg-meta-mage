"""ETL pipeline for loading TopDeck tournament data into PostgreSQL"""

import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from psycopg2.extras import execute_batch

from src.clients.topdeck_client import TopDeckClient
from src.etl.database.connection import DatabaseConnection
from src.etl.etl_utils import parse_deck, get_last_load_timestamp, update_load_metadata, find_fuzzy_card_match
from src.etl.base_pipeline import BasePipeline

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


class TournamentsPipeline(BasePipeline):
    """ETL pipeline for TopDeck tournament data"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize tournaments pipeline
        
        Args:
            api_key: TopDeck API key (optional, uses env var if not provided)
        """
        self.client = TopDeckClient(api_key)
        DatabaseConnection.initialize_pool()
    
    def is_commander_format(self, format_name: str) -> bool:
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
    
    def is_limited_format(self, format_name: str) -> bool:
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
    
    def should_include_tournament(self, tournament: Dict) -> bool:
        """
        Determine if a tournament should be included in the database
        
        Args:
            tournament: Tournament dictionary from API
        
        Returns:
            True if tournament should be included, False otherwise
        """
        format_name = tournament.get('format', '')
        
        # Exclude Commander formats
        if self.is_commander_format(format_name):
            logger.debug(f"Excluding Commander tournament: {tournament.get('TID')} - {format_name}")
            return False
        
        # Exclude Limited formats (constructed-only database)
        if self.is_limited_format(format_name):
            logger.debug(f"Excluding Limited tournament: {tournament.get('TID')} - {format_name}")
            return False
        
        # Must be Magic: The Gathering
        game = tournament.get('game', '')
        if game != 'Magic: The Gathering':
            logger.debug(f"Excluding non-MTG tournament: {tournament.get('TID')} - {game}")
            return False
        
        return True
    
    def is_valid_match(self, table_data: Dict) -> bool:
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
    
    def filter_tournaments(self, tournaments: List[Dict]) -> List[Dict]:
        """
        Filter tournaments to exclude Commander formats and Limited formats
        
        Args:
            tournaments: List of tournament dictionaries
        
        Returns:
            Filtered list of tournaments (constructed-only)
        """
        filtered = [t for t in tournaments if self.should_include_tournament(t)]
        excluded_count = len(tournaments) - len(filtered)
        if excluded_count > 0:
            logger.info(f"Filtered out {excluded_count} Commander/Limited/non-MTG tournaments")
        return filtered
    
    def filter_rounds_data(self, rounds_data: List[Dict]) -> List[Dict]:
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
            valid_tables = [t for t in tables if self.is_valid_match(t)]
            
            if valid_tables:
                filtered_round = round_data.copy()
                filtered_round['tables'] = valid_tables
                filtered_rounds.append(filtered_round)
        
        return filtered_rounds
    
    
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
            
            # Convert Unix timestamp (INTEGER) from API to TIMESTAMP
            start_date_unix = tournament.get('startDate')
            start_date_ts = None
            if start_date_unix:
                start_date_ts = datetime.fromtimestamp(start_date_unix)
            
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
                    start_date_ts,
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
    
    def insert_deck_cards(
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
            
            decklist_id = result[0]  # DB primary key
            
            # Parse deck to extract cards
            try:
                parsed_cards = parse_deck(decklist_text)
            except Exception as e:
                logger.error(
                    f"Error parsing deck for player {player_id}, tournament {tournament_id}: {e}"
                )
                return
            
            if not parsed_cards:
                logger.debug(f"No cards found in deck for player {player_id}, tournament {tournament_id}")
                return
            
            # Fetch all available cards once for fuzzy matching (if needed)
            cur.execute("SELECT card_id, name FROM cards LIMIT 100000")
            all_available_cards = cur.fetchall()
            
            # Match cards to cards table and prepare batch data
            deck_cards_data = []
            missing_cards = []
            
            for card in parsed_cards:
                card_name = card['card_name']
                quantity = card['quantity']
                section = card['section']
                
                # Look up card_id by name
                # Try exact match first
                cur.execute(
                    """
                    SELECT card_id FROM cards
                    WHERE name = %s
                    LIMIT 1
                    """,
                    (card_name,)
                )
                card_result = cur.fetchone()
                
                # If not found, try as front face of double-faced card (e.g., "CardName // OtherSide")
                if not card_result:
                    cur.execute(
                        """
                        SELECT card_id FROM cards
                        WHERE name LIKE %s
                        LIMIT 1
                        """,
                        (f'{card_name} // %',)
                    )
                    card_result = cur.fetchone()
                    if card_result:
                        logger.debug(f"Matched '{card_name}' as front face of double-faced card")
                
                # If still not found, try as back face of double-faced card (e.g., "OtherSide // CardName")
                if not card_result:
                    cur.execute(
                        """
                        SELECT card_id FROM cards
                        WHERE name LIKE %s
                        LIMIT 1
                        """,
                        (f'% // {card_name}',)
                    )
                    card_result = cur.fetchone()
                    if card_result:
                        logger.debug(f"Matched '{card_name}' as back face of double-faced card")
                
                # Tier 4: Case-insensitive exact match
                if not card_result:
                    cur.execute(
                        """
                        SELECT card_id, name FROM cards
                        WHERE LOWER(name) = LOWER(%s)
                        LIMIT 1
                        """,
                        (card_name,)
                    )
                    result = cur.fetchone()
                    if result:
                        logger.debug(f"Matched '{card_name}' via case-insensitive search as '{result[1]}'")
                        card_result = (result[0],)
                
                # Tier 5: Fuzzy matching with Levenshtein distance
                if not card_result:
                    fuzzy_match = find_fuzzy_card_match(card_name, all_available_cards, threshold=2)
                    if fuzzy_match:
                        card_id, matched_name = fuzzy_match
                        logger.info(f"Matched '{card_name}' via fuzzy search as '{matched_name}'")
                        card_result = (card_id,)
                
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
                    ON CONFLICT (round_number, tournament_id) DO UPDATE SET
                        round_description = EXCLUDED.round_description
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
                for table_idx, table in enumerate(tables):
                    if not self.is_valid_match(table):
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
                    
                    # Get match_num from API if available, otherwise use table index
                    # match_num is required (NOT NULL in schema)
                    match_num = table.get('table')
                    if not isinstance(match_num, int) or match_num is None:
                        # For byes and other matches without a table number, use index
                        match_num = table_idx + 1
                    
                    match_data.append((
                        round_number,
                        tournament_id,
                        match_num,
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
    
    def insert_all(self, tournament: Dict, include_rounds: bool = True) -> bool:
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
                    # TopDeck API returns player data in 'standings' key
                    players = tournament_details.get('standings', [])
                    logger.debug(f"Tournament details returned {len(players)} players for {tournament_id}")
                    if players:
                        self.insert_players(tournament_id, players, conn)
                        self.insert_decklists(tournament_id, players, conn)
                        
                        # Parse and store decklist cards
                        for player in players:
                            decklist = player.get('decklist')
                            if decklist:
                                try:
                                    self.insert_deck_cards(
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
                        filtered_rounds = self.filter_rounds_data(rounds_data)
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
    
    def load_initial(self, days_back: int = 90, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Perform initial load of tournaments from the past N days
        
        Args:
            days_back: Number of days back to load (default: 90)
            limit: Optional limit on number of tournaments to load (default: None for all)
        
        Returns:
            Dictionary with keys:
            - success: bool - True if load completed without errors
            - objects_loaded: int - Number of tournaments loaded
            - objects_processed: int - Total tournaments processed
            - errors: int - Number of errors
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
                            
                            # Apply limit if specified
                            if limit and len(all_tournaments) >= limit:
                                break
                    
                    logger.debug(f"Found {len(tournaments)} tournaments for {format_name}")
                    
                    # Break outer loop if limit reached
                    if limit and len(all_tournaments) >= limit:
                        break
            except Exception as e:
                logger.warning(f"Error fetching tournaments for format {format_name}: {e}")
                continue
        
        tournaments = all_tournaments
        
        if not tournaments:
            logger.warning("No tournaments found")
            return {
                'success': True,
                'objects_loaded': 0,
                'objects_processed': 0,
                'errors': 0
            }
        
        # Filter out Commander formats
        filtered_tournaments = self.filter_tournaments(tournaments)
        
        # Apply limit after filtering if specified
        if limit and len(filtered_tournaments) > limit:
            filtered_tournaments = filtered_tournaments[:limit]
            logger.info(f"Limited to {limit} tournaments after filtering")
        
        logger.info(f"Found {len(filtered_tournaments)} tournaments to load (after filtering)")
        
        # Load tournaments
        loaded_count = 0
        error_count = 0
        max_timestamp = None
        
        for tournament in filtered_tournaments:
            if self.insert_all(tournament, include_rounds=True):
                loaded_count += 1
                start_date_unix = tournament.get('startDate')
                if start_date_unix:
                    start_date_ts = datetime.fromtimestamp(start_date_unix)
                    if max_timestamp is None or start_date_ts > max_timestamp:
                        max_timestamp = start_date_ts
            else:
                error_count += 1
        
        # Update load metadata
        if loaded_count > 0 and max_timestamp:
            update_load_metadata(
                last_timestamp=max_timestamp, 
                objects_loaded=loaded_count, 
                data_type='tournaments',
                load_type='initial'
            )
        
        logger.info(f"Initial load complete: {loaded_count} tournaments loaded")
        
        # Standardize return format
        return {
            'success': error_count == 0,
            'objects_loaded': loaded_count,
            'objects_processed': len(filtered_tournaments),
            'errors': error_count
        }
    
    def load_incremental(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Perform incremental load of new tournaments since last load
        
        Args:
            limit: Optional limit on number of tournaments to load (default: None for all)
        
        Returns:
            Dictionary with keys:
            - success: bool - True if load completed without errors
            - objects_loaded: int - Number of tournaments loaded
            - objects_processed: int - Total tournaments processed
            - errors: int - Number of errors
        """
        logger.info("Starting incremental load")
        
        # Get last load timestamp
        last_timestamp = get_last_load_timestamp('tournaments')
        if not last_timestamp:
            logger.info("No previous load found, performing initial load instead")
            return self.load_initial(limit=limit)
        
        # Fetch tournaments since last load
        # Calculate days since last load to use 'last' parameter
        # Convert datetime to Unix timestamp for comparison with API
        last_timestamp_unix = last_timestamp.timestamp()
        days_since_last = int((time.time() - last_timestamp_unix) / 86400) + 1  # Add 1 day buffer
        
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
                            
                            # Apply limit if specified
                            if limit and len(all_tournaments) >= limit:
                                break
                    
                    logger.debug(f"Found {len(format_tournaments)} new tournaments for {format_name}")
                    
                    # Break outer loop if limit reached
                    if limit and len(all_tournaments) >= limit:
                        break
            except Exception as e:
                logger.warning(f"Error fetching tournaments for format {format_name}: {e}")
                continue
        
        tournaments = all_tournaments
        
        if not tournaments:
            logger.info("No new tournaments found")
            return {
                'success': True,
                'objects_loaded': 0,
                'objects_processed': 0,
                'errors': 0
            }
        
        # Filter out Commander formats
        filtered_tournaments = self.filter_tournaments(tournaments)
        
        # Apply limit after filtering if specified
        if limit and len(filtered_tournaments) > limit:
            filtered_tournaments = filtered_tournaments[:limit]
            logger.info(f"Limited to {limit} tournaments after filtering")
        
        logger.info(f"Found {len(filtered_tournaments)} new tournaments to load (after filtering)")
        
        # Load tournaments
        loaded_count = 0
        error_count = 0
        max_timestamp = last_timestamp
        
        for tournament in filtered_tournaments:
            if self.insert_all(tournament, include_rounds=True):
                loaded_count += 1
                start_date_unix = tournament.get('startDate')
                if start_date_unix:
                    start_date_ts = datetime.fromtimestamp(start_date_unix)
                    if start_date_ts > max_timestamp:
                        max_timestamp = start_date_ts
            else:
                error_count += 1
        
        # Update load metadata
        if loaded_count > 0 and max_timestamp > last_timestamp:
            update_load_metadata(
                last_timestamp=max_timestamp, 
                objects_loaded=loaded_count, 
                data_type='tournaments', 
                load_type='incremental'
            )
        
        logger.info(f"Incremental load complete: {loaded_count} tournaments loaded")
        
        # Standardize return format
        return {
            'success': error_count == 0,
            'objects_loaded': loaded_count,
            'objects_processed': len(filtered_tournaments),
            'errors': error_count
        }

