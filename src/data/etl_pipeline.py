"""ETL pipeline for loading TopDeck tournament data into PostgreSQL"""

import time
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch

from src.services.topdeck_client import TopDeckClient
from src.data.filters import filter_tournaments, filter_rounds_data, is_valid_match
from src.database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


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

