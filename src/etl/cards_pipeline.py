"""ETL pipeline for loading Scryfall card data into PostgreSQL"""

import time
import json
import logging
from datetime import datetime
from typing import Dict, Optional, Any
from psycopg2.extras import execute_batch, Json

from src.clients.scryfall_client import ScryfallClient
from src.etl.database.connection import DatabaseConnection
from src.etl.etl_utils import get_last_load_timestamp, update_load_metadata
from src.etl.base_pipeline import BasePipeline

logger = logging.getLogger(__name__)


class CardsPipeline(BasePipeline):
    """ETL pipeline for Scryfall card data"""
    
    def __init__(self):
        """Initialize cards pipeline"""
        DatabaseConnection.initialize_pool()
    
    def insert_cards(
        self,
        oracle_cards_url: Optional[str] = None,
        rulings_url: Optional[str] = None,
        batch_size: int = 1000,
        update_existing: bool = True,
        limit: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Load cards from Scryfall bulk data into the database.
        
        Downloads oracle cards and rulings bulk data, joins them, transforms to database
        format, and inserts/updates cards in the database using batch insertion.
        
        Args:
            oracle_cards_url: Optional URL for oracle cards bulk data (fetches if None)
            rulings_url: Optional URL for rulings bulk data (fetches if None)
            batch_size: Number of cards to insert per batch (default: 1000)
            update_existing: If True, updates existing cards on conflict (for initial loads).
                            If False, skips existing cards on conflict (for incremental loads).
                            Default: True
            limit: Optional limit on number of cards to process (default: None for all)
        
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
        
        # Apply limit if specified
        if limit and len(cards) > limit:
            cards = cards[:limit]
            logger.info(f"Limited to {limit} cards")
        
        logger.info(f"Processing {len(cards)} oracle cards")
        
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
                                row.get('scryfall_uri'),
                                Json(row.get('legalities', {}))  # Convert dict to JSONB
                            )
                            for row in batch
                        ]
                        
                        # Execute batch insert with conflict handling
                        if update_existing:
                            # Initial load: update existing cards
                            execute_batch(
                                cur,
                                """
                                INSERT INTO cards (
                                    card_id, set, collector_num, name, oracle_text,
                                    rulings, type_line, mana_cost, cmc, color_identity, scryfall_uri, legalities
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                                    scryfall_uri = EXCLUDED.scryfall_uri,
                                    legalities = EXCLUDED.legalities
                                """,
                                batch_data
                            )
                        else:
                            # Incremental load: skip existing cards, only insert new ones
                            execute_batch(
                                cur,
                                """
                                INSERT INTO cards (
                                    card_id, set, collector_num, name, oracle_text,
                                    rulings, type_line, mana_cost, cmc, color_identity, scryfall_uri, legalities
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (card_id) DO NOTHING
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
    
    def load_initial(self, batch_size: int = 1000, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Perform initial load of cards from Scryfall bulk data.
        
        This loads all cards regardless of when they were last loaded.
        Existing cards with the same primary key (card_id) are overwritten
        with the latest data from Scryfall.
        
        Useful for first-time setup or full refresh to ensure database
        is synchronized with Scryfall's canonical card data.
        
        Args:
            batch_size: Number of cards to insert per batch (default: 1000)
            limit: Optional limit on number of cards to load (default: None for all)
        
        Returns:
            Dictionary with keys:
            - success: bool - True if load completed without errors
            - objects_loaded: int - Number of cards successfully loaded/updated
            - objects_processed: int - Total number of cards processed
            - errors: int - Number of errors encountered
        """
        logger.info("Starting initial card load from Scryfall")
        # Initial load: update all existing cards (overwrite with latest data)
        result = self.insert_cards(batch_size=batch_size, update_existing=True, limit=limit)
        
        # Update load metadata
        if result['cards_loaded'] > 0:
            current_timestamp = datetime.now()
            update_load_metadata(
                last_timestamp=current_timestamp,
                objects_loaded=result['cards_loaded'], 
                data_type='cards',
                load_type='initial'
            )
        
        # Standardize return format
        return {
            'success': result['errors'] == 0,
            'objects_loaded': result['cards_loaded'],
            'objects_processed': result['cards_processed'],
            'errors': result['errors']
        }
    
    def load_incremental(self, batch_size: int = 1000, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Perform incremental load of cards from Scryfall bulk data.
        
        Downloads the latest Scryfall data and only inserts new cards (skips existing cards by primary key).
        Otherwise, skips the load entirely.
        
        Unlike initial load, this preserves existing card records and only adds
        new cards that don't already exist in the database.
        
        Args:
            batch_size: Number of cards to insert per batch (default: 1000)
            limit: Optional limit on number of cards to load (default: None for all)
        
        Returns:
            Dictionary with keys:
            - success: bool - True if load completed without errors
            - objects_loaded: int - Number of new cards successfully inserted (existing cards skipped)
            - objects_processed: int - Total number of cards processed
            - errors: int - Number of errors encountered
        """
        logger.info("Starting incremental card load from Scryfall")
        
        # Get last load timestamp
        last_timestamp = get_last_load_timestamp('cards')
        if not last_timestamp:
            logger.info("No previous card load found, performing initial load instead")
            return self.load_initial(batch_size=batch_size, limit=limit)
        
        # Calculate days since last load
        days_since_last = (time.time() - last_timestamp.timestamp()) / 86400
        
        # For cards, we typically want to reload if it's been more than 7 days
        # since Scryfall updates their bulk data regularly
        logger.info(
            f"Last card load was {days_since_last:.1f} days ago. "
            "Performing full refresh."
        )
        # Incremental load: only insert new cards, skip existing ones
        result = self.insert_cards(batch_size=batch_size, update_existing=False, limit=limit)
        
        # Update load metadata
        if result['cards_loaded'] > 0:
            current_timestamp = datetime.now()
            update_load_metadata(
                last_timestamp=current_timestamp,
                objects_loaded=result['cards_loaded'], 
                data_type='cards',
                load_type='incremental'
            )
        
        # Standardize return format
        return {
            'success': result['errors'] == 0,
            'objects_loaded': result['cards_loaded'],
            'objects_processed': result['cards_processed'],
            'errors': result['errors']
        }

