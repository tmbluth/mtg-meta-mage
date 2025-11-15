"""Main script for loading tournament data and card data"""

import sys
import argparse
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.etl.etl_pipeline import ETLPipeline, load_cards_from_bulk_data
from src.database.connection import DatabaseConnection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_tournaments(args):
    """Load tournament data from TopDeck"""
    pipeline = ETLPipeline()
    
    if args.mode == 'initial':
        logger.info(f"Starting initial tournament load (past {args.days} days)")
        count = pipeline.load_initial(days_back=args.days)
    else:
        logger.info("Starting incremental tournament load")
        count = pipeline.load_incremental()
    
    logger.info(f"Tournament load complete: {count} tournaments loaded")
    return count


def load_cards(args):
    """Load card data from Scryfall"""
    logger.info("Starting Scryfall card data load...")
    
    result = load_cards_from_bulk_data(batch_size=args.batch_size)
    
    logger.info(f"Card load complete:")
    logger.info(f"  - Cards loaded: {result['cards_loaded']}")
    logger.info(f"  - Cards processed: {result['cards_processed']}")
    logger.info(f"  - Errors: {result['errors']}")
    
    return result['cards_loaded']


def main():
    """Main entry point for loading data"""
    parser = argparse.ArgumentParser(
        description='Load tournament data from TopDeck.gg and/or card data from Scryfall'
    )
    parser.add_argument(
        '--data-type',
        choices=['tournaments', 'cards', 'both'],
        default='tournaments',
        help='Type of data to load: tournaments (TopDeck), cards (Scryfall), or both (default: tournaments)'
    )
    parser.add_argument(
        '--mode',
        choices=['initial', 'incremental'],
        default='incremental',
        help='Tournament load mode: initial (past 90 days) or incremental (since last load). Only applies when loading tournaments.'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=90,
        help='Number of days back for initial tournament load (default: 90). Only applies when loading tournaments.'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for card data loading (default: 1000). Only applies when loading cards.'
    )
    
    args = parser.parse_args()
    
    try:
        DatabaseConnection.initialize_pool()
        
        tournament_count = None
        card_count = None
        
        if args.data_type in ['tournaments', 'both']:
            tournament_count = load_tournaments(args)
        
        if args.data_type in ['cards', 'both']:
            card_count = load_cards(args)
        
        # Summary
        if args.data_type == 'both':
            logger.info(f"All loads complete: {tournament_count} tournaments, {card_count} cards")
        elif args.data_type == 'tournaments':
            logger.info(f"Load complete: {tournament_count} tournaments loaded")
        else:
            logger.info(f"Load complete: {card_count} cards loaded")
        
        return 0
    except Exception as e:
        logger.error(f"Load failed: {e}", exc_info=True)
        return 1
    finally:
        DatabaseConnection.close_pool()


if __name__ == '__main__':
    sys.exit(main())

