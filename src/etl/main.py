"""Main script for loading tournament data and card data"""

import sys
import argparse
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.etl.tournaments_pipeline import TournamentsPipeline
from src.etl.cards_pipeline import CardsPipeline
from src.etl.archetype_pipeline import ArchetypeClassificationPipeline
from src.database.connection import DatabaseConnection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_tournaments(args):
    """Load tournament data from TopDeck"""
    pipeline = TournamentsPipeline()
    
    if args.mode == 'initial':
        logger.info(f"Starting initial tournament load (past {args.days} days)")
        result = pipeline.load_initial(days_back=args.days)
    else:
        logger.info("Starting incremental tournament load")
        result = pipeline.load_incremental()
    
    logger.info(f"Tournament load complete:")
    logger.info(f"  - Objects loaded: {result['objects_loaded']}")
    logger.info(f"  - Objects processed: {result['objects_processed']}")
    logger.info(f"  - Errors: {result['errors']}")
    logger.info(f"  - Success: {result['success']}")
    
    return result


def load_cards(args):
    """Load card data from Scryfall"""
    pipeline = CardsPipeline()
    
    if args.mode == 'initial':
        logger.info("Starting initial card load from Scryfall")
        result = pipeline.load_initial(batch_size=args.batch_size)
    else:
        logger.info("Starting incremental card load from Scryfall")
        result = pipeline.load_incremental(batch_size=args.batch_size)
    
    logger.info(f"Card load complete:")
    logger.info(f"  - Objects loaded: {result['objects_loaded']}")
    logger.info(f"  - Objects processed: {result['objects_processed']}")
    logger.info(f"  - Errors: {result['errors']}")
    logger.info(f"  - Success: {result['success']}")
    
    return result


def load_archetypes(args):
    """Load archetype classifications using LLM"""
    import os
    
    # Get model name from environment variable (required)
    model_provider = getattr(args, 'model_provider', None)
    if not model_provider:
        raise ValueError("model_provider is required.")
    
    # Get optional model provider override
    model_provider = getattr(args, 'model_provider', None)
    
    pipeline = ArchetypeClassificationPipeline(
        model_provider=model_provider,
        prompt_id='archetype_classification_v1'
    )
    
    if args.mode == 'initial':
        logger.info("Starting initial archetype classification")
        result = pipeline.load_initial(batch_size=args.batch_size)
    else:
        logger.info("Starting incremental archetype classification")
        result = pipeline.load_incremental(batch_size=args.batch_size)
    
    logger.info(f"Archetype classification complete:")
    logger.info(f"  - Objects loaded: {result['objects_loaded']}")
    logger.info(f"  - Objects processed: {result['objects_processed']}")
    logger.info(f"  - Errors: {result['errors']}")
    logger.info(f"  - Success: {result['success']}")
    
    return result


def main():
    """Main entry point for loading data"""
    parser = argparse.ArgumentParser(
        description='Load tournament data from TopDeck.gg and/or card data from Scryfall'
    )
    parser.add_argument(
        '--data-type',
        choices=['tournaments', 'cards', 'archetypes'],
        help='Type of data to load: tournaments (TopDeck), cards (Scryfall), or archetypes (LLM classification)'
    )
    parser.add_argument(
        '--mode',
        choices=['initial', 'incremental'],
        default='incremental',
        help='Load mode: initial (full load) or incremental (since last load). Applies to both tournaments and cards.'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=180,
        help='Number of days back for initial tournament load (default: 180). Only applies when loading tournaments.'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for data loading (default: 1000 for cards, 50 for archetypes).'
    )
    parser.add_argument(
        '--model-provider',
        choices=['azure_openai', 'anthropic', 'openai', 'aws_bedrock'],
        type=str,
        help='LLM model provider for archetype classification'
    )
    parser.add_argument(
        '--prompt-id',
        type=str,
        help='Prompt version identifier for archetype classification'
    )
    
    args = parser.parse_args()
    
    try:
        DatabaseConnection.initialize_pool()
        
        result = None

        # Load data based on type
        if args.data_type == 'cards':
            result = load_cards(args)
        elif args.data_type == 'tournaments':
            result = load_tournaments(args)
        elif args.data_type == 'archetypes':
            if args.batch_size > 100:
                args.batch_size = 100  # Max batch size for LLM classification
            result = load_archetypes(args)
        
        # Final summary
        if result:
            logger.info(f"Load complete: {result['objects_loaded']} objects loaded")
            if not result['success']:
                logger.warning(f"Load completed with {result['errors']} errors")
                return 1
        
        return 0
    except Exception as e:
        logger.error(f"Load failed: {e}", exc_info=True)
        return 1
    finally:
        DatabaseConnection.close_pool()


if __name__ == '__main__':
    sys.exit(main())

