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


def validate_args(args):
    """
    Validate that required arguments are provided based on data_type.
    
    Args:
        args: Parsed arguments
        
    Raises:
        ValueError: If required arguments are missing for the given data_type
    """
    if not args.data_type:
        raise ValueError("--data-type is required")
    
    if args.data_type == 'tournaments':
        # Tournaments: mode is required, days only needed for initial mode
        if args.mode == 'initial' and args.days is None:
            raise ValueError("--days is required when --data-type=tournaments and --mode=initial")
    
    elif args.data_type == 'cards':
        # Cards: mode is required, batch_size has default so optional
        pass  # No additional required args beyond mode
    
    elif args.data_type == 'archetypes':
        # Archetypes: mode, model_provider, and prompt_id are required
        if not args.model_provider:
            raise ValueError("--model-provider is required when --data-type=archetypes")
        if not args.prompt_id:
            raise ValueError("--prompt-id is required when --data-type=archetypes")
    
    elif args.data_type == 'all':
        # All: tournaments requires days for initial, archetypes optional
        if args.mode == 'initial' and args.days is None:
            raise ValueError("--days is required when --data-type=all and --mode=initial")
        # model_provider and prompt_id are optional for 'all' - will skip archetypes if not provided


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
    # Get model provider (validated in validate_args)
    model_provider = args.model_provider
    
    # Get prompt_id (validated in validate_args)
    prompt_id = args.prompt_id
    
    pipeline = ArchetypeClassificationPipeline(
        model_provider=model_provider,
        prompt_id=prompt_id
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


def load_all(args):
    """
    Load all data types in correct dependency order.
    Order: cards -> tournaments -> archetypes
    
    Dependencies:
    - Tournaments pipeline needs cards table to populate deck_cards
    - Archetypes pipeline needs deck_cards to classify decklists
    
    The pipeline continues if at least 95% of objects are successfully loaded at each step.
    Stops if success rate falls below 95%.
    """
    results = {}
    min_success_rate = 0.95
    
    # Step 1: Load cards FIRST (required for tournament decklist parsing)
    logger.info("=" * 80)
    logger.info("STEP 1/3: Loading cards")
    logger.info("=" * 80)
    results['cards'] = load_cards(args)
    
    # Check success rate
    cards_processed = results['cards']['objects_processed']
    cards_loaded = results['cards']['objects_loaded']
    if cards_processed > 0:
        cards_success_rate = cards_loaded / cards_processed
        if cards_success_rate < min_success_rate:
            logger.error(
                f"Card load success rate {cards_success_rate:.1%} is below threshold "
                f"{min_success_rate:.0%} ({cards_loaded}/{cards_processed}), stopping pipeline"
            )
            return results
        elif cards_success_rate < 1.0:
            logger.warning(
                f"Card load success rate: {cards_success_rate:.1%} "
                f"({cards_loaded}/{cards_processed}), continuing..."
            )
    
    # Step 2: Load tournaments SECOND (matches decklists to cards from step 1)
    logger.info("=" * 80)
    logger.info("STEP 2/3: Loading tournaments")
    logger.info("=" * 80)
    results['tournaments'] = load_tournaments(args)
    
    # Check success rate
    tournaments_processed = results['tournaments']['objects_processed']
    tournaments_loaded = results['tournaments']['objects_loaded']
    if tournaments_processed > 0:
        tournaments_success_rate = tournaments_loaded / tournaments_processed
        if tournaments_success_rate < min_success_rate:
            logger.error(
                f"Tournament load success rate {tournaments_success_rate:.1%} is below threshold "
                f"{min_success_rate:.0%} ({tournaments_loaded}/{tournaments_processed}), stopping pipeline"
            )
            return results
        elif tournaments_success_rate < 1.0:
            logger.warning(
                f"Tournament load success rate: {tournaments_success_rate:.1%} "
                f"({tournaments_loaded}/{tournaments_processed}), continuing..."
            )
    
    # Step 3: Load archetypes THIRD (requires deck_cards from step 2)
    if args.model_provider and args.prompt_id:
        logger.info("=" * 80)
        logger.info("STEP 3/3: Classifying archetypes")
        logger.info("=" * 80)
        if args.batch_size > 50:
            args.batch_size = 50  # Max batch size for LLM classification
        results['archetypes'] = load_archetypes(args)
        
        # Check success rate
        archetypes_processed = results['archetypes']['objects_processed']
        archetypes_loaded = results['archetypes']['objects_loaded']
        if archetypes_processed > 0:
            archetypes_success_rate = archetypes_loaded / archetypes_processed
            if archetypes_success_rate < min_success_rate:
                logger.warning(
                    f"Archetype classification success rate {archetypes_success_rate:.1%} is below threshold "
                    f"{min_success_rate:.0%} ({archetypes_loaded}/{archetypes_processed})"
                )
            elif archetypes_success_rate < 1.0:
                logger.info(
                    f"Archetype classification success rate: {archetypes_success_rate:.1%} "
                    f"({archetypes_loaded}/{archetypes_processed})"
                )
    else:
        logger.info("=" * 80)
        logger.info("STEP 3/3: Skipping archetype classification (no model provider specified)")
        logger.info("=" * 80)
        results['archetypes'] = {'success': True, 'objects_loaded': 0, 'objects_processed': 0, 'errors': 0}
    
    # Summary
    logger.info("=" * 80)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 80)
    for data_type, result in results.items():
        processed = result['objects_processed']
        loaded = result['objects_loaded']
        errors = result['errors']
        
        # Calculate success rate
        if processed > 0:
            success_rate = loaded / processed
            # Determine status symbol
            if success_rate < min_success_rate:
                status = "✗"  # Below threshold
            elif success_rate < 1.0:
                status = "⚠"  # Partial success
            else:
                status = "✓"  # Full success
            logger.info(
                f"{status} {data_type.upper()}: {loaded}/{processed} loaded ({success_rate:.1%}), "
                f"{errors} errors"
            )
        else:
            status = "○"  # Nothing to process
            logger.info(f"{status} {data_type.upper()}: No objects to process")
    
    return results


def main():
    """Main entry point for loading data"""
    parser = argparse.ArgumentParser(
        description='Load tournament data from TopDeck.gg and/or card data from Scryfall'
    )
    parser.add_argument(
        '--data-type',
        choices=['tournaments', 'cards', 'archetypes', 'all'],
        required=True,
        help='Type of data to load: tournaments (TopDeck), cards (Scryfall), archetypes (LLM classification), or all (runs all pipelines in order)'
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
        default=None,
        help='Number of days back for initial tournament load (required for tournaments/initial mode, default: 180).'
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
        help='LLM model provider for archetype classification (required for archetypes, optional for all)'
    )
    parser.add_argument(
        '--prompt-id',
        type=str,
        help='Prompt version identifier for archetype classification (required for archetypes, optional for all)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments based on data_type
    try:
        validate_args(args)
    except ValueError as e:
        parser.error(str(e))
    
    try:
        DatabaseConnection.initialize_pool()
        
        result = None

        # Load data based on type
        if args.data_type == 'all':
            results = load_all(args)
            min_success_rate = 0.95
            
            # Check success rates for each pipeline
            failed = []
            partial = []
            for data_type, result in results.items():
                if result['objects_processed'] > 0:
                    success_rate = result['objects_loaded'] / result['objects_processed']
                    if success_rate < min_success_rate:
                        failed.append(data_type)
                    elif success_rate < 1.0:
                        partial.append(data_type)
            
            if failed:
                logger.error(f"Pipeline(s) below {min_success_rate:.0%} success rate: {', '.join(failed)}")
                return 1
            elif partial:
                logger.warning(f"Pipeline(s) had partial failures: {', '.join(partial)}")
                logger.info("Pipeline completed with warnings")
                return 0
            else:
                logger.info("Pipeline completed successfully")
                return 0
            
        elif args.data_type == 'cards':
            result = load_cards(args)
        elif args.data_type == 'tournaments':
            result = load_tournaments(args)
        elif args.data_type == 'archetypes':
            if args.batch_size > 50:
                args.batch_size = 50  # Max batch size for LLM classification
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

