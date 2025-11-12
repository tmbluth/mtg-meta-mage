"""Main script for loading tournament data"""

import sys
import argparse
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.data.etl_pipeline import ETLPipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for loading tournaments"""
    parser = argparse.ArgumentParser(description='Load tournament data from TopDeck.gg')
    parser.add_argument(
        '--mode',
        choices=['initial', 'incremental'],
        default='incremental',
        help='Load mode: initial (past 90 days) or incremental (since last load)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=90,
        help='Number of days back for initial load (default: 90)'
    )
    
    args = parser.parse_args()
    
    try:
        pipeline = ETLPipeline()
        
        if args.mode == 'initial':
            logger.info(f"Starting initial load (past {args.days} days)")
            count = pipeline.load_initial(days_back=args.days)
        else:
            logger.info("Starting incremental load")
            count = pipeline.load_incremental()
        
        logger.info(f"Load complete: {count} tournaments loaded")
        return 0
    except Exception as e:
        logger.error(f"Load failed: {e}", exc_info=True)
        return 1
    finally:
        from src.database.connection import DatabaseConnection
        DatabaseConnection.close_pool()


if __name__ == '__main__':
    sys.exit(main())

