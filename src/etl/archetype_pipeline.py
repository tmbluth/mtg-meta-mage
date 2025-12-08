"""ETL pipeline for archetype classification using LLM"""

import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
import os
from random import uniform
from dotenv import load_dotenv

from pydantic import BaseModel, Field, field_validator

load_dotenv()

from src.etl.database.connection import DatabaseConnection
from src.etl.etl_utils import get_last_load_timestamp, update_load_metadata
from src.etl.base_pipeline import BasePipeline
from src.clients.llm_client import get_llm_client

logger = logging.getLogger(__name__)


class StrategyType(str, Enum):
    """Valid archetype strategy types"""
    AGGRO = 'aggro'
    MIDRANGE = 'midrange'
    CONTROL = 'control'
    RAMP = 'ramp'
    COMBO = 'combo'


class ArchetypeClassificationResponse(BaseModel):
    """Pydantic model for LLM archetype classification response"""
    main_title: str = Field(..., description="Archetype name based on key cards/themes")
    color_identity: str = Field(..., description="Human-readable color description")
    strategy: StrategyType = Field(..., description="One of: aggro, midrange, control, ramp, combo")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score from 0 to 1")
    reasoning: Optional[str] = Field(default=None, description="Explanation for classification")
    
    @field_validator('confidence')
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        """Validate confidence is in valid range"""
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"Confidence must be between 0 and 1, got {v}")
        return v


class ArchetypeClassificationPipeline(BasePipeline):
    """ETL pipeline for classifying decklists into archetypes using LLM"""
    
    def __init__(
        self,
        model_provider: str,
        prompt_id: str
    ):
        """
        Initialize archetype classification pipeline
        
        Args:
            model_provider: Model provider (anthropic, openai, azure_openai, aws_bedrock)
            prompt_id: Version identifier for prompt template
        """
        if model_provider not in ['azure_openai', 'anthropic', 'openai', 'aws_bedrock']:
            raise ValueError("model_provider must be one of: azure_openai, anthropic, openai, aws_bedrock")
        
        # Get model name from environment variable
        self.model_name = os.getenv('LARGE_LANGUAGE_MODEL')
        if not self.model_name:
            raise ValueError("LARGE_LANGUAGE_MODEL environment variable must be set")
        
        self.model_provider = model_provider
        self.prompt_id = prompt_id
        DatabaseConnection.initialize_pool()
        logger.info(
            f"Initialized ArchetypeClassificationPipeline with provider={model_provider}, "
            f"model={self.model_name}"
        )
    
    def get_unclassified_decklists(self) -> List[Dict[str, Any]]:
        """
        Query decklists that don't have archetype classifications yet.
        
        Returns:
            List of decklist dictionaries with keys: decklist_id, format, tournament_id
        """
        try:
            with DatabaseConnection.get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        d.decklist_id,
                        t.format,
                        d.tournament_id
                    FROM decklists d
                    JOIN tournaments t ON d.tournament_id = t.tournament_id
                    WHERE d.archetype_group_id IS NULL
                    ORDER BY d.decklist_id
                """)
                
                rows = cur.fetchall()
                decklists = [
                    {
                        'decklist_id': row[0],
                        'format': row[1],
                        'tournament_id': row[2]
                    }
                    for row in rows
                ]
                
                logger.info(f"Found {len(decklists)} unclassified decklists")
                return decklists
                
        except Exception as e:
            logger.error(f"Error querying unclassified decklists: {e}", exc_info=True)
            raise
    
    def get_decklists_since_timestamp(self, timestamp: datetime) -> List[Dict[str, Any]]:
        """
        Query decklists from tournaments since a given timestamp.
        
        Args:
            timestamp: datetime to filter tournaments
            
        Returns:
            List of decklist dictionaries with keys: decklist_id, format, tournament_id, start_date
        """
        try:
            with DatabaseConnection.get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        d.decklist_id,
                        t.format,
                        d.tournament_id,
                        t.start_date
                    FROM decklists d
                    JOIN tournaments t ON d.tournament_id = t.tournament_id
                    WHERE t.start_date >= %s
                    ORDER BY t.start_date, d.decklist_id
                """, (timestamp,))
                
                rows = cur.fetchall()
                decklists = [
                    {
                        'decklist_id': row[0],
                        'format': row[1],
                        'tournament_id': row[2],
                        'start_date': row[3]  # This is now a datetime object
                    }
                    for row in rows
                ]
                
                logger.info(
                    f"Found {len(decklists)} decklists from tournaments since {timestamp}"
                )
                return decklists
                
        except Exception as e:
            logger.error(f"Error querying decklists since timestamp: {e}", exc_info=True)
            raise
    
    def get_decklist_mainboard_cards(self, decklist_id: int) -> List[Dict[str, Any]]:
        """
        Get enriched mainboard card data for a decklist.
        
        Joins deck_cards (mainboard only) with cards table to get full card details.
        
        Args:
            decklist_id: ID of the decklist
            
        Returns:
            List of card dictionaries with enriched data
        """
        try:
            with DatabaseConnection.get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        dc.card_id,
                        c.name,
                        dc.quantity,
                        c.type_line,
                        c.mana_cost,
                        c.cmc,
                        c.color_identity,
                        c.oracle_text
                    FROM deck_cards dc
                    JOIN cards c ON dc.card_id = c.card_id
                    WHERE dc.decklist_id = %s AND dc.section = 'mainboard'
                    ORDER BY c.cmc, c.name
                """, (decklist_id,))
                
                rows = cur.fetchall()
                cards = [
                    {
                        'card_id': row[0],
                        'name': row[1],
                        'quantity': row[2],
                        'type_line': row[3] or '',
                        'mana_cost': row[4] or '',
                        'cmc': row[5] or 0,
                        'color_identity': row[6] or [],
                        'oracle_text': row[7] or ''
                    }
                    for row in rows
                ]
                
                logger.debug(
                    f"Retrieved {len(cards)} mainboard cards for decklist {decklist_id}"
                )
                return cards
                
        except Exception as e:
            logger.error(
                f"Error getting mainboard cards for decklist {decklist_id}: {e}",
                exc_info=True
            )
            raise
    
    def format_classification_prompt(
        self,
        cards: List[Dict[str, Any]],
        format_name: str,
        instructions: str
    ) -> str:
        """
        Format mainboard cards into structured JSON prompt for LLM classification.
        
        Args:
            cards: List of card dictionaries with enriched data
            format_name: Tournament format (e.g., "Modern", "Standard")
            instructions: Classification instructions from prompt template
            
        Returns:
            JSON string formatted for LLM input
            
        Raises:
            ValueError: If cards list is empty
        """
        if not cards:
            raise ValueError("Cannot generate prompt with empty card list")
        
        # Build structured prompt
        prompt_data = {
            "task": "Classify MTG decklist archetype",
            "format": format_name,
            "mainboard_cards": [
                {
                    "name": card['name'],
                    "quantity": card['quantity'],
                    "type_line": card.get('type_line', ''),
                    "mana_cost": card.get('mana_cost', ''),
                    "cmc": card.get('cmc', 0),
                    "color_identity": card.get('color_identity', []),
                    "oracle_text": card.get('oracle_text', '')
                }
                for card in cards
            ],
            "instructions": instructions
        }
        
        return json.dumps(prompt_data, indent=2)
    
    def parse_classification_response(self, response_text: str) -> ArchetypeClassificationResponse:
        """
        Parse and validate LLM response using Pydantic model.
        
        Args:
            response_text: Raw response text from LLM (expected to be JSON)
            
        Returns:
            Validated ArchetypeClassificationResponse
            
        Raises:
            json.JSONDecodeError: If response is not valid JSON
            ValidationError: If response doesn't match expected schema
        """
        # Parse JSON
        response_data = json.loads(response_text)
        
        # Validate with Pydantic
        return ArchetypeClassificationResponse(**response_data)
    
    def classify_decklist_llm(
        self,
        cards: List[Dict[str, Any]],
        format_name: str,
        max_retries: int = 1
    ) -> ArchetypeClassificationResponse:
        """
        Classify a decklist using LLM API.
        
        Args:
            cards: List of enriched mainboard cards
            format_name: Tournament format
            max_retries: Maximum number of retry attempts for invalid responses
            
        Returns:
            Validated classification response
            
        Raises:
            Exception: If LLM API call fails or response is invalid after retries
        """
        # Load prompt instructions
        prompt_file = os.path.join(
            os.path.dirname(__file__),
            'prompts',
            self.prompt_id + '.txt'
        )
        
        try:
            with open(prompt_file, 'r', encoding='utf-8') as f:
                instructions = f.read().strip()
        except FileNotFoundError:
            logger.warning(f"Prompt file not found: {prompt_file}, using default instructions")
            instructions = (
                "Analyze the decklist and return a JSON response with: "
                "main_title (archetype name), color_identity (e.g., 'dimir', 'gruul'), "
                "strategy (one of: aggro, midrange, control, ramp, combo), "
                "confidence (0-1), and reasoning (explanation)."
            )
        
        # Get LLM client
        client = get_llm_client(self.model_name, self.model_provider)
        
        # Format prompt
        prompt = self.format_classification_prompt(cards, format_name, instructions)
        
        logger.info(f"Classifying decklist with {len(cards)} cards using {self.model_name}")
        
        # Attempt classification with retries
        last_error = None
        for attempt in range(max_retries + 1):
            try:
                # Call LLM
                response = client.run(prompt)
                response_text = response.text
                
                # Parse and validate response
                classification = self.parse_classification_response(response_text)
                
                logger.info(
                    f"Classification successful: {classification.main_title} "
                    f"({classification.strategy}, confidence: {classification.confidence:.2f})"
                )
                
                return classification
                
            except (json.JSONDecodeError, ValueError) as e:
                last_error = e
                logger.warning(
                    f"Invalid response on attempt {attempt + 1}/{max_retries + 1}: {e}"
                )
                
                if attempt < max_retries:
                    # Retry with clarification
                    clarification = (
                        "The previous response was not valid JSON. "
                        "Please respond with ONLY a JSON object containing: "
                        "main_title, color_identity, strategy, confidence, and reasoning. "
                        "Do not include any other text."
                    )
                    prompt = f"{prompt}\n\n{clarification}"
                else:
                    logger.error(f"Failed to get valid response after {max_retries + 1} attempts")
                    raise
        
        # Should not reach here, but raise last error if we do
        raise last_error or Exception("Classification failed")
    
    def insert_archetype(
        self,
        decklist_id: int,
        format_name: str,
        mainboard_cards: List[Dict[str, Any]]
    ) -> Optional[int]:
        """
        Classify a decklist and store the result in archetype_groups and archetype_classifications.
        
        Args:
            decklist_id: ID of the decklist being classified
            format_name: Tournament format
            mainboard_cards: List of enriched mainboard cards
            
        Returns:
            archetype_group_id on success, None on failure
        """
        # Validate minimum card data
        if not mainboard_cards:
            logger.warning(f"Decklist {decklist_id} has no mainboard cards, skipping")
            return None
        
        # Check for missing card data threshold (>10%)
        total_cards = sum(card['quantity'] for card in mainboard_cards)
        if total_cards < 40:  # Typical constructed deck has 60 cards
            logger.warning(
                f"Decklist {decklist_id} has only {total_cards} cards, may be incomplete"
            )
        
        # Call LLM classification with error handling
        classification = self.classify_decklist_llm(
            cards=mainboard_cards,
            format_name=format_name,
        )
        
        if not classification:
            logger.error(f"Failed to classify decklist {decklist_id}")
            return None
        
        # Log low confidence classifications
        if classification.confidence < 0.5:
            logger.warning(
                f"Low confidence classification for decklist {decklist_id}: "
                f"{classification.main_title} (confidence: {classification.confidence:.2f})"
            )
        
        # Store classification in database
        try:
            with DatabaseConnection.get_cursor(commit=True) as cur:
                # Get or create archetype group
                cur.execute("""
                    INSERT INTO archetype_groups (format, main_title, strategy, color_identity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (format, main_title, strategy, color_identity) 
                    DO UPDATE SET archetype_group_id = archetype_groups.archetype_group_id
                    RETURNING archetype_group_id
                """, (
                    format_name,
                    classification.main_title,
                    classification.strategy.value,
                    classification.color_identity
                ))
                
                archetype_group_id = cur.fetchone()[0]
                
                # Insert classification event
                cur.execute("""
                    INSERT INTO archetype_classifications (
                        decklist_id, archetype_group_id, archetype_confidence,
                        llm_model, prompt_id
                    ) VALUES (%s, %s, %s, %s, %s)
                    RETURNING classification_id
                """, (
                    decklist_id,
                    archetype_group_id,
                    classification.confidence,
                    self.model_name,
                    self.prompt_id
                ))
                
                classification_id = cur.fetchone()[0]
                
                logger.info(
                    f"Classified decklist {decklist_id} as {classification.main_title} "
                    f"(group_id: {archetype_group_id}, classification_id: {classification_id}, "
                    f"confidence: {classification.confidence:.2f})"
                )
                
                return archetype_group_id
                
        except Exception as e:
            logger.error(
                f"Error storing classification for decklist {decklist_id}: {e}",
                exc_info=True
            )
            return None
    
    def update_decklist_archetype(
        self,
        decklist_id: int,
        archetype_group_id: Optional[int]
    ) -> bool:
        """
        Update decklist's archetype_group_id reference to current archetype.
        
        Args:
            decklist_id: ID of the decklist
            archetype_group_id: ID of the archetype group (or None to clear)
            
        Returns:
            True on success, False on failure
        """
        try:
            with DatabaseConnection.get_cursor(commit=True) as cur:
                cur.execute("""
                    UPDATE decklists
                    SET archetype_group_id = %s
                    WHERE decklist_id = %s
                """, (archetype_group_id, decklist_id))
                
                logger.debug(
                    f"Updated decklist {decklist_id} archetype_group_id to {archetype_group_id}"
                )
                return True
                
        except Exception as e:
            logger.error(
                f"Error updating decklist {decklist_id} archetype group: {e}",
                exc_info=True
            )
            return False
    
    def load_initial(self, **kwargs) -> Dict[str, Any]:
        """
        Perform initial classification of all unclassified decklists.
        
        Kwargs:
            batch_size: Number of decklists to process in each batch (default: 50)
            
        Returns:
            Dictionary with keys:
            - success: bool - Whether load completed successfully
            - objects_loaded: int - Number of decklists classified
            - objects_processed: int - Total decklists attempted
            - errors: int - Number of classification errors
        """
        batch_size = kwargs.get('batch_size', 50)
        
        logger.info("Starting initial archetype classification")
        
        try:
            # Get all unclassified decklists
            decklists = self.get_unclassified_decklists()
            
            if not decklists:
                logger.info("No unclassified decklists found")
                return {
                    'success': True,
                    'objects_loaded': 0,
                    'objects_processed': 0,
                    'errors': 0
                }
            
            total = len(decklists)
            classified = 0
            errors = 0
            
            # Process in batches
            for i in range(0, total, batch_size):
                batch = decklists[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total + batch_size - 1) // batch_size
                
                logger.info(
                    f"Processing batch {batch_num}/{total_batches} "
                    f"({len(batch)} decklists)"
                )
                
                for decklist in batch:
                    decklist_id = decklist['decklist_id']
                    format_name = decklist['format']
                    
                    try:
                        # Get mainboard cards
                        cards = self.get_decklist_mainboard_cards(decklist_id)
                        
                        if not cards:
                            logger.warning(
                                f"Decklist {decklist_id} has no mainboard cards, skipping"
                            )
                            errors += 1
                            continue
                        
                        # Classify the decklist
                        archetype_group_id = self.insert_archetype(
                            decklist_id, format_name, cards
                        )
                        
                        if archetype_group_id:
                            # Update decklist reference
                            self.update_decklist_archetype(decklist_id, archetype_group_id)
                            classified += 1
                        else:
                            errors += 1
                            
                    except Exception as e:
                        logger.error(
                            f"Error processing decklist {decklist_id}: {e}",
                            exc_info=True
                        )
                        errors += 1
                
                # Log batch progress
                logger.info(
                    f"Batch {batch_num} complete: {classified}/{total - errors} classified, "
                    f"{errors} errors"
                )
            
            # Update load metadata
            current_timestamp = datetime.now()
            update_load_metadata(
                last_timestamp=current_timestamp,
                objects_loaded=classified,
                data_type='archetypes',
                load_type='initial'
            )
            
            logger.info(
                f"Initial classification complete: {classified}/{total} classified, {errors} errors"
            )
            
            return {
                'success': True,
                'objects_loaded': classified,
                'objects_processed': total,
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Initial classification failed: {e}", exc_info=True)
            return {
                'success': False,
                'objects_loaded': 0,
                'objects_processed': 0,
                'errors': 1
            }
    
    def load_incremental(self, **kwargs) -> Dict[str, Any]:
        """
        Perform incremental classification of new decklists since last load.
        
        Returns:
            Dictionary with keys:
            - success: bool - Whether load completed successfully
            - objects_loaded: int - Number of decklists classified
            - objects_processed: int - Total decklists attempted
            - errors: int - Number of classification errors
        """
        batch_size = kwargs.get('batch_size', 50)
        
        logger.info("Starting incremental archetype classification")
        
        try:
            # Get last classification timestamp
            last_timestamp = get_last_load_timestamp('archetypes')
            
            if not last_timestamp:
                logger.info(
                    "No previous classification found, running initial load"
                )
                return self.load_initial(**kwargs)
            
            logger.info(f"Loading decklists since timestamp {last_timestamp}")
            
            # Get decklists from tournaments since last load
            decklists = self.get_decklists_since_timestamp(last_timestamp)
            
            if not decklists:
                logger.info("No new decklists found")
                return {
                    'success': True,
                    'objects_loaded': 0,
                    'objects_processed': 0,
                    'errors': 0
                }
            
            total = len(decklists)
            classified = 0
            errors = 0
            max_timestamp = last_timestamp
            
            # Process in batches
            for i in range(0, total, batch_size):
                batch = decklists[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total + batch_size - 1) // batch_size
                
                logger.info(
                    f"Processing batch {batch_num}/{total_batches} "
                    f"({len(batch)} decklists)"
                )
                
                for decklist in batch:
                    decklist_id = decklist['decklist_id']
                    format_name = decklist['format']
                    start_date = decklist['start_date']  # This is now a datetime object
                    
                    # Track maximum timestamp
                    if start_date > max_timestamp:
                        max_timestamp = start_date
                    
                    try:
                        # Get mainboard cards
                        cards = self.get_decklist_mainboard_cards(decklist_id)
                        
                        if not cards:
                            logger.warning(
                                f"Decklist {decklist_id} has no mainboard cards, skipping"
                            )
                            errors += 1
                            continue
                        
                        # Classify the decklist
                        archetype_group_id = self.insert_archetype(
                            decklist_id, format_name, cards
                        )
                        
                        if archetype_group_id:
                            # Update decklist reference
                            self.update_decklist_archetype(decklist_id, archetype_group_id)
                            classified += 1
                        else:
                            errors += 1
                            
                    except Exception as e:
                        logger.error(
                            f"Error processing decklist {decklist_id}: {e}",
                            exc_info=True
                        )
                        errors += 1
                
                logger.info(
                    f"Batch {batch_num} complete: {classified}/{total - errors} classified, "
                    f"{errors} errors"
                )
            
            # Update load metadata with maximum tournament timestamp
            update_load_metadata(
                last_timestamp=max_timestamp,
                objects_loaded=classified,
                data_type='archetypes',
                load_type='incremental'
            )
            
            logger.info(
                f"Incremental classification complete: {classified}/{total} classified, "
                f"{errors} errors"
            )
            
            return {
                'success': True,
                'objects_loaded': classified,
                'objects_processed': total,
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Incremental classification failed: {e}", exc_info=True)
            return {
                'success': False,
                'objects_loaded': 0,
                'objects_processed': 0,
                'errors': 1
            }

