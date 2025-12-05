"""
Deck Coaching API Routes - REST API endpoints for deck analysis and optimization.

Routes call MCP tools via MultiServerMCPClient over HTTP (streamable_http transport).
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field

from src.app.api.models import ErrorResponse
from src.app.api.services.mcp_client import call_mcp_tool

logger = logging.getLogger(__name__)

router = APIRouter()


class CardDetail(BaseModel):
    """Card detail model."""
    name: str = Field(..., description="Card name")
    quantity: int = Field(..., ge=1, description="Number of copies")
    section: str = Field(..., description="Section: 'mainboard' or 'sideboard'")
    oracle_text: Optional[str] = Field(None, description="Oracle text")
    rulings: Optional[str] = Field(None, description="Official rulings")
    type_line: Optional[str] = Field(None, description="Card type line")
    color_identity: Optional[list[str]] = Field(None, description="Color identity")
    mana_cost: Optional[str] = Field(None, description="Mana cost")
    cmc: Optional[float] = Field(None, description="Converted mana cost")


class ParseDecklistRequest(BaseModel):
    """Request model for parse_and_validate_decklist."""
    decklist: str = Field(..., description="Raw decklist text")


class ParseDecklistResponse(BaseModel):
    """Response model for parse_and_validate_decklist."""
    card_details: list[CardDetail] = Field(..., description="Enriched card details")
    mainboard_count: int = Field(..., ge=0, description="Total mainboard card count")
    sideboard_count: int = Field(..., ge=0, description="Total sideboard card count")
    errors: list[str] = Field(default=[], description="Cards that couldn't be found")


class OptimizeMainboardRequest(BaseModel):
    """Request model for optimize_mainboard."""
    card_details: list[CardDetail] = Field(..., description="List of card details from parse_and_validate_decklist")
    archetype: str = Field(..., description="Deck archetype name")
    format: str = Field(..., description="Tournament format (e.g., Modern, Pioneer)")
    top_n: int = Field(5, ge=1, le=10, description="Number of top archetypes to optimize against")


class OptimizeSideboardRequest(BaseModel):
    """Request model for optimize_sideboard."""
    card_details: list[CardDetail] = Field(..., description="List of card details from parse_and_validate_decklist")
    archetype: str = Field(..., description="Deck archetype name")
    format: str = Field(..., description="Tournament format (e.g., Modern, Pioneer)")
    top_n: int = Field(5, ge=1, le=10, description="Number of top archetypes to optimize against")


class GenerateMatchupStrategyRequest(BaseModel):
    """Request model for generate_matchup_strategy."""
    card_details: list[CardDetail] = Field(..., description="List of card details from parse_and_validate_decklist")
    archetype: str = Field(..., description="Your deck's archetype")
    opponent_archetype: str = Field(..., description="Opponent's archetype")
    matchup_stats: Optional[dict] = Field(None, description="Optional matchup statistics")


@router.post(
    "/parse-decklist",
    response_model=ParseDecklistResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request - Invalid parameters"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    },
)
async def parse_decklist(request: ParseDecklistRequest) -> ParseDecklistResponse:
    """
    Parse a decklist and enrich with card details from the database.
    
    Returns enriched card data including name, quantity, oracle_text, rulings,
    type_line, color_identity, mana_cost, cmc, and section.
    """
    try:
        result = await call_mcp_tool(
            "parse_and_validate_decklist",
            arguments={"decklist": request.decklist}
        )
        
        return ParseDecklistResponse(**result)
        
    except Exception as e:
        logger.error(f"Error parsing decklist: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "An error occurred while parsing the decklist",
                "details": {"error": str(e)},
            },
        )


@router.post(
    "/optimize-mainboard",
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request - Invalid parameters"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    },
)
async def optimize_mainboard(request: OptimizeMainboardRequest) -> dict:
    """
    Optimize a deck's mainboard by identifying flex spots and recommending replacements.
    
    Analyzes the deck against the top N most frequent archetypes in the format,
    identifies non-essential cards (flex spots), and recommends format-legal
    replacements that improve matchups against the current meta.
    """
    try:
        # Convert CardDetail models to dicts
        card_details_dict = [card.model_dump() for card in request.card_details]
        
        result = await call_mcp_tool(
            "optimize_mainboard",
            arguments={
                "card_details": card_details_dict,
                "archetype": request.archetype,
                "format": request.format,
                "top_n": request.top_n,
            }
        )
        
        # Check for errors
        if "error" in result:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Optimization Failed",
                    "message": result.get("error", "Unknown error"),
                    "details": result,
                },
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error optimizing mainboard: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "An error occurred while optimizing the mainboard",
                "details": {"error": str(e)},
            },
        )


@router.post(
    "/optimize-sideboard",
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request - Invalid parameters"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    },
)
async def optimize_sideboard(request: OptimizeSideboardRequest) -> dict:
    """
    Optimize a deck's sideboard to better answer the top N meta archetypes.
    
    Analyzes the sideboard against the most frequent archetypes, considering
    opponent sideboard plans in post-board games. Recommends additions,
    removals, and provides sideboard guides for each matchup.
    """
    try:
        # Convert CardDetail models to dicts
        card_details_dict = [card.model_dump() for card in request.card_details]
        
        result = await call_mcp_tool(
            "optimize_sideboard",
            arguments={
                "card_details": card_details_dict,
                "archetype": request.archetype,
                "format": request.format,
                "top_n": request.top_n,
            }
        )
        
        # Check for errors
        if "error" in result:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Optimization Failed",
                    "message": result.get("error", "Unknown error"),
                    "details": result,
                },
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error optimizing sideboard: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "An error occurred while optimizing the sideboard",
                "details": {"error": str(e)},
            },
        )


@router.post(
    "/generate-matchup-strategy",
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request - Invalid parameters"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    },
)
async def generate_matchup_strategy(request: GenerateMatchupStrategyRequest) -> dict:
    """
    Generate AI-powered coaching for a specific matchup.
    
    Provides personalized strategy based on the user's exact deck, including
    mulligan guides, key cards, game plan by phase, and sideboard guide.
    """
    try:
        # Convert CardDetail models to dicts
        card_details_dict = [card.model_dump() for card in request.card_details]
        
        arguments = {
            "card_details": card_details_dict,
            "archetype": request.archetype,
            "opponent_archetype": request.opponent_archetype,
        }
        
        if request.matchup_stats:
            arguments["matchup_stats"] = request.matchup_stats
        
        result = await call_mcp_tool(
            "generate_matchup_strategy",
            arguments=arguments
        )
        
        # Check for errors
        if "error" in result:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Strategy Generation Failed",
                    "message": result.get("error", "Unknown error"),
                    "details": result,
                },
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating matchup strategy: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "An error occurred while generating matchup strategy",
                "details": {"error": str(e)},
            },
        )

