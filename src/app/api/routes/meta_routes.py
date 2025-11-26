"""
Meta Analytics API Routes - REST API endpoints for meta analytics.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.app.api.models import (
    ArchetypeQueryParams,
    ArchetypeRankingsResponse,
    ErrorResponse,
    MatchupMatrixResponse,
    MatchupQueryParams,
)
from src.app.api.services.meta_analysis import MetaService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/archetypes",
    response_model=ArchetypeRankingsResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request - Invalid parameters"},
        404: {"model": ErrorResponse, "description": "Not Found - No data available"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    },
)
async def get_archetype_rankings(
    format: str = Query(..., description="Tournament format (e.g., Modern, Pioneer, Standard)"),
    current_days: int = Query(14, ge=1, le=365, description="Number of days back from today for current period"),
    previous_days: int = Query(14, ge=1, le=365, description="Number of days back from end of current period for previous period"),
    color_identity: Optional[str] = Query(None, description="Filter by color identity"),
    strategy: Optional[str] = Query(None, description="Filter by strategy (aggro, midrange, control, ramp, combo)"),
    group_by: Optional[str] = Query(None, description="Group by field (color_identity, strategy)"),
) -> ArchetypeRankingsResponse:
    """
    Get archetype rankings with meta share and win rate for a format.

    Returns ranked list of archetypes with:
    - Meta share (percentage of decklists)
    - Win rate (percentage of matches won)
    - Comparison between current and previous time periods

    Query Parameters:
    - format: Required. Tournament format (e.g., "Modern", "Pioneer")
    - current_days: Days back from today for current period (default: 14)
    - previous_days: Days back from end of current period for previous period (default: 14)
    - color_identity: Optional filter by color (e.g., "dimir", "jeskai")
    - strategy: Optional filter by strategy (aggro, midrange, control, ramp, combo)
    - group_by: Optional grouping (color_identity, strategy)

    Returns:
    - 200: Archetype rankings data with metadata
    - 400: Invalid parameters
    - 404: No data available for the format
    - 500: Internal server error
    """
    try:
        # Validate query parameters
        params = ArchetypeQueryParams(
            format=format,
            current_days=current_days,
            previous_days=previous_days,
            color_identity=color_identity,
            strategy=strategy,
            group_by=group_by,
        )

        # Get rankings from service
        service = MetaService()
        result = service.get_archetype_rankings(
            format=params.format,
            current_days=params.current_days,
            previous_days=params.previous_days,
            color_identity=params.color_identity,
            strategy=params.strategy,
            group_by=params.group_by,
        )

        # Check if we have any data
        if not result["data"]:
            logger.info(f"No archetype data found for format: {format}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "No Data Available",
                    "message": f"No archetype data found for format '{format}' in the specified time window",
                },
            )

        return ArchetypeRankingsResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting archetype rankings: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "An error occurred while processing the request",
                "details": {"error": str(e)},
            },
        )


@router.get(
    "/matchups",
    response_model=MatchupMatrixResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request - Invalid parameters"},
        404: {"model": ErrorResponse, "description": "Not Found - No data available"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    },
)
async def get_matchup_matrix(
    format: str = Query(..., description="Tournament format (e.g., Modern, Pioneer, Standard)"),
    days: int = Query(14, ge=1, le=365, description="Number of days to include in analysis"),
) -> MatchupMatrixResponse:
    """
    Get matchup matrix showing head-to-head win rates between archetypes.

    Returns a matrix where:
    - Rows represent the player's archetype
    - Columns represent the opponent's archetype
    - Cells contain win rate percentage and match count

    Query Parameters:
    - format: Required. Tournament format (e.g., "Modern", "Pioneer")
    - days: Days to include in analysis (default: 14)

    Returns:
    - 200: Matchup matrix with archetypes list and metadata
    - 400: Invalid parameters
    - 404: No data available for the format
    - 500: Internal server error
    """
    try:
        # Validate query parameters
        params = MatchupQueryParams(format=format, days=days)

        # Get matchup matrix from service
        service = MetaService()
        result = service.get_matchup_matrix(
            format=params.format,
            days=params.days,
        )

        # Check if we have any data
        if not result["matrix"]:
            logger.info(f"No matchup data found for format: {format}")
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "No Data Available",
                    "message": f"No matchup data found for format '{format}' in the last {days} days",
                },
            )

        return MatchupMatrixResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting matchup matrix: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Internal Server Error",
                "message": "An error occurred while processing the request",
                "details": {"error": str(e)},
            },
        )

