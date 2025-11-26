"""
Pydantic models for Meta Analytics API request/response schemas.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class ArchetypeRanking(BaseModel):
    """Archetype performance data for a time window."""

    main_title: str = Field(..., description="Archetype name")
    color_identity: Optional[str] = Field(None, description="Color identity (e.g., dimir, jeskai)")
    strategy: str = Field(..., description="Strategy type (aggro, midrange, control, ramp, combo)")
    meta_share_current: float = Field(..., ge=0, le=100, description="Current period meta share percentage")
    meta_share_previous: Optional[float] = Field(None, ge=0, le=100, description="Previous period meta share percentage")
    win_rate_current: Optional[float] = Field(None, ge=0, le=100, description="Current period win rate percentage")
    win_rate_previous: Optional[float] = Field(None, ge=0, le=100, description="Previous period win rate percentage")
    sample_size_current: int = Field(..., ge=0, description="Number of decklists in current period")
    sample_size_previous: Optional[int] = Field(None, ge=0, description="Number of decklists in previous period")
    match_count_current: Optional[int] = Field(None, ge=0, description="Number of matches in current period")
    match_count_previous: Optional[int] = Field(None, ge=0, description="Number of matches in previous period")


class ArchetypeRankingsResponse(BaseModel):
    """Response model for archetype rankings endpoint."""

    data: list[ArchetypeRanking] = Field(..., description="List of archetype rankings")
    metadata: dict = Field(..., description="Query metadata (format, time windows, timestamp)")


class MatchupCell(BaseModel):
    """Single matchup win rate cell."""

    win_rate: Optional[float] = Field(None, ge=0, le=100, description="Win rate percentage for this matchup")
    match_count: int = Field(..., ge=0, description="Number of matches for this matchup")


class MatchupMatrixResponse(BaseModel):
    """Response model for matchup matrix endpoint."""

    matrix: dict[str, dict[str, MatchupCell]] = Field(
        ..., description="Matrix of matchup win rates (row archetype vs column archetype)"
    )
    archetypes: list[str] = Field(..., description="List of all archetypes in the matrix")
    metadata: dict = Field(..., description="Query metadata (format, time window, timestamp)")


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[dict] = Field(None, description="Additional error details")


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(..., description="API health status")
    timestamp: datetime = Field(..., description="Current server timestamp")


class ArchetypeQueryParams(BaseModel):
    """Query parameters for archetype rankings endpoint."""

    format: str = Field(..., description="Tournament format (e.g., Modern, Pioneer, Standard)")
    current_days: int = Field(14, ge=1, le=365, description="Number of days back from today for current period")
    previous_days: int = Field(14, ge=1, le=365, description="Number of days back from end of current period for previous period")
    color_identity: Optional[str] = Field(None, description="Filter by color identity")
    strategy: Optional[str] = Field(None, description="Filter by strategy (aggro, midrange, control, ramp, combo)")
    group_by: Optional[str] = Field(None, description="Group results by field (color_identity, strategy)")

    @field_validator("strategy")
    @classmethod
    def validate_strategy(cls, v: Optional[str]) -> Optional[str]:
        """Validate strategy is one of the allowed values."""
        if v is not None:
            allowed = {"aggro", "midrange", "control", "ramp", "combo"}
            if v.lower() not in allowed:
                raise ValueError(f"Strategy must be one of {allowed}")
            return v.lower()
        return v

    @field_validator("group_by")
    @classmethod
    def validate_group_by(cls, v: Optional[str]) -> Optional[str]:
        """Validate group_by is one of the allowed values."""
        if v is not None:
            allowed = {"color_identity", "strategy"}
            if v.lower() not in allowed:
                raise ValueError(f"group_by must be one of {allowed}")
            return v.lower()
        return v


class MatchupQueryParams(BaseModel):
    """Query parameters for matchup matrix endpoint."""

    format: str = Field(..., description="Tournament format (e.g., Modern, Pioneer, Standard)")
    days: int = Field(14, ge=1, le=365, description="Number of days to include in analysis")

