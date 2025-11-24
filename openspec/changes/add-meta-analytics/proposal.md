# Change: Add Meta Analytics API

## Why
MTG Meta Mage has collected tournament, archetype, and match data for all constructed formats (Standard, Modern, Pioneer, Legacy, Vintage, Pauper) but lacks an interface for users to analyze meta trends. Users need to compare archetype performance across time windows and understand matchup dynamics to make informed competitive decisions across all formats.

## What Changes
- Add FastAPI server with REST API endpoints for meta analytics
- Implement archetype ranking calculation with meta share and win rate for ANY format in database
- Add time window comparison (current vs previous periods) for trend analysis
- Add filtering and grouping by format, color_identity, and strategy
- Implement matchup matrix calculation for head-to-head archetype win rates for ANY format
- Support all constructed, non-Commander formats dynamically (no hardcoded format lists)
- Use polars for efficient data aggregation and analysis
- Add new dependencies: fastapi, uvicorn, polars

## Impact
- Affected specs: New capability `meta-analytics`
- Affected code:
  - New: `src/app/api/main.py` - FastAPI application setup
  - New: `src/app/api/routes/` - API route handlers
  - New: `src/app/api/services/` - Business logic for analytics calculations
  - New: `src/app/api/models.py` - Pydantic models for request/response schemas
  - Modified: `pyproject.toml` - Add FastAPI, uvicorn, polars dependencies

