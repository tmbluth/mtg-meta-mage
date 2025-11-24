"""
Meta Analytics API - FastAPI application entry point.

This module provides the main FastAPI application for querying meta analytics.
Start the server with: uvicorn src.app.api.main:app --reload
"""

import logging
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.app.api.models import HealthResponse
from src.app.api.routes import meta_routes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create FastAPI application
app = FastAPI(
    title="MTG Meta Analytics API",
    description="REST API for MTG Meta Mage",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(meta_routes.router, prefix="/api/v1/meta", tags=["meta-analytics"])


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check() -> HealthResponse:
    """
    Health check endpoint to verify API is running.

    Returns:
        HealthResponse: API health status and timestamp
    """
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc)
    )


@app.on_event("startup")
async def startup_event() -> None:
    """Log startup message."""
    logger.info("MTG Meta Analytics API started")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Log shutdown message."""
    logger.info("MTG Meta Analytics API shutting down")

