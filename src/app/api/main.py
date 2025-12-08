"""Root FastAPI application for MTG Meta Mage."""

from fastapi import FastAPI

from src.app.agent_api.routes import router as agent_router

app = FastAPI(
    title="MTG Meta Mage API",
    description="MTG Meta Mage API providing meta analytics and agent capabilities",
    version="0.1.0",
)

# Mount agent API router
app.include_router(agent_router, tags=["agent"])

__all__ = ["app"]

