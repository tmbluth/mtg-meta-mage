"""FastAPI application for the LangGraph Agent API."""

from fastapi import FastAPI

from .routes import router, conversation_store

app = FastAPI(title="MTG Meta Mage Agent API")
app.include_router(router)

__all__ = ["app", "conversation_store"]

