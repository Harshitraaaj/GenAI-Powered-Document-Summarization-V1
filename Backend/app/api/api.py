
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import APP_TITLE
from app.api.routes import summarization, entities, graph, fact_verification, query
from app.db.vector_store import VectorStore
from app.services.fact_verifier import init_verifier

import logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────────────
    vector_store = VectorStore()
    app.state.vector_store = vector_store
    logger.info("VectorStore initialized and attached to app.state")

    init_verifier(vector_store)
    logger.info("FactVerifier initialized with shared VectorStore")

    yield  # app is running

    # ── Shutdown ──────────────────────────────────────────────────────
    logger.info("App shutting down")


app = FastAPI(title=APP_TITLE, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(summarization.router)
app.include_router(entities.router)
app.include_router(graph.router)
app.include_router(fact_verification.router)
app.include_router(query.router)


@app.get("/", tags=["Health"])
async def health() -> dict:
    return {"status": "ok"}