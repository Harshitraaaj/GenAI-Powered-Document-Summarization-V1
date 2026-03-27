"""modular"""

from fastapi import FastAPI
from app.core.config import APP_TITLE
from app.api.routes import summarization, entities, graph, fact_verification, query
from fastapi.middleware.cors import CORSMiddleware



app = FastAPI(title=APP_TITLE)

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
