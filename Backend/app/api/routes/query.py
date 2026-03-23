from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool
import logging

from app.db.mongodb import MongoDB
from app.db.vector_store import VectorStore

router = APIRouter(tags=["Query"])
logger = logging.getLogger(__name__)
db = MongoDB()
vector_store = VectorStore()


@router.post("/query")
async def semantic_query(doc_id: str, query: str):
    """
    Semantic search over document chunks using vector search.
    Returns most relevant chunks for the query.
    Requires: /summarize to have been called first.
    """
    logger.info(f"Semantic query | doc_id: {doc_id} | query: {query}")

    document = db.get_by_doc_id(doc_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found.")

    try:
        results = await run_in_threadpool(vector_store.search, doc_id, query)
        return {"doc_id": doc_id, "query": query, "results": results}

    except Exception:
        logger.exception("Query failed")
        raise HTTPException(status_code=500, detail="Query failed.")