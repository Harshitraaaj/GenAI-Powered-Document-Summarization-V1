
from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
import logging

from app.services.graph_builder import build_graph, query_graph
from app.db.mongodb import MongoDB

router = APIRouter(tags=["Knowledge Graph"])
logger = logging.getLogger(__name__)
db = MongoDB()


@router.post("/build-graph")
async def build_graph_api(doc_id: str):
    """
    Build a knowledge graph.

    Returns:
        - doc_id
        - nodes_created
        - relationships_created
    """
    logger.info(f"Building graph for doc_id: {doc_id}")

    document = db.get_by_doc_id(doc_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found.")
    if not document.get("entities"):
        raise HTTPException(status_code=400, detail="Entities not found. Run /extract-entities first.")

    try:
        result = await run_in_threadpool(
            build_graph,
            doc_id,
            document["entities"],
            document["chunks"]
        )

        db.update_document(doc_id, {"graph_built": True})  # mark graph ready

        return {
            "doc_id": doc_id,
            "nodes_created": result["nodes"],
            "relationships_created": result["relationships"]
        }

    except Exception:
        logger.exception("Graph building failed")
        raise HTTPException(status_code=500, detail="Graph building failed.")


@router.get("/graph-query")
async def graph_query_api(
    doc_id: str = Query(..., description="Document ID"),
    entity: str = Query(..., description="Entity name")
):
    """
    Get relationships for an entity.

    Returns:
        - doc_id
        - entity
        - relationships
    """
    logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity}")

    document = db.get_by_doc_id(doc_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found.")
    if not document.get("graph_built"):
        raise HTTPException(status_code=400, detail="Graph not built yet. Run /build-graph first.")

    try:
        relationships = await run_in_threadpool(query_graph, doc_id, entity)

        return {
            "doc_id": doc_id,
            "entity": entity,
            "relationships": relationships
        }

    except Exception:
        logger.exception("Graph query failed")
        raise HTTPException(status_code=500, detail="Graph query failed.")


@router.get("/graph-all")
async def get_full_graph_endpoint(doc_id: str):
    """
    Get full graph.

    Returns:
        - doc_id
        - relationship_count
        - relationships
    """
    from app.services.graph_builder import get_full_graph as _get_full_graph

    document = db.get_by_doc_id(doc_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found.")
    if not document.get("graph_built"):
        raise HTTPException(status_code=400, detail="Graph not built yet. Run /build-graph first.")

    try:
        results = await run_in_threadpool(_get_full_graph, doc_id)

        return {
            "doc_id": doc_id,
            "relationship_count": len(results),
            "relationships": results
        }

    except Exception:
        logger.exception("Full graph query failed")
        raise HTTPException(status_code=500, detail="Full graph query failed.")