from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool
import logging

from app.services.extractor import extract_entities_from_chunks, compute_entity_accuracy
from app.db.mongodb import MongoDB

router = APIRouter(tags=["Entity Extraction"])
logger = logging.getLogger(__name__)
db = MongoDB()


@router.post("/extract-entities")
async def extract_entities(doc_id: str):
    """
    Extracts entities from an already-ingested document.
    Requires: /summarize to have been called first.

    Returns:
        - entity_count: total deduplicated entities
        - entities: full entity list with type, context, source_chunk_ids
        - accuracy_metrics: entity extraction quality scores
            - type_accuracy:    % entities with a valid recognized type
            - context_accuracy: % entities with a non-empty context
            - source_accuracy:  % entities grounded in a source chunk
            - overall_accuracy: average of above 3 dimensions
    """
    logger.info(f"Entity extraction for doc_id: {doc_id}")

    document = db.get_by_doc_id(doc_id)
    if not document:
        raise HTTPException(
            status_code=404,
            detail="Document not found. Run /summarize first."
        )

    try:
        entities = await run_in_threadpool(
            extract_entities_from_chunks,
            document["chunks"]
        )

        # Compute entity extraction accuracy metrics
        accuracy_metrics = compute_entity_accuracy(entities)

        db.update_document(doc_id, {"entities": entities})

        return {
            "doc_id": doc_id,
            "entity_count": len(entities),
            "entities": entities,
            "accuracy_metrics": accuracy_metrics
        }

    except Exception:
        logger.exception("Entity extraction failed")
        raise HTTPException(status_code=500, detail="Entity extraction failed.")