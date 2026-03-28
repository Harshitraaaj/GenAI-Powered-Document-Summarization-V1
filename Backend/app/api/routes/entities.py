
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
    Extract entities from a processed document.

    Returns:
        - doc_id
        - entity_count
        - entities
        - accuracy_metrics
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

        # Compute accuracy metrics for extracted entities
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