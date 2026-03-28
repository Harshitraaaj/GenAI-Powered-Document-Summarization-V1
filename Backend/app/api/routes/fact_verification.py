
from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool
import logging

from app.services.fact_verifier import verify_facts
from app.db.mongodb import MongoDB

router = APIRouter(tags=["Fact Verification"])
logger = logging.getLogger(__name__)
db = MongoDB()


@router.post("/verify-facts")
async def verify_facts_api(doc_id: str):
    """
    Verify summary claims against source chunks.

    Returns:
        - doc_id
        - coverage_score
        - total_claims
        - supported_claims
        - flagged_claims
        - status
    """
    logger.info(f"Fact verification for doc_id: {doc_id}")

    document = db.get_by_doc_id(doc_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found.")

    try:
        result = await run_in_threadpool(
            verify_facts,
            doc_id,
            document["summary"],
            document["chunks"]
        )

        # Persist verification results
        db.update_document(
            doc_id,
            {
                "facts_verified": True,
                "coverage_score": result["coverage_score"]
            }
        )

        return {
            "doc_id": doc_id,
            "coverage_score": result["coverage_score"],
            "total_claims": result["total_claims"],
            "supported_claims": result["supported_claims"],
            "flagged_claims": result["flagged_claims"],
            "status": result["status"]
        }

    except Exception:
        logger.exception("Fact verification failed")
        raise HTTPException(status_code=500, detail="Fact verification failed.")