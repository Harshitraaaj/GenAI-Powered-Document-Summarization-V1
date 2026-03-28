
from fastapi import APIRouter, UploadFile, File, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
import uuid
import logging

from app.services.pipeline import run_pipeline
from app.core.config import MAX_FILE_SIZE_MB, ALLOWED_FILE_TYPES, TEMP_DIR, TEMP_FILE_PREFIX

router = APIRouter(tags=["Summarization"])
logger = logging.getLogger(__name__)


@router.post("/summarize")
async def summarize_document(request: Request, file: UploadFile = File(...)):

    logger.info(f"Summarization request: {file.filename}")

    if not file.filename.lower().endswith(ALLOWED_FILE_TYPES):
        raise HTTPException(status_code=400, detail="Only supported file types allowed.")

    contents = await file.read()
    if len(contents) / (1024 * 1024) > MAX_FILE_SIZE_MB:
        raise HTTPException(status_code=400, detail=f"File too large. Max allowed: {MAX_FILE_SIZE_MB}MB.")

    temp_filename = f"{TEMP_FILE_PREFIX}_{uuid.uuid4().hex}_{file.filename}"
    temp_path = TEMP_DIR / temp_filename

    # ── Pull vector_store from app.state ──────────────────────────────
    vector_store = getattr(request.app.state, "vector_store", None)
    if vector_store is None:
        logger.warning(
            "vector_store not found in app.state — embeddings will be skipped. "
            "Make sure VectorStore is initialized in main.py startup."
        )

    try:
        with open(temp_path, "wb") as buffer:
            buffer.write(contents)

        result = await run_in_threadpool(
            run_pipeline,
            str(temp_path),
            contents,
            vector_store,       # ← injected here
        )
        logger.info(f"Summarization completed for {file.filename}")
        return result

    except Exception:
        logger.exception("Pipeline execution failed")
        raise HTTPException(status_code=500, detail="Pipeline execution failed.")

    finally:
        if temp_path.exists():
            temp_path.unlink()