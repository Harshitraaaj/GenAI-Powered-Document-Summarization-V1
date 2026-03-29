
import uuid
import logging
from fastapi import APIRouter, UploadFile, File, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from app.services.pipeline import run_pipeline
from app.core.config import settings

router = APIRouter(tags=["Summarization"])
logger = logging.getLogger(__name__)


@router.post("/summarize")
async def summarize_document(request: Request, file: UploadFile = File(...)):
    logger.info("Summarization request | filename=%s", file.filename)

    if not file.filename.lower().endswith(settings.ALLOWED_FILE_TYPES):
        raise HTTPException(status_code=400, detail="Only supported file types allowed.")

    contents = await file.read()
    if len(contents) / settings.BYTES_PER_MB > settings.MAX_FILE_SIZE_MB:
        raise HTTPException(
            status_code=400,
            detail="File too large. Max allowed: %dMB." % settings.MAX_FILE_SIZE_MB,
        )

    temp_path = settings.TEMP_DIR / f"{settings.TEMP_FILE_PREFIX}_{uuid.uuid4().hex}_{file.filename}"

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
            vector_store,
        )
        logger.info("Summarization completed | filename=%s", file.filename)
        return result

    except Exception:
        logger.exception("Pipeline execution failed | filename=%s", file.filename)
        raise HTTPException(status_code=500, detail="Pipeline execution failed.")

    finally:
        if temp_path.exists():
            temp_path.unlink()