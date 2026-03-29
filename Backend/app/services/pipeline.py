
import json
import logging
import time
import uuid

from app.services.ingestion import load_document
from app.services.chunking import chunk_document
from app.services.summarizer import run_hierarchical_summarization
from app.models.schema import SummarizationOutput

from app.db.cache import CacheService
from app.utils.hashing import get_pdf_hash

logger = logging.getLogger(__name__)

cache_service = CacheService()

def run_pipeline(
    file_path: str,
    file_bytes: bytes = None,
    vector_store=None,
) -> dict:

    logger.info(f"Pipeline started for file: {file_path}")
    pipeline_start = time.time()

    try:
        # ── HASH + UPLOAD COUNT ────────────────────────────────────────
        pdf_hash     = get_pdf_hash(file_bytes) if file_bytes else None
        upload_count = None

        if file_bytes:
            upload_count = cache_service.db.increment_upload_count(pdf_hash)
            logger.info(f"Upload count | hash={pdf_hash} | count={upload_count}")

        # ── CACHE CHECK ────────────────────────────────────────────────
        if file_bytes and upload_count is not None and upload_count >= 3:
            cached = cache_service.check_cache(file_bytes)
            if cached:
                logger.info(
                    f"Cache hit — returning stored result | doc_id={cached.get('doc_id')}"
                )
                return cached
            else:
                logger.info("Cache threshold met but not yet marked — running pipeline")
        else:
            logger.info("Skipping cache check (threshold not met)")

        # ── NEW doc_id ─────────────────────────────────────────────────
        doc_id = str(uuid.uuid4())

        # ── Ingestion ─────────────────────────────────────────────────
        ingestion_start = time.time()
        text = load_document(file_path)

        if not text or not text.strip():
            raise RuntimeError("Document contains no extractable text.")

        logger.info(
            f"Document ingestion completed | "
            f"Characters: {len(text)} | "
            f"Time: {round(time.time() - ingestion_start, 2)}s"
        )

        # ── Chunking ──────────────────────────────────────────────────
        chunking_start = time.time()
        chunks = chunk_document(text)

        if not chunks:
            raise RuntimeError("Chunking failed.")

        logger.info(
            f"Chunking completed | "
            f"Chunks: {len(chunks)} | "
            f"Time: {round(time.time() - chunking_start, 2)}s"
        )

        # ── Summarization + Parallel Embedding ────────────────────────
        summarization_start = time.time()
        raw_result = run_hierarchical_summarization(
            chunks,
            doc_id=doc_id if file_bytes else None,
            vector_store=vector_store if file_bytes else None,
        )

        logger.info(
            f"Summarization completed | "
            f"Time: {round(time.time() - summarization_start, 2)}s"
        )

        # ── Validation ────────────────────────────────────────────────
        validated_output = SummarizationOutput(**raw_result)
        result  = validated_output.model_dump()
        result["doc_id"] = doc_id

        # ── Store document ────────────────────────────────────────────
        if file_bytes:
            store_start = time.time()
            cache_service.store_result(
                pdf_hash=pdf_hash,
                doc_id=doc_id,
                chunks=chunks,
                summary=result,
            )
            logger.info(
                f"Document stored | doc_id={doc_id} | "
                f"Time: {round(time.time() - store_start, 2)}s"
            )

        # ── Cache promotion ───────────────────────────────────────────
        if file_bytes and upload_count is not None and upload_count >= 3:
            cache_service.mark_as_cached(pdf_hash)
            logger.info(
                f"Cache threshold met — result marked for future cache hits | "
                f"hash={pdf_hash} | doc_id={doc_id}"
            )
        else:
            logger.info("Skipping cache promotion (threshold not met)")

        total_time = round(time.time() - pipeline_start, 2)
        logger.info(f"Pipeline completed | Total time: {total_time}s")

        return result

    except Exception:
        logger.exception("Pipeline execution failed")
        raise


if __name__ == "__main__":
    result = run_pipeline("./source/sample.pdf")

    with open("full_summary_output.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)

    print("Pipeline completed successfully.")