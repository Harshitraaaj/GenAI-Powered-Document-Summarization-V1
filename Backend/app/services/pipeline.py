# import json
# import logging
# import time
# import uuid
# from pydantic import ValidationError
# from app.services.ingestion import load_document
# from app.services.chunking import chunk_document
# from app.services.summarizer import run_hierarchical_summarization
# from app.models.schema import SummarizationOutput

# from app.db.cache import CacheService
# from app.db.vector_store import VectorStore
# from app.utils.hashing import get_pdf_hash

# logger = logging.getLogger(__name__)

# # Initialize once at module level
# cache_service = CacheService()
# vector_store  = VectorStore()


# def run_pipeline(file_path: str, file_bytes: bytes = None) -> dict:
#     """
#     Executes full document summarization pipeline:

#     1. Cache check
#     2. Document ingestion
#     3. Intelligent chunking
#     4. Hierarchical summarization  <-- embeddings now run INSIDE here, in
#                                        parallel with chunk summarization.
#                                        Titan cold start overlaps with Llama
#                                        cold start instead of running after.
#     5. Schema validation
#     6. Store summary in MongoDB cache
#     """

#     logger.info(f"Pipeline started for file: {file_path}")
#     pipeline_start = time.time()

#     try:
#         # ── Cache Check ───────────────────────────────────────────────
#         if file_bytes is not None:
#             cached = cache_service.check_cache(file_bytes)
#             if cached:
#                 logger.info("Cache hit — returning stored result, skipping pipeline")
#                 return cached

#         pdf_hash = get_pdf_hash(file_bytes) if file_bytes else None
#         doc_id   = str(uuid.uuid4())

#         # ── Ingestion ─────────────────────────────────────────────────
#         ingestion_start = time.time()
#         text = load_document(file_path)

#         if not text or not text.strip():
#             logger.error("Document contains no extractable text.")
#             raise RuntimeError("Document contains no extractable text.")

#         logger.info(
#             f"Document ingestion completed | "
#             f"Characters extracted: {len(text)} | "
#             f"Time: {round(time.time() - ingestion_start, 2)}s"
#         )

#         # ── Chunking ──────────────────────────────────────────────────
#         chunking_start = time.time()
#         chunks = chunk_document(text)

#         if not chunks:
#             logger.error("Chunking failed. No chunks were generated.")
#             raise RuntimeError("Chunking failed. No chunks were generated.")

#         logger.info(
#             f"Chunking completed | "
#             f"Total chunks: {len(chunks)} | "
#             f"Time: {round(time.time() - chunking_start, 2)}s"
#         )

#         # ── Hierarchical Summarization (+ parallel embedding) ─────────
#         # CHANGE: pass doc_id and vector_store so the summarizer fires
#         # store_embeddings in a background thread at t=0, running Titan
#         # embedding concurrently with Llama chunk summarization.
#         #
#         # Titan embedding (~13s) finishes inside the Llama chunk
#         # summarization window (~30s) — costs zero extra time.
#         #
#         # In CLI mode (file_bytes=None) we pass None for both so the
#         # summarizer skips embedding exactly as before.
#         summarization_start = time.time()
#         raw_result = run_hierarchical_summarization(
#             chunks,
#             doc_id=doc_id       if file_bytes is not None else None,
#             vector_store=vector_store if file_bytes is not None else None,
#         )

#         logger.info(
#             f"Hierarchical summarization completed | "
#             f"Time: {round(time.time() - summarization_start, 2)}s"
#         )

#         # ── Schema Validation ─────────────────────────────────────────
#         validation_start = time.time()
#         try:
#             validated_output = SummarizationOutput(**raw_result)
#         except ValidationError as e:
#             logger.exception("Schema validation failed.")
#             raise RuntimeError(f"Schema validation failed: {e}")

#         logger.info(
#             f"Schema validation successful | "
#             f"Time: {round(time.time() - validation_start, 2)}s"
#         )

#         result = validated_output.model_dump()

#         # ── Store summary in MongoDB cache ────────────────────────────
#         # NOTE: store_embeddings already ran inside summarization above.
#         # This only stores the summary + metadata for cache lookup.
#         if file_bytes is not None:
#             store_start = time.time()

#             cache_service.store_result(
#                 pdf_hash=pdf_hash,
#                 doc_id=doc_id,
#                 chunks=chunks,
#                 summary=result
#             )

#             # store_embeddings call REMOVED — now handled inside summarizer
#             logger.info(
#                 f"Stored in MongoDB | "
#                 f"doc_id: {doc_id} | "
#                 f"Time: {round(time.time() - store_start, 2)}s"
#             )

#         result["doc_id"] = doc_id

#         total_time = round(time.time() - pipeline_start, 2)
#         logger.info(
#             f"Pipeline completed successfully | "
#             f"Total execution time: {total_time}s"
#         )

#         return result

#     except Exception:
#         logger.exception("Pipeline execution failed")
#         raise


# # CLI Support — file_bytes=None skips cache + embedding automatically
# if __name__ == "__main__":
#     result = run_pipeline("./source/sample.pdf")

#     with open("full_summary_output.json", "w", encoding="utf-8") as f:
#         json.dump(result, f, indent=4, ensure_ascii=False)

#     print("Full hierarchical summarization pipeline completed successfully.")


import json
import logging
import time
import uuid
from pydantic import ValidationError

from app.services.ingestion import load_document
from app.services.chunking import chunk_document
from app.services.summarizer import run_hierarchical_summarization
from app.models.schema import SummarizationOutput

from app.db.cache import CacheService
from app.db.vector_store import VectorStore
from app.utils.hashing import get_pdf_hash

logger = logging.getLogger(__name__)

cache_service = CacheService()
vector_store  = VectorStore()


def run_pipeline(file_path: str, file_bytes: bytes = None) -> dict:

    logger.info(f"Pipeline started for file: {file_path}")
    pipeline_start = time.time()

    try:
        # ── HASH + UPLOAD COUNT (🔥 FIX STARTS HERE) ───────────────────
        pdf_hash = get_pdf_hash(file_bytes) if file_bytes else None

        upload_count = None
        if file_bytes:
            upload_count = cache_service.db.increment_upload_count(pdf_hash)
            logger.info(f"Upload count | hash={pdf_hash} | count={upload_count}")

        # ── CONDITIONAL CACHE CHECK (🔥 FIX) ───────────────────────────
        if file_bytes and upload_count is not None and upload_count >= 3:
            cached = cache_service.check_cache(file_bytes)
            if cached:
                logger.info("Cache hit — returning stored result, skipping pipeline")
                return cached
        else:
            logger.info("Skipping cache check (threshold not met)")

        # ── CONTINUE PIPELINE ──────────────────────────────────────────
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

        # ── Summarization + Embedding ─────────────────────────────────
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
        result = validated_output.model_dump()

        # ── STORE CACHE (🔥 FIX) ──────────────────────────────────────
        if file_bytes and upload_count is not None:
            if upload_count >= 3:
                store_start = time.time()

                cache_service.store_result(
                    pdf_hash=pdf_hash,
                    doc_id=doc_id,
                    chunks=chunks,
                    summary=result
                )

                logger.info(
                    f"Cache stored | doc_id={doc_id} | "
                    f"Time: {round(time.time() - store_start, 2)}s"
                )
            else:
                logger.info("Skipping cache storage (threshold not met)")

        result["doc_id"] = doc_id

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