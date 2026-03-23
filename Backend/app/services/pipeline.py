# import json
# import logging
# import time
# from pydantic import ValidationError
# from app.services.ingestion import load_document
# from app.services.chunking import chunk_document
# from app.services.summarizer import run_hierarchical_summarization
# from app.models.schema import SummarizationOutput

# logger = logging.getLogger(__name__)


# def run_pipeline(file_path: str) -> dict:
#     """
#     Executes full document summarization pipeline:

#     1. Document ingestion
#     2. Intelligent chunking
#     3. Hierarchical summarization
#     4. Schema validation (Responsible AI layer)
#     """

#     logger.info(f"Pipeline started for file: {file_path}")

#     pipeline_start = time.time()

#     try:
#         #Ingestion
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

#         #Chunking
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

#         # Hierarchial Summarization
#         summarization_start = time.time()

#         raw_result = run_hierarchical_summarization(chunks)

#         logger.info(
#             f"Hierarchical summarization completed | "
#             f"Time: {round(time.time() - summarization_start, 2)}s"
#         )

#         #Schema Validation
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

#         total_time = round(time.time() - pipeline_start, 2)

#         logger.info(
#             f"Pipeline completed successfully | "
#             f"Total execution time: {total_time}s"
#         )

#         return validated_output.model_dump()

#     except Exception:
#         logger.exception("Pipeline execution failed")
#         raise


# #Cli Support ( Optional )
# if __name__ == "__main__":
#     result = run_pipeline("./source/sample.pdf")

#     with open("full_summary_output.json", "w", encoding="utf-8") as f:
#         json.dump(result, f, indent=4, ensure_ascii=False)

#     print("Full hierarchical summarization pipeline completed successfully.")

"Antropic"

# import json
# import logging
# import time
# import uuid
# from pydantic import ValidationError
# from app.services.ingestion import load_document
# from app.services.chunking import chunk_document
# from app.services.summarizer import run_hierarchical_summarization
# from app.models.schema import SummarizationOutput

# # NEW imports
# from app.db.cache import CacheService
# from app.db.vector_store import VectorStore
# from app.utils.hashing import get_pdf_hash

# logger = logging.getLogger(__name__)

# # NEW — initialize once at module level
# cache_service = CacheService()
# vector_store = VectorStore()


# def run_pipeline(file_path: str, file_bytes: bytes = None) -> dict:
#     """
#     Executes full document summarization pipeline:

#     1. Cache check (NEW)
#     2. Document ingestion
#     3. Intelligent chunking
#     4. Hierarchical summarization
#     5. Schema validation (Responsible AI layer)
#     6. Store in MongoDB + Vector Store (NEW)
#     """

#     logger.info(f"Pipeline started for file: {file_path}")
#     pipeline_start = time.time()

#     try:
#         # ── NEW: Cache Check ──────────────────────────────────────────
#         # file_bytes is passed from the API endpoint (the raw uploaded bytes)
#         # CLI mode skips cache (file_bytes will be None)
#         if file_bytes is not None:
#             cached = cache_service.check_cache(file_bytes)
#             if cached:
#                 logger.info("Cache hit — returning stored result, skipping pipeline")
#                 return cached

#         pdf_hash = get_pdf_hash(file_bytes) if file_bytes else None
#         doc_id = str(uuid.uuid4())  # NEW — generate unique doc_id
#         # ─────────────────────────────────────────────────────────────

#         # Ingestion — NO CHANGE
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

#         # Chunking — NO CHANGE
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

#         # Hierarchical Summarization — NO CHANGE
#         summarization_start = time.time()
#         raw_result = run_hierarchical_summarization(chunks)

#         logger.info(
#             f"Hierarchical summarization completed | "
#             f"Time: {round(time.time() - summarization_start, 2)}s"
#         )

#         # Schema Validation — NO CHANGE
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

#         # ── NEW: Store in MongoDB + Vector Store ──────────────────────
#         if file_bytes is not None:  # skip in CLI mode
#             store_start = time.time()

#             # Store summary + chunks + hash in MongoDB (for caching)
#             cache_service.store_result(
#                 pdf_hash=pdf_hash,
#                 doc_id=doc_id,
#                 chunks=chunks,
#                 summary=result
#             )

#             # Store chunk embeddings in MongoDB Vector Search
#             vector_store.store_embeddings(
#                 doc_id=doc_id,
#                 chunks=chunks
#             )

#             logger.info(
#                 f"Stored in MongoDB + Vector Store | "
#                 f"doc_id: {doc_id} | "
#                 f"Time: {round(time.time() - store_start, 2)}s"
#             )

#         # Add doc_id to result so API can return it to user
#         result["doc_id"] = doc_id
#         # ─────────────────────────────────────────────────────────────

#         total_time = round(time.time() - pipeline_start, 2)
#         logger.info(
#             f"Pipeline completed successfully | "
#             f"Total execution time: {total_time}s"
#         )

#         return result

#     except Exception:
#         logger.exception("Pipeline execution failed")
#         raise


# # CLI Support — NO CHANGE (file_bytes=None skips cache automatically)
# if __name__ == "__main__":
#     result = run_pipeline("./source/sample.pdf")

#     with open("full_summary_output.json", "w", encoding="utf-8") as f:
#         json.dump(result, f, indent=4, ensure_ascii=False)

#     print("Full hierarchical summarization pipeline completed successfully.")

"""test success"""

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

# Initialize once at module level
cache_service = CacheService()
vector_store  = VectorStore()


def run_pipeline(file_path: str, file_bytes: bytes = None) -> dict:
    """
    Executes full document summarization pipeline:

    1. Cache check
    2. Document ingestion
    3. Intelligent chunking
    4. Hierarchical summarization  <-- embeddings now run INSIDE here, in
                                       parallel with chunk summarization.
                                       Titan cold start overlaps with Llama
                                       cold start instead of running after.
    5. Schema validation
    6. Store summary in MongoDB cache
    """

    logger.info(f"Pipeline started for file: {file_path}")
    pipeline_start = time.time()

    try:
        # ── Cache Check ───────────────────────────────────────────────
        if file_bytes is not None:
            cached = cache_service.check_cache(file_bytes)
            if cached:
                logger.info("Cache hit — returning stored result, skipping pipeline")
                return cached

        pdf_hash = get_pdf_hash(file_bytes) if file_bytes else None
        doc_id   = str(uuid.uuid4())

        # ── Ingestion ─────────────────────────────────────────────────
        ingestion_start = time.time()
        text = load_document(file_path)

        if not text or not text.strip():
            logger.error("Document contains no extractable text.")
            raise RuntimeError("Document contains no extractable text.")

        logger.info(
            f"Document ingestion completed | "
            f"Characters extracted: {len(text)} | "
            f"Time: {round(time.time() - ingestion_start, 2)}s"
        )

        # ── Chunking ──────────────────────────────────────────────────
        chunking_start = time.time()
        chunks = chunk_document(text)

        if not chunks:
            logger.error("Chunking failed. No chunks were generated.")
            raise RuntimeError("Chunking failed. No chunks were generated.")

        logger.info(
            f"Chunking completed | "
            f"Total chunks: {len(chunks)} | "
            f"Time: {round(time.time() - chunking_start, 2)}s"
        )

        # ── Hierarchical Summarization (+ parallel embedding) ─────────
        # CHANGE: pass doc_id and vector_store so the summarizer fires
        # store_embeddings in a background thread at t=0, running Titan
        # embedding concurrently with Llama chunk summarization.
        #
        # Titan embedding (~13s) finishes inside the Llama chunk
        # summarization window (~30s) — costs zero extra time.
        #
        # In CLI mode (file_bytes=None) we pass None for both so the
        # summarizer skips embedding exactly as before.
        summarization_start = time.time()
        raw_result = run_hierarchical_summarization(
            chunks,
            doc_id=doc_id       if file_bytes is not None else None,
            vector_store=vector_store if file_bytes is not None else None,
        )

        logger.info(
            f"Hierarchical summarization completed | "
            f"Time: {round(time.time() - summarization_start, 2)}s"
        )

        # ── Schema Validation ─────────────────────────────────────────
        validation_start = time.time()
        try:
            validated_output = SummarizationOutput(**raw_result)
        except ValidationError as e:
            logger.exception("Schema validation failed.")
            raise RuntimeError(f"Schema validation failed: {e}")

        logger.info(
            f"Schema validation successful | "
            f"Time: {round(time.time() - validation_start, 2)}s"
        )

        result = validated_output.model_dump()

        # ── Store summary in MongoDB cache ────────────────────────────
        # NOTE: store_embeddings already ran inside summarization above.
        # This only stores the summary + metadata for cache lookup.
        if file_bytes is not None:
            store_start = time.time()

            cache_service.store_result(
                pdf_hash=pdf_hash,
                doc_id=doc_id,
                chunks=chunks,
                summary=result
            )

            # store_embeddings call REMOVED — now handled inside summarizer
            logger.info(
                f"Stored in MongoDB | "
                f"doc_id: {doc_id} | "
                f"Time: {round(time.time() - store_start, 2)}s"
            )

        result["doc_id"] = doc_id

        total_time = round(time.time() - pipeline_start, 2)
        logger.info(
            f"Pipeline completed successfully | "
            f"Total execution time: {total_time}s"
        )

        return result

    except Exception:
        logger.exception("Pipeline execution failed")
        raise


# CLI Support — file_bytes=None skips cache + embedding automatically
if __name__ == "__main__":
    result = run_pipeline("./source/sample.pdf")

    with open("full_summary_output.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)

    print("Full hierarchical summarization pipeline completed successfully.")