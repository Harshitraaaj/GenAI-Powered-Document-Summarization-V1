import logging
from app.db.mongodb import MongoDB
from app.utils.hashing import get_pdf_hash

logger = logging.getLogger(__name__)


class CacheService:
    """
    Handles PDF-level caching using MongoDB.

    Cache key  = MD5 hash of raw PDF bytes
    Cache hit  = same PDF was already processed, return stored result
    Cache miss = new PDF, run full pipeline then store result

    documents collection stores text-only chunks (no embeddings).
    Embeddings are stored separately in chunk_embeddings collection
    by VectorStore.store_embeddings() — linked via doc_id.
    """

    def __init__(self):
        self.db = MongoDB()

    # -----------------------------
    # Cache Check
    # -----------------------------
    def check_cache(self, file_bytes: bytes) -> dict | None:
        """
        Check if this exact PDF was already processed.

        Returns the cached result dict if found, None if not.

        Usage in pipeline.py:
            cached = cache_service.check_cache(file_bytes)
            if cached:
                return cached   # skip LLM entirely
        """
        try:
            pdf_hash = get_pdf_hash(file_bytes)

            logger.info(f"Cache check | hash: {pdf_hash}")

            existing = self.db.get_by_hash(pdf_hash)

            if existing:
                logger.info(f"Cache HIT | hash: {pdf_hash} | doc_id: {existing.get('doc_id')}")
                return self._format_cached_response(existing)

            logger.info(f"Cache MISS | hash: {pdf_hash}")
            return None

        except Exception:
            logger.exception("Cache check failed — treating as cache miss")
            return None

    # -----------------------------
    # Cache Store
    # -----------------------------
    def store_result(
        self,
        pdf_hash: str,
        doc_id: str,
        chunks: list[dict],
        summary: dict,
        filename: str = None
    ) -> bool:
        """
        Store the pipeline result in MongoDB after processing.

        FIX: Chunks are stored WITHOUT embeddings here.
        Embeddings are stored separately in chunk_embeddings collection
        by VectorStore.store_embeddings() — this keeps documents collection
        lean and separates concerns cleanly.

        documents collection:
        {
            doc_id, pdf_hash, summary,
            chunks: [{ chunk_id, content }]   ← text only, no embeddings
        }

        chunk_embeddings collection (handled by VectorStore):
        {
            doc_id, chunk_id, content, embedding: [...]
        }
        """
        try:
            # Strip embeddings from chunks before storing in documents collection
            # Embeddings go to chunk_embeddings collection via VectorStore
            clean_chunks = [
                {k: v for k, v in chunk.items() if k != "embedding"}
                for chunk in chunks
            ]

            document = {
                "pdf_hash": pdf_hash,
                "doc_id": doc_id,
                "chunks": clean_chunks,     # text only — no embeddings
                "summary": summary,
                "filename": filename,
                # downstream fields — populated later by other endpoints
                "entities": None,
                "graph_built": False,
                "facts_verified": False,
            }

            success = self.db.insert_document(document)

            if success:
                logger.info(f"Cache stored | doc_id: {doc_id} | hash: {pdf_hash}")
            else:
                logger.error(f"Cache store failed | doc_id: {doc_id}")

            return success

        except Exception:
            logger.exception("store_result failed")
            return False

    # -----------------------------
    # Helpers
    # -----------------------------
    def _format_cached_response(self, document: dict) -> dict:
        """
        Format the MongoDB document into a clean API response.
        Strips internal fields and marks the response as cached.
        """
        return {
            "doc_id": document.get("doc_id"),
            "cached": True,
            "filename": document.get("filename"),
            "summary": document.get("summary", {}),
        }

    def is_cached(self, file_bytes: bytes) -> bool:
        """
        Quick boolean check — useful for logging/metrics
        without fetching the full document.
        """
        try:
            pdf_hash = get_pdf_hash(file_bytes)
            return self.db.get_by_hash(pdf_hash) is not None

        except Exception:
            return False