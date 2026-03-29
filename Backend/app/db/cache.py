import logging
from app.db.mongodb import MongoDB
from app.utils.hashing import get_pdf_hash

logger = logging.getLogger(__name__)


class CacheService:
    def __init__(self):
        self.db = MongoDB()

    def check_cache(self, file_bytes: bytes):
        """
        Return cached result only if explicitly marked as cached.
        """
        try:
            pdf_hash = get_pdf_hash(file_bytes)
            existing = self.db.get_by_hash(pdf_hash)

            if existing and existing.get("cached") is True:
                logger.info(f"Cache HIT | hash={pdf_hash} | doc_id={existing.get('doc_id')}")
                return self._format(existing)

            logger.info(f"Cache MISS | hash={pdf_hash}")
            return None

        except Exception:
            logger.exception("Cache check failed")
            return None

    def mark_as_cached(self, pdf_hash: str):
        """
        Mark document as cache-eligible.
        """
        try:
            self.db.update_by_hash(pdf_hash, {"cached": True})
            logger.info(f"Marked as cached | hash={pdf_hash}")
        except Exception:
            logger.exception("mark_as_cached failed")

    def store_result(self, pdf_hash: str, doc_id: str, chunks: list, summary: dict, filename: str = None):
        """
        Store pipeline result; cached=False by default.
        """
        try:
            # Remove embeddings before storing
            clean_chunks = [
                {k: v for k, v in c.items() if k != "embedding"}
                for c in chunks
            ]

            document = {
                "pdf_hash": pdf_hash,
                "doc_id": doc_id,
                "chunks": clean_chunks,
                "summary": summary,
                "filename": filename,
                "entities": None,
                "graph_built": False,
                "facts_verified": False,
                "cached": False,
            }

            return self.db.upsert_document(document)

        except Exception:
            logger.exception("store_result failed")
            return False

    def _format(self, doc: dict) -> dict:
        """
        Format cached response.
        """
        return {
            "doc_id": doc.get("doc_id"),
            "cached": True,
            "filename": doc.get("filename"),
            "summary": doc.get("summary", {}),
        }