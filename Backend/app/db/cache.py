# import logging
# from app.db.mongodb import MongoDB
# from app.utils.hashing import get_pdf_hash

# logger = logging.getLogger(__name__)


# class CacheService:
#     """
#     Handles PDF-level caching using MongoDB.

#     Cache key = MD5 hash of PDF bytes
#     """

#     def __init__(self):
#         self.db = MongoDB()


#     def check_cache(self, file_bytes: bytes) -> dict | None:
#         """
#         Return cached result if PDF was already processed, else None.
#         """
#         try:
#             pdf_hash = get_pdf_hash(file_bytes)

#             logger.info(f"Cache check | hash: {pdf_hash}")

#             existing = self.db.get_by_hash(pdf_hash)

#             if existing:
#                 logger.info(f"Cache HIT | hash: {pdf_hash} | doc_id: {existing.get('doc_id')}")
#                 return self._format_cached_response(existing)

#             logger.info(f"Cache MISS | hash: {pdf_hash}")
#             return None

#         except Exception:
#             logger.exception("Cache check failed — treating as cache miss")
#             return None

#     def store_result(
#         self,
#         pdf_hash: str,
#         doc_id: str,
#         chunks: list[dict],
#         summary: dict,
#         filename: str = None
#     ) -> bool:
#         """
#         Store processed result in MongoDB (chunks stored without embeddings).
#         """
#         try:
#             clean_chunks = [
#                 {k: v for k, v in chunk.items() if k != "embedding"}
#                 for chunk in chunks
#             ]

#             document = {
#                 "pdf_hash": pdf_hash,
#                 "doc_id": doc_id,
#                 "chunks": clean_chunks,     
#                 "summary": summary,
#                 "filename": filename,
#                 "entities": None,
#                 "graph_built": False,
#                 "facts_verified": False,
#             }

#             success = self.db.insert_document(document)

#             if success:
#                 logger.info(f"Cache stored | doc_id: {doc_id} | hash: {pdf_hash}")
#             else:
#                 logger.error(f"Cache store failed | doc_id: {doc_id}")

#             return success

#         except Exception:
#             logger.exception("store_result failed")
#             return False

#     def _format_cached_response(self, document: dict) -> dict:
#         """
#         Format the MongoDB document into a clean API response.
#         Strips internal fields and marks the response as cached.
#         """
#         return {
#             "doc_id": document.get("doc_id"),
#             "cached": True,
#             "filename": document.get("filename"),
#             "summary": document.get("summary", {}),
#         }

#     def is_cached(self, file_bytes: bytes) -> bool:
#         """
#         Quick boolean check — useful for logging/metrics
#         without fetching the full document.
#         """
#         try:
#             pdf_hash = get_pdf_hash(file_bytes)
#             return self.db.get_by_hash(pdf_hash) is not None

#         except Exception:
#             return False

import logging
from app.db.mongodb import MongoDB
from app.utils.hashing import get_pdf_hash

logger = logging.getLogger(__name__)


class CacheService:
    def __init__(self):
        self.db = MongoDB()

    def check_cache(self, file_bytes: bytes):
        try:
            pdf_hash = get_pdf_hash(file_bytes)
            existing = self.db.get_by_hash(pdf_hash)

            if existing:
                logger.info(f"Cache HIT | {pdf_hash}")
                return self._format(existing)

            logger.info(f"Cache MISS | {pdf_hash}")
            return None

        except Exception:
            logger.exception("Cache check failed")
            return None

    # 🔥 NEW
    def increment_upload_count(self, file_bytes: bytes) -> int:
        pdf_hash = get_pdf_hash(file_bytes)
        return self.db.increment_upload_count(pdf_hash)

    def store_result(self, pdf_hash, doc_id, chunks, summary, filename=None):
        try:
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
            }

            return self.db.insert_document(document)

        except Exception:
            logger.exception("store_result failed")
            return False

    def _format(self, doc):
        return {
            "doc_id": doc.get("doc_id"),
            "cached": True,
            "filename": doc.get("filename"),
            "summary": doc.get("summary", {}),
        }