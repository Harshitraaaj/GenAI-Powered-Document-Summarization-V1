# import logging
# from pymongo import MongoClient, ASCENDING
# from pymongo.errors import ConnectionFailure, OperationFailure
# from app.core.config import MONGODB_URI, MONGODB_DB_NAME

# logger = logging.getLogger(__name__)


# class MongoDB:
#     """
#     Handles all raw MongoDB operations.
#     Single responsibility — only DB read/write, no business logic.

#     Local MongoDB — no SSL/certifi needed.
#     """

#     _client: MongoClient = None

#     def __init__(self):
#         self._connect()

#     def _connect(self):
#         try:
#             self._client = MongoClient(
#                 MONGODB_URI,
#                 serverSelectionTimeoutMS=30000,
#                 connectTimeoutMS=20000,
#                 socketTimeoutMS=20000,
#             )

#             self._client.admin.command("ping")

#             self.db = self._client[MONGODB_DB_NAME]
#             self.docs = self.db["documents"]

#             self._create_indexes()

#             logger.info(f"MongoDB connected | DB: {MONGODB_DB_NAME}")

#         except ConnectionFailure:
#             logger.exception("MongoDB connection failed")
#             raise RuntimeError("Could not connect to MongoDB. Check MONGODB_URI in .env")

#     def _create_indexes(self):
#         """
#         Create indexes for fast lookups.
#         Called once on startup — safe to call multiple times (idempotent).
#         """
#         try:
#             self.docs.create_index(
#                 [("pdf_hash", ASCENDING)],
#                 unique=True,
#                 sparse=True,
#                 name="pdf_hash_unique"
#             )

#             self.docs.create_index(
#                 [("doc_id", ASCENDING)],
#                 unique=True,
#                 name="doc_id_unique"
#             )

#             logger.info("MongoDB indexes ensured")

#         except OperationFailure:
#             logger.warning("Index creation skipped — may already exist")

#     # -----------------------------
#     # Read Operations
#     # -----------------------------
#     def get_by_hash(self, pdf_hash: str) -> dict | None:
#         try:
#             doc = self.docs.find_one(
#                 {"pdf_hash": pdf_hash},
#                 {"_id": 0}
#             )
#             return doc

#         except Exception:
#             logger.exception(f"get_by_hash failed for hash: {pdf_hash}")
#             return None

#     def get_by_doc_id(self, doc_id: str) -> dict | None:
#         try:
#             doc = self.docs.find_one(
#                 {"doc_id": doc_id},
#                 {"_id": 0}
#             )
#             return doc

#         except Exception:
#             logger.exception(f"get_by_doc_id failed for doc_id: {doc_id}")
#             return None

#     # -----------------------------
#     # Write Operations
#     # -----------------------------
#     def insert_document(self, document: dict) -> bool:
#         try:
#             self.docs.insert_one(document)
#             logger.info(f"Document inserted | doc_id: {document.get('doc_id')}")
#             return True

#         except Exception:
#             logger.exception("insert_document failed")
#             return False

#     def update_document(self, doc_id: str, update_fields: dict) -> bool:
#         try:
#             result = self.docs.update_one(
#                 {"doc_id": doc_id},
#                 {"$set": update_fields}
#             )

#             if result.matched_count == 0:
#                 logger.warning(f"update_document — doc_id not found: {doc_id}")
#                 return False

#             logger.info(f"Document updated | doc_id: {doc_id} | fields: {list(update_fields.keys())}")
#             return True

#         except Exception:
#             logger.exception(f"update_document failed for doc_id: {doc_id}")
#             return False

#     def document_exists(self, doc_id: str) -> bool:
#         try:
#             count = self.docs.count_documents({"doc_id": doc_id}, limit=1)
#             return count > 0

#         except Exception:
#             logger.exception(f"document_exists check failed for doc_id: {doc_id}")
#             return False

#     # -----------------------------
#     # Cleanup
#     # -----------------------------
#     def close(self):
#         if self._client:
#             self._client.close()
#             logger.info("MongoDB connection closed")


import logging
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from app.core.config import MONGODB_URI, MONGODB_DB_NAME

logger = logging.getLogger(__name__)


class MongoDB:
    _client: MongoClient = None

    def __init__(self):
        self._connect()

    def _connect(self):
        try:
            self._client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=20000,
                socketTimeoutMS=20000,
            )

            self._client.admin.command("ping")

            self.db = self._client[MONGODB_DB_NAME]
            self.docs = self.db["documents"]

            # 🔥 NEW COLLECTION
            self.cache_meta = self.db["cache_meta"]

            self._create_indexes()

            logger.info(f"MongoDB connected | DB: {MONGODB_DB_NAME}")

        except ConnectionFailure:
            logger.exception("MongoDB connection failed")
            raise RuntimeError("Could not connect to MongoDB.")

    def _create_indexes(self):
        try:
            self.docs.create_index(
                [("pdf_hash", ASCENDING)],
                unique=True,
                sparse=True,
                name="pdf_hash_unique"
            )

            self.docs.create_index(
                [("doc_id", ASCENDING)],
                unique=True,
                name="doc_id_unique"
            )

            # 🔥 NEW INDEX
            self.cache_meta.create_index(
                [("pdf_hash", ASCENDING)],
                unique=True,
                name="cache_meta_hash_unique"
            )

            logger.info("MongoDB indexes ensured")

        except OperationFailure:
            logger.warning("Index creation skipped")

    # -----------------------------
    # NEW: upload counter
    # -----------------------------
    def increment_upload_count(self, pdf_hash: str) -> int:
        try:
            result = self.cache_meta.find_one_and_update(
                {"pdf_hash": pdf_hash},
                {"$inc": {"count": 1}},
                upsert=True,
                return_document=True
            )
            return result.get("count", 1) if result else 1

        except Exception:
            logger.exception("increment_upload_count failed")
            return 1

    # -----------------------------
    # Read
    # -----------------------------
    def get_by_hash(self, pdf_hash: str):
        try:
            return self.docs.find_one({"pdf_hash": pdf_hash}, {"_id": 0})
        except Exception:
            logger.exception("get_by_hash failed")
            return None

    def get_by_doc_id(self, doc_id: str):
        try:
            return self.docs.find_one({"doc_id": doc_id}, {"_id": 0})
        except Exception:
            logger.exception("get_by_doc_id failed")
            return None

    # -----------------------------
    # Write
    # -----------------------------
    def insert_document(self, document: dict) -> bool:
        try:
            self.docs.insert_one(document)
            return True
        except Exception:
            logger.exception("insert_document failed")
            return False

    def update_document(self, doc_id: str, update_fields: dict) -> bool:
        try:
            result = self.docs.update_one(
                {"doc_id": doc_id},
                {"$set": update_fields}
            )
            return result.matched_count > 0
        except Exception:
            logger.exception("update_document failed")
            return False