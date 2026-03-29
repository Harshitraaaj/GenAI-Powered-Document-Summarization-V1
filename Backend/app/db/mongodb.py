
import logging
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from app.core.config import settings

logger = logging.getLogger(__name__)


class MongoDB:
    _client: MongoClient = None

    def __init__(self):
        self._connect()

    def _connect(self):
        try:
            self._client = MongoClient(
                settings.MONGODB_URI,
                serverSelectionTimeoutMS=settings.MONGO_SERVER_SELECTION_TIMEOUT_MS,
                connectTimeoutMS=settings.MONGO_CONNECT_TIMEOUT_MS,
                socketTimeoutMS=settings.MONGO_SOCKET_TIMEOUT_MS,
            )

            self._client.admin.command("ping")

            self.db = self._client[settings.MONGODB_DB_NAME]
            self.docs = self.db[settings.MONGO_DOCUMENTS_COLLECTION]
            self.cache_meta = self.db[settings.MONGO_CACHE_META_COLLECTION]

            self._create_indexes()

            logger.info("MongoDB connected | DB: %s", settings.MONGODB_DB_NAME)

        except ConnectionFailure:
            logger.exception("MongoDB connection failed")
            raise RuntimeError("Could not connect to MongoDB.")

    def _create_indexes(self):
        try:
            self.docs.create_index(
                [("pdf_hash", ASCENDING)],
                unique=True,
                sparse=True,
                name=settings.MONGO_IDX_PDF_HASH,
            )
            self.docs.create_index(
                [("doc_id", ASCENDING)],
                unique=True,
                name=settings.MONGO_IDX_DOC_ID,
            )
            self.cache_meta.create_index(
                [("pdf_hash", ASCENDING)],
                unique=True,
                name=settings.MONGO_IDX_CACHE_META_HASH,
            )
            logger.info("MongoDB indexes ensured")

        except OperationFailure:
            logger.warning("Index creation skipped — indexes may already exist")

    # --- Read ---

    def get_by_hash(self, pdf_hash: str):
        try:
            return self.docs.find_one({"pdf_hash": pdf_hash}, {"_id": 0})
        except Exception:
            logger.exception("get_by_hash failed | pdf_hash=%s", pdf_hash)
            return None

    def get_by_doc_id(self, doc_id: str):
        try:
            return self.docs.find_one({"doc_id": doc_id}, {"_id": 0})
        except Exception:
            logger.exception("get_by_doc_id failed | doc_id=%s", doc_id)
            return None

    # --- Write ---

    def insert_document(self, document: dict) -> bool:
        try:
            self.docs.insert_one(document)
            return True
        except Exception:
            logger.exception("insert_document failed")
            return False

    def upsert_document(self, document: dict) -> bool:
        try:
            self.docs.update_one(
                {"pdf_hash": document["pdf_hash"]},
                {"$set": document},
                upsert=True,
            )
            return True
        except Exception:
            logger.exception("upsert_document failed")
            return False

    def update_document(self, doc_id: str, update_fields: dict) -> bool:
        try:
            result = self.docs.update_one(
                {"doc_id": doc_id},
                {"$set": update_fields},
            )
            return result.matched_count > 0
        except Exception:
            logger.exception("update_document failed | doc_id=%s", doc_id)
            return False

    def update_by_hash(self, pdf_hash: str, fields: dict) -> bool:
        try:
            result = self.docs.update_one(
                {"pdf_hash": pdf_hash},
                {"$set": fields},
            )
            return result.matched_count > 0
        except Exception:
            logger.exception("update_by_hash failed | pdf_hash=%s", pdf_hash)
            return False

    def increment_upload_count(self, pdf_hash: str) -> int:
        try:
            result = self.cache_meta.find_one_and_update(
                {"pdf_hash": pdf_hash},
                {"$inc": {"count": 1}},
                upsert=True,
                return_document=True,
            )
            return result.get("count", 1) if result else 1
        except Exception:
            logger.exception("increment_upload_count failed | pdf_hash=%s", pdf_hash)
            return 1
        