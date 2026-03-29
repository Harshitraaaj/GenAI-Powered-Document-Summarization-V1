
import time
import threading
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config

import boto3
from pymongo import MongoClient
import certifi

from app.core.config import settings

logger = logging.getLogger(__name__)

_thread_lock = threading.Lock()
_active_threads = 0


class VectorStore:

    def __init__(self):
        client = MongoClient(settings.MONGODB_URI, tlsCAFile=certifi.where())
        db = client[settings.MONGODB_DB_NAME]
        self.embeddings_col = db[settings.EMBEDDINGS_COLLECTION_NAME]

        self.bedrock = boto3.client(
            service_name="bedrock-runtime",
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            config=Config(
                max_pool_connections=settings.EMBED_POOL_CONNECTIONS,
                retries={
                    "max_attempts": settings.MODEL_RETRIES,
                    "mode": settings.BEDROCK_RETRY_MODE,
                },
            ),
        )

        self._warmup_bedrock_connection()
        logger.info("VectorStore initialized")

    def _warmup_bedrock_connection(self):
        try:
            body = json.dumps({
                "inputText": settings.BEDROCK_WARMUP_TEXT,
                "dimensions": settings.VECTOR_DIMENSIONS,
                "normalize": True,
            })
            self.bedrock.invoke_model(
                modelId=settings.EMBEDDING_MODEL_ID,
                body=body,
                contentType="application/json",
                accept="application/json",
            )
        except Exception as e:
            logger.warning("Bedrock warmup failed: %s", str(e)[:settings.LOG_ERROR_TRUNCATE_LENGTH])

    def _generate_embedding(self, text: str, idx: int = -1) -> list[float]:
        global _active_threads

        with _thread_lock:
            _active_threads += 1

        try:
            body = json.dumps({
                "inputText": text[:settings.EMBED_INPUT_MAX_CHARS],
                "dimensions": settings.VECTOR_DIMENSIONS,
                "normalize": True,
            })

            response = self.bedrock.invoke_model(
                modelId=settings.EMBEDDING_MODEL_ID,
                body=body,
                contentType="application/json",
                accept="application/json",
            )

            return json.loads(response["body"].read())["embedding"]

        except Exception as e:
            logger.exception(
                "Embedding failed | idx=%d | error=%s",
                idx,
                str(e)[:settings.LOG_ERROR_TRUNCATE_LENGTH],
            )
            return []

        finally:
            with _thread_lock:
                _active_threads -= 1

    def _generate_embeddings_concurrent(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        total = len(texts)
        logger.info("Embedding generation started | chunks=%d", total)

        results: dict[int, list[float]] = {}
        start = time.time()

        with ThreadPoolExecutor(max_workers=settings.EMBED_WORKERS) as pool:
            futures = {
                pool.submit(self._generate_embedding, text, idx): idx
                for idx, text in enumerate(texts)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results[idx] = future.result()
                except Exception:
                    results[idx] = []

        elapsed = round(time.time() - start, 3)
        avg_time = round(elapsed / total, 3)
        logger.info(
            "Embedding generation completed | chunks=%d | elapsed=%.3fs | avg=%.3fs",
            total, elapsed, avg_time,
        )

        return [results[i] for i in range(total)]

    def store_embeddings(self, doc_id: str, chunks: list[dict]) -> bool:
        try:
            logger.info("Storing embeddings | doc_id=%s | chunks=%d", doc_id, len(chunks))

            self.embeddings_col.delete_many({"doc_id": doc_id})

            valid_chunks = [c for c in chunks if c.get("content", "").strip()]
            if not valid_chunks:
                return False

            texts = [c["content"].strip() for c in valid_chunks]
            embeddings = self._generate_embeddings_concurrent(texts)

            docs = [
                {
                    "doc_id": doc_id,
                    "chunk_id": chunk.get("chunk_id"),
                    "content": chunk["content"].strip(),
                    "embedding": emb,
                }
                for chunk, emb in zip(valid_chunks, embeddings)
                if emb
            ]

            if not docs:
                return False

            self.embeddings_col.insert_many(docs)
            logger.info("Embeddings stored | doc_id=%s | count=%d", doc_id, len(docs))
            return True

        except Exception:
            logger.exception("store_embeddings failed | doc_id=%s", doc_id)
            return False

    def search(self, doc_id: str, query: str, top_k: int = None) -> list[dict]:
        top_k = top_k or settings.VECTOR_TOP_K

        try:
            logger.info("Vector search | doc_id=%s | query=%s", doc_id, query[:50])

            query_embedding = self._generate_embedding(query)
            if not query_embedding:
                return []

            pipeline = [
                {
                    "$vectorSearch": {
                        "index": settings.VECTOR_SEARCH_INDEX_NAME,
                        "path": "embedding",
                        "queryVector": query_embedding,
                        "numCandidates": top_k * settings.VECTOR_CANDIDATES_MULTIPLIER,
                        "limit": top_k,
                        "filter": {"doc_id": doc_id},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "doc_id": 1,
                        "chunk_id": 1,
                        "content": 1,
                        "score": {"$meta": "vectorSearchScore"},
                    }
                },
            ]

            return list(self.embeddings_col.aggregate(pipeline))

        except Exception:
            logger.exception("Vector search failed | doc_id=%s", doc_id)
            return []

    def find_supporting_chunks(self, doc_id: str, claim: str, top_k: int = None) -> list[dict]:
        top_k = top_k or settings.FACT_VERIFIER_TOP_K
        return self.search(doc_id, claim, top_k=top_k)