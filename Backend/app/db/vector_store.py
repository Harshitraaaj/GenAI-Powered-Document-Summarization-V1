import os
import time
import threading
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config

import boto3
from pymongo import MongoClient
import certifi

from app.core.config import (
    AWS_REGION,
    EMBEDDING_MODEL_ID,
    VECTOR_SEARCH_INDEX_NAME,
    VECTOR_DIMENSIONS,
    VECTOR_TOP_K,
    MONGODB_URI,
    MONGODB_DB_NAME,
    EMBED_WORKERS,
    EMBED_POOL_CONNECTIONS,
)

logger = logging.getLogger(__name__)

_active_threads = 0
_thread_lock = threading.Lock()


class VectorStore:

    def __init__(self):
        client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
        db = client[MONGODB_DB_NAME]
        self.embeddings_col = db["chunk_embeddings"]

        self.bedrock = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION,
            config=Config(
                max_pool_connections=EMBED_POOL_CONNECTIONS,
                retries={"max_attempts": 3, "mode": "adaptive"}
            )
        )

        self._warmup_bedrock_connection()
        logger.info("VectorStore initialized")

    def _warmup_bedrock_connection(self):
        try:
            body = json.dumps({
                "inputText": "warmup",
                "dimensions": VECTOR_DIMENSIONS,
                "normalize": True
            })
            self.bedrock.invoke_model(
                modelId=EMBEDDING_MODEL_ID,
                body=body,
                contentType="application/json",
                accept="application/json"
            )
        except Exception as e:
            logger.warning("Warmup failed: %s", str(e)[:100])

    def _generate_embedding(self, text: str, idx: int = -1) -> list[float]:
        global _active_threads

        with _thread_lock:
            _active_threads += 1

        try:
            body = json.dumps({
                "inputText": text[:8000],
                "dimensions": VECTOR_DIMENSIONS,
                "normalize": True
            })

            response = self.bedrock.invoke_model(
                modelId=EMBEDDING_MODEL_ID,
                body=body,
                contentType="application/json",
                accept="application/json"
            )

            raw = response["body"].read()
            return json.loads(raw)["embedding"]

        except Exception as e:
            logger.exception("Embedding failed | idx=%d | error=%s", idx, str(e)[:100])
            return []

        finally:
            with _thread_lock:
                _active_threads -= 1

    def _generate_embeddings_concurrent(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        total = len(texts)
        logger.info("Embedding generation started | chunks=%d", total)

        results = {}

        with ThreadPoolExecutor(max_workers=EMBED_WORKERS) as pool:
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

        avg_time = round((time.time()) / total, 3) if total else 0
        logger.info("Embedding generation completed")

        return [results[i] for i in range(len(texts))]

    def store_embeddings(self, doc_id: str, chunks: list[dict]) -> bool:
        try:
            logger.info("Storing embeddings | doc_id=%s | chunks=%d", doc_id, len(chunks))

            self.embeddings_col.delete_many({"doc_id": doc_id})

            valid_chunks = [c for c in chunks if c.get("content", "").strip()]

            if not valid_chunks:
                return False

            texts = [c["content"].strip() for c in valid_chunks]
            embeddings = self._generate_embeddings_concurrent(texts)

            docs = []
            for chunk, emb in zip(valid_chunks, embeddings):
                if not emb:
                    continue
                docs.append({
                    "doc_id": doc_id,
                    "chunk_id": chunk.get("chunk_id"),
                    "content": chunk["content"].strip(),
                    "embedding": emb,
                })

            if not docs:
                return False

            self.embeddings_col.insert_many(docs)

            logger.info("Embeddings stored successfully | doc_id=%s", doc_id)
            return True

        except Exception:
            logger.exception("store_embeddings failed | doc_id=%s", doc_id)
            return False

    def search(self, doc_id: str, query: str, top_k: int = VECTOR_TOP_K) -> list[dict]:
        try:
            logger.info("Search | doc_id=%s | query=%s", doc_id, query[:50])

            query_embedding = self._generate_embedding(query)

            if not query_embedding:
                return []

            pipeline = [
                {
                    "$vectorSearch": {
                        "index": VECTOR_SEARCH_INDEX_NAME,
                        "path": "embedding",
                        "queryVector": query_embedding,
                        "numCandidates": top_k * 10,
                        "limit": top_k,
                        "filter": {"doc_id": doc_id}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "doc_id": 1,
                        "chunk_id": 1,
                        "content": 1,
                        "score": {"$meta": "vectorSearchScore"}
                    }
                }
            ]

            return list(self.embeddings_col.aggregate(pipeline))

        except Exception:
            logger.exception("Search failed | doc_id=%s", doc_id)
            return []

    def find_supporting_chunks(self, doc_id: str, claim: str, top_k: int = 3) -> list[dict]:
        return self.search(doc_id, claim, top_k=top_k)