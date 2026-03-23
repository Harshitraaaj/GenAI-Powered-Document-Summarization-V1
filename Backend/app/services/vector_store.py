# import logging
# import boto3
# import json
# from pymongo import MongoClient
# import certifi
# from app.core.config import (
#     AWS_REGION,
#     EMBEDDING_MODEL_ID,
#     VECTOR_SEARCH_INDEX_NAME,
#     VECTOR_DIMENSIONS,
#     VECTOR_TOP_K,
#     MONGODB_URI,
#     MONGODB_DB_NAME,
# )

# logger = logging.getLogger(__name__)


# class VectorStore:
#     """
#     Handles embedding generation and vector search.

#     Stores embeddings in a SEPARATE 'chunk_embeddings' collection,
#     linked to documents collection via doc_id.

#     documents collection    → text data, summary, entities
#     chunk_embeddings collection → embeddings only, linked by doc_id
#     """

#     def __init__(self):
#         # Own MongoDB connection — separate from MongoDB class
#         # so vector store is fully independent
#         client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
#         db = client[MONGODB_DB_NAME]
#         self.embeddings_col = db["chunk_embeddings"]   # separate collection

#         self.bedrock = boto3.client(
#             service_name="bedrock-runtime",
#             region_name=AWS_REGION
#         )

#     # -----------------------------
#     # Embedding Generation
#     # -----------------------------
#     def _generate_embedding(self, text: str) -> list[float]:
#         try:
#             body = json.dumps({
#                 "inputText": text[:8000],
#                 "dimensions": VECTOR_DIMENSIONS,
#                 "normalize": True
#             })

#             response = self.bedrock.invoke_model(
#                 modelId=EMBEDDING_MODEL_ID,
#                 body=body,
#                 contentType="application/json",
#                 accept="application/json"
#             )

#             response_body = json.loads(response["body"].read())
#             return response_body["embedding"]

#         except Exception:
#             logger.exception(f"Embedding generation failed for text: {text[:50]}...")
#             return []

#     # -----------------------------
#     # Store Embeddings (Separate Collection)
#     # -----------------------------
#     def store_embeddings(self, doc_id: str, chunks: list[dict]) -> bool:
#         """
#         Generate embeddings and store in chunk_embeddings collection.
#         Each chunk gets its own document — linked to parent via doc_id.

#         chunk_embeddings document:
#         {
#             "doc_id": "abc123",
#             "chunk_id": 1,
#             "content": "text...",    ← kept for display in search results
#             "embedding": [...]
#         }
#         """
#         try:
#             logger.info(f"Generating embeddings for {len(chunks)} chunks | doc_id: {doc_id}")

#             # Delete existing embeddings for this doc (idempotent)
#             self.embeddings_col.delete_many({"doc_id": doc_id})
#             logger.info(f"Cleared existing embeddings for doc_id: {doc_id}")

#             embedding_docs = []

#             for chunk in chunks:
#                 content = chunk.get("content", "")
#                 if not content.strip():
#                     continue

#                 embedding = self._generate_embedding(content)

#                 if not embedding:
#                     logger.warning(f"Skipping chunk {chunk.get('chunk_id')} — embedding failed")
#                     continue

#                 embedding_docs.append({
#                     "doc_id": doc_id,
#                     "chunk_id": chunk.get("chunk_id"),
#                     "content": content,         # text stored for search result display
#                     "embedding": embedding
#                 })

#             if embedding_docs:
#                 self.embeddings_col.insert_many(embedding_docs)
#                 logger.info(
#                     f"Embeddings stored | doc_id: {doc_id} | "
#                     f"chunks: {len(embedding_docs)}"
#                 )

#             return True

#         except Exception:
#             logger.exception(f"store_embeddings failed for doc_id: {doc_id}")
#             return False

#     # -----------------------------
#     # Vector Search
#     # -----------------------------
#     def search(self, doc_id: str, query: str, top_k: int = VECTOR_TOP_K) -> list[dict]:
#         """
#         Semantic search over chunk_embeddings collection,
#         filtered to specific doc_id.

#         Requires Atlas Vector Search index on chunk_embeddings collection:
#         - path: 'embedding'
#         - dimensions: 1024
#         - filter path: 'doc_id'
#         """
#         try:
#             logger.info(f"Vector search | doc_id: {doc_id} | query: {query[:80]}")

#             query_embedding = self._generate_embedding(query)

#             if not query_embedding:
#                 logger.error("Query embedding generation failed")
#                 return []

#             pipeline = [
#                 {
#                     "$vectorSearch": {
#                         "index": VECTOR_SEARCH_INDEX_NAME,
#                         "path": "embedding",            # direct field now
#                         "queryVector": query_embedding,
#                         "numCandidates": top_k * 10,
#                         "limit": top_k,
#                         "filter": {"doc_id": doc_id}
#                     }
#                 },
#                 {
#                     "$project": {
#                         "_id": 0,
#                         "doc_id": 1,
#                         "chunk_id": 1,
#                         "content": 1,
#                         "score": {"$meta": "vectorSearchScore"}
#                     }
#                 }
#             ]

#             results = list(self.embeddings_col.aggregate(pipeline))
#             logger.info(f"Vector search returned {len(results)} results")

#             return results

#         except Exception:
#             logger.exception(f"Vector search failed for doc_id: {doc_id}")
#             return []

#     # -----------------------------
#     # Fact Verification Search
#     # -----------------------------
#     def find_supporting_chunks(
#         self,
#         doc_id: str,
#         claim: str,
#         top_k: int = 3
#     ) -> list[dict]:
#         """
#         Find chunks most semantically similar to a claim.
#         Used by fact_verifier.py.

#         Returns flat list of chunk dicts with content — no nesting.
#         """
#         results = self.search(doc_id, claim, top_k=top_k)

#         # Results are already flat — chunk_id, content, score
#         # fact_verifier can use content directly
#         return results


import os
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
)

logger = logging.getLogger(__name__)

_CPU_CORES        = os.cpu_count() or 4
_EMBED_WORKERS    = min(_CPU_CORES * 4, 50)
_POOL_CONNECTIONS = _EMBED_WORKERS + 10


class VectorStore:
    """
    Handles embedding generation and vector search.
    Embeddings stored in chunk_embeddings collection, linked via doc_id.
    All embeddings generated concurrently via ThreadPoolExecutor.
    """

    def __init__(self):
        client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
        db = client[MONGODB_DB_NAME]
        self.embeddings_col = db["chunk_embeddings"]

        self.bedrock = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION,
            config=Config(
                max_pool_connections=_POOL_CONNECTIONS,
                retries={"max_attempts": 3, "mode": "adaptive"}
            )
        )

        logger.info(
            f"VectorStore initialized | "
            f"embed_workers: {_EMBED_WORKERS} | pool: {_POOL_CONNECTIONS}"
        )

    def _generate_embedding(self, text: str) -> list[float]:
        """Blocking Bedrock call — thread-safe."""
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
            return json.loads(response["body"].read())["embedding"]
        except Exception:
            logger.exception(f"Embedding failed: {text[:50]}")
            return []

    def _generate_embeddings_concurrent(
        self, texts: list[str]
    ) -> list[list[float]]:
        """
        Fire ALL embed requests simultaneously.
        Fresh pool per call — no contention, works from any context.

        Before: 44 × 0.4s sequential = ~17s
        After:  all 44 fire at once   = ~2-3s
        """
        if not texts:
            return []

        results = {}
        with ThreadPoolExecutor(
            max_workers=min(len(texts), _EMBED_WORKERS)
        ) as pool:
            futures = {
                pool.submit(self._generate_embedding, text): idx
                for idx, text in enumerate(texts)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results[idx] = future.result()
                except Exception:
                    logger.exception(f"Embed future failed at index {idx}")
                    results[idx] = []

        return [results[i] for i in range(len(texts))]

    def store_embeddings(self, doc_id: str, chunks: list[dict]) -> bool:
        try:
            logger.info(
                f"Generating embeddings | doc_id: {doc_id} | chunks: {len(chunks)}"
            )

            self.embeddings_col.delete_many({"doc_id": doc_id})
            logger.info(f"Cleared existing embeddings | doc_id: {doc_id}")

            valid_chunks = [
                c for c in chunks if c.get("content", "").strip()
            ]
            if not valid_chunks:
                logger.error(f"No valid chunks | doc_id: {doc_id}")
                return False

            texts = [c["content"].strip() for c in valid_chunks]

            # All embeddings fire simultaneously
            embeddings = self._generate_embeddings_concurrent(texts)

            embedding_docs = []
            for chunk, emb in zip(valid_chunks, embeddings):
                if not emb:
                    logger.warning(
                        f"Skipping chunk {chunk.get('chunk_id')} — embedding failed"
                    )
                    continue
                embedding_docs.append({
                    "doc_id":    doc_id,
                    "chunk_id":  chunk.get("chunk_id"),
                    "content":   chunk["content"].strip(),
                    "embedding": emb,
                })

            if not embedding_docs:
                logger.error(f"No embeddings to store | doc_id: {doc_id}")
                return False

            self.embeddings_col.insert_many(embedding_docs)
            logger.info(
                f"Stored {len(embedding_docs)} vectors in Atlas | doc_id: {doc_id}"
            )
            return True

        except Exception:
            logger.exception(f"store_embeddings failed | doc_id: {doc_id}")
            return False

    def search(
        self, doc_id: str, query: str, top_k: int = VECTOR_TOP_K
    ) -> list[dict]:
        try:
            logger.info(f"Vector search | doc_id: {doc_id} | query: {query[:80]}")

            query_embedding = self._generate_embedding(query)
            if not query_embedding:
                logger.error("Query embedding generation failed")
                return []

            pipeline = [
                {
                    "$vectorSearch": {
                        "index":         VECTOR_SEARCH_INDEX_NAME,
                        "path":          "embedding",
                        "queryVector":   query_embedding,
                        "numCandidates": top_k * 10,
                        "limit":         top_k,
                        "filter":        {"doc_id": doc_id}
                    }
                },
                {
                    "$project": {
                        "_id":      0,
                        "doc_id":   1,
                        "chunk_id": 1,
                        "content":  1,
                        "score":    {"$meta": "vectorSearchScore"}
                    }
                }
            ]

            results = list(self.embeddings_col.aggregate(pipeline))
            logger.info(f"Vector search returned {len(results)} results")
            return results

        except Exception:
            logger.exception(f"Vector search failed | doc_id: {doc_id}")
            return []

    def find_supporting_chunks(
        self, doc_id: str, claim: str, top_k: int = 3
    ) -> list[dict]:
        return self.search(doc_id, claim, top_k=top_k)