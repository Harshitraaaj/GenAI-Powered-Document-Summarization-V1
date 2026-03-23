"""vector mongo"""

"""
MongoDB Atlas Vector Search — replaces local FAISS store.

Each chunk embedding is stored as a document in the `vectors` collection:
{
    doc_id:    "abc123",
    chunk_id:  5,
    content:   "...",
    embedding: [0.12, -0.34, ...] // 1024-dim vector
}

Vector search uses MongoDB Atlas $vectorSearch aggregation stage.
Same public interface as the FAISS version — all callers unchanged.

Prerequisites:
1. Atlas cluster M10+ (free tier doesn't support vector search)
2. Vector search index named "vector_index" on the vectors collection:
   {
     "fields": [
       { "type": "vector", "path": "embedding", "numDimensions": 1024, "similarity": "cosine" },
       { "type": "filter", "path": "doc_id" }
     ]
   }
3. MONGODB_URI must point to Atlas (not localhost)
"""

# import logging
# import json
# import boto3
# from pymongo import MongoClient, ASCENDING
# from pymongo.errors import OperationFailure
# from app.core.config import (
#     AWS_REGION,
#     EMBEDDING_MODEL_ID,
#     VECTOR_DIMENSIONS,
#     VECTOR_TOP_K,
#     MONGODB_URI,
#     MONGODB_DB_NAME,
# )

# logger = logging.getLogger(__name__)

# VECTORS_COLLECTION  = "vectors"
# VECTOR_INDEX_NAME   = "vector_index"


# class VectorStore:
#     """
#     MongoDB Atlas Vector Search store.

#     Replaces FAISS local store — same interface, everything in Atlas.
#     Embeddings are stored alongside summaries in the same Atlas cluster.
#     """

#     def __init__(self):
#         self.bedrock = boto3.client(
#             service_name="bedrock-runtime",
#             region_name=AWS_REGION
#         )
#         self.client = MongoClient(MONGODB_URI)
#         self.db     = self.client[MONGODB_DB_NAME]
#         self.col    = self.db[VECTORS_COLLECTION]

#         # Ensure index on doc_id for fast deletes + existence checks
#         self.col.create_index([("doc_id", ASCENDING)])

#         logger.info(
#             f"VectorStore (Atlas) initialized | "
#             f"db: {MONGODB_DB_NAME} | collection: {VECTORS_COLLECTION}"
#         )

#     # ── Embedding Generation ──────────────────────────────────
#     def _generate_embedding(self, text: str) -> list[float]:
#         """
#         Generate embedding using Amazon Titan Embed v2.
#         Returns 1024-dimensional normalized vector.
#         """
#         try:
#             body = json.dumps({
#                 "inputText":  text[:8000],
#                 "dimensions": VECTOR_DIMENSIONS,
#                 "normalize":  True
#             })
#             response = self.bedrock.invoke_model(
#                 modelId=EMBEDDING_MODEL_ID,
#                 body=body,
#                 contentType="application/json",
#                 accept="application/json"
#             )
#             return json.loads(response["body"].read())["embedding"]

#         except Exception:
#             logger.exception(f"Embedding generation failed: {text[:50]}")
#             return []

#     # ── Store Embeddings ──────────────────────────────────────
#     def store_embeddings(self, doc_id: str, chunks: list[dict]) -> bool:
#         """
#         Generate embeddings for all chunks and store in Atlas.

#         Deletes existing vectors for this doc_id first (idempotent).
#         Each chunk becomes one MongoDB document in the `vectors` collection.
#         """
#         try:
#             logger.info(
#                 f"Generating embeddings | doc_id: {doc_id} | chunks: {len(chunks)}"
#             )

#             # Delete existing vectors for this document (idempotent rebuild)
#             deleted = self.col.delete_many({"doc_id": doc_id})
#             if deleted.deleted_count:
#                 logger.info(
#                     f"Cleared {deleted.deleted_count} existing vectors | doc_id: {doc_id}"
#                 )

#             docs_to_insert = []

#             for chunk in chunks:
#                 content = chunk.get("content", "").strip()
#                 if not content:
#                     continue

#                 embedding = self._generate_embedding(content)
#                 if not embedding:
#                     logger.warning(
#                         f"Skipping chunk {chunk.get('chunk_id')} — embedding failed"
#                     )
#                     continue

#                 docs_to_insert.append({
#                     "doc_id":   doc_id,
#                     "chunk_id": chunk.get("chunk_id"),
#                     "content":  content,
#                     "embedding": embedding,
#                 })

#             if not docs_to_insert:
#                 logger.error(f"No embeddings generated | doc_id: {doc_id}")
#                 return False

#             result = self.col.insert_many(docs_to_insert)
#             logger.info(
#                 f"Stored {len(result.inserted_ids)} vectors in Atlas | doc_id: {doc_id}"
#             )
#             return True

#         except Exception:
#             logger.exception(f"store_embeddings failed | doc_id: {doc_id}")
#             return False

#     # ── Vector Search ─────────────────────────────────────────
#     def search(
#         self,
#         doc_id: str,
#         query: str,
#         top_k: int = VECTOR_TOP_K
#     ) -> list[dict]:
#         """
#         Semantic search using MongoDB Atlas $vectorSearch.

#         Uses the `vector_index` index with a doc_id filter so we
#         only search within the current document's chunks.

#         Returns:
#             [{ "chunk_id": 5, "content": "...", "score": 0.92 }]
#         """
#         try:
#             logger.info(
#                 f"Vector search | doc_id: {doc_id} | query: {query[:80]}"
#             )

#             query_embedding = self._generate_embedding(query)
#             if not query_embedding:
#                 logger.error("Query embedding generation failed")
#                 return []

#             # Atlas Vector Search aggregation pipeline
#             pipeline = [
#                 {
#                     "$vectorSearch": {
#                         "index":       VECTOR_INDEX_NAME,
#                         "path":        "embedding",
#                         "queryVector": query_embedding,
#                         "numCandidates": top_k * 10,  # oversample for accuracy
#                         "limit":       top_k,
#                         "filter": {
#                             "doc_id": {"$eq": doc_id}
#                         }
#                     }
#                 },
#                 {
#                     "$project": {
#                         "_id":      0,
#                         "chunk_id": 1,
#                         "content":  1,
#                         "score":    {"$meta": "vectorSearchScore"},
#                     }
#                 }
#             ]

#             results = list(self.col.aggregate(pipeline))
#             logger.info(
#                 f"Vector search returned {len(results)} results | doc_id: {doc_id}"
#             )
#             return results

#         except OperationFailure as e:
#             # Likely means vector index doesn't exist yet
#             logger.error(
#                 f"Vector search failed — is 'vector_index' created in Atlas? | "
#                 f"error: {e.details.get('errmsg', str(e))}"
#             )
#             return []

#         except Exception:
#             logger.exception(f"Vector search failed | doc_id: {doc_id}")
#             return []

#     # ── Fact Verification Search ──────────────────────────────
#     def find_supporting_chunks(
#         self,
#         doc_id: str,
#         claim: str,
#         top_k: int = 3
#     ) -> list[dict]:
#         """
#         Find chunks most semantically similar to a claim.
#         Used by fact_verifier.py — same interface as FAISS version.
#         """
#         return self.search(doc_id, claim, top_k=top_k)

#     # ── Index Management ──────────────────────────────────────
#     def index_exists(self, doc_id: str) -> bool:
#         """Check if vectors exist for a document in Atlas."""
#         return self.col.count_documents({"doc_id": doc_id}, limit=1) > 0

#     def delete_index(self, doc_id: str):
#         """Delete all vectors for a document from Atlas."""
#         result = self.col.delete_many({"doc_id": doc_id})
#         logger.info(
#             f"Deleted {result.deleted_count} vectors | doc_id: {doc_id}"
#         )

#     def close(self):
#         if self.client:
#             self.client.close()
#             logger.info("Atlas VectorStore connection closed")

"""final speed"""

"""
vector_store.py — Maximum-speed embedding + Atlas vector search
================================================================
Key optimization:
  All 44 Bedrock embed calls fire SIMULTANEOUSLY via asyncio.gather
  instead of sequentially in a for loop.

  Before: 44 × 0.6s = 26s  (sequential loop)
  After:  max(0.6s)  = ~2s  (all concurrent)

Architecture:
  asyncio.gather  → fires all embed calls at the same time
  ThreadPoolExecutor → runs blocking boto3 calls off the event loop
  Combined via    → loop.run_in_executor()
# """
# import os
# import time
# import threading
# import logging
# import json
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from botocore.config import Config

# import boto3
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

# # Fixed at 5 — Bedrock Titan throttles hard above this.
# # Firing 16+ concurrent requests causes retries/backoff = SLOWER than sequential.
# _EMBED_WORKERS    = 5
# _POOL_CONNECTIONS = 15

# # Track active concurrent threads globally for diagnostics
# _active_threads = 0
# _thread_lock    = threading.Lock()


# class VectorStore:

#     def __init__(self):
#         logger.info("=" * 60)
#         logger.info("VectorStore.__init__ started")
#         logger.info(f"  OS CPU count       : {os.cpu_count()}")
#         logger.info(f"  _EMBED_WORKERS     : {_EMBED_WORKERS}")
#         logger.info(f"  _POOL_CONNECTIONS  : {_POOL_CONNECTIONS}")
#         logger.info(f"  AWS_REGION         : {AWS_REGION}")
#         logger.info(f"  EMBEDDING_MODEL_ID : {EMBEDDING_MODEL_ID}")
#         logger.info(f"  VECTOR_DIMENSIONS  : {VECTOR_DIMENSIONS}")

#         t0 = time.time()
#         client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
#         db = client[MONGODB_DB_NAME]
#         self.embeddings_col = db["chunk_embeddings"]
#         logger.info(f"  MongoDB connected  : {round(time.time()-t0, 3)}s")

#         t1 = time.time()
#         self.bedrock = boto3.client(
#             service_name="bedrock-runtime",
#             region_name=AWS_REGION,
#             config=Config(
#                 max_pool_connections=_POOL_CONNECTIONS,
#                 retries={"max_attempts": 3, "mode": "adaptive"}
#             )
#         )
#         logger.info(f"  Bedrock client init: {round(time.time()-t1, 3)}s")
#         logger.info(f"VectorStore ready | total init: {round(time.time()-t0, 3)}s")
#         logger.info("=" * 60)

#     def _generate_embedding(self, text: str, idx: int = -1) -> list[float]:
#         """
#         Blocking Bedrock call — thread-safe.
#         Logs: thread name, active concurrency, bedrock wait time, parse time.
#         """
#         global _active_threads

#         thread_name = threading.current_thread().name
#         t_start = time.time()

#         with _thread_lock:
#             _active_threads += 1
#             active_now = _active_threads

#         logger.info(
#             f"[EMBED START] idx={idx:>3} | thread={thread_name} | "
#             f"active_concurrent={active_now} | text_len={len(text)}"
#         )

#         try:
#             body = json.dumps({
#                 "inputText": text[:8000],
#                 "dimensions": VECTOR_DIMENSIONS,
#                 "normalize": True
#             })

#             t_request = time.time()
#             response = self.bedrock.invoke_model(
#                 modelId=EMBEDDING_MODEL_ID,
#                 body=body,
#                 contentType="application/json",
#                 accept="application/json"
#             )
#             t_response = time.time()

#             raw = response["body"].read()
#             result = json.loads(raw)["embedding"]
#             t_parse = time.time()

#             logger.info(
#                 f"[EMBED DONE ] idx={idx:>3} | thread={thread_name} | "
#                 f"bedrock_wait={round(t_response - t_request, 3)}s | "
#                 f"parse={round(t_parse - t_response, 3)}s | "
#                 f"total={round(t_parse - t_start, 3)}s | "
#                 f"embedding_dims={len(result)}"
#             )
#             return result

#         except Exception as e:
#             t_fail = time.time()
#             logger.exception(
#                 f"[EMBED FAIL ] idx={idx:>3} | thread={thread_name} | "
#                 f"elapsed={round(t_fail - t_start, 3)}s | "
#                 f"error={type(e).__name__}: {str(e)[:100]}"
#             )
#             return []

#         finally:
#             with _thread_lock:
#                 _active_threads -= 1

#     def _generate_embeddings_concurrent(
#         self, texts: list[str]
#     ) -> list[list[float]]:
#         """
#         Controlled concurrency embedding with full diagnostic logs.
#         Logs: pool spin-up, per-future resolution, wall time, avg per embed.
#         """
#         if not texts:
#             logger.warning("[CONCURRENT] No texts provided — skipping")
#             return []

#         total = len(texts)
#         logger.info("=" * 60)
#         logger.info(f"[CONCURRENT] Starting | chunks={total} | workers={_EMBED_WORKERS}")
#         logger.info(
#             f"[CONCURRENT] Estimated time: {total} / {_EMBED_WORKERS} "
#             f"* ~0.5s = ~{round(total / _EMBED_WORKERS * 0.5, 1)}s"
#         )

#         t_pool_start = time.time()
#         results = {}
#         completed_count = 0
#         failed_count = 0

#         with ThreadPoolExecutor(
#             max_workers=_EMBED_WORKERS,
#             thread_name_prefix="embed_worker"
#         ) as pool:
#             t_pool_ready = time.time()
#             logger.info(
#                 f"[CONCURRENT] ThreadPool ready | "
#                 f"spin_up={round(t_pool_ready - t_pool_start, 3)}s"
#             )

#             # Submit all tasks
#             futures = {}
#             for idx, text in enumerate(texts):
#                 f = pool.submit(self._generate_embedding, text, idx)
#                 futures[f] = idx

#             t_submitted = time.time()
#             logger.info(
#                 f"[CONCURRENT] All {total} tasks submitted | "
#                 f"submit_time={round(t_submitted - t_pool_ready, 3)}s"
#             )

#             # Collect results as they complete
#             for future in as_completed(futures):
#                 idx = futures[future]
#                 t_got = time.time()
#                 try:
#                     results[idx] = future.result()
#                     completed_count += 1
#                     logger.info(
#                         f"[CONCURRENT] Future resolved | idx={idx:>3} | "
#                         f"completed={completed_count}/{total} | "
#                         f"wall_so_far={round(t_got - t_pool_ready, 2)}s"
#                     )
#                 except Exception as e:
#                     failed_count += 1
#                     results[idx] = []
#                     logger.exception(
#                         f"[CONCURRENT] Future exception | idx={idx:>3} | "
#                         f"error={type(e).__name__}: {str(e)[:100]}"
#                     )

#         t_done = time.time()
#         total_wall  = round(t_done - t_pool_start, 3)
#         avg_per_embed = round(total_wall / total, 3) if total else 0

#         logger.info(f"[CONCURRENT] ── Summary ──────────────────────────────")
#         logger.info(f"[CONCURRENT]   Total wall time  : {total_wall}s")
#         logger.info(f"[CONCURRENT]   Avg per embed    : {avg_per_embed}s")
#         logger.info(f"[CONCURRENT]   Completed        : {completed_count}/{total}")
#         logger.info(f"[CONCURRENT]   Failed           : {failed_count}/{total}")

#         # Diagnose what's wrong based on avg time
#         if avg_per_embed > 2.0:
#             logger.warning(
#                 f"[CONCURRENT] ⚠ CRITICAL: avg={avg_per_embed}s >> 1s — "
#                 f"Bedrock is hard throttling. Reduce _EMBED_WORKERS to 3."
#             )
#         elif avg_per_embed > 1.0:
#             logger.warning(
#                 f"[CONCURRENT] ⚠ WARNING: avg={avg_per_embed}s > 1s — "
#                 f"Bedrock is throttling. Reduce _EMBED_WORKERS to 3 or 4."
#             )
#         elif avg_per_embed < 0.6:
#             logger.info(
#                 f"[CONCURRENT] ✓ HEALTHY: avg={avg_per_embed}s < 0.6s — "
#                 f"No throttling detected. Can try increasing _EMBED_WORKERS to 7."
#             )
#         else:
#             logger.info(
#                 f"[CONCURRENT] ✓ OK: avg={avg_per_embed}s — "
#                 f"Within acceptable range."
#             )
#         logger.info("=" * 60)

#         return [results[i] for i in range(len(texts))]

#     def store_embeddings(self, doc_id: str, chunks: list[dict]) -> bool:
#         logger.info("=" * 60)
#         logger.info(f"[STORE] store_embeddings called | doc_id={doc_id} | chunks={len(chunks)}")
#         t_total_start = time.time()

#         try:
#             # ── Step 1: Delete old embeddings ────────────────────────
#             t0 = time.time()
#             deleted = self.embeddings_col.delete_many({"doc_id": doc_id})
#             logger.info(
#                 f"[STORE] Step1 delete_many | "
#                 f"deleted={deleted.deleted_count} | time={round(time.time()-t0, 3)}s"
#             )

#             # ── Step 2: Filter valid chunks ───────────────────────────
#             t0 = time.time()
#             valid_chunks = [c for c in chunks if c.get("content", "").strip()]
#             logger.info(
#                 f"[STORE] Step2 filter chunks | "
#                 f"input={len(chunks)} | valid={len(valid_chunks)} | "
#                 f"dropped={len(chunks) - len(valid_chunks)} | time={round(time.time()-t0, 3)}s"
#             )

#             if not valid_chunks:
#                 logger.error(f"[STORE] No valid chunks — aborting | doc_id={doc_id}")
#                 return False

#             texts = [c["content"].strip() for c in valid_chunks]
#             total_chars = sum(len(t) for t in texts)
#             avg_chars   = round(total_chars / len(texts))
#             logger.info(
#                 f"[STORE] Chunk stats | "
#                 f"total_chars={total_chars} | avg_chars={avg_chars} | "
#                 f"min_chars={min(len(t) for t in texts)} | "
#                 f"max_chars={max(len(t) for t in texts)}"
#             )

#             # ── Step 3: Generate embeddings ───────────────────────────
#             logger.info(f"[STORE] Step3 starting embedding generation...")
#             t0 = time.time()
#             embeddings = self._generate_embeddings_concurrent(texts)
#             embed_time = round(time.time() - t0, 3)
#             logger.info(f"[STORE] Step3 embedding done | time={embed_time}s")

#             # ── Step 4: Build MongoDB docs ────────────────────────────
#             t0 = time.time()
#             embedding_docs = []
#             skipped = 0
#             for chunk, emb in zip(valid_chunks, embeddings):
#                 if not emb:
#                     skipped += 1
#                     logger.warning(
#                         f"[STORE] Skipping chunk {chunk.get('chunk_id')} — empty embedding"
#                     )
#                     continue
#                 embedding_docs.append({
#                     "doc_id":    doc_id,
#                     "chunk_id":  chunk.get("chunk_id"),
#                     "content":   chunk["content"].strip(),
#                     "embedding": emb,
#                 })
#             logger.info(
#                 f"[STORE] Step4 build docs | "
#                 f"docs_ready={len(embedding_docs)} | skipped={skipped} | "
#                 f"time={round(time.time()-t0, 3)}s"
#             )

#             if not embedding_docs:
#                 logger.error(f"[STORE] No embedding docs to insert — aborting")
#                 return False

#             # ── Step 5: Insert to MongoDB Atlas ───────────────────────
#             t0 = time.time()
#             self.embeddings_col.insert_many(embedding_docs)
#             insert_time = round(time.time() - t0, 3)
#             logger.info(
#                 f"[STORE] Step5 insert_many | "
#                 f"docs={len(embedding_docs)} | time={insert_time}s"
#             )

#             total_time = round(time.time() - t_total_start, 3)
#             logger.info(f"[STORE] ── Final Summary ──────────────────────────")
#             logger.info(f"[STORE]   doc_id         : {doc_id}")
#             logger.info(f"[STORE]   chunks stored  : {len(embedding_docs)}")
#             logger.info(f"[STORE]   embed_time     : {embed_time}s")
#             logger.info(f"[STORE]   insert_time    : {insert_time}s")
#             logger.info(f"[STORE]   TOTAL time     : {total_time}s")
#             logger.info("=" * 60)
#             return True

#         except Exception as e:
#             logger.exception(
#                 f"[STORE] FAILED | doc_id={doc_id} | "
#                 f"elapsed={round(time.time()-t_total_start, 3)}s | "
#                 f"error={type(e).__name__}: {str(e)[:200]}"
#             )
#             return False

#     def search(
#         self, doc_id: str, query: str, top_k: int = VECTOR_TOP_K
#     ) -> list[dict]:
#         logger.info(f"[SEARCH] Starting | doc_id={doc_id} | top_k={top_k} | query={query[:80]}")
#         t_total = time.time()

#         try:
#             # Generate query embedding
#             t0 = time.time()
#             query_embedding = self._generate_embedding(query, idx=-1)
#             logger.info(
#                 f"[SEARCH] Query embed done | "
#                 f"time={round(time.time()-t0, 3)}s | dims={len(query_embedding)}"
#             )

#             if not query_embedding:
#                 logger.error("[SEARCH] Query embedding empty — aborting")
#                 return []

#             # Run Atlas vector search
#             t0 = time.time()
#             pipeline = [
#                 {
#                     "$vectorSearch": {
#                         "index":         VECTOR_SEARCH_INDEX_NAME,
#                         "path":          "embedding",
#                         "queryVector":   query_embedding,
#                         "numCandidates": top_k * 10,
#                         "limit":         top_k,
#                         "filter":        {"doc_id": doc_id}
#                     }
#                 },
#                 {
#                     "$project": {
#                         "_id":      0,
#                         "doc_id":   1,
#                         "chunk_id": 1,
#                         "content":  1,
#                         "score":    {"$meta": "vectorSearchScore"}
#                     }
#                 }
#             ]
#             results = list(self.embeddings_col.aggregate(pipeline))
#             logger.info(
#                 f"[SEARCH] Atlas query done | "
#                 f"results={len(results)} | time={round(time.time()-t0, 3)}s"
#             )

#             logger.info(f"[SEARCH] COMPLETE | total_time={round(time.time()-t_total, 3)}s")
#             return results

#         except Exception as e:
#             logger.exception(
#                 f"[SEARCH] FAILED | doc_id={doc_id} | "
#                 f"elapsed={round(time.time()-t_total, 3)}s | "
#                 f"error={type(e).__name__}: {str(e)[:200]}"
#             )
#             return []

#     def find_supporting_chunks(
#         self, doc_id: str, claim: str, top_k: int = 3
#     ) -> list[dict]:
#         logger.info(f"[SUPPORT] find_supporting_chunks | doc_id={doc_id} | top_k={top_k}")
#         return self.search(doc_id, claim, top_k=top_k)

"""gumi gumi good responce"""

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
)

logger = logging.getLogger(__name__)

# 5 workers = safe concurrency for Bedrock Titan without throttling.
# Logs confirmed avg=0.24s per embed at this setting — healthy.
# First batch of 5 pays ~6s cold-start TCP cost — mitigated by warmup in __init__.
_EMBED_WORKERS    = 5
_POOL_CONNECTIONS = 15

# Track active concurrent threads for diagnostics
_active_threads = 0
_thread_lock    = threading.Lock()


class VectorStore:

    def __init__(self):
        logger.info("=" * 60)
        logger.info("VectorStore.__init__ started")
        logger.info("  OS CPU count       : %s", os.cpu_count())
        logger.info("  _EMBED_WORKERS     : %s", _EMBED_WORKERS)
        logger.info("  _POOL_CONNECTIONS  : %s", _POOL_CONNECTIONS)
        logger.info("  AWS_REGION         : %s", AWS_REGION)
        logger.info("  EMBEDDING_MODEL_ID : %s", EMBEDDING_MODEL_ID)
        logger.info("  VECTOR_DIMENSIONS  : %s", VECTOR_DIMENSIONS)

        t0 = time.time()
        client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
        db = client[MONGODB_DB_NAME]
        self.embeddings_col = db["chunk_embeddings"]
        logger.info("  MongoDB connected  : %.3fs", time.time() - t0)

        t1 = time.time()
        self.bedrock = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION,
            config=Config(
                max_pool_connections=_POOL_CONNECTIONS,
                retries={"max_attempts": 3, "mode": "adaptive"}
            )
        )
        logger.info("  Bedrock client init: %.3fs", time.time() - t1)

        # Warm up Bedrock TCP connection now so the first real embed batch
        # does not pay the ~6s cold-start cost.
        # Logs showed idx=0..4 each took 5.6s bedrock_wait on first run.
        self._warmup_bedrock_connection()

        logger.info("VectorStore ready | total init: %.3fs", time.time() - t0)
        logger.info("=" * 60)

    def _warmup_bedrock_connection(self):
        """
        Fire a single tiny embed request at startup to pre-open the TCP
        connection to Bedrock. This eliminates the ~6s cold-start penalty
        on the first real embed batch.
        """
        logger.info("[WARMUP] Warming up Bedrock TCP connection...")
        t0 = time.time()
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
            logger.info("[WARMUP] Connection warm | took %.3fs", time.time() - t0)
        except Exception as e:
            # Non-fatal — warmup failure just means first batch may be slow
            logger.warning("[WARMUP] Warmup failed (non-fatal): %s", str(e)[:100])

    def _generate_embedding(self, text: str, idx: int = -1) -> list[float]:
        """
        Blocking Bedrock call — thread-safe.
        Logs: thread name, active concurrency, bedrock wait, parse time.
        """
        global _active_threads

        thread_name = threading.current_thread().name
        t_start = time.time()

        with _thread_lock:
            _active_threads += 1
            active_now = _active_threads

        logger.info(
            "[EMBED START] idx=%3d | thread=%s | active_concurrent=%d | text_len=%d",
            idx, thread_name, active_now, len(text)
        )

        try:
            body = json.dumps({
                "inputText": text[:8000],
                "dimensions": VECTOR_DIMENSIONS,
                "normalize": True
            })

            t_request = time.time()
            response = self.bedrock.invoke_model(
                modelId=EMBEDDING_MODEL_ID,
                body=body,
                contentType="application/json",
                accept="application/json"
            )
            t_response = time.time()

            raw = response["body"].read()
            result = json.loads(raw)["embedding"]
            t_parse = time.time()

            logger.info(
                "[EMBED DONE ] idx=%3d | thread=%s | bedrock_wait=%.3fs | parse=%.3fs | total=%.3fs | dims=%d",
                idx, thread_name,
                t_response - t_request,
                t_parse - t_response,
                t_parse - t_start,
                len(result)
            )
            return result

        except Exception as e:
            t_fail = time.time()
            logger.exception(
                "[EMBED FAIL ] idx=%3d | thread=%s | elapsed=%.3fs | error=%s: %s",
                idx, thread_name,
                t_fail - t_start,
                type(e).__name__, str(e)[:100]
            )
            return []

        finally:
            with _thread_lock:
                _active_threads -= 1

    def _generate_embeddings_concurrent(
        self, texts: list[str]
    ) -> list[list[float]]:
        """
        Controlled concurrency embedding.
        5 workers avoids Bedrock throttling (confirmed avg=0.24s per embed).
        Cold start handled by warmup in __init__.
        """
        if not texts:
            logger.warning("[CONCURRENT] No texts provided — skipping")
            return []

        total = len(texts)
        estimated = round(total / _EMBED_WORKERS * 0.5, 1)
        logger.info("=" * 60)
        logger.info("[CONCURRENT] Starting | chunks=%d | workers=%d | estimated=~%ss", total, _EMBED_WORKERS, estimated)

        t_pool_start = time.time()
        results = {}
        completed_count = 0
        failed_count = 0

        with ThreadPoolExecutor(
            max_workers=_EMBED_WORKERS,
            thread_name_prefix="embed_worker"
        ) as pool:
            t_pool_ready = time.time()
            logger.info("[CONCURRENT] ThreadPool ready | spin_up=%.3fs", t_pool_ready - t_pool_start)

            futures = {}
            for idx, text in enumerate(texts):
                f = pool.submit(self._generate_embedding, text, idx)
                futures[f] = idx

            t_submitted = time.time()
            logger.info("[CONCURRENT] All %d tasks submitted | submit_time=%.3fs", total, t_submitted - t_pool_ready)

            for future in as_completed(futures):
                idx = futures[future]
                t_got = time.time()
                try:
                    results[idx] = future.result()
                    completed_count += 1
                    logger.info(
                        "[CONCURRENT] Future resolved | idx=%3d | completed=%d/%d | wall_so_far=%.2fs",
                        idx, completed_count, total, t_got - t_pool_ready
                    )
                except Exception as e:
                    failed_count += 1
                    results[idx] = []
                    logger.exception(
                        "[CONCURRENT] Future exception | idx=%3d | error=%s: %s",
                        idx, type(e).__name__, str(e)[:100]
                    )

        t_done = time.time()
        total_wall    = round(t_done - t_pool_start, 3)
        avg_per_embed = round(total_wall / total, 3) if total else 0

        logger.info("[CONCURRENT] --- Summary ---")
        logger.info("[CONCURRENT]   Total wall time  : %.3fs", total_wall)
        logger.info("[CONCURRENT]   Avg per embed    : %.3fs", avg_per_embed)
        logger.info("[CONCURRENT]   Completed        : %d/%d", completed_count, total)
        logger.info("[CONCURRENT]   Failed           : %d/%d", failed_count, total)

        # Auto-diagnose based on avg time
        if avg_per_embed > 2.0:
            logger.warning(
                "[CONCURRENT] CRITICAL: avg=%.3fs >> 1s — "
                "Bedrock is hard throttling. Reduce _EMBED_WORKERS to 3.",
                avg_per_embed
            )
        elif avg_per_embed > 1.0:
            logger.warning(
                "[CONCURRENT] WARNING: avg=%.3fs > 1s — "
                "Bedrock is throttling. Reduce _EMBED_WORKERS to 3 or 4.",
                avg_per_embed
            )
        elif avg_per_embed < 0.6:
            logger.info(
                "[CONCURRENT] HEALTHY: avg=%.3fs — "
                "No throttling. Safe to try _EMBED_WORKERS=7 for more speed.",
                avg_per_embed
            )
        else:
            logger.info(
                "[CONCURRENT] OK: avg=%.3fs — Within acceptable range.",
                avg_per_embed
            )
        logger.info("=" * 60)

        return [results[i] for i in range(len(texts))]

    def store_embeddings(self, doc_id: str, chunks: list[dict]) -> bool:
        logger.info("=" * 60)
        logger.info("[STORE] store_embeddings called | doc_id=%s | chunks=%d", doc_id, len(chunks))
        t_total_start = time.time()

        try:
            # Step 1: Delete old embeddings
            t0 = time.time()
            deleted = self.embeddings_col.delete_many({"doc_id": doc_id})
            logger.info("[STORE] Step1 delete_many | deleted=%d | time=%.3fs", deleted.deleted_count, time.time() - t0)

            # Step 2: Filter valid chunks
            t0 = time.time()
            valid_chunks = [c for c in chunks if c.get("content", "").strip()]
            logger.info(
                "[STORE] Step2 filter chunks | input=%d | valid=%d | dropped=%d | time=%.3fs",
                len(chunks), len(valid_chunks), len(chunks) - len(valid_chunks), time.time() - t0
            )

            if not valid_chunks:
                logger.error("[STORE] No valid chunks — aborting | doc_id=%s", doc_id)
                return False

            texts = [c["content"].strip() for c in valid_chunks]
            total_chars = sum(len(t) for t in texts)
            logger.info(
                "[STORE] Chunk stats | total_chars=%d | avg_chars=%d | min=%d | max=%d",
                total_chars,
                round(total_chars / len(texts)),
                min(len(t) for t in texts),
                max(len(t) for t in texts)
            )

            # Step 3: Generate embeddings (warmup already done in __init__)
            logger.info("[STORE] Step3 starting embedding generation...")
            t0 = time.time()
            embeddings = self._generate_embeddings_concurrent(texts)
            embed_time = round(time.time() - t0, 3)
            logger.info("[STORE] Step3 embedding done | time=%.3fs", embed_time)

            # Step 4: Build MongoDB docs
            t0 = time.time()
            embedding_docs = []
            skipped = 0
            for chunk, emb in zip(valid_chunks, embeddings):
                if not emb:
                    skipped += 1
                    logger.warning("[STORE] Skipping chunk %s — empty embedding", chunk.get("chunk_id"))
                    continue
                embedding_docs.append({
                    "doc_id":    doc_id,
                    "chunk_id":  chunk.get("chunk_id"),
                    "content":   chunk["content"].strip(),
                    "embedding": emb,
                })
            logger.info(
                "[STORE] Step4 build docs | docs_ready=%d | skipped=%d | time=%.3fs",
                len(embedding_docs), skipped, time.time() - t0
            )

            if not embedding_docs:
                logger.error("[STORE] No embedding docs to insert — aborting")
                return False

            # Step 5: Insert to MongoDB Atlas
            t0 = time.time()
            self.embeddings_col.insert_many(embedding_docs)
            insert_time = round(time.time() - t0, 3)
            logger.info("[STORE] Step5 insert_many | docs=%d | time=%.3fs", len(embedding_docs), insert_time)

            total_time = round(time.time() - t_total_start, 3)
            logger.info("[STORE] --- Final Summary ---")
            logger.info("[STORE]   doc_id         : %s", doc_id)
            logger.info("[STORE]   chunks stored  : %d", len(embedding_docs))
            logger.info("[STORE]   embed_time     : %.3fs", embed_time)
            logger.info("[STORE]   insert_time    : %.3fs", insert_time)
            logger.info("[STORE]   TOTAL time     : %.3fs", total_time)
            logger.info("=" * 60)
            return True

        except Exception as e:
            logger.exception(
                "[STORE] FAILED | doc_id=%s | elapsed=%.3fs | error=%s: %s",
                doc_id, time.time() - t_total_start,
                type(e).__name__, str(e)[:200]
            )
            return False

    def search(
        self, doc_id: str, query: str, top_k: int = VECTOR_TOP_K
    ) -> list[dict]:
        logger.info("[SEARCH] Starting | doc_id=%s | top_k=%d | query=%s", doc_id, top_k, query[:80])
        t_total = time.time()

        try:
            t0 = time.time()
            query_embedding = self._generate_embedding(query, idx=-1)
            logger.info("[SEARCH] Query embed done | time=%.3fs | dims=%d", time.time() - t0, len(query_embedding))

            if not query_embedding:
                logger.error("[SEARCH] Query embedding empty — aborting")
                return []

            t0 = time.time()
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
            logger.info("[SEARCH] Atlas query done | results=%d | time=%.3fs", len(results), time.time() - t0)
            logger.info("[SEARCH] COMPLETE | total_time=%.3fs", time.time() - t_total)
            return results

        except Exception as e:
            logger.exception(
                "[SEARCH] FAILED | doc_id=%s | elapsed=%.3fs | error=%s: %s",
                doc_id, time.time() - t_total,
                type(e).__name__, str(e)[:200]
            )
            return []

    def find_supporting_chunks(
        self, doc_id: str, claim: str, top_k: int = 3
    ) -> list[dict]:
        logger.info("[SUPPORT] find_supporting_chunks | doc_id=%s | top_k=%d", doc_id, top_k)
        return self.search(doc_id, claim, top_k=top_k)