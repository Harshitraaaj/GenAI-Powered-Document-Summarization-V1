# # import os
# # import time
# # import threading
# # import logging
# # import json
# # from concurrent.futures import ThreadPoolExecutor, as_completed
# # from botocore.config import Config

# # import boto3
# # from pymongo import MongoClient
# # import certifi

# # from app.core.config import (
# #     AWS_REGION,
# #     EMBEDDING_MODEL_ID,
# #     VECTOR_SEARCH_INDEX_NAME,
# #     VECTOR_DIMENSIONS,
# #     VECTOR_TOP_K,
# #     MONGODB_URI,
# #     MONGODB_DB_NAME,
# # )

# # logger = logging.getLogger(__name__)

# # _EMBED_WORKERS    = 5
# # _POOL_CONNECTIONS = 15

# # # Track active concurrent threads for diagnostics
# # _active_threads = 0
# # _thread_lock    = threading.Lock()


# # class VectorStore:

# #     def __init__(self):
# #         logger.info("=" * 60)
# #         logger.info("VectorStore.__init__ started")
# #         logger.info("  OS CPU count       : %s", os.cpu_count())
# #         logger.info("  _EMBED_WORKERS     : %s", _EMBED_WORKERS)
# #         logger.info("  _POOL_CONNECTIONS  : %s", _POOL_CONNECTIONS)
# #         logger.info("  AWS_REGION         : %s", AWS_REGION)
# #         logger.info("  EMBEDDING_MODEL_ID : %s", EMBEDDING_MODEL_ID)
# #         logger.info("  VECTOR_DIMENSIONS  : %s", VECTOR_DIMENSIONS)

# #         t0 = time.time()
# #         client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
# #         db = client[MONGODB_DB_NAME]
# #         self.embeddings_col = db["chunk_embeddings"]
# #         logger.info("  MongoDB connected  : %.3fs", time.time() - t0)

# #         t1 = time.time()
# #         self.bedrock = boto3.client(
# #             service_name="bedrock-runtime",
# #             region_name=AWS_REGION,
# #             config=Config(
# #                 max_pool_connections=_POOL_CONNECTIONS,
# #                 retries={"max_attempts": 3, "mode": "adaptive"}
# #             )
# #         )
# #         logger.info("  Bedrock client init: %.3fs", time.time() - t1)
# #         self._warmup_bedrock_connection()

# #         logger.info("VectorStore ready | total init: %.3fs", time.time() - t0)
# #         logger.info("=" * 60)

# #     def _warmup_bedrock_connection(self):
# #         """
# #         Fire a single tiny embed request at startup to pre-open the TCP
# #         connection to Bedrock. This eliminates the ~6s cold-start penalty
# #         on the first real embed batch.
# #         """
# #         logger.info("[WARMUP] Warming up Bedrock TCP connection...")
# #         t0 = time.time()
# #         try:
# #             body = json.dumps({
# #                 "inputText": "warmup",
# #                 "dimensions": VECTOR_DIMENSIONS,
# #                 "normalize": True
# #             })
# #             self.bedrock.invoke_model(
# #                 modelId=EMBEDDING_MODEL_ID,
# #                 body=body,
# #                 contentType="application/json",
# #                 accept="application/json"
# #             )
# #             logger.info("[WARMUP] Connection warm | took %.3fs", time.time() - t0)
# #         except Exception as e:
# #             # Non-fatal — warmup failure just means first batch may be slow
# #             logger.warning("[WARMUP] Warmup failed (non-fatal): %s", str(e)[:100])

# #     def _generate_embedding(self, text: str, idx: int = -1) -> list[float]:
# #         """
# #         Blocking Bedrock call — thread-safe.
# #         Logs: thread name, active concurrency, bedrock wait, parse time.
# #         """
# #         global _active_threads

# #         thread_name = threading.current_thread().name
# #         t_start = time.time()

# #         with _thread_lock:
# #             _active_threads += 1
# #             active_now = _active_threads

# #         logger.info(
# #             "[EMBED START] idx=%3d | thread=%s | active_concurrent=%d | text_len=%d",
# #             idx, thread_name, active_now, len(text)
# #         )

# #         try:
# #             body = json.dumps({
# #                 "inputText": text[:8000],
# #                 "dimensions": VECTOR_DIMENSIONS,
# #                 "normalize": True
# #             })

# #             t_request = time.time()
# #             response = self.bedrock.invoke_model(
# #                 modelId=EMBEDDING_MODEL_ID,
# #                 body=body,
# #                 contentType="application/json",
# #                 accept="application/json"
# #             )
# #             t_response = time.time()

# #             raw = response["body"].read()
# #             result = json.loads(raw)["embedding"]
# #             t_parse = time.time()

# #             logger.info(
# #                 "[EMBED DONE ] idx=%3d | thread=%s | bedrock_wait=%.3fs | parse=%.3fs | total=%.3fs | dims=%d",
# #                 idx, thread_name,
# #                 t_response - t_request,
# #                 t_parse - t_response,
# #                 t_parse - t_start,
# #                 len(result)
# #             )
# #             return result

# #         except Exception as e:
# #             t_fail = time.time()
# #             logger.exception(
# #                 "[EMBED FAIL ] idx=%3d | thread=%s | elapsed=%.3fs | error=%s: %s",
# #                 idx, thread_name,
# #                 t_fail - t_start,
# #                 type(e).__name__, str(e)[:100]
# #             )
# #             return []

# #         finally:
# #             with _thread_lock:
# #                 _active_threads -= 1

# #     def _generate_embeddings_concurrent(
# #         self, texts: list[str]
# #     ) -> list[list[float]]:
# #         """
# #         Controlled concurrency embedding.
# #         5 workers avoids Bedrock throttling (confirmed avg=0.24s per embed).
# #         Cold start handled by warmup in __init__.
# #         """
# #         if not texts:
# #             logger.warning("[CONCURRENT] No texts provided — skipping")
# #             return []

# #         total = len(texts)
# #         estimated = round(total / _EMBED_WORKERS * 0.5, 1)
# #         logger.info("=" * 60)
# #         logger.info("[CONCURRENT] Starting | chunks=%d | workers=%d | estimated=~%ss", total, _EMBED_WORKERS, estimated)

# #         t_pool_start = time.time()
# #         results = {}
# #         completed_count = 0
# #         failed_count = 0

# #         with ThreadPoolExecutor(
# #             max_workers=_EMBED_WORKERS,
# #             thread_name_prefix="embed_worker"
# #         ) as pool:
# #             t_pool_ready = time.time()
# #             logger.info("[CONCURRENT] ThreadPool ready | spin_up=%.3fs", t_pool_ready - t_pool_start)

# #             futures = {}
# #             for idx, text in enumerate(texts):
# #                 f = pool.submit(self._generate_embedding, text, idx)
# #                 futures[f] = idx

# #             t_submitted = time.time()
# #             logger.info("[CONCURRENT] All %d tasks submitted | submit_time=%.3fs", total, t_submitted - t_pool_ready)

# #             for future in as_completed(futures):
# #                 idx = futures[future]
# #                 t_got = time.time()
# #                 try:
# #                     results[idx] = future.result()
# #                     completed_count += 1
# #                     logger.info(
# #                         "[CONCURRENT] Future resolved | idx=%3d | completed=%d/%d | wall_so_far=%.2fs",
# #                         idx, completed_count, total, t_got - t_pool_ready
# #                     )
# #                 except Exception as e:
# #                     failed_count += 1
# #                     results[idx] = []
# #                     logger.exception(
# #                         "[CONCURRENT] Future exception | idx=%3d | error=%s: %s",
# #                         idx, type(e).__name__, str(e)[:100]
# #                     )

# #         t_done = time.time()
# #         total_wall    = round(t_done - t_pool_start, 3)
# #         avg_per_embed = round(total_wall / total, 3) if total else 0

# #         logger.info("[CONCURRENT] --- Summary ---")
# #         logger.info("[CONCURRENT]   Total wall time  : %.3fs", total_wall)
# #         logger.info("[CONCURRENT]   Avg per embed    : %.3fs", avg_per_embed)
# #         logger.info("[CONCURRENT]   Completed        : %d/%d", completed_count, total)
# #         logger.info("[CONCURRENT]   Failed           : %d/%d", failed_count, total)

# #         # Auto-diagnose based on avg time
# #         if avg_per_embed > 2.0:
# #             logger.warning(
# #                 "[CONCURRENT] CRITICAL: avg=%.3fs >> 1s — "
# #                 "Bedrock is hard throttling. Reduce _EMBED_WORKERS to 3.",
# #                 avg_per_embed
# #             )
# #         elif avg_per_embed > 1.0:
# #             logger.warning(
# #                 "[CONCURRENT] WARNING: avg=%.3fs > 1s — "
# #                 "Bedrock is throttling. Reduce _EMBED_WORKERS to 3 or 4.",
# #                 avg_per_embed
# #             )
# #         elif avg_per_embed < 0.6:
# #             logger.info(
# #                 "[CONCURRENT] HEALTHY: avg=%.3fs — "
# #                 "No throttling. Safe to try _EMBED_WORKERS=7 for more speed.",
# #                 avg_per_embed
# #             )
# #         else:
# #             logger.info(
# #                 "[CONCURRENT] OK: avg=%.3fs — Within acceptable range.",
# #                 avg_per_embed
# #             )
# #         logger.info("=" * 60)

# #         return [results[i] for i in range(len(texts))]

# #     def store_embeddings(self, doc_id: str, chunks: list[dict]) -> bool:
# #         logger.info("=" * 60)
# #         logger.info("[STORE] store_embeddings called | doc_id=%s | chunks=%d", doc_id, len(chunks))
# #         t_total_start = time.time()

# #         try:
# #             # Step 1: Delete old embeddings
# #             t0 = time.time()
# #             deleted = self.embeddings_col.delete_many({"doc_id": doc_id})
# #             logger.info("[STORE] Step1 delete_many | deleted=%d | time=%.3fs", deleted.deleted_count, time.time() - t0)

# #             # Step 2: Filter valid chunks
# #             t0 = time.time()
# #             valid_chunks = [c for c in chunks if c.get("content", "").strip()]
# #             logger.info(
# #                 "[STORE] Step2 filter chunks | input=%d | valid=%d | dropped=%d | time=%.3fs",
# #                 len(chunks), len(valid_chunks), len(chunks) - len(valid_chunks), time.time() - t0
# #             )

# #             if not valid_chunks:
# #                 logger.error("[STORE] No valid chunks — aborting | doc_id=%s", doc_id)
# #                 return False

# #             texts = [c["content"].strip() for c in valid_chunks]
# #             total_chars = sum(len(t) for t in texts)
# #             logger.info(
# #                 "[STORE] Chunk stats | total_chars=%d | avg_chars=%d | min=%d | max=%d",
# #                 total_chars,
# #                 round(total_chars / len(texts)),
# #                 min(len(t) for t in texts),
# #                 max(len(t) for t in texts)
# #             )

# #             # Step 3: Generate embeddings (warmup already done in __init__)
# #             logger.info("[STORE] Step3 starting embedding generation...")
# #             t0 = time.time()
# #             embeddings = self._generate_embeddings_concurrent(texts)
# #             embed_time = round(time.time() - t0, 3)
# #             logger.info("[STORE] Step3 embedding done | time=%.3fs", embed_time)

# #             # Step 4: Build MongoDB docs
# #             t0 = time.time()
# #             embedding_docs = []
# #             skipped = 0
# #             for chunk, emb in zip(valid_chunks, embeddings):
# #                 if not emb:
# #                     skipped += 1
# #                     logger.warning("[STORE] Skipping chunk %s — empty embedding", chunk.get("chunk_id"))
# #                     continue
# #                 embedding_docs.append({
# #                     "doc_id":    doc_id,
# #                     "chunk_id":  chunk.get("chunk_id"),
# #                     "content":   chunk["content"].strip(),
# #                     "embedding": emb,
# #                 })
# #             logger.info(
# #                 "[STORE] Step4 build docs | docs_ready=%d | skipped=%d | time=%.3fs",
# #                 len(embedding_docs), skipped, time.time() - t0
# #             )

# #             if not embedding_docs:
# #                 logger.error("[STORE] No embedding docs to insert — aborting")
# #                 return False

# #             # Step 5: Insert to MongoDB Atlas
# #             t0 = time.time()
# #             self.embeddings_col.insert_many(embedding_docs)
# #             insert_time = round(time.time() - t0, 3)
# #             logger.info("[STORE] Step5 insert_many | docs=%d | time=%.3fs", len(embedding_docs), insert_time)

# #             total_time = round(time.time() - t_total_start, 3)
# #             logger.info("[STORE] --- Final Summary ---")
# #             logger.info("[STORE]   doc_id         : %s", doc_id)
# #             logger.info("[STORE]   chunks stored  : %d", len(embedding_docs))
# #             logger.info("[STORE]   embed_time     : %.3fs", embed_time)
# #             logger.info("[STORE]   insert_time    : %.3fs", insert_time)
# #             logger.info("[STORE]   TOTAL time     : %.3fs", total_time)
# #             logger.info("=" * 60)
# #             return True

# #         except Exception as e:
# #             logger.exception(
# #                 "[STORE] FAILED | doc_id=%s | elapsed=%.3fs | error=%s: %s",
# #                 doc_id, time.time() - t_total_start,
# #                 type(e).__name__, str(e)[:200]
# #             )
# #             return False

# #     def search(
# #         self, doc_id: str, query: str, top_k: int = VECTOR_TOP_K
# #     ) -> list[dict]:
# #         logger.info("[SEARCH] Starting | doc_id=%s | top_k=%d | query=%s", doc_id, top_k, query[:80])
# #         t_total = time.time()

# #         try:
# #             t0 = time.time()
# #             query_embedding = self._generate_embedding(query, idx=-1)
# #             logger.info("[SEARCH] Query embed done | time=%.3fs | dims=%d", time.time() - t0, len(query_embedding))

# #             if not query_embedding:
# #                 logger.error("[SEARCH] Query embedding empty — aborting")
# #                 return []

# #             t0 = time.time()
# #             pipeline = [
# #                 {
# #                     "$vectorSearch": {
# #                         "index":         VECTOR_SEARCH_INDEX_NAME,
# #                         "path":          "embedding",
# #                         "queryVector":   query_embedding,
# #                         "numCandidates": top_k * 10,
# #                         "limit":         top_k,
# #                         "filter":        {"doc_id": doc_id}
# #                     }
# #                 },
# #                 {
# #                     "$project": {
# #                         "_id":      0,
# #                         "doc_id":   1,
# #                         "chunk_id": 1,
# #                         "content":  1,
# #                         "score":    {"$meta": "vectorSearchScore"}
# #                     }
# #                 }
# #             ]
# #             results = list(self.embeddings_col.aggregate(pipeline))
# #             logger.info("[SEARCH] Atlas query done | results=%d | time=%.3fs", len(results), time.time() - t0)
# #             logger.info("[SEARCH] COMPLETE | total_time=%.3fs", time.time() - t_total)
# #             return results

# #         except Exception as e:
# #             logger.exception(
# #                 "[SEARCH] FAILED | doc_id=%s | elapsed=%.3fs | error=%s: %s",
# #                 doc_id, time.time() - t_total,
# #                 type(e).__name__, str(e)[:200]
# #             )
# #             return []

# #     def find_supporting_chunks(
# #         self, doc_id: str, claim: str, top_k: int = 3
# #     ) -> list[dict]:
# #         logger.info("[SUPPORT] find_supporting_chunks | doc_id=%s | top_k=%d", doc_id, top_k)
# #         return self.search(doc_id, claim, top_k=top_k)

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
#     EMBED_WORKERS,
#     EMBED_POOL_CONNECTIONS,
# )

# logger = logging.getLogger(__name__)

# # Track active concurrent threads for diagnostics
# _active_threads = 0
# _thread_lock    = threading.Lock()


# class VectorStore:

#     def __init__(self):
#         logger.info("=" * 60)
#         logger.info("VectorStore.__init__ started")
#         logger.info("  OS CPU count       : %s", os.cpu_count())
#         logger.info("  EMBED_WORKERS      : %s", EMBED_WORKERS)
#         logger.info("  EMBED_POOL_CONNECTIONS : %s", EMBED_POOL_CONNECTIONS)
#         logger.info("  AWS_REGION         : %s", AWS_REGION)
#         logger.info("  EMBEDDING_MODEL_ID : %s", EMBEDDING_MODEL_ID)
#         logger.info("  VECTOR_DIMENSIONS  : %s", VECTOR_DIMENSIONS)

#         t0 = time.time()
#         client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
#         db = client[MONGODB_DB_NAME]
#         self.embeddings_col = db["chunk_embeddings"]
#         logger.info("  MongoDB connected  : %.3fs", time.time() - t0)

#         t1 = time.time()
#         self.bedrock = boto3.client(
#             service_name="bedrock-runtime",
#             region_name=AWS_REGION,
#             config=Config(
#                 max_pool_connections=EMBED_POOL_CONNECTIONS,
#                 retries={"max_attempts": 3, "mode": "adaptive"}
#             )
#         )
#         logger.info("  Bedrock client init: %.3fs", time.time() - t1)
#         self._warmup_bedrock_connection()

#         logger.info("VectorStore ready | total init: %.3fs", time.time() - t0)
#         logger.info("=" * 60)

#     def _warmup_bedrock_connection(self):
#         """Pre-open TCP connection to eliminate cold-start penalty on first embed batch."""
#         logger.info("[WARMUP] Warming up Bedrock TCP connection...")
#         t0 = time.time()
#         try:
#             body = json.dumps({
#                 "inputText": "warmup",
#                 "dimensions": VECTOR_DIMENSIONS,
#                 "normalize": True
#             })
#             self.bedrock.invoke_model(
#                 modelId=EMBEDDING_MODEL_ID,
#                 body=body,
#                 contentType="application/json",
#                 accept="application/json"
#             )
#             logger.info("[WARMUP] Connection warm | took %.3fs", time.time() - t0)
#         except Exception as e:
#             logger.warning("[WARMUP] Warmup failed (non-fatal): %s", str(e)[:100])

#     def _generate_embedding(self, text: str, idx: int = -1) -> list[float]:
#         global _active_threads

#         thread_name = threading.current_thread().name
#         t_start = time.time()

#         with _thread_lock:
#             _active_threads += 1
#             active_now = _active_threads

#         logger.info(
#             "[EMBED START] idx=%3d | thread=%s | active_concurrent=%d | text_len=%d",
#             idx, thread_name, active_now, len(text)
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
#                 "[EMBED DONE ] idx=%3d | thread=%s | bedrock_wait=%.3fs | parse=%.3fs | total=%.3fs | dims=%d",
#                 idx, thread_name,
#                 t_response - t_request,
#                 t_parse - t_response,
#                 t_parse - t_start,
#                 len(result)
#             )
#             return result

#         except Exception as e:
#             t_fail = time.time()
#             logger.exception(
#                 "[EMBED FAIL ] idx=%3d | thread=%s | elapsed=%.3fs | error=%s: %s",
#                 idx, thread_name,
#                 t_fail - t_start,
#                 type(e).__name__, str(e)[:100]
#             )
#             return []

#         finally:
#             with _thread_lock:
#                 _active_threads -= 1

#     def _generate_embeddings_concurrent(self, texts: list[str]) -> list[list[float]]:
#         if not texts:
#             logger.warning("[CONCURRENT] No texts provided — skipping")
#             return []

#         total     = len(texts)
#         estimated = round(total / EMBED_WORKERS * 0.5, 1)
#         logger.info("=" * 60)
#         logger.info(
#             "[CONCURRENT] Starting | chunks=%d | workers=%d | estimated=~%ss",
#             total, EMBED_WORKERS, estimated
#         )

#         t_pool_start    = time.time()
#         results         = {}
#         completed_count = 0
#         failed_count    = 0

#         with ThreadPoolExecutor(
#             max_workers=EMBED_WORKERS,
#             thread_name_prefix="embed_worker"
#         ) as pool:
#             t_pool_ready = time.time()
#             logger.info("[CONCURRENT] ThreadPool ready | spin_up=%.3fs", t_pool_ready - t_pool_start)

#             futures = {pool.submit(self._generate_embedding, text, idx): idx
#                        for idx, text in enumerate(texts)}

#             t_submitted = time.time()
#             logger.info(
#                 "[CONCURRENT] All %d tasks submitted | submit_time=%.3fs",
#                 total, t_submitted - t_pool_ready
#             )

#             for future in as_completed(futures):
#                 idx   = futures[future]
#                 t_got = time.time()
#                 try:
#                     results[idx] = future.result()
#                     completed_count += 1
#                     logger.info(
#                         "[CONCURRENT] Future resolved | idx=%3d | completed=%d/%d | wall_so_far=%.2fs",
#                         idx, completed_count, total, t_got - t_pool_ready
#                     )
#                 except Exception as e:
#                     failed_count    += 1
#                     results[idx]     = []
#                     logger.exception(
#                         "[CONCURRENT] Future exception | idx=%3d | error=%s: %s",
#                         idx, type(e).__name__, str(e)[:100]
#                     )

#         t_done        = time.time()
#         total_wall    = round(t_done - t_pool_start, 3)
#         avg_per_embed = round(total_wall / total, 3) if total else 0

#         logger.info("[CONCURRENT] --- Summary ---")
#         logger.info("[CONCURRENT]   Total wall time  : %.3fs", total_wall)
#         logger.info("[CONCURRENT]   Avg per embed    : %.3fs", avg_per_embed)
#         logger.info("[CONCURRENT]   Completed        : %d/%d", completed_count, total)
#         logger.info("[CONCURRENT]   Failed           : %d/%d", failed_count, total)

#         if avg_per_embed > 2.0:
#             logger.warning(
#                 "[CONCURRENT] CRITICAL: avg=%.3fs — Bedrock hard throttling. Reduce EMBED_WORKERS to 3.",
#                 avg_per_embed
#             )
#         elif avg_per_embed > 1.0:
#             logger.warning(
#                 "[CONCURRENT] WARNING: avg=%.3fs — Bedrock throttling. Reduce EMBED_WORKERS to 3 or 4.",
#                 avg_per_embed
#             )
#         elif avg_per_embed < 0.6:
#             logger.info(
#                 "[CONCURRENT] HEALTHY: avg=%.3fs — No throttling. Safe to try EMBED_WORKERS=7.",
#                 avg_per_embed
#             )
#         else:
#             logger.info("[CONCURRENT] OK: avg=%.3fs — Within acceptable range.", avg_per_embed)

#         logger.info("=" * 60)
#         return [results[i] for i in range(len(texts))]

#     def store_embeddings(self, doc_id: str, chunks: list[dict]) -> bool:
#         logger.info("=" * 60)
#         logger.info("[STORE] store_embeddings called | doc_id=%s | chunks=%d", doc_id, len(chunks))
#         t_total_start = time.time()

#         try:
#             t0      = time.time()
#             deleted = self.embeddings_col.delete_many({"doc_id": doc_id})
#             logger.info("[STORE] Step1 delete_many | deleted=%d | time=%.3fs", deleted.deleted_count, time.time() - t0)

#             t0           = time.time()
#             valid_chunks = [c for c in chunks if c.get("content", "").strip()]
#             logger.info(
#                 "[STORE] Step2 filter chunks | input=%d | valid=%d | dropped=%d | time=%.3fs",
#                 len(chunks), len(valid_chunks), len(chunks) - len(valid_chunks), time.time() - t0
#             )

#             if not valid_chunks:
#                 logger.error("[STORE] No valid chunks — aborting | doc_id=%s", doc_id)
#                 return False

#             texts       = [c["content"].strip() for c in valid_chunks]
#             total_chars = sum(len(t) for t in texts)
#             logger.info(
#                 "[STORE] Chunk stats | total_chars=%d | avg_chars=%d | min=%d | max=%d",
#                 total_chars,
#                 round(total_chars / len(texts)),
#                 min(len(t) for t in texts),
#                 max(len(t) for t in texts)
#             )

#             logger.info("[STORE] Step3 starting embedding generation...")
#             t0         = time.time()
#             embeddings = self._generate_embeddings_concurrent(texts)
#             embed_time = round(time.time() - t0, 3)
#             logger.info("[STORE] Step3 embedding done | time=%.3fs", embed_time)

#             t0             = time.time()
#             embedding_docs = []
#             skipped        = 0
#             for chunk, emb in zip(valid_chunks, embeddings):
#                 if not emb:
#                     skipped += 1
#                     logger.warning("[STORE] Skipping chunk %s — empty embedding", chunk.get("chunk_id"))
#                     continue
#                 embedding_docs.append({
#                     "doc_id":    doc_id,
#                     "chunk_id":  chunk.get("chunk_id"),
#                     "content":   chunk["content"].strip(),
#                     "embedding": emb,
#                 })
#             logger.info(
#                 "[STORE] Step4 build docs | docs_ready=%d | skipped=%d | time=%.3fs",
#                 len(embedding_docs), skipped, time.time() - t0
#             )

#             if not embedding_docs:
#                 logger.error("[STORE] No embedding docs to insert — aborting")
#                 return False

#             t0          = time.time()
#             self.embeddings_col.insert_many(embedding_docs)
#             insert_time = round(time.time() - t0, 3)
#             logger.info("[STORE] Step5 insert_many | docs=%d | time=%.3fs", len(embedding_docs), insert_time)

#             total_time = round(time.time() - t_total_start, 3)
#             logger.info("[STORE] --- Final Summary ---")
#             logger.info("[STORE]   doc_id         : %s", doc_id)
#             logger.info("[STORE]   chunks stored  : %d", len(embedding_docs))
#             logger.info("[STORE]   embed_time     : %.3fs", embed_time)
#             logger.info("[STORE]   insert_time    : %.3fs", insert_time)
#             logger.info("[STORE]   TOTAL time     : %.3fs", total_time)
#             logger.info("=" * 60)
#             return True

#         except Exception as e:
#             logger.exception(
#                 "[STORE] FAILED | doc_id=%s | elapsed=%.3fs | error=%s: %s",
#                 doc_id, time.time() - t_total_start,
#                 type(e).__name__, str(e)[:200]
#             )
#             return False

#     def search(self, doc_id: str, query: str, top_k: int = VECTOR_TOP_K) -> list[dict]:
#         logger.info("[SEARCH] Starting | doc_id=%s | top_k=%d | query=%s", doc_id, top_k, query[:80])
#         t_total = time.time()

#         try:
#             t0              = time.time()
#             query_embedding = self._generate_embedding(query, idx=-1)
#             logger.info("[SEARCH] Query embed done | time=%.3fs | dims=%d", time.time() - t0, len(query_embedding))

#             if not query_embedding:
#                 logger.error("[SEARCH] Query embedding empty — aborting")
#                 return []

#             t0       = time.time()
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
#             logger.info("[SEARCH] Atlas query done | results=%d | time=%.3fs", len(results), time.time() - t0)
#             logger.info("[SEARCH] COMPLETE | total_time=%.3fs", time.time() - t_total)
#             return results

#         except Exception as e:
#             logger.exception(
#                 "[SEARCH] FAILED | doc_id=%s | elapsed=%.3fs | error=%s: %s",
#                 doc_id, time.time() - t_total,
#                 type(e).__name__, str(e)[:200]
#             )
#             return []

#     def find_supporting_chunks(self, doc_id: str, claim: str, top_k: int = 3) -> list[dict]:
#         logger.info("[SUPPORT] find_supporting_chunks | doc_id=%s | top_k=%d", doc_id, top_k)
#         return self.search(doc_id, claim, top_k=top_k)

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