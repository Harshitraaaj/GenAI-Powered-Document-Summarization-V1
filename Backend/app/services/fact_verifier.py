# import json
# import logging
# from app.services.summarizer import invoke_model
# from app.db.vector_store import VectorStore

# logger = logging.getLogger(__name__)


# FACT_VERIFICATION_PROMPT = """
# You are a fact verification expert.

# Your job is to check whether a given claim is supported by the provided source text.

# Claim:
# {claim}

# Source Text:
# {source_text}

# Respond ONLY with valid JSON, no preamble:
# {{
#     "supported": true or false,
#     "confidence": 0.0 to 1.0,
#     "reason": "one-line explanation of why the claim is or is not supported"
# }}
# """


# class FactVerifier:
#     """
#     Verifies summary claims against source document chunks.

#     Flow:
#         1. Extract key claims from the summary
#         2. For each claim, use vector search to find most relevant chunks
#         3. Ask LLM if the claim is supported by those chunks
#         4. Compute coverage score and flag unsupported claims
#     """

#     def __init__(self):
#         self.vector_store = VectorStore()

#     # -----------------------------
#     # Main Verification Entry Point
#     # -----------------------------
#     def verify_facts(
#         self,
#         doc_id: str,
#         summary: dict,
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Verify the executive summary's key points against source chunks.

#         Args:
#             doc_id:  document identifier (for vector search scoping)
#             summary: full summary dict from pipeline (validated output)
#             chunks:  raw chunks list (fallback if vector search fails)

#         Returns:
#             {
#                 "coverage_score": 0.87,
#                 "total_claims": 10,
#                 "supported_claims": 9,
#                 "flagged_claims": [...],
#                 "status": "ok" | "low_coverage_warning"
#             }
#         """
#         logger.info(f"Starting fact verification | doc_id: {doc_id}")

#         # Extract claims from executive summary key_points
#         claims = self._extract_claims(summary)

#         if not claims:
#             logger.warning("No claims found in summary to verify")
#             return {
#                 "coverage_score": 0.0,
#                 "total_claims": 0,
#                 "supported_claims": 0,
#                 "flagged_claims": [],
#                 "status": "no_claims_found"
#             }

#         logger.info(f"Claims to verify: {len(claims)}")

#         supported = []
#         flagged = []

#         for claim in claims:
#             result = self._verify_single_claim(doc_id, claim, chunks)

#             if result["supported"]:
#                 supported.append(claim)
#             else:
#                 flagged.append({
#                     "claim": claim,
#                     "reason": result["reason"],
#                     "confidence": result["confidence"]
#                 })

#         total = len(claims)
#         supported_count = len(supported)
#         coverage_score = round(supported_count / total, 2) if total > 0 else 0.0

#         status = "ok"
#         if coverage_score < 0.7:
#             status = "low_coverage_warning"

#         logger.info(
#             f"Fact verification complete | "
#             f"doc_id: {doc_id} | "
#             f"coverage: {coverage_score} | "
#             f"flagged: {len(flagged)}"
#         )

#         return {
#             "coverage_score": coverage_score,
#             "total_claims": total,
#             "supported_claims": supported_count,
#             "flagged_claims": flagged,
#             "status": status
#         }

#     # -----------------------------
#     # Extract Claims from Summary
#     # -----------------------------
#     def _extract_claims(self, summary: dict) -> list[str]:
#         """
#         Pull verifiable claims from the summary.
#         Uses executive_summary key_points as the primary source.
#         Falls back to section-level key_points if empty.
#         """
#         claims = []

#         # Try executive summary key_points first
#         exec_summary = summary.get("executive_summary", {})
#         key_points = exec_summary.get("key_points", [])

#         if key_points:
#             claims.extend(key_points)

#         # Fallback — collect from section summaries
#         if not claims:
#             for section in summary.get("section_summaries", []):
#                 section_points = section.get("key_points", [])
#                 claims.extend(section_points[:3])  # max 3 per section

#         # Deduplicate
#         seen = set()
#         unique_claims = []
#         for claim in claims:
#             if claim.lower() not in seen:
#                 seen.add(claim.lower())
#                 unique_claims.append(claim)

#         return unique_claims[:20]   # cap at 20 claims to limit LLM calls

#     # -----------------------------
#     # Verify Single Claim
#     # -----------------------------
#     def _verify_single_claim(
#         self,
#         doc_id: str,
#         claim: str,
#         fallback_chunks: list[dict]
#     ) -> dict:
#         """
#         Verify one claim against the most relevant source chunks.

#         1. Use vector search to find top-3 relevant chunks for this claim
#         2. Ask LLM if claim is supported by those chunks
#         3. Return verification result
#         """
#         try:
#             # Step 1 — find relevant chunks via vector search
#             relevant_chunks = self.vector_store.find_supporting_chunks(
#                 doc_id=doc_id,
#                 claim=claim,
#                 top_k=3
#             )

#             # Fallback — if vector search returns nothing, use first 3 raw chunks
#             if not relevant_chunks:
#                 logger.warning(f"Vector search returned no results for claim — using fallback chunks")
#                 relevant_chunks = fallback_chunks[:3]
#                 source_texts = [c.get("content", "") for c in relevant_chunks]
#             else:
#                 # Vector search returns full doc with chunks array
#                 source_texts = []
#                 for result in relevant_chunks:
#                     for chunk in result.get("chunks", []):
#                         source_texts.append(chunk.get("content", ""))

#             combined_source = "\n\n".join(source_texts)[:3000]

#             if not combined_source.strip():
#                 return {
#                     "supported": False,
#                     "confidence": 0.0,
#                     "reason": "No source text found to verify against"
#                 }

#             # Step 2 — ask LLM
#             prompt = FACT_VERIFICATION_PROMPT.format(
#                 claim=claim,
#                 source_text=combined_source
#             )

#             output = invoke_model(prompt, max_tokens=300)

#             if output.startswith("```"):
#                 output = output.replace("```json", "").replace("```", "").strip()

#             parsed = json.loads(output)

#             return {
#                 "supported": parsed.get("supported", False),
#                 "confidence": parsed.get("confidence", 0.0),
#                 "reason": parsed.get("reason", "")
#             }

#         except json.JSONDecodeError:
#             logger.error(f"JSON parse failed for claim: {claim[:50]}")
#             return {"supported": False, "confidence": 0.0, "reason": "Verification parse error"}

#         except Exception:
#             logger.exception(f"Claim verification failed: {claim[:50]}")
#             return {"supported": False, "confidence": 0.0, "reason": "Verification error"}


# # -----------------------------
# # Module-level function
# # called from api.py
# # -----------------------------
# _verifier = None


# def _get_verifier() -> FactVerifier:
#     global _verifier
#     if _verifier is None:
#         _verifier = FactVerifier()
#     return _verifier


# def verify_facts(doc_id: str, summary: dict, chunks: list[dict]) -> dict:
#     return _get_verifier().verify_facts(doc_id, summary, chunks)

"""V1"""
# import json
# import logging
# from app.services.summarizer import invoke_model
# from app.db.vector_store import VectorStore

# logger = logging.getLogger(__name__)


# FACT_VERIFICATION_PROMPT = """
# You are a fact verification expert.

# Your job is to check whether a given claim is supported by the provided source text.

# Claim:
# {claim}

# Source Text:
# {source_text}

# Respond ONLY with valid JSON, no preamble:
# {{
#     "supported": true or false,
#     "confidence": 0.0 to 1.0,
#     "reason": "one-line explanation of why the claim is or is not supported"
# }}
# """


# class FactVerifier:
#     """
#     Verifies summary claims against source document chunks.

#     Flow:
#         1. Extract key claims from the summary
#         2. For each claim, use vector search to find most relevant chunks
#         3. Ask LLM if the claim is supported by those chunks
#         4. Compute coverage score and flag unsupported claims
#     """

#     def __init__(self):
#         self.vector_store = VectorStore()

#     # -----------------------------
#     # Main Verification Entry Point
#     # -----------------------------
#     def verify_facts(
#         self,
#         doc_id: str,
#         summary: dict,
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Verify summary claims against source chunks.

#         Args:
#             doc_id:  document identifier (for vector search scoping)
#             summary: full summary dict from pipeline (validated output)
#             chunks:  raw chunks list (fallback if vector search fails)

#         Returns:
#             {
#                 "coverage_score": 0.87,
#                 "total_claims": 15,
#                 "supported_claims": 13,
#                 "flagged_claims": [...],
#                 "status": "ok" | "low_coverage_warning"
#             }
#         """
#         logger.info(f"Starting fact verification | doc_id: {doc_id}")

#         claims = self._extract_claims(summary)

#         if not claims:
#             logger.warning("No claims found in summary to verify")
#             return {
#                 "coverage_score": 0.0,
#                 "total_claims": 0,
#                 "supported_claims": 0,
#                 "flagged_claims": [],
#                 "status": "no_claims_found"
#             }

#         logger.info(f"Claims to verify: {len(claims)}")

#         supported = []
#         flagged = []

#         for claim in claims:
#             result = self._verify_single_claim(doc_id, claim, chunks)

#             if result["supported"]:
#                 supported.append(claim)
#             else:
#                 flagged.append({
#                     "claim": claim,
#                     "reason": result["reason"],
#                     "confidence": result["confidence"]
#                 })

#         total = len(claims)
#         supported_count = len(supported)
#         coverage_score = round(supported_count / total, 2) if total > 0 else 0.0

#         status = "ok"
#         if coverage_score < 0.7:
#             status = "low_coverage_warning"

#         logger.info(
#             f"Fact verification complete | "
#             f"doc_id: {doc_id} | "
#             f"coverage: {coverage_score} | "
#             f"flagged: {len(flagged)}"
#         )

#         return {
#             "coverage_score": coverage_score,
#             "total_claims": total,
#             "supported_claims": supported_count,
#             "flagged_claims": flagged,
#             "status": status
#         }

#     # -----------------------------
#     # Extract Claims from Summary
#     # -----------------------------
#     def _extract_claims(self, summary: dict) -> list[str]:
#         """
#         Pull verifiable claims from the summary.

#         Priority order:
#         1. Executive summary — key_points, risks, action_items
#         2. Section summaries — 2 key_points + 1 risk per section

#         Previously only pulled executive summary key_points (3 claims).
#         Now pulls from all levels giving 15-20 claims for a meaningful score.
#         """
#         claims = []

#         # 1. Executive summary — all fields
#         exec_summary = summary.get("executive_summary", {})
#         claims.extend(exec_summary.get("key_points", []))
#         claims.extend(exec_summary.get("risks", []))
#         claims.extend(exec_summary.get("action_items", []))

#         # 2. Section summaries — 2 key_points + 1 risk per section
#         for section in summary.get("section_summaries", []):
#             claims.extend(section.get("key_points", [])[:2])
#             claims.extend(section.get("risks", [])[:1])

#         # Deduplicate
#         seen = set()
#         unique_claims = []
#         for claim in claims:
#             if claim and claim.strip() and claim.lower() not in seen:
#                 seen.add(claim.lower())
#                 unique_claims.append(claim)

#         logger.info(f"Total claims to verify: {len(unique_claims)}")

#         return unique_claims[:20]   # cap at 20 to limit LLM calls

#     # -----------------------------
#     # Verify Single Claim
#     # -----------------------------
#     def _verify_single_claim(
#         self,
#         doc_id: str,
#         claim: str,
#         fallback_chunks: list[dict]
#     ) -> dict:
#         """
#         Verify one claim against the most relevant source chunks.

#         1. Use vector search to find top-3 relevant chunks for this claim
#         2. Ask LLM if claim is supported by those chunks
#         3. Return verification result
#         """
#         try:
#             # Step 1 — find relevant chunks via vector search
#             relevant_chunks = self.vector_store.find_supporting_chunks(
#                 doc_id=doc_id,
#                 claim=claim,
#                 top_k=3
#             )

#             # Fallback — if vector search returns nothing, use first 3 raw chunks
#             if not relevant_chunks:
#                 logger.warning(f"Vector search returned no results for claim — using fallback chunks")
#                 relevant_chunks = fallback_chunks[:3]
#                 source_texts = [c.get("content", "") for c in relevant_chunks]
#             else:
#                 # Vector search returns full doc with chunks array
#                 source_texts = []
#                 for result in relevant_chunks:
#                     for chunk in result.get("chunks", []):
#                         source_texts.append(chunk.get("content", ""))

#             combined_source = "\n\n".join(source_texts)[:3000]

#             if not combined_source.strip():
#                 return {
#                     "supported": False,
#                     "confidence": 0.0,
#                     "reason": "No source text found to verify against"
#                 }

#             # Step 2 — ask LLM
#             prompt = FACT_VERIFICATION_PROMPT.format(
#                 claim=claim,
#                 source_text=combined_source
#             )

#             output = invoke_model(prompt, max_tokens=300)

#             # Clean output
#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             # Extract JSON safely
#             start = output.find("{")
#             end = output.rfind("}") + 1
#             if start != -1 and end > start:
#                 output = output[start:end]

#             parsed = json.loads(output)

#             return {
#                 "supported": parsed.get("supported", False),
#                 "confidence": parsed.get("confidence", 0.0),
#                 "reason": parsed.get("reason", "")
#             }

#         except json.JSONDecodeError:
#             logger.error(f"JSON parse failed for claim: {claim[:50]}")
#             return {"supported": False, "confidence": 0.0, "reason": "Verification parse error"}

#         except Exception:
#             logger.exception(f"Claim verification failed: {claim[:50]}")
#             return {"supported": False, "confidence": 0.0, "reason": "Verification error"}


# # -----------------------------
# # Module-level function
# # called from api.py
# # -----------------------------
# _verifier = None


# def _get_verifier() -> FactVerifier:
#     global _verifier
#     if _verifier is None:
#         _verifier = FactVerifier()
#     return _verifier


# def verify_facts(doc_id: str, summary: dict, chunks: list[dict]) -> dict:
#     return _get_verifier().verify_facts(doc_id, summary, chunks)

"""V3"""

# import json
# import logging
# from app.services.summarizer import invoke_model
# from app.db.vector_store import VectorStore

# logger = logging.getLogger(__name__)


# FACT_VERIFICATION_PROMPT = """
# You are a fact verification expert.

# Your job is to check whether a given claim is supported by the provided source text.

# Claim:
# {claim}

# Source Text:
# {source_text}

# Respond ONLY with valid JSON, no preamble:
# {{
#     "supported": true or false,
#     "confidence": 0.0 to 1.0,
#     "reason": "one-line explanation of why the claim is or is not supported"
# }}
# """


# class FactVerifier:
#     """
#     Verifies summary claims against source document chunks.

#     Flow:
#         1. Extract key claims from the summary
#         2. For each claim, use FAISS vector search to find most relevant chunks
#         3. Ask LLM if the claim is supported by those chunks
#         4. Compute coverage score and flag unsupported claims
#     """

#     def __init__(self):
#         self.vector_store = VectorStore()

#     # -----------------------------
#     # Main Verification Entry Point
#     # -----------------------------
#     def verify_facts(
#         self,
#         doc_id: str,
#         summary: dict,
#         chunks: list[dict]
#     ) -> dict:

#         logger.info(f"Starting fact verification | doc_id: {doc_id}")

#         claims = self._extract_claims(summary)

#         if not claims:
#             logger.warning("No claims found in summary to verify")
#             return {
#                 "coverage_score": 0.0,
#                 "total_claims": 0,
#                 "supported_claims": 0,
#                 "flagged_claims": [],
#                 "status": "no_claims_found"
#             }

#         logger.info(f"Claims to verify: {len(claims)}")

#         supported = []
#         flagged = []

#         for claim in claims:
#             result = self._verify_single_claim(doc_id, claim, chunks)

#             if result["supported"]:
#                 supported.append(claim)
#             else:
#                 flagged.append({
#                     "claim": claim,
#                     "reason": result["reason"],
#                     "confidence": result["confidence"]
#                 })

#         total = len(claims)
#         supported_count = len(supported)
#         coverage_score = round(supported_count / total, 2) if total > 0 else 0.0

#         status = "ok"
#         if coverage_score < 0.7:
#             status = "low_coverage_warning"

#         logger.info(
#             f"Fact verification complete | "
#             f"doc_id: {doc_id} | "
#             f"coverage: {coverage_score} | "
#             f"flagged: {len(flagged)}"
#         )

#         return {
#             "coverage_score": coverage_score,
#             "total_claims": total,
#             "supported_claims": supported_count,
#             "flagged_claims": flagged,
#             "status": status
#         }

#     # -----------------------------
#     # Extract Claims from Summary
#     # -----------------------------
#     def _extract_claims(self, summary: dict) -> list[str]:
#         """
#         Pull verifiable claims from the summary.

#         Priority order:
#         1. Executive summary — key_points, risks, action_items
#         2. Section summaries — 2 key_points + 1 risk per section
#         """
#         claims = []

#         # 1. Executive summary — all fields
#         exec_summary = summary.get("executive_summary", {})
#         claims.extend(exec_summary.get("key_points", []))
#         claims.extend(exec_summary.get("risks", []))
#         claims.extend(exec_summary.get("action_items", []))

#         # 2. Section summaries — 2 key_points + 1 risk per section
#         for section in summary.get("section_summaries", []):
#             claims.extend(section.get("key_points", [])[:2])
#             claims.extend(section.get("risks", [])[:1])

#         # Deduplicate
#         seen = set()
#         unique_claims = []
#         for claim in claims:
#             if claim and claim.strip() and claim.lower() not in seen:
#                 seen.add(claim.lower())
#                 unique_claims.append(claim)

#         logger.info(f"Total claims to verify: {len(unique_claims)}")

#         return unique_claims[:20]

#     # -----------------------------
#     # Verify Single Claim
#     # -----------------------------
#     def _verify_single_claim(
#         self,
#         doc_id: str,
#         claim: str,
#         fallback_chunks: list[dict]
#     ) -> dict:
#         """
#         Verify one claim against the most relevant source chunks.

#         1. Use FAISS vector search to find top-3 relevant chunks
#         2. Fallback to keyword matching if FAISS index not found
#         3. Ask LLM if claim is supported by those chunks
#         """
#         try:
#             # Step 1 — FAISS vector search
#             relevant_chunks = self.vector_store.find_supporting_chunks(
#                 doc_id=doc_id,
#                 claim=claim,
#                 top_k=3
#             )

#             if not relevant_chunks:
#                 # Fallback — keyword matching against raw chunks
#                 # Better than using first 3 chunks blindly
#                 logger.warning(f"FAISS returned no results — using keyword fallback")

#                 claim_words = set(claim.lower().split())
#                 scored_chunks = []

#                 for chunk in fallback_chunks:
#                     content = chunk.get("content", "").lower()
#                     # Score = number of meaningful claim words found in chunk
#                     score = sum(
#                         1 for word in claim_words
#                         if len(word) > 4 and word in content
#                     )
#                     scored_chunks.append((score, chunk))

#                 # Sort by relevance — most matching words first
#                 scored_chunks.sort(key=lambda x: x[0], reverse=True)
#                 source_texts = [c.get("content", "") for _, c in scored_chunks[:3]]

#             else:
#                 # FAISS returns flat dicts with 'content' directly
#                 source_texts = [r.get("content", "") for r in relevant_chunks]

#             combined_source = "\n\n".join(source_texts)[:3000]

#             if not combined_source.strip():
#                 return {
#                     "supported": False,
#                     "confidence": 0.0,
#                     "reason": "No source text found to verify against"
#                 }

#             # Step 2 — ask LLM
#             prompt = FACT_VERIFICATION_PROMPT.format(
#                 claim=claim,
#                 source_text=combined_source
#             )

#             output = invoke_model(prompt, max_tokens=300)

#             # Clean output
#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             # Extract JSON safely
#             start = output.find("{")
#             end = output.rfind("}") + 1
#             if start != -1 and end > start:
#                 output = output[start:end]

#             parsed = json.loads(output)

#             return {
#                 "supported": parsed.get("supported", False),
#                 "confidence": parsed.get("confidence", 0.0),
#                 "reason": parsed.get("reason", "")
#             }

#         except json.JSONDecodeError:
#             logger.error(f"JSON parse failed for claim: {claim[:50]}")
#             return {"supported": False, "confidence": 0.0, "reason": "Verification parse error"}

#         except Exception:
#             logger.exception(f"Claim verification failed: {claim[:50]}")
#             return {"supported": False, "confidence": 0.0, "reason": "Verification error"}


# # -----------------------------
# # Module-level function
# # called from api.py
# # -----------------------------
# _verifier = None


# def _get_verifier() -> FactVerifier:
#     global _verifier
#     if _verifier is None:
#         _verifier = FactVerifier()
#     return _verifier


# def verify_facts(doc_id: str, summary: dict, chunks: list[dict]) -> dict:
#     return _get_verifier().verify_facts(doc_id, summary, chunks)

"""v4"""
# import json
# import logging
# from app.services.summarizer import invoke_model
# from app.db.vector_store import VectorStore

# logger = logging.getLogger(__name__)


# FACT_VERIFICATION_PROMPT = """
# You are a fact verification expert.

# Your job is to check whether a given claim is supported by the provided source text.

# Claim:
# {claim}

# Source Text:
# {source_text}

# Instructions:
# - A claim is SUPPORTED if the source text mentions the same MEANING or FACT,
#   even if the exact wording is different.
# - A claim is SUPPORTED if the source text contains numbers or statistics that
#   confirm the claim, even partially.
# - A claim is FLAGGED only if:
#   (a) the source text explicitly contradicts the claim, OR
#   (b) the source text has absolutely no relevant information about the claim topic.
# - Do NOT flag a claim just because the exact wording is not in the source text.
# - Do NOT flag a claim just because it is a short phrase — check if the concept is present.

# Respond ONLY with valid JSON, no preamble:
# {{
#     "supported": true or false,
#     "confidence": 0.0 to 1.0,
#     "reason": "one-line explanation of why the claim is or is not supported"
# }}
# """


# # -----------------------------
# # Minimum claim length (words)
# # Claims shorter than this are too vague to verify reliably
# # -----------------------------
# MIN_CLAIM_WORDS = 4

# # -----------------------------
# # Table chunk detection threshold
# # If >30% of tokens are numeric, treat as table chunk
# # and deprioritize for verification
# # -----------------------------
# TABLE_CHUNK_NUMERIC_THRESHOLD = 0.30


# class FactVerifier:
#     """
#     Verifies summary claims against source document chunks.

#     Flow:
#         1. Extract key claims from the summary
#         2. Filter out vague/short claims
#         3. For each claim, enrich the search query with context
#         4. Use FAISS vector search to find most relevant PROSE chunks
#         5. Ask LLM if the claim is supported by those chunks
#         6. Compute coverage score and flag unsupported claims
#     """

#     def __init__(self):
#         self.vector_store = VectorStore()

#     # -----------------------------
#     # Main Verification Entry Point
#     # -----------------------------
#     def verify_facts(
#         self,
#         doc_id: str,
#         summary: dict,
#         chunks: list[dict]
#     ) -> dict:

#         logger.info(f"Starting fact verification | doc_id: {doc_id}")

#         # Pre-classify chunks into prose vs table
#         # Prose chunks are better for verification
#         prose_chunks, table_chunks = self._classify_chunks(chunks)
#         logger.info(
#             f"Chunk classification | "
#             f"prose: {len(prose_chunks)} | table: {len(table_chunks)}"
#         )

#         claims = self._extract_claims(summary)

#         if not claims:
#             logger.warning("No claims found in summary to verify")
#             return {
#                 "coverage_score": 0.0,
#                 "total_claims": 0,
#                 "supported_claims": 0,
#                 "flagged_claims": [],
#                 "status": "no_claims_found"
#             }

#         logger.info(f"Claims to verify: {len(claims)}")

#         supported = []
#         flagged = []

#         for claim in claims:
#             # Use prose chunks as fallback — more verifiable than tables
#             result = self._verify_single_claim(
#                 doc_id,
#                 claim,
#                 prose_chunks if prose_chunks else chunks
#             )

#             if result["supported"]:
#                 supported.append(claim)
#             else:
#                 flagged.append({
#                     "claim": claim,
#                     "reason": result["reason"],
#                     "confidence": result["confidence"]
#                 })

#         total = len(claims)
#         supported_count = len(supported)
#         coverage_score = round(supported_count / total, 2) if total > 0 else 0.0

#         status = "ok"
#         if coverage_score < 0.7:
#             status = "low_coverage_warning"

#         logger.info(
#             f"Fact verification complete | "
#             f"coverage: {coverage_score} | flagged: {len(flagged)}"
#         )

#         return {
#             "coverage_score": coverage_score,
#             "total_claims": total,
#             "supported_claims": supported_count,
#             "flagged_claims": flagged,
#             "status": status
#         }

#     # -----------------------------
#     # Classify Chunks (Prose vs Table)
#     # -----------------------------
#     def _classify_chunks(
#         self,
#         chunks: list[dict]
#     ) -> tuple[list[dict], list[dict]]:
#         """
#         Split chunks into prose and table chunks.

#         Table chunks contain mostly numbers/statistics and are poor
#         for fact verification — the LLM can't reason about them well.

#         Prose chunks contain narrative text and are much better for
#         verifying claims.
#         """
#         prose = []
#         tables = []

#         for chunk in chunks:
#             content = chunk.get("content", "")
#             if self._is_table_chunk(content):
#                 tables.append(chunk)
#             else:
#                 prose.append(chunk)

#         return prose, tables

#     def _is_table_chunk(self, content: str) -> bool:
#         """
#         Returns True if >30% of tokens are numeric.
#         These are statistical table chunks — poor for claim verification.
#         """
#         tokens = content.split()
#         if not tokens:
#             return False

#         numeric_count = sum(
#             1 for token in tokens
#             if any(char.isdigit() for char in token)
#         )

#         ratio = numeric_count / len(tokens)
#         return ratio > TABLE_CHUNK_NUMERIC_THRESHOLD

#     # -----------------------------
#     # Extract Claims from Summary
#     # -----------------------------
#     def _extract_claims(self, summary: dict) -> list[str]:
#         """
#         Pull verifiable claims from the summary.

#         Priority order:
#         1. Executive summary key_points (most specific)
#         2. Section key_points (2 per section)
#         3. Executive risks (filtered for length)
#         4. Section risks (1 per section, filtered for length)

#         FIX: Filter out claims that are too short/vague to verify.
#         Single-word or two-word claims like "job loss" or "dropout"
#         cannot be reliably verified via semantic search.
#         """
#         claims = []

#         exec_summary = summary.get("executive_summary", {})

#         # Key points are usually the most specific and verifiable
#         claims.extend(exec_summary.get("key_points", []))

#         # Risks from executive summary — filter for length
#         for risk in exec_summary.get("risks", []):
#             if self._is_verifiable_claim(risk):
#                 claims.append(risk)

#         # Action items
#         for action in exec_summary.get("action_items", []):
#             if self._is_verifiable_claim(action):
#                 claims.append(action)

#         # Section summaries — 2 key_points + 1 risk per section
#         for section in summary.get("section_summaries", []):
#             for kp in section.get("key_points", [])[:2]:
#                 if self._is_verifiable_claim(kp):
#                     claims.append(kp)

#             for risk in section.get("risks", [])[:1]:
#                 if self._is_verifiable_claim(risk):
#                     claims.append(risk)

#         # Deduplicate
#         seen = set()
#         unique_claims = []
#         for claim in claims:
#             normalized = claim.strip().lower()
#             if normalized and normalized not in seen:
#                 seen.add(normalized)
#                 unique_claims.append(claim.strip())

#         logger.info(f"Verifiable claims extracted: {len(unique_claims)}")

#         return unique_claims[:20]

#     def _is_verifiable_claim(self, claim: str) -> bool:
#         """
#         Returns True if a claim is specific enough to verify.

#         FIX: Filters out vague single/two-word claims like:
#         - "job loss"
#         - "dropout"
#         - "delayed graduation"
#         - "lower earnings"

#         These are too short and generic to match via semantic search.
#         A verifiable claim needs enough context to retrieve the right chunk.
#         """
#         if not claim or not claim.strip():
#             return False

#         word_count = len(claim.strip().split())
#         return word_count >= MIN_CLAIM_WORDS

#     # -----------------------------
#     # Verify Single Claim
#     # -----------------------------
#     def _verify_single_claim(
#         self,
#         doc_id: str,
#         claim: str,
#         fallback_chunks: list[dict]
#     ) -> dict:
#         """
#         Verify one claim against the most relevant source chunks.

#         FIX 1: Enrich the search query with broader context
#                 so FAISS retrieves prose chunks not table chunks.

#         FIX 2: Use keyword fallback that scores on word overlap
#                 and prefers longer, more specific matches.

#         FIX 3: Softer verification prompt that checks MEANING
#                 not exact wording.
#         """
#         try:
#             # FIX 1 — enrich the search query
#             # Raw claim: "13% of students delayed graduation"
#             # Enriched:  "13% of students delayed graduation impact study"
#             enriched_query = self._enrich_query(claim)

#             logger.debug(f"Verifying: '{claim}' | query: '{enriched_query}'")

#             # Step 1 — FAISS vector search with enriched query
#             relevant_chunks = self.vector_store.find_supporting_chunks(
#                 doc_id=doc_id,
#                 claim=enriched_query,
#                 top_k=3
#             )

#             if not relevant_chunks:
#                 logger.warning(
#                     f"FAISS returned no results for: {claim[:50]} "
#                     f"— using keyword fallback"
#                 )
#                 relevant_chunks = self._keyword_fallback(claim, fallback_chunks)

#             else:
#                 # FIX 2 — filter out table chunks from FAISS results
#                 # If all results are table chunks, fall back to keyword search
#                 prose_results = [
#                     r for r in relevant_chunks
#                     if not self._is_table_chunk(r.get("content", ""))
#                 ]

#                 if not prose_results:
#                     logger.debug(
#                         f"All FAISS results are table chunks for: {claim[:50]} "
#                         f"— using keyword fallback"
#                     )
#                     relevant_chunks = self._keyword_fallback(claim, fallback_chunks)
#                 else:
#                     relevant_chunks = prose_results

#             source_texts = [r.get("content", "") for r in relevant_chunks]
#             combined_source = "\n\n".join(source_texts)[:3000]

#             if not combined_source.strip():
#                 return {
#                     "supported": False,
#                     "confidence": 0.0,
#                     "reason": "No source text found to verify against"
#                 }

#             # Step 2 — ask LLM (FIX 3 — softer prompt already set above)
#             prompt = FACT_VERIFICATION_PROMPT.format(
#                 claim=claim,
#                 source_text=combined_source
#             )

#             output = invoke_model(prompt, max_tokens=300)

#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             start = output.find("{")
#             end = output.rfind("}") + 1
#             if start != -1 and end > start:
#                 output = output[start:end]

#             parsed = json.loads(output)

#             return {
#                 "supported": parsed.get("supported", False),
#                 "confidence": parsed.get("confidence", 0.0),
#                 "reason": parsed.get("reason", "")
#             }

#         except json.JSONDecodeError:
#             logger.error(f"JSON parse failed for claim: {claim[:50]}")
#             return {
#                 "supported": False,
#                 "confidence": 0.0,
#                 "reason": "Verification parse error"
#             }

#         except Exception:
#             logger.exception(f"Claim verification failed: {claim[:50]}")
#             return {
#                 "supported": False,
#                 "confidence": 0.0,
#                 "reason": "Verification error"
#             }

#     # -----------------------------
#     # Enrich Search Query
#     # -----------------------------
#     def _enrich_query(self, claim: str) -> str:
#         """
#         Enrich the claim text to produce a better FAISS search query.

#         Short claims retrieve better when padded with context words.
#         This significantly improves chunk retrieval accuracy.

#         Examples:
#         "delayed graduation"
#             → "delayed graduation students pandemic impact"
#         "13% of students delayed graduation"
#             → "13% of students delayed graduation" (already long enough)
#         "financial shocks"
#             → "financial shocks impact students outcomes"
#         """
#         word_count = len(claim.strip().split())

#         # Only enrich short claims — long ones are already specific enough
#         if word_count >= 6:
#             return claim

#         # Generic context suffix that helps retrieve narrative chunks
#         return f"{claim} impact study findings outcomes"

#     # -----------------------------
#     # Keyword Fallback
#     # -----------------------------
#     def _keyword_fallback(
#         self,
#         claim: str,
#         chunks: list[dict]
#     ) -> list[dict]:
#         """
#         When FAISS fails or returns only table chunks,
#         use keyword matching to find relevant prose chunks.

#         FIX: Score by meaningful word overlap (words >4 chars)
#              and prefer prose chunks over table chunks.
#         """
#         claim_words = set(
#             word.lower() for word in claim.split()
#             if len(word) > 4  # skip short words like "the", "and", "was"
#         )

#         scored_chunks = []

#         for chunk in chunks:
#             content = chunk.get("content", "").lower()

#             # Score = number of meaningful claim words found in chunk
#             score = sum(1 for word in claim_words if word in content)

#             # Boost prose chunks, penalize table chunks
#             if self._is_table_chunk(content):
#                 score = score * 0.5

#             scored_chunks.append((score, chunk))

#         # Sort by score descending
#         scored_chunks.sort(key=lambda x: x[0], reverse=True)

#         # Return top 3 chunks that have at least 1 matching word
#         top_chunks = [
#             chunk for score, chunk in scored_chunks[:5]
#             if score > 0
#         ]

#         # If nothing matches, return first 3 prose chunks as last resort
#         if not top_chunks:
#             return chunks[:3]

#         return top_chunks[:3]


# # -----------------------------
# # Module-level function
# # called from api.py
# # -----------------------------
# _verifier = None


# def _get_verifier() -> FactVerifier:
#     global _verifier
#     if _verifier is None:
#         _verifier = FactVerifier()
#     return _verifier


# def verify_facts(doc_id: str, summary: dict, chunks: list[dict]) -> dict:
#     return _get_verifier().verify_facts(doc_id, summary, chunks)
# """Clean code working"""
# import json
# import logging
# from app.services.summarizer import invoke_model
# from app.db.vector_store import VectorStore
# from app.prompts.fact_verifier import FACT_VERIFICATION_PROMPT
# from app.core.config import (
#     MIN_CLAIM_WORDS,
#     TABLE_CHUNK_NUMERIC_THRESHOLD,
#     FACT_VERIFIER_TOP_K,
#     FACT_VERIFIER_MAX_CLAIMS,
#     FACT_VERIFIER_MAX_SOURCE_CHARS,
#     FACT_VERIFIER_COVERAGE_THRESHOLD,
#     QUERY_ENRICHMENT_MIN_WORDS,
#     KEYWORD_FALLBACK_MIN_WORD_LENGTH,
# )

# logger = logging.getLogger(__name__)


# class FactVerifier:
#     """
#     Verifies summary claims against source document chunks using
#     vector search + LLM verification.
#     """

#     def __init__(self):
#         self.vector_store = VectorStore()

#     def verify_facts(self, doc_id: str, summary: dict, chunks: list[dict]) -> dict:
#         logger.info(f"Starting fact verification | doc_id: {doc_id}")

#         prose_chunks, table_chunks = self._classify_chunks(chunks)
#         logger.info(f"Chunk classification | prose: {len(prose_chunks)} | table: {len(table_chunks)}")

#         claims = self._extract_claims(summary)

#         if not claims:
#             logger.warning("No claims found in summary to verify")
#             return {
#                 "coverage_score": 0.0,
#                 "total_claims": 0,
#                 "supported_claims": 0,
#                 "flagged_claims": [],
#                 "status": "no_claims_found"
#             }

#         logger.info(f"Claims to verify: {len(claims)}")

#         supported = []
#         flagged = []

#         for claim in claims:
#             result = self._verify_single_claim(
#                 doc_id,
#                 claim,
#                 prose_chunks if prose_chunks else chunks
#             )

#             if result["supported"]:
#                 supported.append(claim)
#             else:
#                 flagged.append({
#                     "claim": claim,
#                     "reason": result["reason"],
#                     "confidence": result["confidence"]
#                 })

#         total = len(claims)
#         supported_count = len(supported)
#         coverage_score = round(supported_count / total, 2) if total > 0 else 0.0
#         status = "ok" if coverage_score >= FACT_VERIFIER_COVERAGE_THRESHOLD else "low_coverage_warning"

#         logger.info(f"Fact verification complete | coverage: {coverage_score} | flagged: {len(flagged)}")

#         return {
#             "coverage_score": coverage_score,
#             "total_claims": total,
#             "supported_claims": supported_count,
#             "flagged_claims": flagged,
#             "status": status
#         }

#     def _classify_chunks(self, chunks: list[dict]) -> tuple[list[dict], list[dict]]:
#         prose, tables = [], []
#         for chunk in chunks:
#             (tables if self._is_table_chunk(chunk.get("content", "")) else prose).append(chunk)
#         return prose, tables

#     def _is_table_chunk(self, content: str) -> bool:
#         tokens = content.split()
#         if not tokens:
#             return False
#         numeric_count = sum(1 for t in tokens if any(c.isdigit() for c in t))
#         return (numeric_count / len(tokens)) > TABLE_CHUNK_NUMERIC_THRESHOLD

#     def _extract_claims(self, summary: dict) -> list[str]:
#         claims = []
#         exec_summary = summary.get("executive_summary", {})

#         claims.extend(exec_summary.get("key_points", []))

#         for risk in exec_summary.get("risks", []):
#             if self._is_verifiable_claim(risk):
#                 claims.append(risk)

#         for action in exec_summary.get("action_items", []):
#             if self._is_verifiable_claim(action):
#                 claims.append(action)

#         for section in summary.get("section_summaries", []):
#             for kp in section.get("key_points", [])[:2]:
#                 if self._is_verifiable_claim(kp):
#                     claims.append(kp)
#             for risk in section.get("risks", [])[:1]:
#                 if self._is_verifiable_claim(risk):
#                     claims.append(risk)

#         seen = set()
#         unique_claims = []
#         for claim in claims:
#             normalized = claim.strip().lower()
#             if normalized and normalized not in seen:
#                 seen.add(normalized)
#                 unique_claims.append(claim.strip())

#         logger.info(f"Verifiable claims extracted: {len(unique_claims)}")
#         return unique_claims[:FACT_VERIFIER_MAX_CLAIMS]

#     def _is_verifiable_claim(self, claim: str) -> bool:
#         if not claim or not claim.strip():
#             return False
#         return len(claim.strip().split()) >= MIN_CLAIM_WORDS

#     def _verify_single_claim(self, doc_id: str, claim: str, fallback_chunks: list[dict]) -> dict:
#         try:
#             enriched_query = self._enrich_query(claim)
#             logger.debug(f"Verifying: '{claim}' | query: '{enriched_query}'")

#             relevant_chunks = self.vector_store.find_supporting_chunks(
#                 doc_id=doc_id,
#                 claim=enriched_query,
#                 top_k=FACT_VERIFIER_TOP_K
#             )

#             if not relevant_chunks:
#                 logger.warning(f"FAISS returned no results for: {claim[:50]} — using keyword fallback")
#                 relevant_chunks = self._keyword_fallback(claim, fallback_chunks)
#             else:
#                 prose_results = [r for r in relevant_chunks if not self._is_table_chunk(r.get("content", ""))]
#                 relevant_chunks = prose_results if prose_results else self._keyword_fallback(claim, fallback_chunks)

#             combined_source = "\n\n".join(r.get("content", "") for r in relevant_chunks)[:FACT_VERIFIER_MAX_SOURCE_CHARS]

#             if not combined_source.strip():
#                 return {"supported": False, "confidence": 0.0, "reason": "No source text found to verify against"}

#             prompt = FACT_VERIFICATION_PROMPT.format(claim=claim, source_text=combined_source)
#             output = invoke_model(prompt, max_tokens=300).strip()

#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             start, end = output.find("{"), output.rfind("}") + 1
#             if start != -1 and end > start:
#                 output = output[start:end]

#             parsed = json.loads(output)
#             return {
#                 "supported": parsed.get("supported", False),
#                 "confidence": parsed.get("confidence", 0.0),
#                 "reason": parsed.get("reason", "")
#             }

#         except json.JSONDecodeError:
#             logger.error(f"JSON parse failed for claim: {claim[:50]}")
#             return {"supported": False, "confidence": 0.0, "reason": "Verification parse error"}

#         except Exception:
#             logger.exception(f"Claim verification failed: {claim[:50]}")
#             return {"supported": False, "confidence": 0.0, "reason": "Verification error"}

#     def _enrich_query(self, claim: str) -> str:
#         if len(claim.strip().split()) >= QUERY_ENRICHMENT_MIN_WORDS:
#             return claim
#         return f"{claim} impact study findings outcomes"

#     def _keyword_fallback(self, claim: str, chunks: list[dict]) -> list[dict]:
#         claim_words = {w.lower() for w in claim.split() if len(w) > KEYWORD_FALLBACK_MIN_WORD_LENGTH}

#         scored = []
#         for chunk in chunks:
#             content = chunk.get("content", "").lower()
#             score = sum(1 for w in claim_words if w in content)
#             if self._is_table_chunk(content):
#                 score *= 0.5
#             scored.append((score, chunk))

#         scored.sort(key=lambda x: x[0], reverse=True)
#         top = [chunk for score, chunk in scored[:5] if score > 0]
#         return top[:3] if top else chunks[:3]


# _verifier = None


# def _get_verifier() -> FactVerifier:
#     global _verifier
#     if _verifier is None:
#         _verifier = FactVerifier()
#     return _verifier


# def verify_facts(doc_id: str, summary: dict, chunks: list[dict]) -> dict:
#     return _get_verifier().verify_facts(doc_id, summary, chunks)

"""speed"""
import json
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from app.services.summarizer import invoke_model
from app.db.vector_store import VectorStore
from app.prompts.fact_verifier import FACT_VERIFICATION_PROMPT
from app.core.config import (
    MIN_CLAIM_WORDS,
    TABLE_CHUNK_NUMERIC_THRESHOLD,
    FACT_VERIFIER_TOP_K,
    FACT_VERIFIER_MAX_CLAIMS,
    FACT_VERIFIER_MAX_SOURCE_CHARS,
    FACT_VERIFIER_COVERAGE_THRESHOLD,
    QUERY_ENRICHMENT_MIN_WORDS,
    KEYWORD_FALLBACK_MIN_WORD_LENGTH,
)

logger = logging.getLogger(__name__)

# ── Async worker count ────────────────────────────────────────
# One Bedrock call per claim — all I/O bound, safe to run in parallel.
# FACT_VERIFIER_MAX_CLAIMS typically 20-30, so 16 workers covers most runs.
_VERIFY_WORKERS = 16


class FactVerifier:
    """
    Verifies summary claims against source document chunks using
    vector search + LLM verification.
    """

    def __init__(self):
        self.vector_store = VectorStore()

    # ══════════════════════════════════════════════════════════
    # Public entry point
    # ══════════════════════════════════════════════════════════

    def verify_facts(self, doc_id: str, summary: dict, chunks: list[dict]) -> dict:
        logger.info(f"Starting fact verification | doc_id: {doc_id}")

        prose_chunks, table_chunks = self._classify_chunks(chunks)
        logger.info(f"Chunk classification | prose: {len(prose_chunks)} | table: {len(table_chunks)}")

        claims = self._extract_claims(summary)

        if not claims:
            logger.warning("No claims found in summary to verify")
            return {
                "coverage_score":  0.0,
                "total_claims":    0,
                "supported_claims": 0,
                "flagged_claims":  [],
                "status":          "no_claims_found"
            }

        logger.info(f"Claims to verify: {len(claims)}")

        fallback_chunks = prose_chunks if prose_chunks else chunks

        # OPTIMIZATION: verify ALL claims simultaneously
        # OLD: for claim in claims → sequential, each Bedrock call waits
        #      20 claims × ~2s = ~40s
        # NEW: asyncio.gather → all fire at once
        #      wall time = slowest single claim ~2-3s
        results = self._verify_all_claims_parallel(doc_id, claims, fallback_chunks)

        supported = []
        flagged   = []
        for claim, result in zip(claims, results):
            if result["supported"]:
                supported.append(claim)
            else:
                flagged.append({
                    "claim":      claim,
                    "reason":     result["reason"],
                    "confidence": result["confidence"]
                })

        total           = len(claims)
        supported_count = len(supported)
        coverage_score  = round(supported_count / total, 2) if total > 0 else 0.0
        status          = "ok" if coverage_score >= FACT_VERIFIER_COVERAGE_THRESHOLD else "low_coverage_warning"

        logger.info(f"Fact verification complete | coverage: {coverage_score} | flagged: {len(flagged)}")

        return {
            "coverage_score":   coverage_score,
            "total_claims":     total,
            "supported_claims": supported_count,
            "flagged_claims":   flagged,
            "status":           status
        }

    # ══════════════════════════════════════════════════════════
    # OPTIMIZATION: parallel claim verification
    # ══════════════════════════════════════════════════════════
    # OLD: sequential for loop in verify_facts
    #      each _verify_single_claim blocks until Bedrock responds
    #      20 claims × ~2s per claim = ~40s total
    #
    # NEW: asyncio.gather fires all claims simultaneously
    #      vector search + invoke_model both offloaded to thread pool
    #      wall time = slowest single claim ~2-3s regardless of count

    def _verify_all_claims_parallel(
        self,
        doc_id: str,
        claims: list[str],
        fallback_chunks: list[dict]
    ) -> list[dict]:
        """Fire all claim verifications simultaneously. Returns results in same order as claims."""

        async def _run_all():
            loop     = asyncio.get_event_loop()
            executor = ThreadPoolExecutor(
                max_workers=_VERIFY_WORKERS,
                thread_name_prefix="verify_worker"
            )

            logger.info(
                f"[ASYNC] Firing {len(claims)} claim verifications simultaneously | "
                f"workers={_VERIFY_WORKERS}"
            )

            async def _one_claim(claim: str, idx: int) -> dict:
                try:
                    result = await loop.run_in_executor(
                        executor,
                        lambda: self._verify_single_claim(doc_id, claim, fallback_chunks)
                    )
                    status = "supported" if result["supported"] else "flagged"
                    logger.info(f"Claim {idx+1}/{len(claims)} {status} | conf={result['confidence']}")
                    return result
                except Exception as e:
                    logger.error(f"Claim {idx+1} verification error: {e}")
                    return {"supported": False, "confidence": 0.0, "reason": "Verification error"}

            try:
                results = await asyncio.gather(
                    *[_one_claim(claim, i) for i, claim in enumerate(claims)],
                    return_exceptions=True
                )
            finally:
                executor.shutdown(wait=False)

            # Replace any exceptions that slipped through with safe defaults
            return [
                r if isinstance(r, dict)
                else {"supported": False, "confidence": 0.0, "reason": str(r)}
                for r in results
            ]

        # Safe event loop handling — works inside FastAPI and standalone
        try:
            asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, _run_all()).result()
        except RuntimeError:
            return asyncio.run(_run_all())

    # ══════════════════════════════════════════════════════════
    # Single claim verification  (unchanged logic)
    # ══════════════════════════════════════════════════════════
    # Called by _verify_all_claims_parallel via run_in_executor.
    # Logic identical to original — just no longer called in a loop.

    def _verify_single_claim(
        self, doc_id: str, claim: str, fallback_chunks: list[dict]
    ) -> dict:
        try:
            enriched_query = self._enrich_query(claim)
            logger.debug(f"Verifying: '{claim}' | query: '{enriched_query}'")

            relevant_chunks = self.vector_store.find_supporting_chunks(
                doc_id=doc_id,
                claim=enriched_query,
                top_k=FACT_VERIFIER_TOP_K
            )

            if not relevant_chunks:
                logger.warning(f"Vector search returned no results for: {claim[:50]} — using keyword fallback")
                relevant_chunks = self._keyword_fallback(claim, fallback_chunks)
            else:
                prose_results  = [r for r in relevant_chunks if not self._is_table_chunk(r.get("content", ""))]
                relevant_chunks = prose_results if prose_results else self._keyword_fallback(claim, fallback_chunks)

            combined_source = "\n\n".join(
                r.get("content", "") for r in relevant_chunks
            )[:FACT_VERIFIER_MAX_SOURCE_CHARS]

            if not combined_source.strip():
                return {"supported": False, "confidence": 0.0, "reason": "No source text found to verify against"}

            prompt = FACT_VERIFICATION_PROMPT.format(claim=claim, source_text=combined_source)
            output = invoke_model(prompt, max_tokens=300).strip()

            if "```" in output:
                output = output.replace("```json", "").replace("```", "").strip()

            start, end = output.find("{"), output.rfind("}") + 1
            if start != -1 and end > start:
                output = output[start:end]

            parsed = json.loads(output)
            return {
                "supported":  parsed.get("supported", False),
                "confidence": parsed.get("confidence", 0.0),
                "reason":     parsed.get("reason", "")
            }

        except json.JSONDecodeError:
            logger.error(f"JSON parse failed for claim: {claim[:50]}")
            return {"supported": False, "confidence": 0.0, "reason": "Verification parse error"}

        except Exception:
            logger.exception(f"Claim verification failed: {claim[:50]}")
            return {"supported": False, "confidence": 0.0, "reason": "Verification error"}

    # ══════════════════════════════════════════════════════════
    # All helper methods  (unchanged logic)
    # ══════════════════════════════════════════════════════════

    def _classify_chunks(self, chunks: list[dict]) -> tuple[list[dict], list[dict]]:
        prose, tables = [], []
        for chunk in chunks:
            (tables if self._is_table_chunk(chunk.get("content", "")) else prose).append(chunk)
        return prose, tables

    def _is_table_chunk(self, content: str) -> bool:
        tokens = content.split()
        if not tokens:
            return False
        numeric_count = sum(1 for t in tokens if any(c.isdigit() for c in t))
        return (numeric_count / len(tokens)) > TABLE_CHUNK_NUMERIC_THRESHOLD

    def _extract_claims(self, summary: dict) -> list[str]:
        claims      = []
        exec_summary = summary.get("executive_summary", {})

        claims.extend(exec_summary.get("key_points", []))

        for risk in exec_summary.get("risks", []):
            if self._is_verifiable_claim(risk):
                claims.append(risk)

        for action in exec_summary.get("action_items", []):
            if self._is_verifiable_claim(action):
                claims.append(action)

        for section in summary.get("section_summaries", []):
            for kp in section.get("key_points", [])[:2]:
                if self._is_verifiable_claim(kp):
                    claims.append(kp)
            for risk in section.get("risks", [])[:1]:
                if self._is_verifiable_claim(risk):
                    claims.append(risk)

        seen, unique_claims = set(), []
        for claim in claims:
            normalized = claim.strip().lower()
            if normalized and normalized not in seen:
                seen.add(normalized)
                unique_claims.append(claim.strip())

        logger.info(f"Verifiable claims extracted: {len(unique_claims)}")
        return unique_claims[:FACT_VERIFIER_MAX_CLAIMS]

    def _is_verifiable_claim(self, claim: str) -> bool:
        if not claim or not claim.strip():
            return False
        return len(claim.strip().split()) >= MIN_CLAIM_WORDS

    def _enrich_query(self, claim: str) -> str:
        if len(claim.strip().split()) >= QUERY_ENRICHMENT_MIN_WORDS:
            return claim
        return f"{claim} impact study findings outcomes"

    def _keyword_fallback(self, claim: str, chunks: list[dict]) -> list[dict]:
        claim_words = {
            w.lower() for w in claim.split()
            if len(w) > KEYWORD_FALLBACK_MIN_WORD_LENGTH
        }

        scored = []
        for chunk in chunks:
            content = chunk.get("content", "").lower()
            score   = sum(1 for w in claim_words if w in content)
            if self._is_table_chunk(content):
                score *= 0.5
            scored.append((score, chunk))

        scored.sort(key=lambda x: x[0], reverse=True)
        top = [chunk for score, chunk in scored[:5] if score > 0]
        return top[:3] if top else chunks[:3]


# ══════════════════════════════════════════════════════════════
# Singleton + public API  (unchanged)
# ══════════════════════════════════════════════════════════════

_verifier = None


def _get_verifier() -> FactVerifier:
    global _verifier
    if _verifier is None:
        _verifier = FactVerifier()
    return _verifier


def verify_facts(doc_id: str, summary: dict, chunks: list[dict]) -> dict:
    return _get_verifier().verify_facts(doc_id, summary, chunks)