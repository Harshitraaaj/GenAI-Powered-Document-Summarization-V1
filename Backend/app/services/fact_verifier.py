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
    FACT_VERIFIER_WORKERS,
)

logger = logging.getLogger(__name__)


class FactVerifier:

    def __init__(self, vector_store: VectorStore = None):
        if vector_store is not None:
            self.vector_store = vector_store
        else:
            logger.warning(
                "FactVerifier created without injected VectorStore — "
                "creating its own. This adds ~22s cold-start on first call. "
                "Pass vector_store= from app.state to avoid this."
            )
            self.vector_store = VectorStore()

    def verify_facts(self, doc_id: str, summary: dict, chunks: list[dict]) -> dict:
        logger.info(f"Starting fact verification | doc_id: {doc_id}")

        prose_chunks, table_chunks = self._classify_chunks(chunks)
        logger.info(f"Chunk classification | prose: {len(prose_chunks)} | table: {len(table_chunks)}")

        claims = self._extract_claims(summary)

        if not claims:
            logger.warning("No claims found in summary to verify")
            return {
                "coverage_score":   0.0,
                "total_claims":     0,
                "supported_claims": 0,
                "flagged_claims":   [],
                "status":           "no_claims_found"
            }

        logger.info(f"Claims to verify: {len(claims)}")

        fallback_chunks = prose_chunks if prose_chunks else chunks

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

    def _verify_all_claims_parallel(
        self,
        doc_id: str,
        claims: list[str],
        fallback_chunks: list[dict]
    ) -> list[dict]:

        async def _run_all():
            loop     = asyncio.get_event_loop()
            executor = ThreadPoolExecutor(
                max_workers=FACT_VERIFIER_WORKERS,
                thread_name_prefix="verify_worker"
            )

            logger.info(
                f"[ASYNC] Firing {len(claims)} claim verifications simultaneously | "
                f"workers={FACT_VERIFIER_WORKERS}"
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

            return [
                r if isinstance(r, dict)
                else {"supported": False, "confidence": 0.0, "reason": str(r)}
                for r in results
            ]

        try:
            asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, _run_all()).result()
        except RuntimeError:
            return asyncio.run(_run_all())

    def _verify_single_claim(
        self, doc_id: str, claim: str, fallback_chunks: list[dict]
    ) -> dict:
        try:
            enriched_query = self._enrich_query(claim)
            logger.debug(f"Verifying: '{claim}' | query: '{enriched_query}'")

            vector_chunks = self.vector_store.find_supporting_chunks(
                doc_id=doc_id,
                claim=enriched_query,
                top_k=FACT_VERIFIER_TOP_K
            )

            keyword_chunks = self._keyword_fallback(claim, fallback_chunks)

            if not vector_chunks:
                logger.warning(f"Vector search empty for: {claim[:50]} — using keyword only")
                relevant_chunks = keyword_chunks
            else:
                seen_ids = {r.get("chunk_id") for r in vector_chunks}
                merged   = list(vector_chunks)
                for kc in keyword_chunks:
                    if kc.get("chunk_id") not in seen_ids:
                        merged.append(kc)
                        seen_ids.add(kc.get("chunk_id"))
                relevant_chunks = merged
                logger.debug(
                    f"Merged chunks | vector={len(vector_chunks)} "
                    f"keyword={len(keyword_chunks)} merged={len(relevant_chunks)}"
                )

            combined_source = "\n\n".join(
                r.get("content", "") for r in relevant_chunks
            )[:FACT_VERIFIER_MAX_SOURCE_CHARS]

            if not combined_source.strip():
                return {
                    "supported":  False,
                    "confidence": 0.5,
                    "reason":     "No source text found to verify against"
                }

            prompt = FACT_VERIFICATION_PROMPT.format(
                claim=claim,
                source_text=combined_source
            )
            output = invoke_model(prompt, max_tokens=300).strip()
            parsed = self._extract_json(output)

            confidence = parsed.get("confidence")
            if confidence is None:
                logger.warning(
                    f"Model returned no confidence for: {claim[:50]} — defaulting to 0.5"
                )
                confidence = 0.5
            confidence = max(0.0, min(1.0, float(confidence)))

            return {
                "supported":  parsed.get("supported", False),
                "confidence": confidence,
                "reason":     parsed.get("reason", "")
            }

        except json.JSONDecodeError:
            logger.error(f"JSON parse failed for claim: {claim[:50]}")
            return {"supported": False, "confidence": 0.0, "reason": "Verification parse error"}

        except Exception:
            logger.exception(f"Claim verification failed: {claim[:50]}")
            return {"supported": False, "confidence": 0.0, "reason": "Verification error"}

    @staticmethod
    def _extract_json(text: str) -> dict:
        text = text.strip()
        if "```" in text:
            text = text.replace("```json", "").replace("```", "").strip()
        start = text.find("{")
        end   = text.rfind("}") + 1
        if start != -1 and end > start:
            text = text[start:end]
        return json.loads(text)

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
        claims       = []
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


_verifier: FactVerifier | None = None


def init_verifier(vector_store: VectorStore) -> None:
    global _verifier
    _verifier = FactVerifier(vector_store=vector_store)
    logger.info("FactVerifier initialized with shared VectorStore")


def _get_verifier() -> FactVerifier:
    global _verifier
    if _verifier is None:
        _verifier = FactVerifier()
    return _verifier


def verify_facts(doc_id: str, summary: dict, chunks: list[dict]) -> dict:
    return _get_verifier().verify_facts(doc_id, summary, chunks)