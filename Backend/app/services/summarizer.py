import json
import time
import boto3
import logging
from typing import Dict, Any, List, Optional
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import pytesseract

from app.prompts.summarizer_prompts import (
    SYSTEM_PROMPT,
    CHUNK_SUMMARY_PROMPT,
    SECTION_SUMMARY_PROMPT,
    EXECUTIVE_SUMMARY_PROMPT,
)
from app.core.config import settings

pytesseract.pytesseract.tesseract_cmd = settings.TESSERACT_CMD

logger = logging.getLogger(__name__)

bedrock_client = boto3.client(
    service_name="bedrock-runtime",
    region_name=settings.AWS_REGION,
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    aws_session_token=settings.AWS_SESSION_TOKEN,
)


def clean_model_output(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()
    return text


def extract_json_from_output(text: str) -> dict:
    text = text.strip()
    if "```" in text:
        text = text.replace("```json", "").replace("```", "").strip()
    start = text.find("{")
    end   = text.rfind("}") + 1
    if start != -1 and end > start:
        text = text[start:end]
    return json.loads(text)


def safe_json_parse(text: str) -> Dict[str, Any]:
    if not text:
        logger.warning("Empty model output received")
        return {"error": "Empty model output"}
    try:
        return extract_json_from_output(text)
    except json.JSONDecodeError:
        logger.error("Invalid JSON returned by model")
        return {"error": "Invalid JSON returned by model", "raw_output": text}


def normalize_summary_fields(parsed: Dict[str, Any]) -> Dict[str, Any]:
    required_fields = {
        "summary":      "",
        "tldr":         "",
        "key_points":   [],
        "risks":        [],
        "action_items": [],
    }
    for field, default in required_fields.items():
        parsed.setdefault(field, default)
    return parsed


def invoke_model(
    prompt: str,
    max_tokens: int = settings.MODEL_MAX_TOKENS,
    temperature: float = settings.MODEL_TEMPERATURE,
    retries: int = settings.MODEL_RETRIES,
) -> str:
    last_exception = None
    logger.info("Invoking Bedrock model")

    formatted_prompt = (
        "<|begin_of_text|>"
        "<|start_header_id|>system<|end_header_id|>"
        f"{SYSTEM_PROMPT}"
        "<|eot_id|>"
        "<|start_header_id|>user<|end_header_id|>"
        f"{prompt}"
        "<|eot_id|>"
        "<|start_header_id|>assistant<|end_header_id|>"
    )

    body = {
        "prompt":      formatted_prompt,
        "max_gen_len": max_tokens,
        "temperature": temperature,
        "top_p":       settings.MODEL_TOP_P,
    }

    start_time = time.time()

    for attempt in range(retries + 1):
        try:
            response = bedrock_client.invoke_model(
                modelId=settings.BEDROCK_MODEL_ID,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json",
            )

            latency = round(time.time() - start_time, 2)
            logger.info("Bedrock response received | latency=%.2fs", latency)

            response_body = json.loads(response["body"].read())
            content       = response_body.get("generation", "")

            if not content:
                raise RuntimeError("Empty model response")

            return clean_model_output(content)

        except ClientError as e:
            last_exception = e
            error_code = e.response["Error"]["Code"]
            logger.warning("Bedrock ClientError | code=%s", error_code)
            if error_code in ("ThrottlingException", "TooManyRequestsException"):
                time.sleep(settings.RETRY_BASE_DELAY ** attempt)
                continue
            time.sleep(settings.RETRY_BASE_DELAY)

        except Exception as e:
            last_exception = e
            logger.exception("Model invocation failed")
            time.sleep(settings.RETRY_BASE_DELAY)

    logger.error("Bedrock invocation failed after retries")
    raise RuntimeError(f"Bedrock invocation failed: {last_exception}")


def summarize_chunk(chunk: Dict) -> Dict:
    logger.info("Summarizing chunk | chunk_id=%s", chunk["chunk_id"])
    prompt = CHUNK_SUMMARY_PROMPT.format(content=chunk["content"])
    output = invoke_model(prompt)
    parsed = normalize_summary_fields(safe_json_parse(output))
    parsed["chunk_id"] = chunk["chunk_id"]
    return parsed


def summarize_section(section_id: int, chunk_summaries: List[Dict]) -> Dict:
    logger.info("Generating section summary | section_id=%d", section_id)

    combined_text = ""
    source_chunks = []

    for c in chunk_summaries:
        if c.get("summary"):
            combined_text += f"\nChunk {c['chunk_id']}:\n{c['summary']}\n"
            source_chunks.append(c["chunk_id"])

    if not combined_text.strip():
        logger.warning("Section skipped — no content | section_id=%d", section_id)
        return {
            "section_id":    section_id,
            "summary":       "",
            "tldr":          "",
            "key_points":    [],
            "risks":         [],
            "action_items":  [],
            "source_chunks": [],
        }

    prompt = SECTION_SUMMARY_PROMPT.format(content=combined_text)
    output = invoke_model(prompt, max_tokens=settings.SECTION_MAX_TOKENS)
    parsed = normalize_summary_fields(safe_json_parse(output))

    if not parsed.get("summary") and combined_text.strip():
        logger.warning("Section returned empty summary — retrying | section_id=%d", section_id)
        output = invoke_model(prompt, max_tokens=settings.SECTION_MAX_TOKENS)
        parsed = normalize_summary_fields(safe_json_parse(output))

        if not parsed.get("summary"):
            logger.error("Section retry failed — using fallback | section_id=%d", section_id)
            fallback_summary = " ".join(
                c.get("tldr", "") for c in chunk_summaries if c.get("tldr")
            )
            parsed["summary"] = (
                fallback_summary[:settings.SECTION_FALLBACK_SUMMARY_LIMIT]
                if fallback_summary else ""
            )

    parsed["section_id"]    = section_id
    parsed["source_chunks"] = source_chunks
    return parsed


def summarize_executive(section_summaries: List[Dict]) -> Dict:
    logger.info("Generating executive summary")

    combined_text   = ""
    source_sections = []

    for s in section_summaries:
        if s.get("summary"):
            combined_text += f"\nSection {s['section_id']}:\n{s['summary']}\n"
            if s.get("risks"):
                risks_text     = "\n".join(f"- {r}" for r in s["risks"])
                combined_text += f"Risks:\n{risks_text}\n"
            if s.get("action_items"):
                actions_text   = "\n".join(f"- {a}" for a in s["action_items"])
                combined_text += f"Action items:\n{actions_text}\n"
            source_sections.append(s["section_id"])

    if not combined_text.strip():
        logger.warning("Executive summary skipped — no section content")
        return normalize_summary_fields({"source_sections": []})

    prompt = EXECUTIVE_SUMMARY_PROMPT.format(content=combined_text)
    output = invoke_model(prompt, max_tokens=settings.EXECUTIVE_MAX_TOKENS)
    parsed = normalize_summary_fields(safe_json_parse(output))
    parsed["source_sections"] = source_sections
    return parsed


def parallel_chunk_summarization(chunks: List[Dict]) -> List[Dict]:
    logger.info("Parallel chunk summarization | workers=%d", settings.MAX_WORKERS)

    results = []
    with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
        futures = {
            executor.submit(summarize_chunk, chunk): chunk["chunk_id"]
            for chunk in chunks
        }
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda x: x["chunk_id"])
    return results


def parallel_section_summarization(grouped: List[List[Dict]]) -> List[Dict]:
    logger.info(
        "Parallel section summarization | sections=%d | workers=%d",
        len(grouped), settings.MAX_WORKERS,
    )

    results = {}
    with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
        futures = {
            executor.submit(summarize_section, i + 1, sec): i
            for i, sec in enumerate(grouped)
        }
        for future in as_completed(futures):
            i          = futures[future]
            results[i] = future.result()

    return [results[i] for i in sorted(results.keys())]


def group_into_sections(
    chunk_summaries: List[Dict],
    group_size: int = settings.SECTION_GROUP_SIZE,
):
    return [
        chunk_summaries[i: i + group_size]
        for i in range(0, len(chunk_summaries), group_size)
    ]


def _delayed_store_embeddings(
    vector_store, doc_id: str, chunks: List[Dict], delay: float
) -> bool:
    logger.info("Delaying embedding start by %.1fs to avoid competing cold starts", delay)
    time.sleep(delay)
    logger.info("Delay done — firing store_embeddings | doc_id=%s", doc_id)
    return vector_store.store_embeddings(doc_id, chunks)


def run_hierarchical_summarization(
    chunks: List[Dict],
    doc_id: Optional[str] = None,
    vector_store=None,
) -> Dict:
    logger.info("Hierarchical summarization started")

    if not chunks:
        raise ValueError("No chunks provided")

    total_chunks   = len(chunks)
    embed_future: Optional[Future] = None
    embed_executor = None

    if doc_id and vector_store:
        logger.info(
            "Scheduling store_embeddings with %ss delay | doc_id=%s",
            settings.EMBED_START_DELAY_SECONDS, doc_id,
        )
        embed_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=settings.EMBED_THREAD_NAME_PREFIX,
        )
        embed_future = embed_executor.submit(
            _delayed_store_embeddings,
            vector_store, doc_id, chunks, settings.EMBED_START_DELAY_SECONDS,
        )
    else:
        logger.info("No doc_id/vector_store — embedding skipped")

    t0 = time.time()
    chunk_summaries = parallel_chunk_summarization(chunks)
    logger.info("Chunk summarization done | elapsed=%.2fs", time.time() - t0)

    valid_chunks = [c for c in chunk_summaries if c.get("summary")]
    grouped      = group_into_sections(valid_chunks)

    t0 = time.time()
    section_summaries = parallel_section_summarization(grouped)
    logger.info("Section summarization done | elapsed=%.2fs", time.time() - t0)

    valid_sections = [s for s in section_summaries if s.get("summary")]

    t0 = time.time()
    executive_summary = summarize_executive(valid_sections)
    logger.info("Executive summary done | elapsed=%.2fs", time.time() - t0)

    if embed_future is not None:
        t0 = time.time()
        try:
            embed_success = embed_future.result()
            wait_time     = round(time.time() - t0, 3)
            if wait_time > settings.EMBED_WAIT_WARN_THRESHOLD:
                logger.warning(
                    "Waited %.3fs for embedding to finish — "
                    "consider reducing EMBED_START_DELAY_SECONDS from %s to %s",
                    wait_time,
                    settings.EMBED_START_DELAY_SECONDS,
                    max(0, settings.EMBED_START_DELAY_SECONDS - settings.EMBED_DELAY_REDUCTION_HINT),
                )
            else:
                logger.info(
                    "Embedding already done | waited=%.3fs | success=%s",
                    wait_time, embed_success,
                )
        except Exception:
            logger.exception("Background embedding failed — summary returned without vector search for this doc")
        finally:
            embed_executor.shutdown(wait=False)

    all_chunk_ids     = {c["chunk_id"] for c in chunks}
    summarized_ids    = {c["chunk_id"] for c in chunk_summaries if c.get("summary")}
    missing_chunk_ids = sorted(all_chunk_ids - summarized_ids)
    missing_sections  = len([s for s in section_summaries if not s.get("summary")])
    covered_chunk_ids = sorted(
        {cid for s in section_summaries for cid in s.get("source_chunks", [])}
    )

    synthesis_coverage_percent = round(
        (len(summarized_ids) / total_chunks) * 100, 2
    )

    status = "ok"
    if synthesis_coverage_percent < settings.LOW_COVERAGE_THRESHOLD:
        status = "low_coverage_warning"
    if missing_chunk_ids:
        status = "chunks_missing_warning"
        logger.warning("Missing chunk summaries | chunk_ids=%s", missing_chunk_ids)
    if missing_sections > 0:
        status = "section_missing_warning"
        logger.warning("Missing section summaries | count=%d", missing_sections)

    metadata = {
        "total_chunks":     total_chunks,
        "valid_chunks":     len(summarized_ids),
        "coverage_percent": synthesis_coverage_percent,
        "missing_sections": missing_sections,
        "status":           status,
        "coverage_details": {
            "covered_chunk_ids": covered_chunk_ids,
            "missing_chunk_ids": missing_chunk_ids,
        },
    }

    if missing_chunk_ids:
        logger.info(
            "Coverage | total=%d | summarized=%d | missing_chunks=%s | missing_sections=%d",
            total_chunks, len(summarized_ids), missing_chunk_ids, missing_sections,
        )

    return {
        "metadata":          metadata,
        "chunk_summaries":   chunk_summaries,
        "section_summaries": section_summaries,
        "executive_summary": executive_summary,
    }