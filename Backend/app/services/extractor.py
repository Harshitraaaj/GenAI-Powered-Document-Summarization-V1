
import re
import json
import asyncio
import logging
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from app.services.summarizer import invoke_model
from app.utils.doc_id import generate_entity_id
from app.prompts.extractor import (
    ENTITY_EXTRACTION_PROMPT,
    REFERENCE_CHUNK_ENTITY_PROMPT,
)
from app.core.config import settings, REFERENCE_CHUNK_KEYWORDS

logger = logging.getLogger(__name__)

VALID_ENTITY_TYPES = {
    "PERSON", "ORGANIZATION", "LOCATION", "TECHNOLOGY",
    "CONCEPT", "EVENT", "PRODUCT", "DATE", "MODEL", "DATASET"
}


def _is_reference_chunk(content: str) -> bool:
    """Detect if a chunk is a reference/citation list."""
    citation_matches = re.findall(r"\[\d+\]", content)
    if len(citation_matches) >= settings.CITATION_COUNT_THRESHOLD:
        logger.debug(f"Reference chunk detected via citation count: {len(citation_matches)}")
        return True

    keyword_count = sum(
        1 for kw in REFERENCE_CHUNK_KEYWORDS
        if kw.lower() in content.lower()
    )
    if keyword_count >= settings.CITATION_KEYWORD_THRESHOLD:
        logger.debug(f"Reference chunk detected via keyword count: {keyword_count}")
        return True

    return False


def _clean_json_output(output: str) -> str:
    """Strip markdown fences and extract JSON object from LLM output."""
    output = output.strip()
    if output.startswith("```"):
        output = output.replace("```json", "").replace("```", "").strip()

    start = output.find("{")
    end   = output.rfind("}") + 1
    if start != -1 and end > start:
        return output[start:end]

    return output


def _clean_text_for_entities(text: str) -> str:
    """Normalize research paper text before entity extraction."""
    text = re.sub(r"\b[A-Z][a-zA-Z\-]+ et al\.,?\s?\d{0,4}", "", text)
    text = re.sub(r"(Table|Figure)\s+\d+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"(Section|Sections)\s+[A-Za-z0-9\.\-–]+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b[A-Z]\.\d+.*", "", text)
    text = re.sub(r"arXiv:\d+\.\d+", "", text)
    text = re.sub(r"http[s]?://\S+", "", text)
    text = re.sub(r"\[\d+\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


async def _extract_chunk_async(
    chunk: dict,
    loop: asyncio.AbstractEventLoop,
    executor: ThreadPoolExecutor,
) -> list[dict]:
    """Extract entities from a single chunk asynchronously."""
    chunk_id      = chunk.get("chunk_id", "unknown")
    content       = chunk.get("content", "")
    clean_content = _clean_text_for_entities(content)

    if not content.strip():
        logger.warning(f"Empty chunk skipped | chunk_id: {chunk_id}")
        return []

    try:
        logger.info(f"Extracting entities from chunk: {chunk_id}")

        if _is_reference_chunk(content):
            logger.info(f"Reference chunk | chunk_id: {chunk_id}")
            prompt     = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=clean_content[:settings.EXTRACTOR_CONTENT_CHAR_LIMIT])
            max_tokens = settings.EXTRACTOR_REFERENCE_MAX_TOKENS
        else:
            prompt     = ENTITY_EXTRACTION_PROMPT.format(content=clean_content[:settings.EXTRACTOR_CONTENT_CHAR_LIMIT])
            max_tokens = settings.EXTRACTOR_REGULAR_MAX_TOKENS

        invoke_coro = loop.run_in_executor(
            executor,
            lambda: invoke_model(prompt, max_tokens=max_tokens)
        )
        output = await asyncio.wait_for(invoke_coro, timeout=settings.CHUNK_TIMEOUT_SECONDS)

        output   = _clean_json_output(output)
        parsed   = json.loads(output)
        entities = parsed.get("entities", [])

        for entity in entities:
            entity["source_chunk_id"] = chunk_id

        logger.info(f"Extracted {len(entities)} entities from chunk {chunk_id}")
        return entities

    except asyncio.TimeoutError:
        logger.warning(f"Chunk {chunk_id} timed out after {settings.CHUNK_TIMEOUT_SECONDS}s — skipping")
        return []

    except json.JSONDecodeError:
        logger.error(f"JSON parse failed for chunk {chunk_id} — retrying")
        return await _retry_async(chunk_id, content, loop, executor)

    except Exception:
        logger.exception(f"Entity extraction failed for chunk {chunk_id}")
        return []


async def _retry_async(
    chunk_id: int,
    content: str,
    loop: asyncio.AbstractEventLoop,
    executor: ThreadPoolExecutor,
) -> list[dict]:
    """Retry extraction with reduced content on JSON parse failure."""
    try:
        logger.info(f"Retrying chunk {chunk_id} with reduced content")
        retry_prompt = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:settings.EXTRACTOR_RETRY_CONTENT_LIMIT])

        invoke_coro  = loop.run_in_executor(
            executor,
            lambda: invoke_model(retry_prompt, max_tokens=settings.EXTRACTOR_RETRY_MAX_TOKENS)
        )
        retry_output = await asyncio.wait_for(invoke_coro, timeout=settings.RETRY_TIMEOUT_SECONDS)

        retry_output   = _clean_json_output(retry_output)
        retry_parsed   = json.loads(retry_output)
        retry_entities = retry_parsed.get("entities", [])

        for entity in retry_entities:
            entity["source_chunk_id"] = chunk_id

        logger.info(f"Retry successful | chunk {chunk_id} | entities: {len(retry_entities)}")
        return retry_entities

    except Exception:
        logger.warning(f"Retry also failed for chunk {chunk_id} — returning empty")
        return []


async def _extract_all_async(chunks: list[dict]) -> list[dict]:
    """Fire all chunk extractions simultaneously and return flat entity list."""
    loop             = asyncio.get_event_loop()
    effective_workers = max(settings.EXTRACTOR_MAX_WORKERS, settings.EXTRACTOR_ASYNC_WORKERS)
    executor         = ThreadPoolExecutor(
        max_workers=effective_workers,
        thread_name_prefix="entity_worker"
    )

    logger.info(
        f"[ASYNC] Firing {len(chunks)} chunk extractions simultaneously | "
        f"workers={effective_workers}"
    )

    try:
        results = await asyncio.gather(
            *[_extract_chunk_async(chunk, loop, executor) for chunk in chunks],
            return_exceptions=True
        )
    finally:
        executor.shutdown(wait=False)

    all_entities = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Chunk {i} raised exception in gather: {result}")
        elif isinstance(result, list):
            all_entities.extend(result)

    return all_entities


def extract_entities_from_chunks(chunks: list[dict]) -> list[dict]:
    """Extract and deduplicate entities across all chunks."""
    if not chunks:
        logger.warning("No chunks provided for entity extraction")
        return []

    logger.info(f"Starting entity extraction | total chunks: {len(chunks)}")

    try:
        # Already inside a running event loop (e.g. FastAPI async context)
        asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            all_entities = pool.submit(asyncio.run, _extract_all_async(chunks)).result()
    except RuntimeError:
        all_entities = asyncio.run(_extract_all_async(chunks))

    logger.info(f"Raw entities extracted: {len(all_entities)}")

    filtered = filter_entities(all_entities)
    logger.info(f"Entities after filtering: {len(filtered)}")

    deduplicated = deduplicate_entities(filtered)
    logger.info(f"Entities after deduplication: {len(deduplicated)}")

    return deduplicated


def extract_entities_from_chunk(chunk: dict) -> list[dict]:
    """Single-chunk synchronous extraction. Use extract_entities_from_chunks() for bulk."""
    chunk_id = chunk.get("chunk_id", "unknown")
    content  = chunk.get("content", "")

    if not content.strip():
        return []

    try:
        if _is_reference_chunk(content):
            prompt     = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:settings.EXTRACTOR_CONTENT_CHAR_LIMIT])
            max_tokens = settings.EXTRACTOR_REFERENCE_MAX_TOKENS
        else:
            prompt     = ENTITY_EXTRACTION_PROMPT.format(content=content[:settings.EXTRACTOR_CONTENT_CHAR_LIMIT])
            max_tokens = settings.EXTRACTOR_REGULAR_MAX_TOKENS

        output   = invoke_model(prompt, max_tokens=max_tokens)
        output   = _clean_json_output(output)
        parsed   = json.loads(output)
        entities = parsed.get("entities", [])

        for entity in entities:
            entity["source_chunk_id"] = chunk_id

        return entities

    except json.JSONDecodeError:
        return _retry_extraction_sync(chunk_id, content)
    except Exception:
        logger.exception(f"Entity extraction failed for chunk {chunk_id}")
        return []


def _retry_extraction_sync(chunk_id: int, content: str) -> list[dict]:
    """Sync fallback retry — used only by extract_entities_from_chunk."""
    try:
        retry_prompt   = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:settings.EXTRACTOR_RETRY_CONTENT_LIMIT])
        retry_output   = invoke_model(retry_prompt, max_tokens=settings.EXTRACTOR_RETRY_MAX_TOKENS)
        retry_output   = _clean_json_output(retry_output)
        retry_parsed   = json.loads(retry_output)
        retry_entities = retry_parsed.get("entities", [])

        for entity in retry_entities:
            entity["source_chunk_id"] = chunk_id

        return retry_entities
    except Exception:
        logger.warning(f"Retry also failed for chunk {chunk_id}")
        return []


def filter_entities(entities: list[dict]) -> list[dict]:
    """Remove low-quality entities before deduplication."""
    filtered = []
    for entity in entities:
        name        = entity.get("name", "").strip()
        entity_type = entity.get("type", "OTHER")

        if len(name) < settings.MIN_ENTITY_NAME_LENGTH:
            logger.debug(f"Filtered short entity: '{name}'")
            continue
        if name.isdigit():
            logger.debug(f"Filtered numeric entity: '{name}'")
            continue
        if entity_type in settings.SKIP_ENTITY_TYPES:
            logger.debug(f"Filtered type '{entity_type}' entity: '{name}'")
            continue

        filtered.append(entity)
    return filtered


def deduplicate_entities(entities: list[dict]) -> list[dict]:
    """
    Merge duplicate entities by name.
    Name-only dedup ensures a single Neo4j node per entity regardless of
    which type was assigned per chunk.
    """
    seen: dict[str, dict] = {}
    for entity in entities:
        name        = entity.get("name", "").strip().lower()
        entity_type = entity.get("type", "OTHER")

        if name not in seen:
            seen[name] = {
                "name":             entity.get("name", "").strip(),
                "type":             entity_type,
                "context":          entity.get("context", ""),
                "source_chunk_ids": []
            }

        source = entity.get("source_chunk_id")
        if source and source not in seen[name]["source_chunk_ids"]:
            seen[name]["source_chunk_ids"].append(source)

    return list(seen.values())


def format_entities_for_graph(doc_id: str, entities: list[dict]) -> list[dict]:
    """Attach entity_id to each entity for Neo4j node creation."""
    return [
        {
            **entity,
            "entity_id": generate_entity_id(doc_id, entity["name"], entity["type"]),
            "doc_id":    doc_id,
        }
        for entity in entities
    ]


def compute_entity_accuracy(entities: list[dict]) -> dict:
    """
    Compute entity extraction accuracy across 3 quality dimensions:
    - Type accuracy:    % with a valid entity type
    - Context accuracy: % with non-empty context
    - Source accuracy:  % grounded in a source chunk
    Overall = average of the 3 dimensions.
    """
    total = len(entities)

    if total == 0:
        logger.warning("No entities to compute accuracy on")
        return {
            "total_entities":   0,
            "type_accuracy":    0.0,
            "context_accuracy": 0.0,
            "source_accuracy":  0.0,
            "overall_accuracy": 0.0
        }

    typed_correctly = sum(1 for e in entities if e.get("type", "").upper() in VALID_ENTITY_TYPES)
    has_context     = sum(1 for e in entities if e.get("context", "").strip())
    has_source      = sum(1 for e in entities if e.get("source_chunk_ids"))

    type_accuracy    = round(typed_correctly / total, 2)
    context_accuracy = round(has_context / total, 2)
    source_accuracy  = round(has_source / total, 2)
    overall_accuracy = round((type_accuracy + context_accuracy + source_accuracy) / 3, 2)

    logger.info(
        f"Entity accuracy | type: {type_accuracy} | "
        f"context: {context_accuracy} | source: {source_accuracy} | "
        f"overall: {overall_accuracy}"
    )

    return {
        "total_entities":   total,
        "type_accuracy":    type_accuracy,
        "context_accuracy": context_accuracy,
        "source_accuracy":  source_accuracy,
        "overall_accuracy": overall_accuracy
    }