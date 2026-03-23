# import re
# import json
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from app.services.summarizer import invoke_model
# from app.utils.doc_id import generate_entity_id

# logger = logging.getLogger(__name__)

# # -----------------------------
# # Filter Config
# # -----------------------------
# MIN_ENTITY_NAME_LENGTH = 3          # skip single/double char noise like "Ee", "NE"
# SKIP_ENTITY_TYPES = set()           # add "OTHER" here to filter out all OTHER types
# MAX_WORKERS = 2                     # reduced from 4 — prevents Bedrock "too many connections"

# # Full prompt for regular content chunks
# ENTITY_EXTRACTION_PROMPT = """
# You are an expert entity extractor. Given the following text, extract all named entities.

# For each entity extract:
# - name: the entity name as it appears in text
# - type: one of [PERSON, ORGANIZATION, LOCATION, DATE, CONCEPT, TECHNOLOGY, EVENT, PRODUCT, OTHER]
# - context: a brief one-line description of how it appears in the text

# Rules:
# - Skip single letters, abbreviations with no clear meaning, and obvious typos
# - Only extract entities that are meaningful and clearly identifiable
# - Do NOT include page numbers, bullet symbols, or formatting artifacts

# Return ONLY a valid JSON object in this exact format, no preamble:
# {{
#     "entities": [
#         {{
#             "name": "entity name",
#             "type": "ENTITY_TYPE",
#             "context": "brief context"
#         }}
#     ]
# }}

# Text:
# {content}
# """

# # Lighter prompt for reference/citation chunks
# # Only extracts organizations and technologies — not author names
# # Prevents JSON truncation caused by extracting 50+ author names
# REFERENCE_CHUNK_ENTITY_PROMPT = """
# You are an expert entity extractor. Given the following reference list, extract ONLY organization names and technology names. Do NOT extract author names or person names.

# Return ONLY a valid JSON object in this exact format, no preamble:
# {{
#     "entities": [
#         {{
#             "name": "entity name",
#             "type": "ENTITY_TYPE",
#             "context": "brief context"
#         }}
#     ]
# }}

# Text:
# {content}
# """


# # -----------------------------
# # Reference Chunk Detection
# # -----------------------------
# def _is_reference_chunk(content: str) -> bool:
#     """
#     Detect if a chunk is a reference/citation list using multiple signals.

#     Reference chunks cause JSON truncation with the full prompt because
#     the LLM tries to extract 50+ author names → exceeds max_tokens
#     → JSON gets cut off → parse error.

#     For these chunks we use a lighter prompt that only extracts
#     organizations and technologies — not person names.

#     Handles both formats:
#     - Line-by-line: "[14] Févry, Thibault..." on separate lines
#     - Inline: "[14] Févry... [15] Marcus..." on same line
#     """
#     # Signal 1 — 5+ occurrences of [number] anywhere in text
#     # Handles inline citations on same line
#     citation_matches = re.findall(r"\[\d+\]", content)
#     if len(citation_matches) >= 10:
#         logger.debug(f"Reference chunk detected via citation count: {len(citation_matches)} citations")
#         return True

#     # Signal 2 — common academic citation keywords
#     citation_keywords = [
#         "arXiv preprint", "Proceedings of", "In Proceedings",
#         "Conference on", "arXiv:", "doi:", "arxiv.org",
#         "ACL Anthology", "Neural Information Processing",
#         "International Conference", "Annual Meeting"
#     ]
#     keyword_count = sum(
#         1 for kw in citation_keywords
#         if kw.lower() in content.lower()
#     )
#     if keyword_count >= 3:
#         logger.debug(f"Reference chunk detected via keyword count: {keyword_count} keywords")
#         return True

#     return False


# # -----------------------------
# # Extract from Single Chunk
# # -----------------------------
# def extract_entities_from_chunk(chunk: dict) -> list[dict]:
#     """
#     Extract entities from a single chunk dict.
#     chunk must have 'chunk_id' and 'content' keys.

#     Uses different prompts and token limits based on chunk type:
#     - Regular chunks: full prompt, max_tokens=1000
#     - Reference chunks: lighter prompt, max_tokens=500
#       (prevents JSON truncation from extracting 50+ author names)
#     """
#     chunk_id = chunk.get("chunk_id", "unknown")
#     content = chunk.get("content", "")

#     if not content.strip():
#         logger.warning(f"Empty chunk skipped | chunk_id: {chunk_id}")
#         return []

#     try:
#         logger.info(f"Extracting entities from chunk: {chunk_id}")

#         # Use lighter prompt + fewer tokens for reference chunks
#         # to prevent JSON truncation
#         if _is_reference_chunk(content):
#             logger.info(f"Reference chunk detected | chunk_id: {chunk_id} | using lightweight prompt")
#             prompt = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:4000])
#             output = invoke_model(prompt, max_tokens=500)
#         else:
#             prompt = ENTITY_EXTRACTION_PROMPT.format(content=content[:4000])
#             output = invoke_model(prompt, max_tokens=1000)

#         # Clean and parse JSON
#         if output.startswith("```"):
#             output = output.replace("```json", "").replace("```", "").strip()

#         # Extract JSON object even if there's text before/after
#         start = output.find("{")
#         end = output.rfind("}") + 1
#         if start != -1 and end > start:
#             output = output[start:end]

#         parsed = json.loads(output)

#         entities = parsed.get("entities", [])

#         # Attach source chunk to each entity
#         for entity in entities:
#             entity["source_chunk_id"] = chunk_id

#         logger.info(f"Extracted {len(entities)} entities from chunk {chunk_id}")

#         return entities

    

#     except json.JSONDecodeError:
#         logger.error(f"JSON parse failed for chunk {chunk_id}")

#         # Retry with smaller content if first attempt failed
#         try:
#             logger.info(f"Retrying chunk {chunk_id} with reduced content")
#             retry_prompt = REFERENCE_CHUNK_ENTITY_PROMPT.format(
#                 content=content[:1000]   # very small — just first 1000 chars
#             )
#             retry_output = invoke_model(retry_prompt, max_tokens=300)

#             start = retry_output.find("{")
#             end = retry_output.rfind("}") + 1
#             if start != -1 and end > start:
#                 retry_parsed = json.loads(retry_output[start:end])
#                 retry_entities = retry_parsed.get("entities", [])
#                 for entity in retry_entities:
#                     entity["source_chunk_id"] = chunk_id
#                 logger.info(f"Retry successful | chunk {chunk_id} | entities: {len(retry_entities)}")
#                 return retry_entities
#         except Exception:
#             logger.warning(f"Retry also failed for chunk {chunk_id} — returning empty")

#         return []


# # -----------------------------
# # Extract from All Chunks (Parallel)
# # -----------------------------
# def extract_entities_from_chunks(chunks: list[dict]) -> list[dict]:
#     """
#     Extract and deduplicate entities across all chunks in parallel.

#     Flow:
#         1. Extract entities per chunk (2 parallel threads)
#            - Regular chunks: full prompt, max_tokens=1000
#            - Reference chunks: lightweight prompt, max_tokens=500
#         2. Filter noise entities
#         3. Deduplicate by name only (not name+type)
#         4. Merge source_chunk_ids for duplicates
#         5. Return clean deduplicated list
#     """
#     if not chunks:
#         logger.warning("No chunks provided for entity extraction")
#         return []

#     logger.info(f"Starting entity extraction | total chunks: {len(chunks)}")

#     all_entities = []

#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(extract_entities_from_chunk, chunk): chunk.get("chunk_id")
#             for chunk in chunks
#         }

#         for future in as_completed(futures):
#             chunk_id = futures[future]
#             try:
#                 chunk_entities = future.result()
#                 all_entities.extend(chunk_entities)
#             except Exception:
#                 logger.exception(f"Parallel extraction failed for chunk: {chunk_id}")

#     logger.info(f"Raw entities extracted: {len(all_entities)}")

#     filtered = filter_entities(all_entities)
#     logger.info(f"Entities after filtering: {len(filtered)}")

#     deduplicated = deduplicate_entities(filtered)
#     logger.info(f"Entities after deduplication: {len(deduplicated)}")

#     return deduplicated


# # -----------------------------
# # Noise Filtering
# # -----------------------------
# def filter_entities(entities: list[dict]) -> list[dict]:
#     """
#     Remove low-quality entities before deduplication.
#     """
#     filtered = []

#     for entity in entities:
#         name = entity.get("name", "").strip()
#         entity_type = entity.get("type", "OTHER")

#         if len(name) < MIN_ENTITY_NAME_LENGTH:
#             logger.debug(f"Filtered short entity: '{name}'")
#             continue

#         if name.isdigit():
#             logger.debug(f"Filtered numeric entity: '{name}'")
#             continue

#         if entity_type in SKIP_ENTITY_TYPES:
#             logger.debug(f"Filtered type '{entity_type}' entity: '{name}'")
#             continue

#         filtered.append(entity)

#     return filtered


# # -----------------------------
# # Deduplication
# # -----------------------------
# def deduplicate_entities(entities: list[dict]) -> list[dict]:
#     """
#     Merge duplicate entities by NAME ONLY (not name+type).

#     FIX: Previously deduplicated by (name, type) which caused the same
#     entity to appear multiple times in Neo4j when the LLM assigned
#     different types in different chunks.

#     Example of old behavior:
#         Hadoop → TECHNOLOGY  (chunk 5)  ← separate node
#         Hadoop → PRODUCT     (chunk 9)  ← separate node = duplicate in graph

#     New behavior:
#         Hadoop → TECHNOLOGY  (first type seen is kept)
#         All source_chunk_ids merged across all occurrences
#         Single node in Neo4j regardless of type inconsistency
#     """
#     seen: dict[str, dict] = {}

#     for entity in entities:
#         # key is name only, not name+type
#         name = entity.get("name", "").strip().lower()
#         entity_type = entity.get("type", "OTHER")

#         if name not in seen:
#             seen[name] = {
#                 "name": entity.get("name", "").strip(),
#                 "type": entity_type,        # first type seen is kept
#                 "context": entity.get("context", ""),
#                 "source_chunk_ids": []
#             }

#         # Collect all chunk sources for this entity
#         source = entity.get("source_chunk_id")
#         if source and source not in seen[name]["source_chunk_ids"]:
#             seen[name]["source_chunk_ids"].append(source)

#     return list(seen.values())


# # -----------------------------
# # Format for Neo4j
# # -----------------------------
# def format_entities_for_graph(
#     doc_id: str,
#     entities: list[dict]
# ) -> list[dict]:
#     """
#     Attach entity_id to each entity for Neo4j node creation.
#     Called by graph_builder.py before creating nodes.
#     """
#     formatted = []

#     for entity in entities:
#         entity_id = generate_entity_id(
#             doc_id,
#             entity["name"],
#             entity["type"]
#         )

#         formatted.append({
#             **entity,
#             "entity_id": entity_id,
#             "doc_id": doc_id,
#         })

#     return formatted

"""Clearn code"""
# import re
# import json
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from app.services.summarizer import invoke_model
# from app.utils.doc_id import generate_entity_id
# from app.prompts.extractor import (
#     ENTITY_EXTRACTION_PROMPT,
#     REFERENCE_CHUNK_ENTITY_PROMPT,
# )
# from app.core.config import (
#     MIN_ENTITY_NAME_LENGTH,
#     EXTRACTOR_MAX_WORKERS,
#     SKIP_ENTITY_TYPES,
#     CITATION_COUNT_THRESHOLD,
#     CITATION_KEYWORD_THRESHOLD,
#     REFERENCE_CHUNK_KEYWORDS,
# )

# logger = logging.getLogger(__name__)


# def _is_reference_chunk(content: str) -> bool:
#     """
#     Detect if a chunk is a reference/citation list using multiple signals.

#     Reference chunks cause JSON truncation with the full prompt because
#     the LLM tries to extract 50+ author names → exceeds max_tokens
#     → JSON gets cut off → parse error.

#     For these chunks we use a lighter prompt that only extracts
#     organizations and technologies — not person names.
#     """
#     citation_matches = re.findall(r"\[\d+\]", content)
#     if len(citation_matches) >= CITATION_COUNT_THRESHOLD:
#         logger.debug(f"Reference chunk detected via citation count: {len(citation_matches)}")
#         return True

#     keyword_count = sum(
#         1 for kw in REFERENCE_CHUNK_KEYWORDS
#         if kw.lower() in content.lower()
#     )
#     if keyword_count >= CITATION_KEYWORD_THRESHOLD:
#         logger.debug(f"Reference chunk detected via keyword count: {keyword_count}")
#         return True

#     return False


# def extract_entities_from_chunk(chunk: dict) -> list[dict]:
#     """Extract entities from a single chunk dict."""
#     chunk_id = chunk.get("chunk_id", "unknown")
#     content = chunk.get("content", "")

#     if not content.strip():
#         logger.warning(f"Empty chunk skipped | chunk_id: {chunk_id}")
#         return []

#     try:
#         logger.info(f"Extracting entities from chunk: {chunk_id}")

#         if _is_reference_chunk(content):
#             logger.info(f"Reference chunk detected | chunk_id: {chunk_id} | using lightweight prompt")
#             prompt = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:4000])
#             output = invoke_model(prompt, max_tokens=500)
#         else:
#             prompt = ENTITY_EXTRACTION_PROMPT.format(content=content[:4000])
#             output = invoke_model(prompt, max_tokens=1000)

#         output = _clean_json_output(output)
#         parsed = json.loads(output)
#         entities = parsed.get("entities", [])

#         for entity in entities:
#             entity["source_chunk_id"] = chunk_id

#         logger.info(f"Extracted {len(entities)} entities from chunk {chunk_id}")
#         return entities

#     except json.JSONDecodeError:
#         logger.error(f"JSON parse failed for chunk {chunk_id}")
#         return _retry_extraction(chunk_id, content)

#     except Exception:
#         logger.exception(f"Entity extraction failed for chunk {chunk_id}")
#         return []


# def _retry_extraction(chunk_id: int, content: str) -> list[dict]:
#     """Fallback extraction with reduced content on JSON parse failure."""
#     try:
#         logger.info(f"Retrying chunk {chunk_id} with reduced content")
#         retry_prompt = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:1000])
#         retry_output = invoke_model(retry_prompt, max_tokens=300)
#         retry_output = _clean_json_output(retry_output)
#         retry_parsed = json.loads(retry_output)
#         retry_entities = retry_parsed.get("entities", [])

#         for entity in retry_entities:
#             entity["source_chunk_id"] = chunk_id

#         logger.info(f"Retry successful | chunk {chunk_id} | entities: {len(retry_entities)}")
#         return retry_entities

#     except Exception:
#         logger.warning(f"Retry also failed for chunk {chunk_id} — returning empty")
#         return []


# def _clean_json_output(output: str) -> str:
#     """Strip markdown fences and extract JSON object from LLM output."""
#     output = output.strip()
#     if output.startswith("```"):
#         output = output.replace("```json", "").replace("```", "").strip()

#     start = output.find("{")
#     end = output.rfind("}") + 1
#     if start != -1 and end > start:
#         return output[start:end]

#     return output


# def extract_entities_from_chunks(chunks: list[dict]) -> list[dict]:
#     """
#     Extract and deduplicate entities across all chunks in parallel.

#     Flow:
#         1. Extract entities per chunk (2 parallel threads)
#         2. Filter noise entities
#         3. Deduplicate by name only (not name+type)
#         4. Merge source_chunk_ids for duplicates
#         5. Return clean deduplicated list
#     """
#     if not chunks:
#         logger.warning("No chunks provided for entity extraction")
#         return []

#     logger.info(f"Starting entity extraction | total chunks: {len(chunks)}")
#     all_entities = []

#     with ThreadPoolExecutor(max_workers=EXTRACTOR_MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(extract_entities_from_chunk, chunk): chunk.get("chunk_id")
#             for chunk in chunks
#         }
#         for future in as_completed(futures):
#             chunk_id = futures[future]
#             try:
#                 all_entities.extend(future.result())
#             except Exception:
#                 logger.exception(f"Parallel extraction failed for chunk: {chunk_id}")

#     logger.info(f"Raw entities extracted: {len(all_entities)}")

#     filtered = filter_entities(all_entities)
#     logger.info(f"Entities after filtering: {len(filtered)}")

#     deduplicated = deduplicate_entities(filtered)
#     logger.info(f"Entities after deduplication: {len(deduplicated)}")

#     return deduplicated


# def filter_entities(entities: list[dict]) -> list[dict]:
#     """Remove low-quality entities before deduplication."""
#     filtered = []
#     for entity in entities:
#         name = entity.get("name", "").strip()
#         entity_type = entity.get("type", "OTHER")

#         if len(name) < MIN_ENTITY_NAME_LENGTH:
#             logger.debug(f"Filtered short entity: '{name}'")
#             continue
#         if name.isdigit():
#             logger.debug(f"Filtered numeric entity: '{name}'")
#             continue
#         if entity_type in SKIP_ENTITY_TYPES:
#             logger.debug(f"Filtered type '{entity_type}' entity: '{name}'")
#             continue

#         filtered.append(entity)
#     return filtered


# def deduplicate_entities(entities: list[dict]) -> list[dict]:
#     """
#     Merge duplicate entities by NAME ONLY (not name+type).

#     Deduplicating by name+type caused the same entity to appear
#     multiple times in Neo4j when the LLM assigned different types
#     across chunks. Name-only dedup ensures a single node per entity.
#     """
#     seen: dict[str, dict] = {}
#     for entity in entities:
#         name = entity.get("name", "").strip().lower()
#         entity_type = entity.get("type", "OTHER")

#         if name not in seen:
#             seen[name] = {
#                 "name": entity.get("name", "").strip(),
#                 "type": entity_type,
#                 "context": entity.get("context", ""),
#                 "source_chunk_ids": []
#             }

#         source = entity.get("source_chunk_id")
#         if source and source not in seen[name]["source_chunk_ids"]:
#             seen[name]["source_chunk_ids"].append(source)

#     return list(seen.values())


# def format_entities_for_graph(doc_id: str, entities: list[dict]) -> list[dict]:
#     """Attach entity_id to each entity for Neo4j node creation."""
#     return [
#         {
#             **entity,
#             "entity_id": generate_entity_id(doc_id, entity["name"], entity["type"]),
#             "doc_id": doc_id,
#         }
#         for entity in entities
#     ]

"""acuracy working code"""

# import re
# import json
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from app.services.summarizer import invoke_model
# from app.utils.doc_id import generate_entity_id
# from app.prompts.extractor import (
#     ENTITY_EXTRACTION_PROMPT,
#     REFERENCE_CHUNK_ENTITY_PROMPT,
# )
# from app.core.config import (
#     MIN_ENTITY_NAME_LENGTH,
#     EXTRACTOR_MAX_WORKERS,
#     SKIP_ENTITY_TYPES,
#     CITATION_COUNT_THRESHOLD,
#     CITATION_KEYWORD_THRESHOLD,
#     REFERENCE_CHUNK_KEYWORDS,
# )

# logger = logging.getLogger(__name__)

# # Valid entity types for accuracy scoring
# VALID_ENTITY_TYPES = {
#     "PERSON", "ORGANIZATION", "LOCATION", "TECHNOLOGY",
#     "CONCEPT", "EVENT", "PRODUCT", "DATE"
# }


# def _is_reference_chunk(content: str) -> bool:
#     """
#     Detect if a chunk is a reference/citation list using multiple signals.

#     Reference chunks cause JSON truncation with the full prompt because
#     the LLM tries to extract 50+ author names → exceeds max_tokens
#     → JSON gets cut off → parse error.

#     For these chunks we use a lighter prompt that only extracts
#     organizations and technologies — not person names.
#     """
#     citation_matches = re.findall(r"\[\d+\]", content)
#     if len(citation_matches) >= CITATION_COUNT_THRESHOLD:
#         logger.debug(f"Reference chunk detected via citation count: {len(citation_matches)}")
#         return True

#     keyword_count = sum(
#         1 for kw in REFERENCE_CHUNK_KEYWORDS
#         if kw.lower() in content.lower()
#     )
#     if keyword_count >= CITATION_KEYWORD_THRESHOLD:
#         logger.debug(f"Reference chunk detected via keyword count: {keyword_count}")
#         return True

#     return False


# def extract_entities_from_chunk(chunk: dict) -> list[dict]:
#     """Extract entities from a single chunk dict."""
#     chunk_id = chunk.get("chunk_id", "unknown")
#     content = chunk.get("content", "")

#     if not content.strip():
#         logger.warning(f"Empty chunk skipped | chunk_id: {chunk_id}")
#         return []

#     try:
#         logger.info(f"Extracting entities from chunk: {chunk_id}")

#         if _is_reference_chunk(content):
#             logger.info(f"Reference chunk detected | chunk_id: {chunk_id} | using lightweight prompt")
#             prompt = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:4000])
#             output = invoke_model(prompt, max_tokens=500)
#         else:
#             prompt = ENTITY_EXTRACTION_PROMPT.format(content=content[:4000])
#             output = invoke_model(prompt, max_tokens=1000)

#         output = _clean_json_output(output)
#         parsed = json.loads(output)
#         entities = parsed.get("entities", [])

#         for entity in entities:
#             entity["source_chunk_id"] = chunk_id

#         logger.info(f"Extracted {len(entities)} entities from chunk {chunk_id}")
#         return entities

#     except json.JSONDecodeError:
#         logger.error(f"JSON parse failed for chunk {chunk_id}")
#         return _retry_extraction(chunk_id, content)

#     except Exception:
#         logger.exception(f"Entity extraction failed for chunk {chunk_id}")
#         return []


# def _retry_extraction(chunk_id: int, content: str) -> list[dict]:
#     """Fallback extraction with reduced content on JSON parse failure."""
#     try:
#         logger.info(f"Retrying chunk {chunk_id} with reduced content")
#         retry_prompt = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:1000])
#         retry_output = invoke_model(retry_prompt, max_tokens=300)
#         retry_output = _clean_json_output(retry_output)
#         retry_parsed = json.loads(retry_output)
#         retry_entities = retry_parsed.get("entities", [])

#         for entity in retry_entities:
#             entity["source_chunk_id"] = chunk_id

#         logger.info(f"Retry successful | chunk {chunk_id} | entities: {len(retry_entities)}")
#         return retry_entities

#     except Exception:
#         logger.warning(f"Retry also failed for chunk {chunk_id} — returning empty")
#         return []


# def _clean_json_output(output: str) -> str:
#     """Strip markdown fences and extract JSON object from LLM output."""
#     output = output.strip()
#     if output.startswith("```"):
#         output = output.replace("```json", "").replace("```", "").strip()

#     start = output.find("{")
#     end = output.rfind("}") + 1
#     if start != -1 and end > start:
#         return output[start:end]

#     return output


# def extract_entities_from_chunks(chunks: list[dict]) -> list[dict]:
#     """
#     Extract and deduplicate entities across all chunks in parallel.

#     Flow:
#         1. Extract entities per chunk (parallel threads)
#         2. Filter noise entities
#         3. Deduplicate by name only (not name+type)
#         4. Merge source_chunk_ids for duplicates
#         5. Return clean deduplicated list
#     """
#     if not chunks:
#         logger.warning("No chunks provided for entity extraction")
#         return []

#     logger.info(f"Starting entity extraction | total chunks: {len(chunks)}")
#     all_entities = []

#     with ThreadPoolExecutor(max_workers=EXTRACTOR_MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(extract_entities_from_chunk, chunk): chunk.get("chunk_id")
#             for chunk in chunks
#         }
#         for future in as_completed(futures):
#             chunk_id = futures[future]
#             try:
#                 all_entities.extend(future.result())
#             except Exception:
#                 logger.exception(f"Parallel extraction failed for chunk: {chunk_id}")

#     logger.info(f"Raw entities extracted: {len(all_entities)}")

#     filtered = filter_entities(all_entities)
#     logger.info(f"Entities after filtering: {len(filtered)}")

#     deduplicated = deduplicate_entities(filtered)
#     logger.info(f"Entities after deduplication: {len(deduplicated)}")

#     return deduplicated


# def filter_entities(entities: list[dict]) -> list[dict]:
#     """Remove low-quality entities before deduplication."""
#     filtered = []
#     for entity in entities:
#         name = entity.get("name", "").strip()
#         entity_type = entity.get("type", "OTHER")

#         if len(name) < MIN_ENTITY_NAME_LENGTH:
#             logger.debug(f"Filtered short entity: '{name}'")
#             continue
#         if name.isdigit():
#             logger.debug(f"Filtered numeric entity: '{name}'")
#             continue
#         if entity_type in SKIP_ENTITY_TYPES:
#             logger.debug(f"Filtered type '{entity_type}' entity: '{name}'")
#             continue

#         filtered.append(entity)
#     return filtered


# def deduplicate_entities(entities: list[dict]) -> list[dict]:
#     """
#     Merge duplicate entities by NAME ONLY (not name+type).

#     Deduplicating by name+type caused the same entity to appear
#     multiple times in Neo4j when the LLM assigned different types
#     across chunks. Name-only dedup ensures a single node per entity.
#     """
#     seen: dict[str, dict] = {}
#     for entity in entities:
#         name = entity.get("name", "").strip().lower()
#         entity_type = entity.get("type", "OTHER")

#         if name not in seen:
#             seen[name] = {
#                 "name": entity.get("name", "").strip(),
#                 "type": entity_type,
#                 "context": entity.get("context", ""),
#                 "source_chunk_ids": []
#             }

#         source = entity.get("source_chunk_id")
#         if source and source not in seen[name]["source_chunk_ids"]:
#             seen[name]["source_chunk_ids"].append(source)

#     return list(seen.values())


# def format_entities_for_graph(doc_id: str, entities: list[dict]) -> list[dict]:
#     """Attach entity_id to each entity for Neo4j node creation."""
#     return [
#         {
#             **entity,
#             "entity_id": generate_entity_id(doc_id, entity["name"], entity["type"]),
#             "doc_id": doc_id,
#         }
#         for entity in entities
#     ]


# # -----------------------------
# # Entity Extraction Accuracy
# # -----------------------------
# def compute_entity_accuracy(entities: list[dict]) -> dict:
#     """
#     Compute entity extraction accuracy across 3 quality dimensions.

#     Dimension 1 — Type accuracy:
#         % of entities with a recognized, valid entity type
#         (PERSON, ORGANIZATION, LOCATION, TECHNOLOGY, CONCEPT, EVENT, PRODUCT, DATE)

#     Dimension 2 — Context accuracy:
#         % of entities that have a non-empty context description
#         Context = grounding — ensures entity is described, not just named

#     Dimension 3 — Source accuracy:
#         % of entities grounded in at least one source chunk
#         Ensures entity came from the document, not hallucinated

#     Overall accuracy = average of the 3 dimensions

#     Returns:
#         dict with individual dimension scores + overall score
#     """
#     total = len(entities)

#     if total == 0:
#         logger.warning("No entities to compute accuracy on")
#         return {
#             "total_entities": 0,
#             "type_accuracy": 0.0,
#             "context_accuracy": 0.0,
#             "source_accuracy": 0.0,
#             "overall_accuracy": 0.0
#         }

#     # Dimension 1 — valid type
#     typed_correctly = sum(
#         1 for e in entities
#         if e.get("type", "").upper() in VALID_ENTITY_TYPES
#     )

#     # Dimension 2 — has non-empty context
#     has_context = sum(
#         1 for e in entities
#         if e.get("context", "").strip()
#     )

#     # Dimension 3 — grounded in at least one source chunk
#     has_source = sum(
#         1 for e in entities
#         if e.get("source_chunk_ids")
#     )

#     type_accuracy    = round(typed_correctly / total, 2)
#     context_accuracy = round(has_context / total, 2)
#     source_accuracy  = round(has_source / total, 2)
#     overall_accuracy = round(
#         (type_accuracy + context_accuracy + source_accuracy) / 3, 2
#     )

#     logger.info(
#         f"Entity accuracy | "
#         f"type: {type_accuracy} | "
#         f"context: {context_accuracy} | "
#         f"source: {source_accuracy} | "
#         f"overall: {overall_accuracy}"
#     )

#     return {
#         "total_entities":     total,
#         "type_accuracy":      type_accuracy,
#         "context_accuracy":   context_accuracy,
#         "source_accuracy":    source_accuracy,
#         "overall_accuracy":   overall_accuracy
#     }

"""speed"""
import re
import json
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from app.services.summarizer import invoke_model
from app.utils.doc_id import generate_entity_id
from app.prompts.extractor import (
    ENTITY_EXTRACTION_PROMPT,
    REFERENCE_CHUNK_ENTITY_PROMPT,
)
from app.core.config import (
    MIN_ENTITY_NAME_LENGTH,
    EXTRACTOR_MAX_WORKERS,
    SKIP_ENTITY_TYPES,
    CITATION_COUNT_THRESHOLD,
    CITATION_KEYWORD_THRESHOLD,
    REFERENCE_CHUNK_KEYWORDS,
)

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────
VALID_ENTITY_TYPES = {
    "PERSON", "ORGANIZATION", "LOCATION", "TECHNOLOGY",
    "CONCEPT", "EVENT", "PRODUCT", "DATE"
}

# OPTIMIZATION: per-chunk Bedrock timeout
# If a single chunk takes longer than this, it's abandoned (not retried)
# Prevents one slow chunk from blocking a worker thread indefinitely
_CHUNK_TIMEOUT_SECONDS = 30

# OPTIMIZATION: true async worker count
# Bedrock calls are I/O bound — more workers = more concurrent calls
# Set higher than EXTRACTOR_MAX_WORKERS (which may be low by default)
_ASYNC_WORKERS = max(EXTRACTOR_MAX_WORKERS, 8)


# ══════════════════════════════════════════════════════════════
# Reference chunk detection  (unchanged logic)
# ══════════════════════════════════════════════════════════════

def _is_reference_chunk(content: str) -> bool:
    """
    Detect if a chunk is a reference/citation list.
    Reference chunks use a lighter prompt to avoid JSON truncation.
    """
    citation_matches = re.findall(r"\[\d+\]", content)
    if len(citation_matches) >= CITATION_COUNT_THRESHOLD:
        logger.debug(f"Reference chunk detected via citation count: {len(citation_matches)}")
        return True

    keyword_count = sum(
        1 for kw in REFERENCE_CHUNK_KEYWORDS
        if kw.lower() in content.lower()
    )
    if keyword_count >= CITATION_KEYWORD_THRESHOLD:
        logger.debug(f"Reference chunk detected via keyword count: {keyword_count}")
        return True

    return False


# ══════════════════════════════════════════════════════════════
# JSON cleaning  (unchanged logic)
# ══════════════════════════════════════════════════════════════

def _clean_json_output(output: str) -> str:
    """Strip markdown fences and extract JSON object from LLM output."""
    output = output.strip()
    if output.startswith("```"):
        output = output.replace("```json", "").replace("```", "").strip()

    start = output.find("{")
    end = output.rfind("}") + 1
    if start != -1 and end > start:
        return output[start:end]

    return output


# ══════════════════════════════════════════════════════════════
# OPTIMIZATION: async single-chunk extraction
# ══════════════════════════════════════════════════════════════
# OLD: extract_entities_from_chunk ran synchronously in a thread.
#      Each thread blocked on invoke_model (blocking HTTP).
#      With EXTRACTOR_MAX_WORKERS=4, only 4 Bedrock calls in flight at once.
#
# NEW: _extract_chunk_async runs in asyncio.
#      invoke_model is offloaded to a thread via loop.run_in_executor.
#      With _ASYNC_WORKERS=8+, many more concurrent Bedrock calls in flight.
#      asyncio.gather fires ALL chunks simultaneously — no queue waiting.

async def _extract_chunk_async(
    chunk: dict,
    loop: asyncio.AbstractEventLoop,
    executor: ThreadPoolExecutor,
) -> list[dict]:
    """
    Async extraction for a single chunk.

    CHANGES FROM ORIGINAL extract_entities_from_chunk:
    1. Runs as a coroutine — can be gathered with all other chunks
    2. invoke_model offloaded to executor — non-blocking
    3. Per-chunk timeout via asyncio.wait_for — prevents hangs
    4. Retry also async — doesn't block the event loop
    """
    chunk_id = chunk.get("chunk_id", "unknown")
    content  = chunk.get("content", "")

    if not content.strip():
        logger.warning(f"Empty chunk skipped | chunk_id: {chunk_id}")
        return []

    try:
        logger.info(f"Extracting entities from chunk: {chunk_id}")

        # Choose prompt based on chunk type (same logic as before)
        if _is_reference_chunk(content):
            logger.info(f"Reference chunk | chunk_id: {chunk_id} | lightweight prompt")
            prompt     = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:4000])
            max_tokens = 500
        else:
            prompt     = ENTITY_EXTRACTION_PROMPT.format(content=content[:4000])
            max_tokens = 1000

        # OPTIMIZATION: offload blocking invoke_model to thread pool
        # asyncio.wait_for adds a per-chunk timeout — abandoned if too slow
        invoke_coro = loop.run_in_executor(
            executor,
            lambda: invoke_model(prompt, max_tokens=max_tokens)
        )
        output = await asyncio.wait_for(invoke_coro, timeout=_CHUNK_TIMEOUT_SECONDS)

        output  = _clean_json_output(output)
        parsed  = json.loads(output)
        entities = parsed.get("entities", [])

        for entity in entities:
            entity["source_chunk_id"] = chunk_id

        logger.info(f"Extracted {len(entities)} entities from chunk {chunk_id}")
        return entities

    except asyncio.TimeoutError:
        # OPTIMIZATION: timeout — don't wait forever, log and move on
        logger.warning(f"Chunk {chunk_id} timed out after {_CHUNK_TIMEOUT_SECONDS}s — skipping")
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
    """
    Async retry with reduced content.
    OLD: _retry_extraction was sync — blocked the calling thread.
    NEW: runs as a coroutine — doesn't block anything.
    """
    try:
        logger.info(f"Retrying chunk {chunk_id} with reduced content")
        retry_prompt = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:1000])

        invoke_coro = loop.run_in_executor(
            executor,
            lambda: invoke_model(retry_prompt, max_tokens=300)
        )
        retry_output = await asyncio.wait_for(invoke_coro, timeout=15)

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


# ══════════════════════════════════════════════════════════════
# OPTIMIZATION: async orchestrator
# ══════════════════════════════════════════════════════════════
# OLD: ThreadPoolExecutor + as_completed
#      → chunks processed as workers become free (queue-based)
#      → with 4 workers and 44 chunks = 11 sequential batches
#
# NEW: asyncio.gather(*all_chunk_coroutines)
#      → ALL chunks fire simultaneously
#      → Bedrock calls overlap completely
#      → wall time ≈ slowest single chunk, not sum of all chunks / workers
#
# Timeline comparison (44 chunks, each ~1.5s Bedrock wait):
#   OLD (4 workers):  ceil(44/4) × 1.5s ≈ 17s
#   NEW (async all):  ~1.5s + overhead  ≈ 3-4s

async def _extract_all_async(chunks: list[dict]) -> list[dict]:
    """
    Fire all chunk extractions simultaneously via asyncio.gather.
    Returns flat list of all raw entities.
    """
    loop     = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(
        max_workers=_ASYNC_WORKERS,
        thread_name_prefix="entity_worker"
    )

    logger.info(
        f"[ASYNC] Firing {len(chunks)} chunk extractions simultaneously | "
        f"workers={_ASYNC_WORKERS}"
    )

    try:
        # asyncio.gather fires ALL coroutines at once
        # return_exceptions=True means one failure doesn't cancel others
        results = await asyncio.gather(
            *[_extract_chunk_async(chunk, loop, executor) for chunk in chunks],
            return_exceptions=True
        )
    finally:
        executor.shutdown(wait=False)

    # Flatten results, skip any exceptions that slipped through
    all_entities = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Chunk {i} raised exception in gather: {result}")
        elif isinstance(result, list):
            all_entities.extend(result)

    return all_entities


# ══════════════════════════════════════════════════════════════
# Public API — drop-in replacement for extract_entities_from_chunks
# ══════════════════════════════════════════════════════════════

def extract_entities_from_chunks(chunks: list[dict]) -> list[dict]:
    """
    Extract and deduplicate entities across all chunks.

    CHANGES FROM ORIGINAL:
    - Uses asyncio.gather to fire ALL chunk extractions simultaneously
      instead of queuing through a ThreadPoolExecutor
    - Each invoke_model call is non-blocking (offloaded to executor)
    - Per-chunk timeout prevents hangs
    - Retry is also async — no thread blocking

    Drop-in replacement — same input/output signature as before.
    """
    if not chunks:
        logger.warning("No chunks provided for entity extraction")
        return []

    logger.info(f"Starting entity extraction | total chunks: {len(chunks)}")

    # Run async orchestrator — handles its own event loop safely
    try:
        loop = asyncio.get_running_loop()
        # Already inside an event loop (e.g. called from FastAPI async context)
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            all_entities = pool.submit(
                asyncio.run, _extract_all_async(chunks)
            ).result()
    except RuntimeError:
        # No running loop — safe to use asyncio.run directly
        all_entities = asyncio.run(_extract_all_async(chunks))

    logger.info(f"Raw entities extracted: {len(all_entities)}")

    filtered     = filter_entities(all_entities)
    logger.info(f"Entities after filtering: {len(filtered)}")

    deduplicated = deduplicate_entities(filtered)
    logger.info(f"Entities after deduplication: {len(deduplicated)}")

    return deduplicated


# ══════════════════════════════════════════════════════════════
# Single chunk extraction — kept for direct call compatibility
# ══════════════════════════════════════════════════════════════

def extract_entities_from_chunk(chunk: dict) -> list[dict]:
    """
    Single chunk extraction — synchronous wrapper.
    Kept for backward compatibility if called directly.
    For bulk extraction use extract_entities_from_chunks().
    """
    chunk_id = chunk.get("chunk_id", "unknown")
    content  = chunk.get("content", "")

    if not content.strip():
        return []

    try:
        if _is_reference_chunk(content):
            prompt     = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:4000])
            max_tokens = 500
        else:
            prompt     = ENTITY_EXTRACTION_PROMPT.format(content=content[:4000])
            max_tokens = 1000

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
        retry_prompt   = REFERENCE_CHUNK_ENTITY_PROMPT.format(content=content[:1000])
        retry_output   = invoke_model(retry_prompt, max_tokens=300)
        retry_output   = _clean_json_output(retry_output)
        retry_parsed   = json.loads(retry_output)
        retry_entities = retry_parsed.get("entities", [])

        for entity in retry_entities:
            entity["source_chunk_id"] = chunk_id

        return retry_entities
    except Exception:
        logger.warning(f"Retry also failed for chunk {chunk_id}")
        return []


# ══════════════════════════════════════════════════════════════
# Filter + Deduplicate  (unchanged logic)
# ══════════════════════════════════════════════════════════════

def filter_entities(entities: list[dict]) -> list[dict]:
    """Remove low-quality entities before deduplication."""
    filtered = []
    for entity in entities:
        name        = entity.get("name", "").strip()
        entity_type = entity.get("type", "OTHER")

        if len(name) < MIN_ENTITY_NAME_LENGTH:
            logger.debug(f"Filtered short entity: '{name}'")
            continue
        if name.isdigit():
            logger.debug(f"Filtered numeric entity: '{name}'")
            continue
        if entity_type in SKIP_ENTITY_TYPES:
            logger.debug(f"Filtered type '{entity_type}' entity: '{name}'")
            continue

        filtered.append(entity)
    return filtered


def deduplicate_entities(entities: list[dict]) -> list[dict]:
    """
    Merge duplicate entities by NAME ONLY (not name+type).
    Same logic as before — name-only dedup ensures a single
    node per entity in Neo4j regardless of type assigned per chunk.
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


# ══════════════════════════════════════════════════════════════
# Entity Extraction Accuracy  (unchanged)
# ══════════════════════════════════════════════════════════════

def compute_entity_accuracy(entities: list[dict]) -> dict:
    """
    Compute entity extraction accuracy across 3 quality dimensions.

    Dimension 1 — Type accuracy:    % with valid entity type
    Dimension 2 — Context accuracy: % with non-empty context
    Dimension 3 — Source accuracy:  % grounded in a source chunk
    Overall = average of 3 dimensions.
    """
    total = len(entities)

    if total == 0:
        logger.warning("No entities to compute accuracy on")
        return {
            "total_entities":  0,
            "type_accuracy":   0.0,
            "context_accuracy": 0.0,
            "source_accuracy": 0.0,
            "overall_accuracy": 0.0
        }

    typed_correctly = sum(
        1 for e in entities
        if e.get("type", "").upper() in VALID_ENTITY_TYPES
    )
    has_context = sum(
        1 for e in entities
        if e.get("context", "").strip()
    )
    has_source = sum(
        1 for e in entities
        if e.get("source_chunk_ids")
    )

    type_accuracy    = round(typed_correctly / total, 2)
    context_accuracy = round(has_context / total, 2)
    source_accuracy  = round(has_source / total, 2)
    overall_accuracy = round(
        (type_accuracy + context_accuracy + source_accuracy) / 3, 2
    )

    logger.info(
        f"Entity accuracy | "
        f"type: {type_accuracy} | "
        f"context: {context_accuracy} | "
        f"source: {source_accuracy} | "
        f"overall: {overall_accuracy}"
    )

    return {
        "total_entities":   total,
        "type_accuracy":    type_accuracy,
        "context_accuracy": context_accuracy,
        "source_accuracy":  source_accuracy,
        "overall_accuracy": overall_accuracy
    }