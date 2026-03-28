import json
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from app.core.config import (
    NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD,
    LOW_VALUE_ENTITY_TYPES, LOW_VALUE_ENTITY_NAMES, HIGH_VALUE_TYPES,
    GRAPH_ANCHOR_TOP_N, GRAPH_CHUNK_BATCH_SIZE, GRAPH_ENTITY_BATCH_SIZE,
    GRAPH_MAX_ENTITIES_PER_LLM_CALL, GRAPH_CROSS_CLUSTER_THRESHOLD,
    GRAPH_CONTENT_CHAR_LIMIT, GRAPH_QUERY_LIMIT, GRAPH_MIN_ENTITY_NAME_LENGTH,
    GRAPH_REL_ASYNC_WORKERS,
)
from app.services.summarizer import invoke_model
from app.services.extractor import format_entities_for_graph
from app.prompts.graph_builder import RELATIONSHIP_EXTRACTION_PROMPT

logger = logging.getLogger(__name__)


class GraphBuilder:
    """Handles Neo4j knowledge graph construction and querying."""

    def __init__(self):
        self.driver = None
        self._connect()

    def _connect(self):
        try:
            self.driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
            )
            self.driver.verify_connectivity()
            logger.info("Neo4j connected successfully")
        except AuthError:
            logger.exception("Neo4j authentication failed")
            raise RuntimeError("Neo4j auth failed. Check credentials.")
        except ServiceUnavailable:
            logger.exception("Neo4j service unavailable")
            raise RuntimeError("Neo4j unavailable. Is it running?")

    # ══════════════════════════════════════════════════════════
    # Public entry point
    # ══════════════════════════════════════════════════════════

    def build_graph(self, doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
        if not entities:
            logger.warning(f"No entities | doc_id: {doc_id}")
            return {"nodes": 0, "relationships": 0}

        logger.info(f"Building graph | doc_id: {doc_id} | entities: {len(entities)}")

        self._clear_existing_graph(doc_id)

        formatted_entities = format_entities_for_graph(doc_id, entities)

        nodes_created = self._create_entity_nodes_batch(doc_id, formatted_entities)

        rel_entities  = [e for e in formatted_entities if not self._is_low_value_entity(e)]
        logger.info(f"Entities for relationship extraction: {len(rel_entities)} / {len(formatted_entities)}")

        anchor_entities = self._detect_anchor_entities(rel_entities)
        anchor_names    = {a["name"] for a in anchor_entities}

        # Build all extraction jobs (same batching logic as before)
        extraction_jobs = []
        chunk_batches   = [
            chunks[i:i + GRAPH_CHUNK_BATCH_SIZE]
            for i in range(0, len(chunks), GRAPH_CHUNK_BATCH_SIZE)
        ]

        for batch_idx, chunk_batch in enumerate(chunk_batches):
            combined_text = " ".join(c.get("content", "") for c in chunk_batch)

            entity_start   = (batch_idx * GRAPH_ENTITY_BATCH_SIZE) % max(len(rel_entities), 1)
            rotating_batch = rel_entities[entity_start:entity_start + GRAPH_ENTITY_BATCH_SIZE]

            if len(rotating_batch) < 3 and len(rel_entities) >= GRAPH_ENTITY_BATCH_SIZE:
                rotating_batch = rel_entities[-GRAPH_ENTITY_BATCH_SIZE:]

            non_anchor   = [e for e in rotating_batch if e["name"] not in anchor_names]
            entity_batch = (anchor_entities + non_anchor)[:GRAPH_MAX_ENTITIES_PER_LLM_CALL]

            extraction_jobs.append((entity_batch, combined_text))

        # Cross-cluster pass (same logic as before)
        if len(rel_entities) > GRAPH_CROSS_CLUSTER_THRESHOLD:
            logger.info("Adding cross-cluster pass to extraction jobs")
            non_anchor_entities = [e for e in rel_entities if e["name"] not in anchor_names]
            mid          = len(non_anchor_entities) // 2
            cross_sample = non_anchor_entities[:3] + non_anchor_entities[mid:mid + 3]
            cross_batch  = (anchor_entities + cross_sample)[:GRAPH_MAX_ENTITIES_PER_LLM_CALL]
            cross_text   = " ".join(c.get("content", "") for c in chunks[:GRAPH_CHUNK_BATCH_SIZE])
            extraction_jobs.append((cross_batch, cross_text))

        all_relationships = self._extract_relationships_parallel(extraction_jobs)

        all_relationships = self._filter_low_value_relationships(all_relationships)
        all_relationships = self._deduplicate_relationships(all_relationships)
        logger.info(f"Total unique relationships to create: {len(all_relationships)}")

    
        rels_created = self._create_relationships_batch(doc_id, all_relationships)
        logger.info(f"Graph built | nodes: {nodes_created} | relationships: {rels_created}")

        return {"nodes": nodes_created, "relationships": rels_created}

    
    def _extract_relationships_parallel(
        self, jobs: list[tuple[list[dict], str]]
    ) -> list[dict]:
        """Fire all relationship extraction jobs simultaneously."""

        async def _run_all():
            loop     = asyncio.get_event_loop()
            executor = ThreadPoolExecutor(
                max_workers=GRAPH_REL_ASYNC_WORKERS,
                thread_name_prefix="rel_worker"
            )
            try:
                logger.info(
                    f"[ASYNC] Firing {len(jobs)} relationship extraction jobs simultaneously | "
                    f"workers={GRAPH_REL_ASYNC_WORKERS}"
                )

                async def _one_job(entity_batch, text, job_idx):
                    result = await loop.run_in_executor(
                        executor,
                        lambda: self._extract_relationships(entity_batch, text)
                    )
                    logger.info(f"Job {job_idx+1}/{len(jobs)} done | rels: {len(result)}")
                    return result

                results = await asyncio.gather(
                    *[_one_job(eb, tx, i) for i, (eb, tx) in enumerate(jobs)],
                    return_exceptions=True
                )
            finally:
                executor.shutdown(wait=False)

            all_rels = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Extraction job {i} failed: {result}")
                elif isinstance(result, list):
                    all_rels.extend(result)
            return all_rels

        # Safe event loop handling (works inside FastAPI and standalone)
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, _run_all()).result()
        except RuntimeError:
            return asyncio.run(_run_all())

    def _create_entity_nodes_batch(self, doc_id: str, entities: list[dict]) -> int:
        """
        Create all entity nodes in a single UNWIND Cypher call.
        CHANGES FROM _create_entity_nodes:
        - One session.run instead of N session.run calls
        - UNWIND processes the full list server-side in Neo4j
        """
        if not entities:
            return 0

        try:
            with self.driver.session() as session:
                result = session.run(
                    """
                    UNWIND $entities AS e
                    MERGE (n:Entity {entity_id: e.entity_id})
                    SET n.name    = e.name,
                        n.type    = e.type,
                        n.context = e.context,
                        n.doc_id  = e.doc_id
                    RETURN count(n) AS created
                    """,
                    entities=[
                        {
                            "entity_id": entity["entity_id"],
                            "name":      entity["name"],
                            "type":      entity["type"],
                            "context":   entity.get("context", ""),
                            "doc_id":    doc_id,
                        }
                        for entity in entities
                    ]
                )
                record = result.single()
                count  = record["created"] if record else 0
                logger.info(f"Nodes created: {count}")
                return count

        except Exception:
            logger.exception("Batch node creation failed — falling back to one-by-one")
            return self._create_entity_nodes_fallback(doc_id, entities)

    def _create_entity_nodes_fallback(self, doc_id: str, entities: list[dict]) -> int:
        """Fallback: original one-by-one method if batch fails."""
        count = 0
        with self.driver.session() as session:
            for entity in entities:
                try:
                    session.run(
                        """
                        MERGE (e:Entity {entity_id: $entity_id})
                        SET e.name = $name, e.type = $type,
                            e.context = $context, e.doc_id = $doc_id
                        """,
                        entity_id=entity["entity_id"],
                        name=entity["name"],
                        type=entity["type"],
                        context=entity.get("context", ""),
                        doc_id=doc_id
                    )
                    count += 1
                except Exception:
                    logger.exception(f"Node creation failed: {entity.get('name')}")
        logger.info(f"Nodes created (fallback): {count}")
        return count

    def _create_relationships_batch(self, doc_id: str, relationships: list[dict]) -> int:

        if not relationships:
            return 0

        try:
            with self.driver.session() as session:
                result = session.run(
                    """
                    UNWIND $rels AS rel
                    MATCH (a:Entity {name: rel.source, doc_id: $doc_id})
                    MATCH (b:Entity {name: rel.target, doc_id: $doc_id})
                    MERGE (a)-[r:RELATES_TO {type: rel.rel_type}]->(b)
                    SET r.description = rel.description,
                        r.doc_id      = $doc_id
                    RETURN count(r) AS created
                    """,
                    doc_id=doc_id,
                    rels=[
                        {
                            "source":      rel.get("source"),
                            "target":      rel.get("target"),
                            "rel_type":    rel.get("relationship", "RELATED_TO"),
                            "description": rel.get("description", ""),
                        }
                        for rel in relationships
                    ]
                )
                record = result.single()
                count  = record["created"] if record else 0
                logger.info(f"Relationships created: {count}")
                return count

        except Exception:
            logger.exception("Batch relationship creation failed — falling back to one-by-one")
            return self._create_relationships_fallback(doc_id, relationships)

    def _create_relationships_fallback(self, doc_id: str, relationships: list[dict]) -> int:
        """Fallback: original one-by-one method if batch fails."""
        count = 0
        with self.driver.session() as session:
            for rel in relationships:
                try:
                    result = session.run(
                        """
                        MATCH (a:Entity {name: $source, doc_id: $doc_id})
                        MATCH (b:Entity {name: $target, doc_id: $doc_id})
                        MERGE (a)-[r:RELATES_TO {type: $rel_type}]->(b)
                        SET r.description = $description, r.doc_id = $doc_id
                        RETURN count(r) AS created
                        """,
                        source=rel.get("source"),
                        target=rel.get("target"),
                        rel_type=rel.get("relationship", "RELATED_TO"),
                        description=rel.get("description", ""),
                        doc_id=doc_id
                    )
                    record = result.single()
                    if record and record["created"] > 0:
                        count += 1
                except Exception:
                    logger.exception(f"Rel creation failed: {rel.get('source')} -> {rel.get('target')}")
        logger.info(f"Relationships created (fallback): {count}")
        return count

    def _clear_existing_graph(self, doc_id: str):
        """
        Clear existing graph in one Cypher call.
        CHANGES FROM ORIGINAL:
        - DETACH DELETE removes node + all connected rels atomically
        - Was: 2 queries (delete rels first, then nodes)
        - Now: 1 query
        """
        try:
            with self.driver.session() as session:
                session.run(
                    "MATCH (e:Entity {doc_id: $doc_id}) DETACH DELETE e",
                    doc_id=doc_id
                )
                logger.info(f"Cleared graph | doc_id: {doc_id}")
        except Exception:
            logger.exception(f"Failed to clear graph | doc_id: {doc_id}")

    # ══════════════════════════════════════════════════════════
    # Relationship extraction — single job (unchanged logic)
    # ══════════════════════════════════════════════════════════

    def _extract_relationships(self, entities: list[dict], text: str) -> list[dict]:
        """Single relationship extraction call. Called by _extract_relationships_parallel."""
        output = ""
        try:
            entity_names = [
                f"{e['name']} ({e['type']})"
                for e in entities[:GRAPH_MAX_ENTITIES_PER_LLM_CALL]
            ]
            prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
                entities="\n".join(entity_names),
                content=text[:GRAPH_CONTENT_CHAR_LIMIT]
            )
            output = invoke_model(prompt, max_tokens=1200).strip()

            if "```" in output:
                output = output.replace("```json", "").replace("```", "").strip()

            start, end = output.find("{"), output.rfind("}") + 1
            if start == -1 or end == 0:
                logger.error(f"No JSON found in output: {output[:300]}")
                return []

            parsed        = json.loads(output[start:end])
            relationships = parsed.get("relationships", [])
            logger.info(f"LLM extracted {len(relationships)} relationships")
            return relationships

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse failed: {e} | output: {output[:300]}")
            return []
        except Exception:
            logger.exception("Relationship extraction failed")
            return []

    # ══════════════════════════════════════════════════════════
    # Anchor detection + filters  (unchanged logic)
    # ══════════════════════════════════════════════════════════

    def _detect_anchor_entities(self, entities: list[dict]) -> list[dict]:
        scored = []
        for entity in entities:
            if self._is_low_value_entity(entity):
                continue
            etype       = entity.get("type", "").upper()
            chunk_count = len(entity.get("source_chunk_ids", []))
            score       = chunk_count * (2 if etype in HIGH_VALUE_TYPES else 1)
            scored.append((score, entity))

        scored.sort(key=lambda x: x[0], reverse=True)
        anchors = [e for _, e in scored[:GRAPH_ANCHOR_TOP_N]]
        logger.info(f"Detected anchor entities: {[a['name'] for a in anchors]}")
        return anchors

    def _is_low_value_entity(self, entity: dict) -> bool:
        name  = entity.get("name", "").lower().strip()
        etype = entity.get("type", "").upper()
        return (
            etype in LOW_VALUE_ENTITY_TYPES
            or name in LOW_VALUE_ENTITY_NAMES
            or "@" in name
            or len(name) <= GRAPH_MIN_ENTITY_NAME_LENGTH
        )

    def _filter_low_value_relationships(self, relationships: list[dict]) -> list[dict]:
        filtered, skipped = [], 0
        for rel in relationships:
            source = rel.get("source", "").lower().strip()
            target = rel.get("target", "").lower().strip()
            if (
                source in LOW_VALUE_ENTITY_NAMES
                or target in LOW_VALUE_ENTITY_NAMES
                or "@" in source or "@" in target
                or len(source) <= GRAPH_MIN_ENTITY_NAME_LENGTH
                or len(target) <= GRAPH_MIN_ENTITY_NAME_LENGTH
            ):
                skipped += 1
                continue
            filtered.append(rel)

        if skipped:
            logger.info(f"Filtered {skipped} low-value relationships")
        return filtered

    def _deduplicate_relationships(self, relationships: list[dict]) -> list[dict]:
        seen, unique = set(), []
        for rel in relationships:
            key = (
                f"{rel.get('source', '').lower().strip()}_"
                f"{rel.get('target', '').lower().strip()}_"
                f"{rel.get('relationship', '').upper().strip()}"
            )
            if key not in seen:
                seen.add(key)
                unique.append(rel)

        logger.info(f"Dedup: {len(unique)} unique (from {len(relationships)})")
        return unique

    # ══════════════════════════════════════════════════════════
    # Query methods  (unchanged)
    # ══════════════════════════════════════════════════════════

    def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
        logger.info(f"Graph query | entity: {entity_name}")
        results = []
        try:
            with self.driver.session() as session:
                records = session.run(
                    """
                    MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
                    WHERE toLower(a.name) CONTAINS toLower($entity_name)
                       OR toLower(b.name) CONTAINS toLower($entity_name)
                    RETURN DISTINCT
                        a.name AS source,
                        r.type AS relationship,
                        b.name AS target,
                        r.description AS description
                    ORDER BY source, relationship
                    LIMIT $limit
                    """,
                    doc_id=doc_id,
                    entity_name=entity_name,
                    limit=GRAPH_QUERY_LIMIT
                )
                for record in records:
                    results.append({
                        "source":       record["source"],
                        "relationship": record["relationship"],
                        "target":       record["target"],
                        "description":  record["description"],
                    })
        except Exception:
            logger.exception(f"Graph query failed: {entity_name}")

        logger.info(f"Graph query returned {len(results)} relationships")
        return results

    def close(self):
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")


# ══════════════════════════════════════════════════════════════
# Singleton + public API  (unchanged)
# ══════════════════════════════════════════════════════════════

_graph_builder = None


def _get_graph_builder() -> GraphBuilder:
    global _graph_builder
    if _graph_builder is None:
        _graph_builder = GraphBuilder()
    return _graph_builder


def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
    return _get_graph_builder().build_graph(doc_id, entities, chunks)


def query_graph(doc_id: str, entity_name: str) -> list[dict]:
    return _get_graph_builder().query_graph(doc_id, entity_name)


def get_full_graph(doc_id: str) -> list[dict]:
    """Return all relationships for the entire document graph."""
    results = []
    try:
        with _get_graph_builder().driver.session() as session:
            records = session.run(
                """
                MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
                RETURN DISTINCT
                    a.name AS source,
                    a.type AS source_type,
                    r.type AS relationship,
                    b.name AS target,
                    b.type AS target_type,
                    r.description AS description
                ORDER BY source
                """,
                doc_id=doc_id
            )
            for record in records:
                results.append({
                    "source":       record["source"],
                    "source_type":  record["source_type"],
                    "relationship": record["relationship"],
                    "target":       record["target"],
                    "target_type":  record["target_type"],
                    "description":  record["description"],
                })
    except Exception:
        logger.exception(f"Full graph query failed | doc_id: {doc_id}")
    return results