# import logging
# import json
# from neo4j import GraphDatabase
# from neo4j.exceptions import ServiceUnavailable, AuthError
# from app.core.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
# from app.services.summarizer import invoke_model
# from app.services.extractor import format_entities_for_graph

# logger = logging.getLogger(__name__)


# RELATIONSHIP_EXTRACTION_PROMPT = """
# You are an expert knowledge graph builder.

# Given the following list of entities and the source text, identify meaningful relationships between entities.

# Entities:
# {entities}

# Source Text:
# {content}

# For each relationship return:
# - source: name of the first entity
# - target: name of the second entity
# - relationship: a short UPPERCASE_SNAKE_CASE label (e.g. WORKS_FOR, LOCATED_IN, PUBLISHED_BY, RELATED_TO)
# - description: one-line description of the relationship

# Return ONLY valid JSON, no preamble:
# {{
#     "relationships": [
#         {{
#             "source": "Entity A",
#             "target": "Entity B",
#             "relationship": "RELATIONSHIP_TYPE",
#             "description": "brief description"
#         }}
#     ]
# }}
# """


# class GraphBuilder:
#     """
#     Handles Neo4j knowledge graph operations.

#     Nodes  = entities (PERSON, ORG, CONCEPT etc.)
#     Edges  = relationships between entities

#     Each node and relationship is scoped to a doc_id
#     so graphs from different documents don't mix.
#     """

#     def __init__(self):
#         self.driver = None
#         self._connect()

#     # -----------------------------
#     # Connection
#     # -----------------------------
#     def _connect(self):
#         try:
#             self.driver = GraphDatabase.driver(
#                 NEO4J_URI,
#                 auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
#             )
#             self.driver.verify_connectivity()
#             logger.info("Neo4j connected successfully")

#         except AuthError:
#             logger.exception("Neo4j authentication failed — check credentials")
#             raise RuntimeError("Neo4j auth failed. Check NEO4J_USERNAME/PASSWORD in config.")

#         except ServiceUnavailable:
#             logger.exception("Neo4j service unavailable")
#             raise RuntimeError("Neo4j unavailable. Is it running? Check NEO4J_URI in config.")

#     # -----------------------------
#     # Build Graph
#     # -----------------------------
#     def build_graph(
#         self,
#         doc_id: str,
#         entities: list[dict],
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Full graph build pipeline:
#         1. Format entities with IDs
#         2. Create entity nodes in Neo4j
#         3. Extract relationships using LLM
#         4. Create relationship edges in Neo4j

#         Returns summary of nodes and relationships created.
#         """
#         if not entities:
#             logger.warning(f"No entities provided for graph | doc_id: {doc_id}")
#             return {"nodes": 0, "relationships": 0}

#         logger.info(f"Building graph | doc_id: {doc_id} | entities: {len(entities)}")

#         # Step 1 — attach entity_ids
#         formatted_entities = format_entities_for_graph(doc_id, entities)

#         # Step 2 — create nodes
#         nodes_created = self._create_entity_nodes(doc_id, formatted_entities)

#         # Step 3 — extract relationships using LLM
#         combined_text = " ".join(
#             chunk.get("content", "") for chunk in chunks[:5]  # limit context size
#         )

#         relationships = self._extract_relationships(formatted_entities, combined_text)

#         # Step 4 — create edges
#         rels_created = self._create_relationships(doc_id, relationships)

#         logger.info(
#             f"Graph built | doc_id: {doc_id} | "
#             f"nodes: {nodes_created} | relationships: {rels_created}"
#         )

#         return {
#             "nodes": nodes_created,
#             "relationships": rels_created
#         }

#     # -----------------------------
#     # Create Nodes
#     # -----------------------------
#     def _create_entity_nodes(
#         self,
#         doc_id: str,
#         entities: list[dict]
#     ) -> int:
#         """
#         Create a Neo4j node for each entity.
#         Node label = entity type (e.g. :PERSON, :ORGANIZATION)
#         """
#         count = 0

#         with self.driver.session() as session:
#             for entity in entities:
#                 try:
#                     session.run(
#                         """
#                         MERGE (e:Entity {entity_id: $entity_id})
#                         SET e.name = $name,
#                             e.type = $type,
#                             e.context = $context,
#                             e.doc_id = $doc_id
#                         """,
#                         entity_id=entity["entity_id"],
#                         name=entity["name"],
#                         type=entity["type"],
#                         context=entity.get("context", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1

#                 except Exception:
#                     logger.exception(f"Node creation failed for entity: {entity.get('name')}")

#         logger.info(f"Nodes created: {count}")
#         return count

#     # -----------------------------
#     # Extract Relationships via LLM
#     # -----------------------------
#     def _extract_relationships(
#         self,
#         entities: list[dict],
#         text: str
#     ) -> list[dict]:
#         """
#         Use LLM to identify relationships between entities
#         based on the source document text.
#         """
#         try:
#             entity_names = [
#                 f"{e['name']} ({e['type']})" for e in entities[:20]  # cap at 20
#             ]

#             prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
#                 entities="\n".join(entity_names),
#                 content=text[:3000]
#             )

#             output = invoke_model(prompt, max_tokens=1000)

#             if output.startswith("```"):
#                 output = output.replace("```json", "").replace("```", "").strip()

#             parsed = json.loads(output)

#             relationships = parsed.get("relationships", [])

#             logger.info(f"Relationships extracted by LLM: {len(relationships)}")

#             return relationships

#         except json.JSONDecodeError:
#             logger.error("Relationship JSON parse failed")
#             return []

#         except Exception:
#             logger.exception("Relationship extraction failed")
#             return []

#     # -----------------------------
#     # Create Relationships (Edges)
#     # -----------------------------
#     def _create_relationships(
#         self,
#         doc_id: str,
#         relationships: list[dict]
#     ) -> int:
#         """
#         Create Neo4j edges between entity nodes.
#         Uses MERGE to avoid duplicate relationships.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for rel in relationships:
#                 try:
#                     session.run(
#                         """
#                         MATCH (a:Entity {name: $source, doc_id: $doc_id})
#                         MATCH (b:Entity {name: $target, doc_id: $doc_id})
#                         MERGE (a)-[r:RELATES_TO {type: $rel_type, doc_id: $doc_id}]->(b)
#                         SET r.description = $description
#                         """,
#                         source=rel.get("source"),
#                         target=rel.get("target"),
#                         rel_type=rel.get("relationship", "RELATED_TO"),
#                         description=rel.get("description", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1

#                 except Exception:
#                     logger.exception(
#                         f"Relationship creation failed: "
#                         f"{rel.get('source')} -> {rel.get('target')}"
#                     )

#         logger.info(f"Relationships created: {count}")
#         return count

#     # -----------------------------
#     # Query Graph
#     # -----------------------------
#     def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
#         """
#         Query all relationships for a given entity within a document.

#         Returns list of relationship dicts:
#         {
#             "entity": "Entity A",
#             "relationship": "WORKS_FOR",
#             "related_to": "Entity B",
#             "description": "..."
#         }
#         """
#         logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity_name}")

#         results = []

#         try:
#             with self.driver.session() as session:
#                 records = session.run(
#                     """
#                     MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
#                     WHERE toLower(a.name) CONTAINS toLower($entity_name)
#                        OR toLower(b.name) CONTAINS toLower($entity_name)
#                     RETURN
#                         a.name AS source,
#                         r.type AS relationship,
#                         b.name AS target,
#                         r.description AS description
#                     LIMIT 50
#                     """,
#                     doc_id=doc_id,
#                     entity_name=entity_name
#                 )

#                 for record in records:
#                     results.append({
#                         "source": record["source"],
#                         "relationship": record["relationship"],
#                         "target": record["target"],
#                         "description": record["description"],
#                     })

#         except Exception:
#             logger.exception(f"Graph query failed for entity: {entity_name}")

#         logger.info(f"Graph query returned {len(results)} relationships")

#         return results

#     # -----------------------------
#     # Cleanup
#     # -----------------------------
#     def close(self):
#         if self.driver:
#             self.driver.close()
#             logger.info("Neo4j connection closed")


# # -----------------------------
# # Module-level functions
# # called from api.py
# # -----------------------------
# _graph_builder = None


# def _get_graph_builder() -> GraphBuilder:
#     global _graph_builder
#     if _graph_builder is None:
#         _graph_builder = GraphBuilder()
#     return _graph_builder


# def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#     return _get_graph_builder().build_graph(doc_id, entities, chunks)


# def query_graph(doc_id: str, entity_name: str) -> list[dict]:
#     return _get_graph_builder().query_graph(doc_id, entity_name)

"""V2"""
# import logging
# import json
# from neo4j import GraphDatabase
# from neo4j.exceptions import ServiceUnavailable, AuthError
# from app.core.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
# from app.services.summarizer import invoke_model
# from app.services.extractor import format_entities_for_graph

# logger = logging.getLogger(__name__)


# RELATIONSHIP_EXTRACTION_PROMPT = """
# You are an expert knowledge graph builder.

# Given the following list of entities and the source text, identify meaningful relationships between entities.

# Entities:
# {entities}

# Source Text:
# {content}

# Rules:
# - Only create relationships between entities that are explicitly mentioned together in the text
# - Use clear, meaningful relationship labels in UPPERCASE_SNAKE_CASE
# - Each relationship must have a source AND target that exist in the entities list above
# - Return a maximum of 15 relationships

# For each relationship return:
# - source: exact name of the first entity (must match entity list)
# - target: exact name of the second entity (must match entity list)
# - relationship: UPPERCASE_SNAKE_CASE label (e.g. WORKS_FOR, PART_OF, USED_BY, RELATED_TO, DEVELOPED_BY)
# - description: one-line description of the relationship

# You MUST return ONLY a raw JSON object. No explanation, no markdown, no code blocks.
# Start your response with {{ and end with }}

# {{
#     "relationships": [
#         {{
#             "source": "Entity A",
#             "target": "Entity B",
#             "relationship": "RELATIONSHIP_TYPE",
#             "description": "brief description"
#         }}
#     ]
# }}
# """


# class GraphBuilder:
#     """
#     Handles Neo4j knowledge graph operations.

#     Nodes  = entities (PERSON, ORG, CONCEPT etc.)
#     Edges  = relationships between entities

#     Each node and relationship is scoped to a doc_id
#     so graphs from different documents don't mix.
#     """

#     def __init__(self):
#         self.driver = None
#         self._connect()

#     # -----------------------------
#     # Connection
#     # -----------------------------
#     def _connect(self):
#         try:
#             self.driver = GraphDatabase.driver(
#                 NEO4J_URI,
#                 auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
#             )
#             self.driver.verify_connectivity()
#             logger.info("Neo4j connected successfully")

#         except AuthError:
#             logger.exception("Neo4j authentication failed — check credentials")
#             raise RuntimeError("Neo4j auth failed. Check NEO4J_USERNAME/PASSWORD in config.")

#         except ServiceUnavailable:
#             logger.exception("Neo4j service unavailable")
#             raise RuntimeError("Neo4j unavailable. Is it running? Check NEO4J_URI in config.")

#     # -----------------------------
#     # Build Graph
#     # -----------------------------
#     def build_graph(
#         self,
#         doc_id: str,
#         entities: list[dict],
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Full graph build pipeline:
#         1. Format entities with IDs
#         2. Create entity nodes in Neo4j
#         3. Extract relationships using LLM (in batches)
#         4. Create relationship edges in Neo4j
#         """
#         if not entities:
#             logger.warning(f"No entities provided for graph | doc_id: {doc_id}")
#             return {"nodes": 0, "relationships": 0}

#         logger.info(f"Building graph | doc_id: {doc_id} | entities: {len(entities)}")

#         # Step 1 — attach entity_ids
#         formatted_entities = format_entities_for_graph(doc_id, entities)

#         # Step 2 — create nodes
#         nodes_created = self._create_entity_nodes(doc_id, formatted_entities)

#         # Step 3 — extract relationships in batches
#         # Each batch uses 3 chunks + 10 entities to stay within token limits
#         all_relationships = []

#         chunk_batches = [chunks[i:i+3] for i in range(0, min(len(chunks), 9), 3)]

#         for batch_idx, chunk_batch in enumerate(chunk_batches):
#             combined_text = " ".join(
#                 chunk.get("content", "") for chunk in chunk_batch
#             )

#             logger.info(f"Extracting relationships | batch {batch_idx + 1}/{len(chunk_batches)}")

#             batch_relationships = self._extract_relationships(
#                 formatted_entities,
#                 combined_text
#             )

#             all_relationships.extend(batch_relationships)

#         # Deduplicate relationships by (source, target, type)
#         all_relationships = self._deduplicate_relationships(all_relationships)

#         logger.info(f"Total unique relationships to create: {len(all_relationships)}")

#         # Step 4 — create edges
#         rels_created = self._create_relationships(doc_id, all_relationships)

#         logger.info(
#             f"Graph built | doc_id: {doc_id} | "
#             f"nodes: {nodes_created} | relationships: {rels_created}"
#         )

#         return {
#             "nodes": nodes_created,
#             "relationships": rels_created
#         }

#     # -----------------------------
#     # Create Nodes
#     # -----------------------------
#     def _create_entity_nodes(
#         self,
#         doc_id: str,
#         entities: list[dict]
#     ) -> int:
#         """
#         Create a Neo4j node for each entity.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for entity in entities:
#                 try:
#                     session.run(
#                         """
#                         MERGE (e:Entity {entity_id: $entity_id})
#                         SET e.name = $name,
#                             e.type = $type,
#                             e.context = $context,
#                             e.doc_id = $doc_id
#                         """,
#                         entity_id=entity["entity_id"],
#                         name=entity["name"],
#                         type=entity["type"],
#                         context=entity.get("context", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1

#                 except Exception:
#                     logger.exception(f"Node creation failed for entity: {entity.get('name')}")

#         logger.info(f"Nodes created: {count}")
#         return count

#     # -----------------------------
#     # Extract Relationships via LLM
#     # -----------------------------
#     def _extract_relationships(
#         self,
#         entities: list[dict],
#         text: str
#     ) -> list[dict]:
#         """
#         Use LLM to identify relationships between entities.

#         Key limits to prevent JSON truncation:
#         - entities[:10]    — only 10 entities per call (was 20, caused huge output)
#         - text[:1500]      — smaller input leaves more token budget for response
#         - max_tokens=3000  — enough room for complete JSON output (was 1000)
#         """
#         output = ""

#         try:
#             # FIX 1 — cap at 10 entities per batch (was 20)
#             # More entities = larger JSON response = truncation at max_tokens
#             entity_names = [
#                 f"{e['name']} ({e['type']})" for e in entities[:10]
#             ]

#             prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
#                 entities="\n".join(entity_names),
#                 content=text[:1500]     # FIX 2 — smaller input (was 3000)
#             )

#             # FIX 3 — increase max_tokens so JSON doesn't get cut off (was 1000)
#             output = invoke_model(prompt, max_tokens=3000)

#             # Log raw output for debugging
#             logger.debug(f"Raw relationship LLM output: {output[:500]}")

#             # Strip markdown code blocks if present
#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             # Extract JSON object even if there's text before/after
#             # Handles cases where LLM adds explanation before/after the JSON
#             start = output.find("{")
#             end = output.rfind("}") + 1

#             if start == -1 or end == 0:
#                 logger.error(f"No JSON object found in LLM output: {output[:300]}")
#                 return []

#             json_str = output[start:end]

#             parsed = json.loads(json_str)

#             relationships = parsed.get("relationships", [])

#             logger.info(f"Relationships extracted by LLM: {len(relationships)}")

#             return relationships

#         except json.JSONDecodeError as e:
#             logger.error(
#                 f"Relationship JSON parse failed | "
#                 f"error: {e} | "
#                 f"raw output: {output[:300]}"
#             )
#             return []

#         except Exception:
#             logger.exception("Relationship extraction failed")
#             return []

#     # -----------------------------
#     # Deduplicate Relationships
#     # -----------------------------
#     def _deduplicate_relationships(
#         self,
#         relationships: list[dict]
#     ) -> list[dict]:
#         """
#         Remove duplicate relationships from batch processing.
#         Key = (source.lower, target.lower, relationship_type)
#         """
#         seen = set()
#         unique = []

#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()
#             rel_type = rel.get("relationship", "").upper().strip()

#             key = f"{source}_{target}_{rel_type}"

#             if key not in seen:
#                 seen.add(key)
#                 unique.append(rel)

#         logger.info(
#             f"Relationships after deduplication: "
#             f"{len(unique)} (from {len(relationships)})"
#         )

#         return unique

#     # -----------------------------
#     # Create Relationships (Edges)
#     # -----------------------------
#     def _create_relationships(
#         self,
#         doc_id: str,
#         relationships: list[dict]
#     ) -> int:
#         """
#         Create Neo4j edges between entity nodes.
#         Uses MERGE to avoid duplicate relationships.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for rel in relationships:
#                 try:
#                     result = session.run(
#                         """
#                         MATCH (a:Entity {name: $source, doc_id: $doc_id})
#                         MATCH (b:Entity {name: $target, doc_id: $doc_id})
#                         MERGE (a)-[r:RELATES_TO {type: $rel_type, doc_id: $doc_id}]->(b)
#                         SET r.description = $description
#                         RETURN count(r) as created
#                         """,
#                         source=rel.get("source"),
#                         target=rel.get("target"),
#                         rel_type=rel.get("relationship", "RELATED_TO"),
#                         description=rel.get("description", ""),
#                         doc_id=doc_id
#                     )

#                     # Only count if both nodes were actually found
#                     record = result.single()
#                     if record and record["created"] > 0:
#                         count += 1
#                     else:
#                         logger.debug(
#                             f"Skipped relationship — node not found: "
#                             f"{rel.get('source')} -> {rel.get('target')}"
#                         )

#                 except Exception:
#                     logger.exception(
#                         f"Relationship creation failed: "
#                         f"{rel.get('source')} -> {rel.get('target')}"
#                     )

#         logger.info(f"Relationships created: {count}")
#         return count

#     # -----------------------------
#     # Query Graph
#     # -----------------------------
#     def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
#         """
#         Query all relationships for a given entity within a document.
#         Searches both directions (entity as source OR target).
#         """
#         logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity_name}")

#         results = []

#         try:
#             with self.driver.session() as session:
#                 records = session.run(
#                     """
#                     MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
#                     WHERE toLower(a.name) CONTAINS toLower($entity_name)
#                        OR toLower(b.name) CONTAINS toLower($entity_name)
#                     RETURN
#                         a.name AS source,
#                         r.type AS relationship,
#                         b.name AS target,
#                         r.description AS description
#                     LIMIT 50
#                     """,
#                     doc_id=doc_id,
#                     entity_name=entity_name
#                 )

#                 for record in records:
#                     results.append({
#                         "source": record["source"],
#                         "relationship": record["relationship"],
#                         "target": record["target"],
#                         "description": record["description"],
#                     })

#         except Exception:
#             logger.exception(f"Graph query failed for entity: {entity_name}")

#         logger.info(f"Graph query returned {len(results)} relationships")

#         return results

#     # -----------------------------
#     # Cleanup
#     # -----------------------------
#     def close(self):
#         if self.driver:
#             self.driver.close()
#             logger.info("Neo4j connection closed")


# # -----------------------------
# # Module-level functions
# # called from api.py
# # -----------------------------
# _graph_builder = None


# def _get_graph_builder() -> GraphBuilder:
#     global _graph_builder
#     if _graph_builder is None:
#         _graph_builder = GraphBuilder()
#     return _graph_builder


# def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#     return _get_graph_builder().build_graph(doc_id, entities, chunks)


# def query_graph(doc_id: str, entity_name: str) -> list[dict]:
#     return _get_graph_builder().query_graph(doc_id, entity_name)

"""V3"""

# import logging
# import json
# from neo4j import GraphDatabase
# from neo4j.exceptions import ServiceUnavailable, AuthError
# from app.core.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
# from app.services.summarizer import invoke_model
# from app.services.extractor import format_entities_for_graph

# logger = logging.getLogger(__name__)


# RELATIONSHIP_EXTRACTION_PROMPT = """
# You are an expert knowledge graph builder.

# Given the following list of entities and the source text, identify meaningful relationships between entities.

# Entities:
# {entities}

# Source Text:
# {content}

# Rules:
# - Only create relationships between entities that are explicitly mentioned together in the text
# - Use clear, meaningful relationship labels in UPPERCASE_SNAKE_CASE
# - Each relationship must have a source AND target that exist in the entities list above
# - Return a maximum of 15 relationships

# For each relationship return:
# - source: exact name of the first entity (must match entity list)
# - target: exact name of the second entity (must match entity list)
# - relationship: UPPERCASE_SNAKE_CASE label (e.g. WORKS_FOR, PART_OF, USED_BY, RELATED_TO, DEVELOPED_BY)
# - description: one-line description of the relationship

# You MUST return ONLY a raw JSON object. No explanation, no markdown, no code blocks.
# Start your response with {{ and end with }}

# {{
#     "relationships": [
#         {{
#             "source": "Entity A",
#             "target": "Entity B",
#             "relationship": "RELATIONSHIP_TYPE",
#             "description": "brief description"
#         }}
#     ]
# }}
# """


# class GraphBuilder:
#     """
#     Handles Neo4j knowledge graph operations.

#     Nodes  = entities (PERSON, ORG, CONCEPT etc.)
#     Edges  = relationships between entities

#     Each node and relationship is scoped to a doc_id
#     so graphs from different documents don't mix.
#     """

#     def __init__(self):
#         self.driver = None
#         self._connect()

#     # -----------------------------
#     # Connection
#     # -----------------------------
#     def _connect(self):
#         try:
#             self.driver = GraphDatabase.driver(
#                 NEO4J_URI,
#                 auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
#             )
#             self.driver.verify_connectivity()
#             logger.info("Neo4j connected successfully")

#         except AuthError:
#             logger.exception("Neo4j authentication failed — check credentials")
#             raise RuntimeError("Neo4j auth failed. Check NEO4J_USERNAME/PASSWORD in config.")

#         except ServiceUnavailable:
#             logger.exception("Neo4j service unavailable")
#             raise RuntimeError("Neo4j unavailable. Is it running? Check NEO4J_URI in config.")

#     # -----------------------------
#     # Build Graph
#     # -----------------------------
#     def build_graph(
#         self,
#         doc_id: str,
#         entities: list[dict],
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Full graph build pipeline:
#         1. Format entities with IDs
#         2. Create entity nodes in Neo4j
#         3. Extract relationships using LLM (in batches)
#         4. Create relationship edges in Neo4j
#         """
#         if not entities:
#             logger.warning(f"No entities provided for graph | doc_id: {doc_id}")
#             return {"nodes": 0, "relationships": 0}

#         logger.info(f"Building graph | doc_id: {doc_id} | entities: {len(entities)}")

#         # Step 1 — attach entity_ids
#         formatted_entities = format_entities_for_graph(doc_id, entities)

#         # Step 2 — create nodes
#         nodes_created = self._create_entity_nodes(doc_id, formatted_entities)

#         # Step 3 — extract relationships in batches
#         # Each batch uses 3 chunks + 10 entities to stay within token limits
#         all_relationships = []

#         chunk_batches = [chunks[i:i+3] for i in range(0, min(len(chunks), 9), 3)]

#         for batch_idx, chunk_batch in enumerate(chunk_batches):
#             combined_text = " ".join(
#                 chunk.get("content", "") for chunk in chunk_batch
#             )

#             logger.info(f"Extracting relationships | batch {batch_idx + 1}/{len(chunk_batches)}")

#             batch_relationships = self._extract_relationships(
#                 formatted_entities,
#                 combined_text
#             )

#             all_relationships.extend(batch_relationships)

#         # Deduplicate relationships by (source, target, type)
#         all_relationships = self._deduplicate_relationships(all_relationships)

#         logger.info(f"Total unique relationships to create: {len(all_relationships)}")

#         # Step 4 — create edges
#         rels_created = self._create_relationships(doc_id, all_relationships)

#         logger.info(
#             f"Graph built | doc_id: {doc_id} | "
#             f"nodes: {nodes_created} | relationships: {rels_created}"
#         )

#         return {
#             "nodes": nodes_created,
#             "relationships": rels_created
#         }

#     # -----------------------------
#     # Create Nodes
#     # -----------------------------
#     def _create_entity_nodes(
#         self,
#         doc_id: str,
#         entities: list[dict]
#     ) -> int:
#         """
#         Create a Neo4j node for each entity.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for entity in entities:
#                 try:
#                     session.run(
#                         """
#                         MERGE (e:Entity {entity_id: $entity_id})
#                         SET e.name = $name,
#                             e.type = $type,
#                             e.context = $context,
#                             e.doc_id = $doc_id
#                         """,
#                         entity_id=entity["entity_id"],
#                         name=entity["name"],
#                         type=entity["type"],
#                         context=entity.get("context", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1

#                 except Exception:
#                     logger.exception(f"Node creation failed for entity: {entity.get('name')}")

#         logger.info(f"Nodes created: {count}")
#         return count

#     # -----------------------------
#     # Extract Relationships via LLM
#     # -----------------------------
#     def _extract_relationships(
#         self,
#         entities: list[dict],
#         text: str
#     ) -> list[dict]:
#         """
#         Use LLM to identify relationships between entities.

#         Key limits to prevent JSON truncation:
#         - entities[:10]    — only 10 entities per call (was 20, caused huge output)
#         - text[:1500]      — smaller input leaves more token budget for response
#         - max_tokens=3000  — enough room for complete JSON output (was 1000)
#         """
#         output = ""

#         try:
#             entity_names = [
#                 f"{e['name']} ({e['type']})" for e in entities[:10]
#             ]

#             prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
#                 entities="\n".join(entity_names),
#                 content=text[:1500]
#             )

#             output = invoke_model(prompt, max_tokens=800)

#             logger.debug(f"Raw relationship LLM output: {output[:500]}")

#             # Strip markdown code blocks if present
#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             # Extract JSON object even if there's text before/after
#             start = output.find("{")
#             end = output.rfind("}") + 1

#             if start == -1 or end == 0:
#                 logger.error(f"No JSON object found in LLM output: {output[:300]}")
#                 return []

#             json_str = output[start:end]
#             parsed = json.loads(json_str)
#             relationships = parsed.get("relationships", [])

#             logger.info(f"Relationships extracted by LLM: {len(relationships)}")
#             return relationships

#         except json.JSONDecodeError as e:
#             logger.error(
#                 f"Relationship JSON parse failed | "
#                 f"error: {e} | "
#                 f"raw output: {output[:300]}"
#             )
#             return []

#         except Exception:
#             logger.exception("Relationship extraction failed")
#             return []

#     # -----------------------------
#     # Deduplicate Relationships
#     # -----------------------------
#     def _deduplicate_relationships(
#         self,
#         relationships: list[dict]
#     ) -> list[dict]:
#         """
#         Remove duplicate relationships from batch processing.
#         Key = (source.lower, target.lower, relationship_type)
#         """
#         seen = set()
#         unique = []

#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()
#             rel_type = rel.get("relationship", "").upper().strip()

#             key = f"{source}_{target}_{rel_type}"

#             if key not in seen:
#                 seen.add(key)
#                 unique.append(rel)

#         logger.info(
#             f"Relationships after deduplication: "
#             f"{len(unique)} (from {len(relationships)})"
#         )

#         return unique

#     # -----------------------------
#     # Create Relationships (Edges)
#     # -----------------------------
#     def _create_relationships(
#         self,
#         doc_id: str,
#         relationships: list[dict]
#     ) -> int:
#         """
#         Create Neo4j edges between entity nodes.

#         FIX: MERGE only on (source, target, type) — doc_id removed from
#         MERGE condition and moved to SET. Previously doc_id in MERGE key
#         caused duplicate edges when /build-graph was called multiple times.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for rel in relationships:
#                 try:
#                     result = session.run(
#                         """
#                         MATCH (a:Entity {name: $source, doc_id: $doc_id})
#                         MATCH (b:Entity {name: $target, doc_id: $doc_id})
#                         MERGE (a)-[r:RELATES_TO {type: $rel_type}]->(b)
#                         SET r.description = $description,
#                             r.doc_id = $doc_id
#                         RETURN count(r) as created
#                         """,
#                         source=rel.get("source"),
#                         target=rel.get("target"),
#                         rel_type=rel.get("relationship", "RELATED_TO"),
#                         description=rel.get("description", ""),
#                         doc_id=doc_id
#                     )

#                     record = result.single()
#                     if record and record["created"] > 0:
#                         count += 1
#                     else:
#                         logger.debug(
#                             f"Skipped relationship — node not found: "
#                             f"{rel.get('source')} -> {rel.get('target')}"
#                         )

#                 except Exception:
#                     logger.exception(
#                         f"Relationship creation failed: "
#                         f"{rel.get('source')} -> {rel.get('target')}"
#                     )

#         logger.info(f"Relationships created: {count}")
#         return count

#     # -----------------------------
#     # Query Graph
#     # -----------------------------
#     def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
#         """
#         Query all relationships for a given entity within a document.
#         Searches both directions (entity as source OR target).

#         FIX: Added DISTINCT + ORDER BY to remove duplicate results
#         that appeared when the same relationship was stored multiple times.
#         """
#         logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity_name}")

#         results = []

#         try:
#             with self.driver.session() as session:
#                 records = session.run(
#                     """
#                     MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
#                     WHERE toLower(a.name) CONTAINS toLower($entity_name)
#                        OR toLower(b.name) CONTAINS toLower($entity_name)
#                     RETURN DISTINCT
#                         a.name AS source,
#                         r.type AS relationship,
#                         b.name AS target,
#                         r.description AS description
#                     ORDER BY source, relationship
#                     LIMIT 50
#                     """,
#                     doc_id=doc_id,
#                     entity_name=entity_name
#                 )

#                 for record in records:
#                     results.append({
#                         "source": record["source"],
#                         "relationship": record["relationship"],
#                         "target": record["target"],
#                         "description": record["description"],
#                     })

#         except Exception:
#             logger.exception(f"Graph query failed for entity: {entity_name}")

#         logger.info(f"Graph query returned {len(results)} relationships")

#         return results

#     # -----------------------------
#     # Cleanup
#     # -----------------------------
#     def close(self):
#         if self.driver:
#             self.driver.close()
#             logger.info("Neo4j connection closed")


# # -----------------------------
# # Module-level functions
# # called from api.py
# # -----------------------------
# _graph_builder = None


# def _get_graph_builder() -> GraphBuilder:
#     global _graph_builder
#     if _graph_builder is None:
#         _graph_builder = GraphBuilder()
#     return _graph_builder


# def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#     return _get_graph_builder().build_graph(doc_id, entities, chunks)


# def query_graph(doc_id: str, entity_name: str) -> list[dict]:
#     return _get_graph_builder().query_graph(doc_id, entity_name)

"""V4"""
# import logging
# import json
# from neo4j import GraphDatabase
# from neo4j.exceptions import ServiceUnavailable, AuthError
# from app.core.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
# from app.services.summarizer import invoke_model
# from app.services.extractor import format_entities_for_graph

# logger = logging.getLogger(__name__)


# RELATIONSHIP_EXTRACTION_PROMPT = """
# You are an expert knowledge graph builder.

# Given the following list of entities and the source text, identify meaningful relationships between entities.

# Entities:
# {entities}

# Source Text:
# {content}

# Rules:
# - Only create relationships between entities that are explicitly mentioned together in the text
# - Use clear, meaningful relationship labels in UPPERCASE_SNAKE_CASE
# - Each relationship must have a source AND target that exist in the entities list above
# - Return a maximum of 15 relationships

# For each relationship return:
# - source: exact name of the first entity (must match entity list)
# - target: exact name of the second entity (must match entity list)
# - relationship: UPPERCASE_SNAKE_CASE label (e.g. WORKS_FOR, PART_OF, USED_BY, RELATED_TO, DEVELOPED_BY)
# - description: one-line description of the relationship

# You MUST return ONLY a raw JSON object. No explanation, no markdown, no code blocks.
# Start your response with {{ and end with }}

# {{
#     "relationships": [
#         {{
#             "source": "Entity A",
#             "target": "Entity B",
#             "relationship": "RELATIONSHIP_TYPE",
#             "description": "brief description"
#         }}
#     ]
# }}
# """


# class GraphBuilder:
#     """
#     Handles Neo4j knowledge graph operations.

#     Nodes  = entities (PERSON, ORG, CONCEPT etc.)
#     Edges  = relationships between entities

#     Each node and relationship is scoped to a doc_id
#     so graphs from different documents don't mix.
#     """

#     def __init__(self):
#         self.driver = None
#         self._connect()

#     # -----------------------------
#     # Connection
#     # -----------------------------
#     def _connect(self):
#         try:
#             self.driver = GraphDatabase.driver(
#                 NEO4J_URI,
#                 auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
#             )
#             self.driver.verify_connectivity()
#             logger.info("Neo4j connected successfully")

#         except AuthError:
#             logger.exception("Neo4j authentication failed — check credentials")
#             raise RuntimeError("Neo4j auth failed. Check NEO4J_USERNAME/PASSWORD in config.")

#         except ServiceUnavailable:
#             logger.exception("Neo4j service unavailable")
#             raise RuntimeError("Neo4j unavailable. Is it running? Check NEO4J_URI in config.")

#     # -----------------------------
#     # Build Graph
#     # -----------------------------
#     def build_graph(
#         self,
#         doc_id: str,
#         entities: list[dict],
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Full graph build pipeline:
#         1. Clear existing graph for this doc (makes it idempotent)
#         2. Format entities with IDs
#         3. Create entity nodes in Neo4j
#         4. Extract relationships using LLM (in batches)
#         5. Create relationship edges in Neo4j

#         Calling /build-graph multiple times always produces
#         the same clean result — no duplicates ever.
#         """
#         if not entities:
#             logger.warning(f"No entities provided for graph | doc_id: {doc_id}")
#             return {"nodes": 0, "relationships": 0}

#         logger.info(f"Building graph | doc_id: {doc_id} | entities: {len(entities)}")

#         # Step 1 — clear existing graph for this doc before rebuilding
#         # This is what makes the function idempotent
#         self._clear_existing_graph(doc_id)

#         # Step 2 — attach entity_ids
#         formatted_entities = format_entities_for_graph(doc_id, entities)

#         # Step 3 — create nodes
#         nodes_created = self._create_entity_nodes(doc_id, formatted_entities)

#         # Step 4 — extract relationships in batches
#         # Each batch uses 3 chunks + 10 entities to stay within token limits
#         all_relationships = []

#         chunk_batches = [chunks[i:i+3] for i in range(0, min(len(chunks), 9), 3)]

#         for batch_idx, chunk_batch in enumerate(chunk_batches):
#             combined_text = " ".join(
#                 chunk.get("content", "") for chunk in chunk_batch
#             )

#             logger.info(f"Extracting relationships | batch {batch_idx + 1}/{len(chunk_batches)}")

#             batch_relationships = self._extract_relationships(
#                 formatted_entities,
#                 combined_text
#             )

#             all_relationships.extend(batch_relationships)

#         # Deduplicate relationships by (source, target, type)
#         all_relationships = self._deduplicate_relationships(all_relationships)

#         logger.info(f"Total unique relationships to create: {len(all_relationships)}")

#         # Step 5 — create edges
#         rels_created = self._create_relationships(doc_id, all_relationships)

#         logger.info(
#             f"Graph built | doc_id: {doc_id} | "
#             f"nodes: {nodes_created} | relationships: {rels_created}"
#         )

#         return {
#             "nodes": nodes_created,
#             "relationships": rels_created
#         }

#     # -----------------------------
#     # Clear Existing Graph (NEW)
#     # -----------------------------
#     def _clear_existing_graph(self, doc_id: str):
#         """
#         Delete all existing nodes and relationships for this doc_id
#         before rebuilding.

#         Makes build_graph fully idempotent — calling it multiple times
#         always produces the same clean result with no duplicates.

#         Note: relationships must be deleted before nodes in Neo4j.
#         """
#         try:
#             with self.driver.session() as session:

#                 # Delete relationships first — Neo4j requires this before deleting nodes
#                 session.run(
#                     """
#                     MATCH (e:Entity {doc_id: $doc_id})-[r]-()
#                     DELETE r
#                     """,
#                     doc_id=doc_id
#                 )

#                 # Then delete nodes
#                 session.run(
#                     """
#                     MATCH (e:Entity {doc_id: $doc_id})
#                     DELETE e
#                     """,
#                     doc_id=doc_id
#                 )

#                 logger.info(f"Cleared existing graph for doc_id: {doc_id}")

#         except Exception:
#             logger.exception(f"Failed to clear graph for doc_id: {doc_id}")

#     # -----------------------------
#     # Create Nodes
#     # -----------------------------
#     def _create_entity_nodes(
#         self,
#         doc_id: str,
#         entities: list[dict]
#     ) -> int:
#         """
#         Create a Neo4j node for each entity.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for entity in entities:
#                 try:
#                     session.run(
#                         """
#                         MERGE (e:Entity {entity_id: $entity_id})
#                         SET e.name = $name,
#                             e.type = $type,
#                             e.context = $context,
#                             e.doc_id = $doc_id
#                         """,
#                         entity_id=entity["entity_id"],
#                         name=entity["name"],
#                         type=entity["type"],
#                         context=entity.get("context", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1

#                 except Exception:
#                     logger.exception(f"Node creation failed for entity: {entity.get('name')}")

#         logger.info(f"Nodes created: {count}")
#         return count

#     # -----------------------------
#     # Extract Relationships via LLM
#     # -----------------------------
#     def _extract_relationships(
#         self,
#         entities: list[dict],
#         text: str
#     ) -> list[dict]:
#         """
#         Use LLM to identify relationships between entities.

#         Key limits to prevent JSON truncation:
#         - entities[:10]   — only 10 entities per call
#         - text[:1500]     — smaller input leaves more token budget for response
#         - max_tokens=800  — enough for 15 relationships in JSON
#         """
#         output = ""

#         try:
#             entity_names = [
#                 f"{e['name']} ({e['type']})" for e in entities[:10]
#             ]

#             prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
#                 entities="\n".join(entity_names),
#                 content=text[:1500]
#             )

#             output = invoke_model(prompt, max_tokens=800)

#             logger.debug(f"Raw relationship LLM output: {output[:500]}")

#             # Strip markdown code blocks if present
#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             # Extract JSON object even if there's text before/after
#             start = output.find("{")
#             end = output.rfind("}") + 1

#             if start == -1 or end == 0:
#                 logger.error(f"No JSON object found in LLM output: {output[:300]}")
#                 return []

#             json_str = output[start:end]
#             parsed = json.loads(json_str)
#             relationships = parsed.get("relationships", [])

#             logger.info(f"Relationships extracted by LLM: {len(relationships)}")
#             return relationships

#         except json.JSONDecodeError as e:
#             logger.error(
#                 f"Relationship JSON parse failed | "
#                 f"error: {e} | "
#                 f"raw output: {output[:300]}"
#             )
#             return []

#         except Exception:
#             logger.exception("Relationship extraction failed")
#             return []

#     # -----------------------------
#     # Deduplicate Relationships
#     # -----------------------------
#     def _deduplicate_relationships(
#         self,
#         relationships: list[dict]
#     ) -> list[dict]:
#         """
#         Remove duplicate relationships from batch processing.
#         Key = (source.lower, target.lower, relationship_type)
#         """
#         seen = set()
#         unique = []

#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()
#             rel_type = rel.get("relationship", "").upper().strip()

#             key = f"{source}_{target}_{rel_type}"

#             if key not in seen:
#                 seen.add(key)
#                 unique.append(rel)

#         logger.info(
#             f"Relationships after deduplication: "
#             f"{len(unique)} (from {len(relationships)})"
#         )

#         return unique

#     # -----------------------------
#     # Create Relationships (Edges)
#     # -----------------------------
#     def _create_relationships(
#         self,
#         doc_id: str,
#         relationships: list[dict]
#     ) -> int:
#         """
#         Create Neo4j edges between entity nodes.
#         MERGE on (type) only — doc_id in SET not MERGE key.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for rel in relationships:
#                 try:
#                     result = session.run(
#                         """
#                         MATCH (a:Entity {name: $source, doc_id: $doc_id})
#                         MATCH (b:Entity {name: $target, doc_id: $doc_id})
#                         MERGE (a)-[r:RELATES_TO {type: $rel_type}]->(b)
#                         SET r.description = $description,
#                             r.doc_id = $doc_id
#                         RETURN count(r) as created
#                         """,
#                         source=rel.get("source"),
#                         target=rel.get("target"),
#                         rel_type=rel.get("relationship", "RELATED_TO"),
#                         description=rel.get("description", ""),
#                         doc_id=doc_id
#                     )

#                     record = result.single()
#                     if record and record["created"] > 0:
#                         count += 1
#                     else:
#                         logger.debug(
#                             f"Skipped relationship — node not found: "
#                             f"{rel.get('source')} -> {rel.get('target')}"
#                         )

#                 except Exception:
#                     logger.exception(
#                         f"Relationship creation failed: "
#                         f"{rel.get('source')} -> {rel.get('target')}"
#                     )

#         logger.info(f"Relationships created: {count}")
#         return count

#     # -----------------------------
#     # Query Graph
#     # -----------------------------
#     def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
#         """
#         Query all relationships for a given entity within a document.
#         Searches both directions (entity as source OR target).
#         DISTINCT + ORDER BY ensures no duplicate results.
#         """
#         logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity_name}")

#         results = []

#         try:
#             with self.driver.session() as session:
#                 records = session.run(
#                     """
#                     MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
#                     WHERE toLower(a.name) CONTAINS toLower($entity_name)
#                        OR toLower(b.name) CONTAINS toLower($entity_name)
#                     RETURN DISTINCT
#                         a.name AS source,
#                         r.type AS relationship,
#                         b.name AS target,
#                         r.description AS description
#                     ORDER BY source, relationship
#                     LIMIT 50
#                     """,
#                     doc_id=doc_id,
#                     entity_name=entity_name
#                 )

#                 for record in records:
#                     results.append({
#                         "source": record["source"],
#                         "relationship": record["relationship"],
#                         "target": record["target"],
#                         "description": record["description"],
#                     })

#         except Exception:
#             logger.exception(f"Graph query failed for entity: {entity_name}")

#         logger.info(f"Graph query returned {len(results)} relationships")

#         return results

#     # -----------------------------
#     # Cleanup
#     # -----------------------------
#     def close(self):
#         if self.driver:
#             self.driver.close()
#             logger.info("Neo4j connection closed")


# # -----------------------------
# # Module-level functions
# # called from api.py
# # -----------------------------
# _graph_builder = None


# def _get_graph_builder() -> GraphBuilder:
#     global _graph_builder
#     if _graph_builder is None:
#         _graph_builder = GraphBuilder()
#     return _graph_builder


# def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#     return _get_graph_builder().build_graph(doc_id, entities, chunks)


# def query_graph(doc_id: str, entity_name: str) -> list[dict]:
#     return _get_graph_builder().query_graph(doc_id, entity_name)

"""V5 separate graph"""
# import logging
# import json
# from neo4j import GraphDatabase
# from neo4j.exceptions import ServiceUnavailable, AuthError
# from app.core.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
# from app.services.summarizer import invoke_model
# from app.services.extractor import format_entities_for_graph

# logger = logging.getLogger(__name__)


# RELATIONSHIP_EXTRACTION_PROMPT = """
# You are an expert knowledge graph builder.

# Given the following list of entities and the source text, identify meaningful relationships between entities.

# Entities:
# {entities}

# Source Text:
# {content}

# Rules:
# - Only create relationships between entities that are explicitly mentioned together in the text
# - Use clear, meaningful relationship labels in UPPERCASE_SNAKE_CASE
# - Each relationship must have a source AND target that exist in the entities list above
# - Return a maximum of 8 relationships

# For each relationship return:
# - source: exact name of the first entity (must match entity list)
# - target: exact name of the second entity (must match entity list)
# - relationship: UPPERCASE_SNAKE_CASE label (e.g. WORKS_FOR, PART_OF, USED_BY, RELATED_TO, DEVELOPED_BY)
# - description: one-line description of the relationship

# You MUST return ONLY a raw JSON object. No explanation, no markdown, no code blocks.
# Start your response with {{ and end with }}

# {{
#     "relationships": [
#         {{
#             "source": "Entity A",
#             "target": "Entity B",
#             "relationship": "RELATIONSHIP_TYPE",
#             "description": "brief description"
#         }}
#     ]
# }}
# """


# class GraphBuilder:
#     """
#     Handles Neo4j knowledge graph operations.

#     Nodes  = entities (PERSON, ORG, CONCEPT etc.)
#     Edges  = relationships between entities

#     Each node and relationship is scoped to a doc_id
#     so graphs from different documents don't mix.
#     """

#     def __init__(self):
#         self.driver = None
#         self._connect()

#     # -----------------------------
#     # Connection
#     # -----------------------------
#     def _connect(self):
#         try:
#             self.driver = GraphDatabase.driver(
#                 NEO4J_URI,
#                 auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
#             )
#             self.driver.verify_connectivity()
#             logger.info("Neo4j connected successfully")

#         except AuthError:
#             logger.exception("Neo4j authentication failed — check credentials")
#             raise RuntimeError("Neo4j auth failed. Check NEO4J_USERNAME/PASSWORD in config.")

#         except ServiceUnavailable:
#             logger.exception("Neo4j service unavailable")
#             raise RuntimeError("Neo4j unavailable. Is it running? Check NEO4J_URI in config.")

#     # -----------------------------
#     # Build Graph
#     # -----------------------------
#     def build_graph(
#         self,
#         doc_id: str,
#         entities: list[dict],
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Full graph build pipeline:
#         1. Clear existing graph for this doc (makes it idempotent)
#         2. Format entities with IDs
#         3. Create entity nodes in Neo4j
#         4. Extract relationships using LLM (in batches)
#            - Uses ALL chunks (not just first 9)
#            - Rotates entity batches so all entities get coverage
#         5. Create relationship edges in Neo4j
#         """
#         if not entities:
#             logger.warning(f"No entities provided for graph | doc_id: {doc_id}")
#             return {"nodes": 0, "relationships": 0}

#         logger.info(f"Building graph | doc_id: {doc_id} | entities: {len(entities)}")

#         # Step 1 — clear existing graph
#         self._clear_existing_graph(doc_id)

#         # Step 2 — attach entity_ids
#         formatted_entities = format_entities_for_graph(doc_id, entities)

#         # Step 3 — create nodes
#         nodes_created = self._create_entity_nodes(doc_id, formatted_entities)

#         # Step 4 — extract relationships in batches
#         all_relationships = []

#         # FIX 1 — use ALL chunks, not just first 9
#         chunk_batches = [chunks[i:i+3] for i in range(0, len(chunks), 3)]

#         logger.info(f"Processing {len(chunk_batches)} chunk batches | total chunks: {len(chunks)}")

#         for batch_idx, chunk_batch in enumerate(chunk_batches):
#             combined_text = " ".join(
#                 chunk.get("content", "") for chunk in chunk_batch
#             )

#             # FIX 2 — rotate entity batches so ALL entities get coverage
#             # Each batch uses a different slice of entities
#             # e.g. batch 0 → entities 0-7, batch 1 → entities 8-15, etc.
#             entity_start = (batch_idx * 8) % len(formatted_entities)
#             entity_batch = formatted_entities[entity_start:entity_start + 8]

#             # Wrap around if slice is too small
#             if len(entity_batch) < 4 and len(formatted_entities) >= 8:
#                 entity_batch = formatted_entities[-8:]

#             logger.info(
#                 f"Extracting relationships | batch {batch_idx + 1}/{len(chunk_batches)} | "
#                 f"entities {entity_start}-{entity_start + len(entity_batch)}"
#             )

#             batch_relationships = self._extract_relationships(
#                 entity_batch,
#                 combined_text
#             )

#             all_relationships.extend(batch_relationships)

#         # Deduplicate relationships by (source, target, type)
#         all_relationships = self._deduplicate_relationships(all_relationships)

#         logger.info(f"Total unique relationships to create: {len(all_relationships)}")

#         # Step 5 — create edges
#         rels_created = self._create_relationships(doc_id, all_relationships)

#         logger.info(
#             f"Graph built | doc_id: {doc_id} | "
#             f"nodes: {nodes_created} | relationships: {rels_created}"
#         )

#         return {
#             "nodes": nodes_created,
#             "relationships": rels_created
#         }

#     # -----------------------------
#     # Clear Existing Graph
#     # -----------------------------
#     def _clear_existing_graph(self, doc_id: str):
#         """
#         Delete all existing nodes and relationships for this doc_id
#         before rebuilding. Makes build_graph fully idempotent.
#         """
#         try:
#             with self.driver.session() as session:

#                 # Delete relationships first
#                 session.run(
#                     """
#                     MATCH (e:Entity {doc_id: $doc_id})-[r]-()
#                     DELETE r
#                     """,
#                     doc_id=doc_id
#                 )

#                 # Then delete nodes
#                 session.run(
#                     """
#                     MATCH (e:Entity {doc_id: $doc_id})
#                     DELETE e
#                     """,
#                     doc_id=doc_id
#                 )

#                 logger.info(f"Cleared existing graph for doc_id: {doc_id}")

#         except Exception:
#             logger.exception(f"Failed to clear graph for doc_id: {doc_id}")

#     # -----------------------------
#     # Create Nodes
#     # -----------------------------
#     def _create_entity_nodes(
#         self,
#         doc_id: str,
#         entities: list[dict]
#     ) -> int:
#         """
#         Create a Neo4j node for each entity.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for entity in entities:
#                 try:
#                     session.run(
#                         """
#                         MERGE (e:Entity {entity_id: $entity_id})
#                         SET e.name = $name,
#                             e.type = $type,
#                             e.context = $context,
#                             e.doc_id = $doc_id
#                         """,
#                         entity_id=entity["entity_id"],
#                         name=entity["name"],
#                         type=entity["type"],
#                         context=entity.get("context", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1

#                 except Exception:
#                     logger.exception(f"Node creation failed for entity: {entity.get('name')}")

#         logger.info(f"Nodes created: {count}")
#         return count

#     # -----------------------------
#     # Extract Relationships via LLM
#     # -----------------------------
#     def _extract_relationships(
#         self,
#         entities: list[dict],
#         text: str
#     ) -> list[dict]:
#         """
#         Use LLM to identify relationships between entities.

#         Key limits to prevent JSON truncation:
#         - entities[:8]    — 8 entities per call (was 10, caused truncation)
#         - text[:1000]     — smaller input (was 1500)
#         - max_tokens=1200 — enough for 8 relationships (was 800, too small)
#         - max 8 rels      — smaller JSON = never truncates
#         """
#         output = ""

#         try:
#             # FIX 3 — cap at 8 entities (was 10)
#             entity_names = [
#                 f"{e['name']} ({e['type']})" for e in entities[:8]
#             ]

#             prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
#                 entities="\n".join(entity_names),
#                 content=text[:1000]     # FIX 4 — smaller input (was 1500)
#             )

#             # FIX 5 — increase max_tokens (was 800, caused truncation)
#             output = invoke_model(prompt, max_tokens=1200)

#             logger.debug(f"Raw relationship LLM output: {output[:500]}")

#             # Strip markdown code blocks if present
#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             # Extract JSON object even if there's text before/after
#             start = output.find("{")
#             end = output.rfind("}") + 1

#             if start == -1 or end == 0:
#                 logger.error(f"No JSON object found in LLM output: {output[:300]}")
#                 return []

#             json_str = output[start:end]
#             parsed = json.loads(json_str)
#             relationships = parsed.get("relationships", [])

#             logger.info(f"Relationships extracted by LLM: {len(relationships)}")
#             return relationships

#         except json.JSONDecodeError as e:
#             logger.error(
#                 f"Relationship JSON parse failed | "
#                 f"error: {e} | "
#                 f"raw output: {output[:300]}"
#             )
#             return []

#         except Exception:
#             logger.exception("Relationship extraction failed")
#             return []

#     # -----------------------------
#     # Deduplicate Relationships
#     # -----------------------------
#     def _deduplicate_relationships(
#         self,
#         relationships: list[dict]
#     ) -> list[dict]:
#         """
#         Remove duplicate relationships from batch processing.
#         Key = (source.lower, target.lower, relationship_type)
#         """
#         seen = set()
#         unique = []

#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()
#             rel_type = rel.get("relationship", "").upper().strip()

#             key = f"{source}_{target}_{rel_type}"

#             if key not in seen:
#                 seen.add(key)
#                 unique.append(rel)

#         logger.info(
#             f"Relationships after deduplication: "
#             f"{len(unique)} (from {len(relationships)})"
#         )

#         return unique

#     # -----------------------------
#     # Create Relationships (Edges)
#     # -----------------------------
#     def _create_relationships(
#         self,
#         doc_id: str,
#         relationships: list[dict]
#     ) -> int:
#         """
#         Create Neo4j edges between entity nodes.
#         MERGE on (type) only — doc_id in SET not MERGE key.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for rel in relationships:
#                 try:
#                     result = session.run(
#                         """
#                         MATCH (a:Entity {name: $source, doc_id: $doc_id})
#                         MATCH (b:Entity {name: $target, doc_id: $doc_id})
#                         MERGE (a)-[r:RELATES_TO {type: $rel_type}]->(b)
#                         SET r.description = $description,
#                             r.doc_id = $doc_id
#                         RETURN count(r) as created
#                         """,
#                         source=rel.get("source"),
#                         target=rel.get("target"),
#                         rel_type=rel.get("relationship", "RELATED_TO"),
#                         description=rel.get("description", ""),
#                         doc_id=doc_id
#                     )

#                     record = result.single()
#                     if record and record["created"] > 0:
#                         count += 1
#                     else:
#                         logger.debug(
#                             f"Skipped relationship — node not found: "
#                             f"{rel.get('source')} -> {rel.get('target')}"
#                         )

#                 except Exception:
#                     logger.exception(
#                         f"Relationship creation failed: "
#                         f"{rel.get('source')} -> {rel.get('target')}"
#                     )

#         logger.info(f"Relationships created: {count}")
#         return count

#     # -----------------------------
#     # Query Graph
#     # -----------------------------
#     def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
#         """
#         Query all relationships for a given entity within a document.
#         Searches both directions (entity as source OR target).
#         DISTINCT + ORDER BY ensures no duplicate results.
#         """
#         logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity_name}")

#         results = []

#         try:
#             with self.driver.session() as session:
#                 records = session.run(
#                     """
#                     MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
#                     WHERE toLower(a.name) CONTAINS toLower($entity_name)
#                        OR toLower(b.name) CONTAINS toLower($entity_name)
#                     RETURN DISTINCT
#                         a.name AS source,
#                         r.type AS relationship,
#                         b.name AS target,
#                         r.description AS description
#                     ORDER BY source, relationship
#                     LIMIT 50
#                     """,
#                     doc_id=doc_id,
#                     entity_name=entity_name
#                 )

#                 for record in records:
#                     results.append({
#                         "source": record["source"],
#                         "relationship": record["relationship"],
#                         "target": record["target"],
#                         "description": record["description"],
#                     })

#         except Exception:
#             logger.exception(f"Graph query failed for entity: {entity_name}")

#         logger.info(f"Graph query returned {len(results)} relationships")

#         return results

#     # -----------------------------
#     # Cleanup
#     # -----------------------------
#     def close(self):
#         if self.driver:
#             self.driver.close()
#             logger.info("Neo4j connection closed")


# # -----------------------------
# # Module-level functions
# # called from api.py
# # -----------------------------
# _graph_builder = None


# def _get_graph_builder() -> GraphBuilder:
#     global _graph_builder
#     if _graph_builder is None:
#         _graph_builder = GraphBuilder()
#     return _graph_builder


# def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#     return _get_graph_builder().build_graph(doc_id, entities, chunks)


# def query_graph(doc_id: str, entity_name: str) -> list[dict]:
#     return _get_graph_builder().query_graph(doc_id, entity_name)

"""V6 finallll working"""

# import logging
# import json
# from neo4j import GraphDatabase
# from neo4j.exceptions import ServiceUnavailable, AuthError
# from app.core.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
# from app.services.summarizer import invoke_model
# from app.services.extractor import format_entities_for_graph

# logger = logging.getLogger(__name__)


# RELATIONSHIP_EXTRACTION_PROMPT = """
# You are an expert knowledge graph builder.

# Given the following list of entities and the source text, identify meaningful relationships between entities.

# Entities:
# {entities}

# Source Text:
# {content}

# Rules:
# - Only create relationships between entities that are explicitly mentioned together in the text
# - Use clear, meaningful relationship labels in UPPERCASE_SNAKE_CASE
# - Each relationship must have a source AND target that exist in the entities list above
# - Return a maximum of 8 relationships

# For each relationship return:
# - source: exact name of the first entity (must match entity list)
# - target: exact name of the second entity (must match entity list)
# - relationship: UPPERCASE_SNAKE_CASE label (e.g. WORKS_FOR, PART_OF, USED_BY, RELATED_TO, DEVELOPED_BY)
# - description: one-line description of the relationship

# You MUST return ONLY a raw JSON object. No explanation, no markdown, no code blocks.
# Start your response with {{ and end with }}

# {{
#     "relationships": [
#         {{
#             "source": "Entity A",
#             "target": "Entity B",
#             "relationship": "RELATIONSHIP_TYPE",
#             "description": "brief description"
#         }}
#     ]
# }}
# """


# class GraphBuilder:
#     """
#     Handles Neo4j knowledge graph operations.

#     Nodes  = entities (PERSON, ORG, CONCEPT etc.)
#     Edges  = relationships between entities

#     Each node and relationship is scoped to a doc_id
#     so graphs from different documents don't mix.
#     """

#     def __init__(self):
#         self.driver = None
#         self._connect()

#     # -----------------------------
#     # Connection
#     # -----------------------------
#     def _connect(self):
#         try:
#             self.driver = GraphDatabase.driver(
#                 NEO4J_URI,
#                 auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
#             )
#             self.driver.verify_connectivity()
#             logger.info("Neo4j connected successfully")

#         except AuthError:
#             logger.exception("Neo4j authentication failed — check credentials")
#             raise RuntimeError("Neo4j auth failed. Check NEO4J_USERNAME/PASSWORD in config.")

#         except ServiceUnavailable:
#             logger.exception("Neo4j service unavailable")
#             raise RuntimeError("Neo4j unavailable. Is it running? Check NEO4J_URI in config.")

#     # -----------------------------
#     # Build Graph
#     # -----------------------------
#     def build_graph(
#         self,
#         doc_id: str,
#         entities: list[dict],
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Full graph build pipeline:
#         1. Clear existing graph for this doc (makes it idempotent)
#         2. Format entities with IDs
#         3. Create entity nodes in Neo4j
#         4. Extract relationships using LLM (in batches with entity rotation)
#         5. Cross-cluster pass to connect isolated clusters
#         6. Create relationship edges in Neo4j
#         """
#         if not entities:
#             logger.warning(f"No entities provided for graph | doc_id: {doc_id}")
#             return {"nodes": 0, "relationships": 0}

#         logger.info(f"Building graph | doc_id: {doc_id} | entities: {len(entities)}")

#         # Step 1 — clear existing graph
#         self._clear_existing_graph(doc_id)

#         # Step 2 — attach entity_ids
#         formatted_entities = format_entities_for_graph(doc_id, entities)

#         # Step 3 — create nodes
#         nodes_created = self._create_entity_nodes(doc_id, formatted_entities)

#         # Step 4 — extract relationships in batches with entity rotation
#         all_relationships = []

#         chunk_batches = [chunks[i:i+3] for i in range(0, len(chunks), 3)]

#         logger.info(f"Processing {len(chunk_batches)} chunk batches | total chunks: {len(chunks)}")

#         for batch_idx, chunk_batch in enumerate(chunk_batches):
#             combined_text = " ".join(
#                 chunk.get("content", "") for chunk in chunk_batch
#             )

#             # Rotate entity batches so ALL entities get coverage
#             entity_start = (batch_idx * 8) % len(formatted_entities)
#             entity_batch = formatted_entities[entity_start:entity_start + 8]

#             # Wrap around if slice is too small
#             if len(entity_batch) < 4 and len(formatted_entities) >= 8:
#                 entity_batch = formatted_entities[-8:]

#             logger.info(
#                 f"Extracting relationships | batch {batch_idx + 1}/{len(chunk_batches)} | "
#                 f"entities {entity_start}-{entity_start + len(entity_batch)}"
#             )

#             batch_relationships = self._extract_relationships(
#                 entity_batch,
#                 combined_text
#             )

#             all_relationships.extend(batch_relationships)

#         # Step 5 — cross-cluster pass to connect isolated clusters
#         # Mixes entities from different batches to bridge disconnected nodes
#         if len(formatted_entities) > 16:
#             logger.info("Running cross-cluster relationship pass")

#             cross_entities = (
#                 formatted_entities[:4] +        # first cluster
#                 formatted_entities[len(formatted_entities)//2:len(formatted_entities)//2 + 4]  # middle
#             )

#             combined_text = " ".join(
#                 chunk.get("content", "") for chunk in chunks[:3]
#             )

#             cross_relationships = self._extract_relationships(
#                 cross_entities,
#                 combined_text
#             )

#             all_relationships.extend(cross_relationships)
#             logger.info(f"Cross-cluster relationships extracted: {len(cross_relationships)}")

#         # Deduplicate relationships by (source, target, type)
#         all_relationships = self._deduplicate_relationships(all_relationships)

#         logger.info(f"Total unique relationships to create: {len(all_relationships)}")

#         # Step 6 — create edges
#         rels_created = self._create_relationships(doc_id, all_relationships)

#         logger.info(
#             f"Graph built | doc_id: {doc_id} | "
#             f"nodes: {nodes_created} | relationships: {rels_created}"
#         )

#         return {
#             "nodes": nodes_created,
#             "relationships": rels_created
#         }

#     # -----------------------------
#     # Clear Existing Graph
#     # -----------------------------
#     def _clear_existing_graph(self, doc_id: str):
#         """
#         Delete all existing nodes and relationships for this doc_id
#         before rebuilding. Makes build_graph fully idempotent.
#         """
#         try:
#             with self.driver.session() as session:

#                 session.run(
#                     """
#                     MATCH (e:Entity {doc_id: $doc_id})-[r]-()
#                     DELETE r
#                     """,
#                     doc_id=doc_id
#                 )

#                 session.run(
#                     """
#                     MATCH (e:Entity {doc_id: $doc_id})
#                     DELETE e
#                     """,
#                     doc_id=doc_id
#                 )

#                 logger.info(f"Cleared existing graph for doc_id: {doc_id}")

#         except Exception:
#             logger.exception(f"Failed to clear graph for doc_id: {doc_id}")

#     # -----------------------------
#     # Create Nodes
#     # -----------------------------
#     def _create_entity_nodes(
#         self,
#         doc_id: str,
#         entities: list[dict]
#     ) -> int:
#         """
#         Create a Neo4j node for each entity.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for entity in entities:
#                 try:
#                     session.run(
#                         """
#                         MERGE (e:Entity {entity_id: $entity_id})
#                         SET e.name = $name,
#                             e.type = $type,
#                             e.context = $context,
#                             e.doc_id = $doc_id
#                         """,
#                         entity_id=entity["entity_id"],
#                         name=entity["name"],
#                         type=entity["type"],
#                         context=entity.get("context", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1

#                 except Exception:
#                     logger.exception(f"Node creation failed for entity: {entity.get('name')}")

#         logger.info(f"Nodes created: {count}")
#         return count

#     # -----------------------------
#     # Extract Relationships via LLM
#     # -----------------------------
#     def _extract_relationships(
#         self,
#         entities: list[dict],
#         text: str
#     ) -> list[dict]:
#         """
#         Use LLM to identify relationships between entities.

#         Key limits to prevent JSON truncation:
#         - entities[:8]    — 8 entities per call
#         - text[:1000]     — smaller input
#         - max_tokens=1200 — enough for 8 relationships
#         - max 8 rels      — smaller JSON = never truncates
#         """
#         output = ""

#         try:
#             entity_names = [
#                 f"{e['name']} ({e['type']})" for e in entities[:8]
#             ]

#             prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
#                 entities="\n".join(entity_names),
#                 content=text[:1000]
#             )

#             output = invoke_model(prompt, max_tokens=1200)

#             logger.debug(f"Raw relationship LLM output: {output[:500]}")

#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             start = output.find("{")
#             end = output.rfind("}") + 1

#             if start == -1 or end == 0:
#                 logger.error(f"No JSON object found in LLM output: {output[:300]}")
#                 return []

#             json_str = output[start:end]
#             parsed = json.loads(json_str)
#             relationships = parsed.get("relationships", [])

#             logger.info(f"Relationships extracted by LLM: {len(relationships)}")
#             return relationships

#         except json.JSONDecodeError as e:
#             logger.error(
#                 f"Relationship JSON parse failed | "
#                 f"error: {e} | "
#                 f"raw output: {output[:300]}"
#             )
#             return []

#         except Exception:
#             logger.exception("Relationship extraction failed")
#             return []

#     # -----------------------------
#     # Deduplicate Relationships
#     # -----------------------------
#     def _deduplicate_relationships(
#         self,
#         relationships: list[dict]
#     ) -> list[dict]:
#         """
#         Remove duplicate relationships from batch processing.
#         Key = (source.lower, target.lower, relationship_type)
#         """
#         seen = set()
#         unique = []

#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()
#             rel_type = rel.get("relationship", "").upper().strip()

#             key = f"{source}_{target}_{rel_type}"

#             if key not in seen:
#                 seen.add(key)
#                 unique.append(rel)

#         logger.info(
#             f"Relationships after deduplication: "
#             f"{len(unique)} (from {len(relationships)})"
#         )

#         return unique

#     # -----------------------------
#     # Create Relationships (Edges)
#     # -----------------------------
#     def _create_relationships(
#         self,
#         doc_id: str,
#         relationships: list[dict]
#     ) -> int:
#         """
#         Create Neo4j edges between entity nodes.
#         MERGE on (type) only — doc_id in SET not MERGE key.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for rel in relationships:
#                 try:
#                     result = session.run(
#                         """
#                         MATCH (a:Entity {name: $source, doc_id: $doc_id})
#                         MATCH (b:Entity {name: $target, doc_id: $doc_id})
#                         MERGE (a)-[r:RELATES_TO {type: $rel_type}]->(b)
#                         SET r.description = $description,
#                             r.doc_id = $doc_id
#                         RETURN count(r) as created
#                         """,
#                         source=rel.get("source"),
#                         target=rel.get("target"),
#                         rel_type=rel.get("relationship", "RELATED_TO"),
#                         description=rel.get("description", ""),
#                         doc_id=doc_id
#                     )

#                     record = result.single()
#                     if record and record["created"] > 0:
#                         count += 1
#                     else:
#                         logger.debug(
#                             f"Skipped relationship — node not found: "
#                             f"{rel.get('source')} -> {rel.get('target')}"
#                         )

#                 except Exception:
#                     logger.exception(
#                         f"Relationship creation failed: "
#                         f"{rel.get('source')} -> {rel.get('target')}"
#                     )

#         logger.info(f"Relationships created: {count}")
#         return count

#     # -----------------------------
#     # Query Graph
#     # -----------------------------
#     def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
#         """
#         Query all relationships for a given entity within a document.
#         Searches both directions (entity as source OR target).
#         DISTINCT + ORDER BY ensures no duplicate results.
#         """
#         logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity_name}")

#         results = []

#         try:
#             with self.driver.session() as session:
#                 records = session.run(
#                     """
#                     MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
#                     WHERE toLower(a.name) CONTAINS toLower($entity_name)
#                        OR toLower(b.name) CONTAINS toLower($entity_name)
#                     RETURN DISTINCT
#                         a.name AS source,
#                         r.type AS relationship,
#                         b.name AS target,
#                         r.description AS description
#                     ORDER BY source, relationship
#                     LIMIT 50
#                     """,
#                     doc_id=doc_id,
#                     entity_name=entity_name
#                 )

#                 for record in records:
#                     results.append({
#                         "source": record["source"],
#                         "relationship": record["relationship"],
#                         "target": record["target"],
#                         "description": record["description"],
#                     })

#         except Exception:
#             logger.exception(f"Graph query failed for entity: {entity_name}")

#         logger.info(f"Graph query returned {len(results)} relationships")

#         return results

#     # -----------------------------
#     # Cleanup
#     # -----------------------------
#     def close(self):
#         if self.driver:
#             self.driver.close()
#             logger.info("Neo4j connection closed")


# # -----------------------------
# # Module-level functions
# # called from api.py
# # -----------------------------
# _graph_builder = None


# def _get_graph_builder() -> GraphBuilder:
#     global _graph_builder
#     if _graph_builder is None:
#         _graph_builder = GraphBuilder()
#     return _graph_builder


# def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#     return _get_graph_builder().build_graph(doc_id, entities, chunks)


# def query_graph(doc_id: str, entity_name: str) -> list[dict]:
#     return _get_graph_builder().query_graph(doc_id, entity_name)

"""Final test"""

# import logging
# import json
# from neo4j import GraphDatabase
# from neo4j.exceptions import ServiceUnavailable, AuthError
# from app.core.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
# from app.services.summarizer import invoke_model
# from app.services.extractor import format_entities_for_graph

# logger = logging.getLogger(__name__)


# # -----------------------------
# # Example Entity Filter
# # -----------------------------
# # These entities appear in the RAG paper as illustrative examples
# # (Jeopardy questions, MSMARCO examples, etc.) and should never
# # form relationships in the knowledge graph
# EXAMPLE_ENTITY_NAMES = {
#     "hemingway", "dante", "the sun also rises", "a farewell to arms",
#     "the divine comedy", "the sun", "lost generation", "world cup",
#     "mount rainier national park", "scotland", "hawaii", "washington",
#     "barack obama", "middle ear", "pound sterling"
# }


# RELATIONSHIP_EXTRACTION_PROMPT = """
# You are an expert knowledge graph builder.

# Given the following list of entities and the source text, identify meaningful relationships between entities.

# Entities:
# {entities}

# Source Text:
# {content}

# Rules:
# - Only create relationships between entities that are explicitly mentioned together in the text
# - Use clear, meaningful relationship labels in UPPERCASE_SNAKE_CASE
# - Each relationship must have a source AND target that exist in the entities list above
# - Return a maximum of 8 relationships
# - Do NOT create relationships based on examples, analogies, or hypothetical scenarios in the text
# - Do NOT create relationships between fictional characters or illustrative examples used in the paper
# - Only create relationships that describe real, factual connections between the actual entities of the document
# - If the text uses examples like "Barack Obama was born in Hawaii" to illustrate a concept, do NOT extract that as a relationship

# For each relationship return:
# - source: exact name of the first entity (must match entity list)
# - target: exact name of the second entity (must match entity list)
# - relationship: UPPERCASE_SNAKE_CASE label (e.g. WORKS_FOR, PART_OF, USED_BY, RELATED_TO, DEVELOPED_BY)
# - description: one-line description of the relationship

# You MUST return ONLY a raw JSON object. No explanation, no markdown, no code blocks.
# Start your response with {{ and end with }}

# {{
#     "relationships": [
#         {{
#             "source": "Entity A",
#             "target": "Entity B",
#             "relationship": "RELATIONSHIP_TYPE",
#             "description": "brief description"
#         }}
#     ]
# }}
# """


# class GraphBuilder:
#     """
#     Handles Neo4j knowledge graph operations.

#     Nodes  = entities (PERSON, ORG, CONCEPT etc.)
#     Edges  = relationships between entities

#     Each node and relationship is scoped to a doc_id
#     so graphs from different documents don't mix.
#     """

#     def __init__(self):
#         self.driver = None
#         self._connect()

#     # -----------------------------
#     # Connection
#     # -----------------------------
#     def _connect(self):
#         try:
#             self.driver = GraphDatabase.driver(
#                 NEO4J_URI,
#                 auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
#             )
#             self.driver.verify_connectivity()
#             logger.info("Neo4j connected successfully")

#         except AuthError:
#             logger.exception("Neo4j authentication failed — check credentials")
#             raise RuntimeError("Neo4j auth failed. Check NEO4J_USERNAME/PASSWORD in config.")

#         except ServiceUnavailable:
#             logger.exception("Neo4j service unavailable")
#             raise RuntimeError("Neo4j unavailable. Is it running? Check NEO4J_URI in config.")

#     # -----------------------------
#     # Build Graph
#     # -----------------------------
#     def build_graph(
#         self,
#         doc_id: str,
#         entities: list[dict],
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Full graph build pipeline:
#         1. Clear existing graph for this doc (makes it idempotent)
#         2. Format entities with IDs
#         3. Create entity nodes in Neo4j
#         4. Extract relationships using LLM (in batches with entity rotation)
#         5. Cross-cluster pass to connect isolated clusters
#         6. Filter out example-entity relationships
#         7. Create relationship edges in Neo4j
#         """
#         if not entities:
#             logger.warning(f"No entities provided for graph | doc_id: {doc_id}")
#             return {"nodes": 0, "relationships": 0}

#         logger.info(f"Building graph | doc_id: {doc_id} | entities: {len(entities)}")

#         # Step 1 — clear existing graph
#         self._clear_existing_graph(doc_id)

#         # Step 2 — attach entity_ids
#         formatted_entities = format_entities_for_graph(doc_id, entities)

#         # Step 3 — create nodes
#         nodes_created = self._create_entity_nodes(doc_id, formatted_entities)

#         # Step 4 — extract relationships in batches with entity rotation
#         all_relationships = []

#         chunk_batches = [chunks[i:i+3] for i in range(0, len(chunks), 3)]

#         logger.info(f"Processing {len(chunk_batches)} chunk batches | total chunks: {len(chunks)}")

#         for batch_idx, chunk_batch in enumerate(chunk_batches):
#             combined_text = " ".join(
#                 chunk.get("content", "") for chunk in chunk_batch
#             )

#             # Rotate entity batches so ALL entities get coverage
#             entity_start = (batch_idx * 8) % len(formatted_entities)
#             entity_batch = formatted_entities[entity_start:entity_start + 8]

#             # Wrap around if slice is too small
#             if len(entity_batch) < 4 and len(formatted_entities) >= 8:
#                 entity_batch = formatted_entities[-8:]

#             logger.info(
#                 f"Extracting relationships | batch {batch_idx + 1}/{len(chunk_batches)} | "
#                 f"entities {entity_start}-{entity_start + len(entity_batch)}"
#             )

#             batch_relationships = self._extract_relationships(
#                 entity_batch,
#                 combined_text
#             )

#             all_relationships.extend(batch_relationships)

#         # Step 5 — cross-cluster pass to connect isolated clusters
#         if len(formatted_entities) > 16:
#             logger.info("Running cross-cluster relationship pass")

#             cross_entities = (
#                 formatted_entities[:4] +
#                 formatted_entities[len(formatted_entities)//2:len(formatted_entities)//2 + 4]
#             )

#             combined_text = " ".join(
#                 chunk.get("content", "") for chunk in chunks[:3]
#             )

#             cross_relationships = self._extract_relationships(
#                 cross_entities,
#                 combined_text
#             )

#             all_relationships.extend(cross_relationships)
#             logger.info(f"Cross-cluster relationships extracted: {len(cross_relationships)}")

#         # Step 6 — filter out example-entity relationships
#         all_relationships = self._filter_example_relationships(all_relationships)

#         # Step 7 — deduplicate relationships by (source, target, type)
#         all_relationships = self._deduplicate_relationships(all_relationships)

#         logger.info(f"Total unique relationships to create: {len(all_relationships)}")

#         # Step 8 — create edges
#         rels_created = self._create_relationships(doc_id, all_relationships)

#         logger.info(
#             f"Graph built | doc_id: {doc_id} | "
#             f"nodes: {nodes_created} | relationships: {rels_created}"
#         )

#         return {
#             "nodes": nodes_created,
#             "relationships": rels_created
#         }

#     # -----------------------------
#     # Clear Existing Graph
#     # -----------------------------
#     def _clear_existing_graph(self, doc_id: str):
#         """
#         Delete all existing nodes and relationships for this doc_id
#         before rebuilding. Makes build_graph fully idempotent.
#         """
#         try:
#             with self.driver.session() as session:

#                 session.run(
#                     """
#                     MATCH (e:Entity {doc_id: $doc_id})-[r]-()
#                     DELETE r
#                     """,
#                     doc_id=doc_id
#                 )

#                 session.run(
#                     """
#                     MATCH (e:Entity {doc_id: $doc_id})
#                     DELETE e
#                     """,
#                     doc_id=doc_id
#                 )

#                 logger.info(f"Cleared existing graph for doc_id: {doc_id}")

#         except Exception:
#             logger.exception(f"Failed to clear graph for doc_id: {doc_id}")

#     # -----------------------------
#     # Create Nodes
#     # -----------------------------
#     def _create_entity_nodes(
#         self,
#         doc_id: str,
#         entities: list[dict]
#     ) -> int:
#         """
#         Create a Neo4j node for each entity.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for entity in entities:
#                 try:
#                     session.run(
#                         """
#                         MERGE (e:Entity {entity_id: $entity_id})
#                         SET e.name = $name,
#                             e.type = $type,
#                             e.context = $context,
#                             e.doc_id = $doc_id
#                         """,
#                         entity_id=entity["entity_id"],
#                         name=entity["name"],
#                         type=entity["type"],
#                         context=entity.get("context", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1

#                 except Exception:
#                     logger.exception(f"Node creation failed for entity: {entity.get('name')}")

#         logger.info(f"Nodes created: {count}")
#         return count

#     # -----------------------------
#     # Extract Relationships via LLM
#     # -----------------------------
#     def _extract_relationships(
#         self,
#         entities: list[dict],
#         text: str
#     ) -> list[dict]:
#         """
#         Use LLM to identify relationships between entities.

#         Key limits to prevent JSON truncation:
#         - entities[:8]    — 8 entities per call
#         - text[:1000]     — smaller input
#         - max_tokens=1200 — enough for 8 relationships
#         - max 8 rels      — smaller JSON = never truncates
#         """
#         output = ""

#         try:
#             entity_names = [
#                 f"{e['name']} ({e['type']})" for e in entities[:8]
#             ]

#             prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
#                 entities="\n".join(entity_names),
#                 content=text[:1000]
#             )

#             output = invoke_model(prompt, max_tokens=1200)

#             logger.debug(f"Raw relationship LLM output: {output[:500]}")

#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             start = output.find("{")
#             end = output.rfind("}") + 1

#             if start == -1 or end == 0:
#                 logger.error(f"No JSON object found in LLM output: {output[:300]}")
#                 return []

#             json_str = output[start:end]
#             parsed = json.loads(json_str)
#             relationships = parsed.get("relationships", [])

#             logger.info(f"Relationships extracted by LLM: {len(relationships)}")
#             return relationships

#         except json.JSONDecodeError as e:
#             logger.error(
#                 f"Relationship JSON parse failed | "
#                 f"error: {e} | "
#                 f"raw output: {output[:300]}"
#             )
#             return []

#         except Exception:
#             logger.exception("Relationship extraction failed")
#             return []

#     # -----------------------------
#     # Filter Example Relationships (NEW)
#     # -----------------------------
#     def _filter_example_relationships(
#         self,
#         relationships: list[dict]
#     ) -> list[dict]:
#         """
#         Remove relationships involving illustrative example entities.

#         The RAG paper uses examples like:
#         - "Barack Obama was born in Hawaii"
#         - "The Divine Comedy" by Dante
#         - Hemingway novels for Jeopardy examples
#         - "World Cup" as a Jeopardy question example

#         These appear in the text as demonstrations of RAG capabilities
#         but are NOT real entities of the paper itself.
#         """
#         filtered = []
#         skipped = 0

#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()

#             if source in EXAMPLE_ENTITY_NAMES or target in EXAMPLE_ENTITY_NAMES:
#                 logger.debug(
#                     f"Filtered example entity relationship: "
#                     f"{rel.get('source')} -> {rel.get('target')}"
#                 )
#                 skipped += 1
#                 continue

#             filtered.append(rel)

#         if skipped > 0:
#             logger.info(f"Filtered {skipped} example-entity relationships")

#         return filtered

#     # -----------------------------
#     # Deduplicate Relationships
#     # -----------------------------
#     def _deduplicate_relationships(
#         self,
#         relationships: list[dict]
#     ) -> list[dict]:
#         """
#         Remove duplicate relationships from batch processing.
#         Key = (source.lower, target.lower, relationship_type)
#         """
#         seen = set()
#         unique = []

#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()
#             rel_type = rel.get("relationship", "").upper().strip()

#             key = f"{source}_{target}_{rel_type}"

#             if key not in seen:
#                 seen.add(key)
#                 unique.append(rel)

#         logger.info(
#             f"Relationships after deduplication: "
#             f"{len(unique)} (from {len(relationships)})"
#         )

#         return unique

#     # -----------------------------
#     # Create Relationships (Edges)
#     # -----------------------------
#     def _create_relationships(
#         self,
#         doc_id: str,
#         relationships: list[dict]
#     ) -> int:
#         """
#         Create Neo4j edges between entity nodes.
#         MERGE on (type) only — doc_id in SET not MERGE key.
#         """
#         count = 0

#         with self.driver.session() as session:
#             for rel in relationships:
#                 try:
#                     result = session.run(
#                         """
#                         MATCH (a:Entity {name: $source, doc_id: $doc_id})
#                         MATCH (b:Entity {name: $target, doc_id: $doc_id})
#                         MERGE (a)-[r:RELATES_TO {type: $rel_type}]->(b)
#                         SET r.description = $description,
#                             r.doc_id = $doc_id
#                         RETURN count(r) as created
#                         """,
#                         source=rel.get("source"),
#                         target=rel.get("target"),
#                         rel_type=rel.get("relationship", "RELATED_TO"),
#                         description=rel.get("description", ""),
#                         doc_id=doc_id
#                     )

#                     record = result.single()
#                     if record and record["created"] > 0:
#                         count += 1
#                     else:
#                         logger.debug(
#                             f"Skipped relationship — node not found: "
#                             f"{rel.get('source')} -> {rel.get('target')}"
#                         )

#                 except Exception:
#                     logger.exception(
#                         f"Relationship creation failed: "
#                         f"{rel.get('source')} -> {rel.get('target')}"
#                     )

#         logger.info(f"Relationships created: {count}")
#         return count

#     # -----------------------------
#     # Query Graph
#     # -----------------------------
#     def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
#         """
#         Query all relationships for a given entity within a document.
#         Searches both directions (entity as source OR target).
#         DISTINCT + ORDER BY ensures no duplicate results.
#         """
#         logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity_name}")

#         results = []

#         try:
#             with self.driver.session() as session:
#                 records = session.run(
#                     """
#                     MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
#                     WHERE toLower(a.name) CONTAINS toLower($entity_name)
#                        OR toLower(b.name) CONTAINS toLower($entity_name)
#                     RETURN DISTINCT
#                         a.name AS source,
#                         r.type AS relationship,
#                         b.name AS target,
#                         r.description AS description
#                     ORDER BY source, relationship
#                     LIMIT 50
#                     """,
#                     doc_id=doc_id,
#                     entity_name=entity_name
#                 )

#                 for record in records:
#                     results.append({
#                         "source": record["source"],
#                         "relationship": record["relationship"],
#                         "target": record["target"],
#                         "description": record["description"],
#                     })

#         except Exception:
#             logger.exception(f"Graph query failed for entity: {entity_name}")

#         logger.info(f"Graph query returned {len(results)} relationships")

#         return results

#     # -----------------------------
#     # Cleanup
#     # -----------------------------
#     def close(self):
#         if self.driver:
#             self.driver.close()
#             logger.info("Neo4j connection closed")


# # -----------------------------
# # Module-level functions
# # called from api.py
# # -----------------------------
# _graph_builder = None


# def _get_graph_builder() -> GraphBuilder:
#     global _graph_builder
#     if _graph_builder is None:
#         _graph_builder = GraphBuilder()
#     return _graph_builder


# def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#     return _get_graph_builder().build_graph(doc_id, entities, chunks)


# def query_graph(doc_id: str, entity_name: str) -> list[dict]:
#     return _get_graph_builder().query_graph(doc_id, entity_name)

"""test 2"""

# import logging
# import json
# from collections import Counter
# from neo4j import GraphDatabase
# from neo4j.exceptions import ServiceUnavailable, AuthError
# from app.core.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
# from app.services.summarizer import invoke_model
# from app.services.extractor import format_entities_for_graph

# logger = logging.getLogger(__name__)


# # -----------------------------
# # Generic Low-Value Entity Filter
# # -----------------------------
# # These are generic metadata/noise entity types that appear in ANY paper
# # but carry no meaningful relationship value in a knowledge graph.
# # This is domain-agnostic — works for any paper.

# LOW_VALUE_ENTITY_TYPES = {
#     "DATE", "OTHER"
# }

# LOW_VALUE_ENTITY_NAMES = {
#     # Email addresses
#     "email", "gmail", "asu.edu", "hotmail",
#     # Generic table/figure references
#     "table 1", "table 2", "table 3", "table 4", "table a1", "table a2",
#     "table a3", "table a4", "figure 1", "figure a1", "figure a2",
#     "appendix table a1", "appendix table a2", "appendix table a4",
#     "appendix figure a2", "panel a", "panel b",
#     # Generic statistical terms
#     "p-value", "ols", "ols regression", "scale",
#     # Generic publication metadata
#     "oclc", "june", "march", "january", "february", "april", "may",
#     "july", "august", "september", "october", "november", "december",
# }


# RELATIONSHIP_EXTRACTION_PROMPT = """
# You are an expert knowledge graph builder.

# Given the following list of entities and the source text, identify meaningful relationships between entities.

# Entities:
# {entities}

# Source Text:
# {content}

# Rules:
# - Only create relationships between entities that are explicitly mentioned together in the text
# - Use clear, meaningful relationship labels in UPPERCASE_SNAKE_CASE
# - Each relationship must have a source AND target that exist in the entities list above
# - Return a maximum of 8 relationships
# - Do NOT create relationships based on examples, analogies, or hypothetical scenarios in the text
# - Do NOT create relationships between fictional characters or illustrative examples used in the paper
# - Only create relationships that describe real, factual connections between the actual entities of the document
# - Do NOT create relationships based on proximity alone (entities mentioned in the same paragraph)
# - Do NOT create relationships between email addresses, dates, or table/figure references

# For each relationship return:
# - source: exact name of the first entity (must match entity list)
# - target: exact name of the second entity (must match entity list)
# - relationship: UPPERCASE_SNAKE_CASE label (e.g. WORKS_FOR, PART_OF, USED_BY, RELATED_TO, DEVELOPED_BY)
# - description: one-line description of the relationship

# You MUST return ONLY a raw JSON object. No explanation, no markdown, no code blocks.
# Start your response with {{ and end with }}

# {{
#     "relationships": [
#         {{
#             "source": "Entity A",
#             "target": "Entity B",
#             "relationship": "RELATIONSHIP_TYPE",
#             "description": "brief description"
#         }}
#     ]
# }}
# """


# class GraphBuilder:
#     """
#     Handles Neo4j knowledge graph operations.

#     Key design decisions:
#     - Anchor entities are computed DYNAMICALLY from entity frequency
#       (not hardcoded) — works for ANY paper domain
#     - Low-value entities filtered by type and name pattern
#     - Cross-cluster pass uses top anchor entities to connect isolated clusters
#     """

#     def __init__(self):
#         self.driver = None
#         self._connect()

#     # -----------------------------
#     # Connection
#     # -----------------------------
#     def _connect(self):
#         try:
#             self.driver = GraphDatabase.driver(
#                 NEO4J_URI,
#                 auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
#             )
#             self.driver.verify_connectivity()
#             logger.info("Neo4j connected successfully")

#         except AuthError:
#             logger.exception("Neo4j authentication failed")
#             raise RuntimeError("Neo4j auth failed. Check credentials.")

#         except ServiceUnavailable:
#             logger.exception("Neo4j service unavailable")
#             raise RuntimeError("Neo4j unavailable. Is it running?")

#     # -----------------------------
#     # Detect Anchor Entities Dynamically
#     # -----------------------------
#     def _detect_anchor_entities(
#         self,
#         entities: list[dict],
#         top_n: int = 4
#     ) -> list[dict]:
#         """
#         Dynamically detect the most important entities in a document
#         by scoring them based on:
#         1. How many chunks they appear in (frequency)
#         2. Their entity type (TECHNOLOGY, CONCEPT, ORGANIZATION score higher)
#         3. Whether they are filtered as low-value

#         This replaces hardcoded anchor lists like ["COVID-19", "RAG"]
#         and works for ANY paper domain.
#         """
#         HIGH_VALUE_TYPES = {
#             "TECHNOLOGY", "CONCEPT", "ORGANIZATION", "EVENT"
#         }

#         scored = []
#         for entity in entities:
#             name = entity.get("name", "").lower().strip()
#             etype = entity.get("type", "").upper()
#             chunk_count = len(entity.get("source_chunk_ids", []))

#             # Skip low-value entities
#             if self._is_low_value_entity(entity):
#                 continue

#             # Score: frequency × type bonus
#             type_bonus = 2 if etype in HIGH_VALUE_TYPES else 1
#             score = chunk_count * type_bonus

#             scored.append((score, entity))

#         # Sort by score descending and return top N
#         scored.sort(key=lambda x: x[0], reverse=True)
#         anchors = [e for _, e in scored[:top_n]]

#         logger.info(
#             f"Detected anchor entities: "
#             f"{[a['name'] for a in anchors]}"
#         )

#         return anchors

#     # -----------------------------
#     # Low Value Entity Check
#     # -----------------------------
#     def _is_low_value_entity(self, entity: dict) -> bool:
#         """
#         Returns True if an entity should be excluded from relationship
#         extraction. Domain-agnostic — based on type and name patterns.
#         """
#         name = entity.get("name", "").lower().strip()
#         etype = entity.get("type", "").upper()

#         if etype in LOW_VALUE_ENTITY_TYPES:
#             return True

#         if name in LOW_VALUE_ENTITY_NAMES:
#             return True

#         # Filter email addresses
#         if "@" in name:
#             return True

#         # Filter very short names (single letters, numbers)
#         if len(name) <= 2:
#             return True

#         return False

#     # -----------------------------
#     # Build Graph
#     # -----------------------------
#     def build_graph(
#         self,
#         doc_id: str,
#         entities: list[dict],
#         chunks: list[dict]
#     ) -> dict:
#         """
#         Full graph build pipeline:
#         1. Clear existing graph
#         2. Format entities
#         3. Create entity nodes
#         4. Extract relationships in batches with entity rotation
#         5. Cross-cluster pass using DYNAMIC anchor entities
#         6. Filter low-value relationships
#         7. Deduplicate
#         8. Create edges
#         """
#         if not entities:
#             logger.warning(f"No entities | doc_id: {doc_id}")
#             return {"nodes": 0, "relationships": 0}

#         logger.info(
#             f"Building graph | doc_id: {doc_id} | "
#             f"entities: {len(entities)}"
#         )

#         # Step 1 — clear existing graph
#         self._clear_existing_graph(doc_id)

#         # Step 2 — format entities
#         formatted_entities = format_entities_for_graph(doc_id, entities)

#         # Step 3 — create nodes (all entities including low-value ones)
#         nodes_created = self._create_entity_nodes(doc_id, formatted_entities)

#         # Step 4 — filter low-value entities BEFORE relationship extraction
#         # This prevents noise relationships like email → paper or date → author
#         rel_entities = [
#             e for e in formatted_entities
#             if not self._is_low_value_entity(e)
#         ]

#         logger.info(
#             f"Entities for relationship extraction: "
#             f"{len(rel_entities)} / {len(formatted_entities)}"
#         )

#         # Step 5 — detect anchor entities dynamically
#         # These are the most important entities in THIS document
#         # For RAG paper: ["RAG", "BERT", "Facebook AI Research", ...]
#         # For COVID paper: ["COVID-19", "Arizona State University", ...]
#         # For any other paper: detected automatically
#         anchor_entities = self._detect_anchor_entities(rel_entities, top_n=4)

#         # Step 6 — extract relationships in batches with entity rotation
#         all_relationships = []
#         chunk_batches = [chunks[i:i+3] for i in range(0, len(chunks), 3)]

#         for batch_idx, chunk_batch in enumerate(chunk_batches):
#             combined_text = " ".join(
#                 c.get("content", "") for c in chunk_batch
#             )

#             # Rotate entity batches so ALL entities get coverage
#             # Always include anchor entities in every batch
#             entity_start = (batch_idx * 6) % max(len(rel_entities), 1)
#             rotating_batch = rel_entities[entity_start:entity_start + 6]

#             # Wrap around if slice too small
#             if len(rotating_batch) < 3 and len(rel_entities) >= 6:
#                 rotating_batch = rel_entities[-6:]

#             # Merge anchors with rotating batch — anchors always present
#             anchor_names = {a["name"] for a in anchor_entities}
#             non_anchor = [
#                 e for e in rotating_batch
#                 if e["name"] not in anchor_names
#             ]
#             entity_batch = anchor_entities + non_anchor
#             entity_batch = entity_batch[:8]  # cap at 8

#             batch_rels = self._extract_relationships(
#                 entity_batch,
#                 combined_text
#             )
#             all_relationships.extend(batch_rels)

#         # Step 7 — cross-cluster pass using dynamic anchors
#         # Connects isolated clusters by forcing anchor entities
#         # to appear with non-anchor entities
#         if len(rel_entities) > 16:
#             logger.info("Running cross-cluster pass with dynamic anchors")

#             # Get non-anchor entities from mid and end of list
#             non_anchor_entities = [
#                 e for e in rel_entities
#                 if e["name"] not in {a["name"] for a in anchor_entities}
#             ]

#             mid = len(non_anchor_entities) // 2
#             cross_sample = (
#                 non_anchor_entities[:3] +
#                 non_anchor_entities[mid:mid + 3]
#             )

#             cross_entity_batch = anchor_entities + cross_sample
#             cross_entity_batch = cross_entity_batch[:8]

#             combined_text = " ".join(
#                 c.get("content", "") for c in chunks[:3]
#             )

#             cross_rels = self._extract_relationships(
#                 cross_entity_batch,
#                 combined_text
#             )

#             all_relationships.extend(cross_rels)
#             logger.info(
#                 f"Cross-cluster relationships extracted: {len(cross_rels)}"
#             )

#         # Step 8 — filter low-value relationships
#         all_relationships = self._filter_low_value_relationships(
#             all_relationships
#         )

#         # Step 9 — deduplicate
#         all_relationships = self._deduplicate_relationships(all_relationships)

#         logger.info(
#             f"Total unique relationships to create: {len(all_relationships)}"
#         )

#         # Step 10 — create edges
#         rels_created = self._create_relationships(doc_id, all_relationships)

#         logger.info(
#             f"Graph built | nodes: {nodes_created} | "
#             f"relationships: {rels_created}"
#         )

#         return {
#             "nodes": nodes_created,
#             "relationships": rels_created
#         }

#     # -----------------------------
#     # Clear Existing Graph
#     # -----------------------------
#     def _clear_existing_graph(self, doc_id: str):
#         try:
#             with self.driver.session() as session:
#                 session.run(
#                     "MATCH (e:Entity {doc_id: $doc_id})-[r]-() DELETE r",
#                     doc_id=doc_id
#                 )
#                 session.run(
#                     "MATCH (e:Entity {doc_id: $doc_id}) DELETE e",
#                     doc_id=doc_id
#                 )
#                 logger.info(f"Cleared graph | doc_id: {doc_id}")
#         except Exception:
#             logger.exception(f"Failed to clear graph | doc_id: {doc_id}")

#     # -----------------------------
#     # Create Entity Nodes
#     # -----------------------------
#     def _create_entity_nodes(
#         self,
#         doc_id: str,
#         entities: list[dict]
#     ) -> int:
#         count = 0
#         with self.driver.session() as session:
#             for entity in entities:
#                 try:
#                     session.run(
#                         """
#                         MERGE (e:Entity {entity_id: $entity_id})
#                         SET e.name = $name,
#                             e.type = $type,
#                             e.context = $context,
#                             e.doc_id = $doc_id
#                         """,
#                         entity_id=entity["entity_id"],
#                         name=entity["name"],
#                         type=entity["type"],
#                         context=entity.get("context", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1
#                 except Exception:
#                     logger.exception(
#                         f"Node creation failed: {entity.get('name')}"
#                     )
#         logger.info(f"Nodes created: {count}")
#         return count

#     # -----------------------------
#     # Extract Relationships via LLM
#     # -----------------------------
#     def _extract_relationships(
#         self,
#         entities: list[dict],
#         text: str
#     ) -> list[dict]:
#         output = ""
#         try:
#             entity_names = [
#                 f"{e['name']} ({e['type']})" for e in entities[:8]
#             ]

#             prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
#                 entities="\n".join(entity_names),
#                 content=text[:1000]
#             )

#             output = invoke_model(prompt, max_tokens=1200)

#             output = output.strip()
#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             start = output.find("{")
#             end = output.rfind("}") + 1

#             if start == -1 or end == 0:
#                 logger.error(f"No JSON found in output: {output[:300]}")
#                 return []

#             parsed = json.loads(output[start:end])
#             relationships = parsed.get("relationships", [])

#             logger.info(f"LLM extracted {len(relationships)} relationships")
#             return relationships

#         except json.JSONDecodeError as e:
#             logger.error(f"JSON parse failed: {e} | output: {output[:300]}")
#             return []

#         except Exception:
#             logger.exception("Relationship extraction failed")
#             return []

#     # -----------------------------
#     # Filter Low-Value Relationships
#     # -----------------------------
#     def _filter_low_value_relationships(
#         self,
#         relationships: list[dict]
#     ) -> list[dict]:
#         """
#         Remove relationships where either entity is a low-value entity.
#         Domain-agnostic — based on type/name patterns, not hardcoded names.
#         """
#         filtered = []
#         skipped = 0

#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()

#             # Check against low-value name list
#             if source in LOW_VALUE_ENTITY_NAMES or target in LOW_VALUE_ENTITY_NAMES:
#                 skipped += 1
#                 continue

#             # Filter email-based relationships
#             if "@" in source or "@" in target:
#                 skipped += 1
#                 continue

#             # Filter very short entity names
#             if len(source) <= 2 or len(target) <= 2:
#                 skipped += 1
#                 continue

#             filtered.append(rel)

#         if skipped > 0:
#             logger.info(f"Filtered {skipped} low-value relationships")

#         return filtered

#     # -----------------------------
#     # Deduplicate Relationships
#     # -----------------------------
#     def _deduplicate_relationships(
#         self,
#         relationships: list[dict]
#     ) -> list[dict]:
#         seen = set()
#         unique = []

#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()
#             rel_type = rel.get("relationship", "").upper().strip()

#             key = f"{source}_{target}_{rel_type}"
#             if key not in seen:
#                 seen.add(key)
#                 unique.append(rel)

#         logger.info(
#             f"Dedup: {len(unique)} unique "
#             f"(from {len(relationships)})"
#         )
#         return unique

#     # -----------------------------
#     # Create Relationships (Edges)
#     # -----------------------------
#     def _create_relationships(
#         self,
#         doc_id: str,
#         relationships: list[dict]
#     ) -> int:
#         count = 0
#         with self.driver.session() as session:
#             for rel in relationships:
#                 try:
#                     result = session.run(
#                         """
#                         MATCH (a:Entity {name: $source, doc_id: $doc_id})
#                         MATCH (b:Entity {name: $target, doc_id: $doc_id})
#                         MERGE (a)-[r:RELATES_TO {type: $rel_type}]->(b)
#                         SET r.description = $description,
#                             r.doc_id = $doc_id
#                         RETURN count(r) as created
#                         """,
#                         source=rel.get("source"),
#                         target=rel.get("target"),
#                         rel_type=rel.get("relationship", "RELATED_TO"),
#                         description=rel.get("description", ""),
#                         doc_id=doc_id
#                     )
#                     record = result.single()
#                     if record and record["created"] > 0:
#                         count += 1
#                     else:
#                         logger.debug(
#                             f"Skipped — node not found: "
#                             f"{rel.get('source')} -> {rel.get('target')}"
#                         )
#                 except Exception:
#                     logger.exception(
#                         f"Rel creation failed: "
#                         f"{rel.get('source')} -> {rel.get('target')}"
#                     )

#         logger.info(f"Relationships created: {count}")
#         return count

#     # -----------------------------
#     # Query Graph
#     # -----------------------------
#     def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
#         logger.info(f"Graph query | entity: {entity_name}")
#         results = []
#         try:
#             with self.driver.session() as session:
#                 records = session.run(
#                     """
#                     MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->
#                           (b:Entity {doc_id: $doc_id})
#                     WHERE toLower(a.name) CONTAINS toLower($entity_name)
#                        OR toLower(b.name) CONTAINS toLower($entity_name)
#                     RETURN DISTINCT
#                         a.name AS source,
#                         r.type AS relationship,
#                         b.name AS target,
#                         r.description AS description
#                     ORDER BY source, relationship
#                     LIMIT 50
#                     """,
#                     doc_id=doc_id,
#                     entity_name=entity_name
#                 )
#                 for record in records:
#                     results.append({
#                         "source": record["source"],
#                         "relationship": record["relationship"],
#                         "target": record["target"],
#                         "description": record["description"],
#                     })
#         except Exception:
#             logger.exception(f"Graph query failed: {entity_name}")

#         logger.info(f"Graph query returned {len(results)} relationships")
#         return results

#     # -----------------------------
#     # Cleanup
#     # -----------------------------
#     def close(self):
#         if self.driver:
#             self.driver.close()
#             logger.info("Neo4j connection closed")


# # -----------------------------
# # Module-level functions
# # -----------------------------
# _graph_builder = None


# def _get_graph_builder() -> GraphBuilder:
#     global _graph_builder
#     if _graph_builder is None:
#         _graph_builder = GraphBuilder()
#     return _graph_builder


# def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#     return _get_graph_builder().build_graph(doc_id, entities, chunks)


# def query_graph(doc_id: str, entity_name: str) -> list[dict]:
#     return _get_graph_builder().query_graph(doc_id, entity_name)

# """clean working"""
# import json
# import logging
# from neo4j import GraphDatabase
# from neo4j.exceptions import ServiceUnavailable, AuthError
# from app.core.config import (
#     NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD,
#     LOW_VALUE_ENTITY_TYPES, LOW_VALUE_ENTITY_NAMES, HIGH_VALUE_TYPES,
#     GRAPH_ANCHOR_TOP_N, GRAPH_CHUNK_BATCH_SIZE, GRAPH_ENTITY_BATCH_SIZE,
#     GRAPH_MAX_ENTITIES_PER_LLM_CALL, GRAPH_CROSS_CLUSTER_THRESHOLD,
#     GRAPH_CONTENT_CHAR_LIMIT, GRAPH_QUERY_LIMIT, GRAPH_MIN_ENTITY_NAME_LENGTH,
# )
# from app.services.summarizer import invoke_model
# from app.services.extractor import format_entities_for_graph
# from app.prompts.graph_builder import RELATIONSHIP_EXTRACTION_PROMPT

# logger = logging.getLogger(__name__)


# class GraphBuilder:
#     """Handles Neo4j knowledge graph construction and querying."""

#     def __init__(self):
#         self.driver = None
#         self._connect()

#     def _connect(self):
#         try:
#             self.driver = GraphDatabase.driver(
#                 NEO4J_URI,
#                 auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
#             )
#             self.driver.verify_connectivity()
#             logger.info("Neo4j connected successfully")
#         except AuthError:
#             logger.exception("Neo4j authentication failed")
#             raise RuntimeError("Neo4j auth failed. Check credentials.")
#         except ServiceUnavailable:
#             logger.exception("Neo4j service unavailable")
#             raise RuntimeError("Neo4j unavailable. Is it running?")

#     def build_graph(self, doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#         if not entities:
#             logger.warning(f"No entities | doc_id: {doc_id}")
#             return {"nodes": 0, "relationships": 0}

#         logger.info(f"Building graph | doc_id: {doc_id} | entities: {len(entities)}")

#         self._clear_existing_graph(doc_id)

#         formatted_entities = format_entities_for_graph(doc_id, entities)
#         nodes_created = self._create_entity_nodes(doc_id, formatted_entities)

#         rel_entities = [e for e in formatted_entities if not self._is_low_value_entity(e)]
#         logger.info(f"Entities for relationship extraction: {len(rel_entities)} / {len(formatted_entities)}")

#         anchor_entities = self._detect_anchor_entities(rel_entities)
#         anchor_names = {a["name"] for a in anchor_entities}

#         all_relationships = []
#         chunk_batches = [
#             chunks[i:i + GRAPH_CHUNK_BATCH_SIZE]
#             for i in range(0, len(chunks), GRAPH_CHUNK_BATCH_SIZE)
#         ]

#         for batch_idx, chunk_batch in enumerate(chunk_batches):
#             combined_text = " ".join(c.get("content", "") for c in chunk_batch)

#             entity_start = (batch_idx * GRAPH_ENTITY_BATCH_SIZE) % max(len(rel_entities), 1)
#             rotating_batch = rel_entities[entity_start:entity_start + GRAPH_ENTITY_BATCH_SIZE]

#             if len(rotating_batch) < 3 and len(rel_entities) >= GRAPH_ENTITY_BATCH_SIZE:
#                 rotating_batch = rel_entities[-GRAPH_ENTITY_BATCH_SIZE:]

#             non_anchor = [e for e in rotating_batch if e["name"] not in anchor_names]
#             entity_batch = (anchor_entities + non_anchor)[:GRAPH_MAX_ENTITIES_PER_LLM_CALL]

#             all_relationships.extend(self._extract_relationships(entity_batch, combined_text))

#         if len(rel_entities) > GRAPH_CROSS_CLUSTER_THRESHOLD:
#             logger.info("Running cross-cluster pass with dynamic anchors")

#             non_anchor_entities = [e for e in rel_entities if e["name"] not in anchor_names]
#             mid = len(non_anchor_entities) // 2
#             cross_sample = non_anchor_entities[:3] + non_anchor_entities[mid:mid + 3]
#             cross_entity_batch = (anchor_entities + cross_sample)[:GRAPH_MAX_ENTITIES_PER_LLM_CALL]
#             combined_text = " ".join(c.get("content", "") for c in chunks[:GRAPH_CHUNK_BATCH_SIZE])

#             cross_rels = self._extract_relationships(cross_entity_batch, combined_text)
#             all_relationships.extend(cross_rels)
#             logger.info(f"Cross-cluster relationships extracted: {len(cross_rels)}")

#         all_relationships = self._filter_low_value_relationships(all_relationships)
#         all_relationships = self._deduplicate_relationships(all_relationships)
#         logger.info(f"Total unique relationships to create: {len(all_relationships)}")

#         rels_created = self._create_relationships(doc_id, all_relationships)
#         logger.info(f"Graph built | nodes: {nodes_created} | relationships: {rels_created}")

#         return {"nodes": nodes_created, "relationships": rels_created}

#     def _detect_anchor_entities(self, entities: list[dict]) -> list[dict]:
#         scored = []
#         for entity in entities:
#             if self._is_low_value_entity(entity):
#                 continue
#             etype = entity.get("type", "").upper()
#             chunk_count = len(entity.get("source_chunk_ids", []))
#             score = chunk_count * (2 if etype in HIGH_VALUE_TYPES else 1)
#             scored.append((score, entity))

#         scored.sort(key=lambda x: x[0], reverse=True)
#         anchors = [e for _, e in scored[:GRAPH_ANCHOR_TOP_N]]
#         logger.info(f"Detected anchor entities: {[a['name'] for a in anchors]}")
#         return anchors

#     def _is_low_value_entity(self, entity: dict) -> bool:
#         name = entity.get("name", "").lower().strip()
#         etype = entity.get("type", "").upper()
#         return (
#             etype in LOW_VALUE_ENTITY_TYPES
#             or name in LOW_VALUE_ENTITY_NAMES
#             or "@" in name
#             or len(name) <= GRAPH_MIN_ENTITY_NAME_LENGTH
#         )

#     def _clear_existing_graph(self, doc_id: str):
#         try:
#             with self.driver.session() as session:
#                 session.run("MATCH (e:Entity {doc_id: $doc_id})-[r]-() DELETE r", doc_id=doc_id)
#                 session.run("MATCH (e:Entity {doc_id: $doc_id}) DELETE e", doc_id=doc_id)
#                 logger.info(f"Cleared graph | doc_id: {doc_id}")
#         except Exception:
#             logger.exception(f"Failed to clear graph | doc_id: {doc_id}")

#     def _create_entity_nodes(self, doc_id: str, entities: list[dict]) -> int:
#         count = 0
#         with self.driver.session() as session:
#             for entity in entities:
#                 try:
#                     session.run(
#                         """
#                         MERGE (e:Entity {entity_id: $entity_id})
#                         SET e.name = $name,
#                             e.type = $type,
#                             e.context = $context,
#                             e.doc_id = $doc_id
#                         """,
#                         entity_id=entity["entity_id"],
#                         name=entity["name"],
#                         type=entity["type"],
#                         context=entity.get("context", ""),
#                         doc_id=doc_id
#                     )
#                     count += 1
#                 except Exception:
#                     logger.exception(f"Node creation failed: {entity.get('name')}")
#         logger.info(f"Nodes created: {count}")
#         return count

#     def _extract_relationships(self, entities: list[dict], text: str) -> list[dict]:
#         output = ""
#         try:
#             entity_names = [f"{e['name']} ({e['type']})" for e in entities[:GRAPH_MAX_ENTITIES_PER_LLM_CALL]]
#             prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
#                 entities="\n".join(entity_names),
#                 content=text[:GRAPH_CONTENT_CHAR_LIMIT]
#             )
#             output = invoke_model(prompt, max_tokens=1200).strip()

#             if "```" in output:
#                 output = output.replace("```json", "").replace("```", "").strip()

#             start, end = output.find("{"), output.rfind("}") + 1
#             if start == -1 or end == 0:
#                 logger.error(f"No JSON found in output: {output[:300]}")
#                 return []

#             parsed = json.loads(output[start:end])
#             relationships = parsed.get("relationships", [])
#             logger.info(f"LLM extracted {len(relationships)} relationships")
#             return relationships

#         except json.JSONDecodeError as e:
#             logger.error(f"JSON parse failed: {e} | output: {output[:300]}")
#             return []
#         except Exception:
#             logger.exception("Relationship extraction failed")
#             return []

#     def _filter_low_value_relationships(self, relationships: list[dict]) -> list[dict]:
#         filtered, skipped = [], 0
#         for rel in relationships:
#             source = rel.get("source", "").lower().strip()
#             target = rel.get("target", "").lower().strip()
#             if (
#                 source in LOW_VALUE_ENTITY_NAMES
#                 or target in LOW_VALUE_ENTITY_NAMES
#                 or "@" in source or "@" in target
#                 or len(source) <= GRAPH_MIN_ENTITY_NAME_LENGTH
#                 or len(target) <= GRAPH_MIN_ENTITY_NAME_LENGTH
#             ):
#                 skipped += 1
#                 continue
#             filtered.append(rel)

#         if skipped:
#             logger.info(f"Filtered {skipped} low-value relationships")
#         return filtered

#     def _deduplicate_relationships(self, relationships: list[dict]) -> list[dict]:
#         seen, unique = set(), []
#         for rel in relationships:
#             key = (
#                 f"{rel.get('source', '').lower().strip()}_"
#                 f"{rel.get('target', '').lower().strip()}_"
#                 f"{rel.get('relationship', '').upper().strip()}"
#             )
#             if key not in seen:
#                 seen.add(key)
#                 unique.append(rel)

#         logger.info(f"Dedup: {len(unique)} unique (from {len(relationships)})")
#         return unique

#     def _create_relationships(self, doc_id: str, relationships: list[dict]) -> int:
#         count = 0
#         with self.driver.session() as session:
#             for rel in relationships:
#                 try:
#                     result = session.run(
#                         """
#                         MATCH (a:Entity {name: $source, doc_id: $doc_id})
#                         MATCH (b:Entity {name: $target, doc_id: $doc_id})
#                         MERGE (a)-[r:RELATES_TO {type: $rel_type}]->(b)
#                         SET r.description = $description,
#                             r.doc_id = $doc_id
#                         RETURN count(r) as created
#                         """,
#                         source=rel.get("source"),
#                         target=rel.get("target"),
#                         rel_type=rel.get("relationship", "RELATED_TO"),
#                         description=rel.get("description", ""),
#                         doc_id=doc_id
#                     )
#                     record = result.single()
#                     if record and record["created"] > 0:
#                         count += 1
#                     else:
#                         logger.debug(f"Skipped — node not found: {rel.get('source')} -> {rel.get('target')}")
#                 except Exception:
#                     logger.exception(f"Rel creation failed: {rel.get('source')} -> {rel.get('target')}")

#         logger.info(f"Relationships created: {count}")
#         return count

#     def query_graph(self, doc_id: str, entity_name: str) -> list[dict]:
#         logger.info(f"Graph query | entity: {entity_name}")
#         results = []
#         try:
#             with self.driver.session() as session:
#                 records = session.run(
#                     """
#                     MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
#                     WHERE toLower(a.name) CONTAINS toLower($entity_name)
#                        OR toLower(b.name) CONTAINS toLower($entity_name)
#                     RETURN DISTINCT
#                         a.name AS source,
#                         r.type AS relationship,
#                         b.name AS target,
#                         r.description AS description
#                     ORDER BY source, relationship
#                     LIMIT $limit
#                     """,
#                     doc_id=doc_id,
#                     entity_name=entity_name,
#                     limit=GRAPH_QUERY_LIMIT
#                 )
#                 for record in records:
#                     results.append({
#                         "source": record["source"],
#                         "relationship": record["relationship"],
#                         "target": record["target"],
#                         "description": record["description"],
#                     })
#         except Exception:
#             logger.exception(f"Graph query failed: {entity_name}")

#         logger.info(f"Graph query returned {len(results)} relationships")
#         return results

#     def close(self):
#         if self.driver:
#             self.driver.close()
#             logger.info("Neo4j connection closed")


# _graph_builder = None


# def _get_graph_builder() -> GraphBuilder:
#     global _graph_builder
#     if _graph_builder is None:
#         _graph_builder = GraphBuilder()
#     return _graph_builder


# def build_graph(doc_id: str, entities: list[dict], chunks: list[dict]) -> dict:
#     return _get_graph_builder().build_graph(doc_id, entities, chunks)


# def query_graph(doc_id: str, entity_name: str) -> list[dict]:
#     return _get_graph_builder().query_graph(doc_id, entity_name)

# def get_full_graph(doc_id: str) -> list[dict]:
#     """
#     Runs: MATCH (a:Entity {doc_id})-[r]->(b) RETURN a, r, b
#     Returns all relationships for the entire document graph.
#     """
#     results = []
#     try:
#         with _get_graph_builder().driver.session() as session:
#             records = session.run(
#                 """
#                 MATCH (a:Entity {doc_id: $doc_id})-[r:RELATES_TO]->(b:Entity {doc_id: $doc_id})
#                 RETURN DISTINCT
#                     a.name AS source,
#                     a.type AS source_type,
#                     r.type AS relationship,
#                     b.name AS target,
#                     b.type AS target_type,
#                     r.description AS description
#                 ORDER BY source
#                 """,
#                 doc_id=doc_id
#             )
#             for record in records:
#                 results.append({
#                     "source":       record["source"],
#                     "source_type":  record["source_type"],
#                     "relationship": record["relationship"],
#                     "target":       record["target"],
#                     "target_type":  record["target_type"],
#                     "description":  record["description"],
#                 })
#     except Exception:
#         logger.exception(f"Full graph query failed | doc_id: {doc_id}")
#     return results

"""fast"""
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
)
from app.services.summarizer import invoke_model
from app.services.extractor import format_entities_for_graph
from app.prompts.graph_builder import RELATIONSHIP_EXTRACTION_PROMPT

logger = logging.getLogger(__name__)

# ── Async worker count for parallel Bedrock calls ─────────────
# Relationship extraction fires one Bedrock call per chunk batch.
# OLD: sequential — each batch waits for the previous to finish.
# NEW: all batches fire simultaneously via asyncio.gather.
_REL_ASYNC_WORKERS = 8


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

        # OPTIMIZATION: single Cypher query clears both nodes + rels
        # OLD: two separate session.run calls (delete rels, then delete nodes)
        # NEW: one DETACH DELETE removes both in one round trip
        self._clear_existing_graph(doc_id)

        formatted_entities = format_entities_for_graph(doc_id, entities)

        # OPTIMIZATION: batch MERGE instead of one-by-one
        # OLD: loop → session.run per entity → N round trips
        # NEW: UNWIND list → single Cypher call → 1 round trip
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

        # OPTIMIZATION: fire ALL extraction jobs simultaneously
        # OLD: sequential for loop → each Bedrock call waits for the previous
        # NEW: asyncio.gather → all jobs fire at once, wall time = slowest job
        all_relationships = self._extract_relationships_parallel(extraction_jobs)

        all_relationships = self._filter_low_value_relationships(all_relationships)
        all_relationships = self._deduplicate_relationships(all_relationships)
        logger.info(f"Total unique relationships to create: {len(all_relationships)}")

        # OPTIMIZATION: batch MERGE instead of one-by-one
        # OLD: loop → session.run per relationship → N round trips
        # NEW: UNWIND list → single Cypher call → 1 round trip
        rels_created = self._create_relationships_batch(doc_id, all_relationships)
        logger.info(f"Graph built | nodes: {nodes_created} | relationships: {rels_created}")

        return {"nodes": nodes_created, "relationships": rels_created}

    # ══════════════════════════════════════════════════════════
    # OPTIMIZATION 1: parallel relationship extraction
    # ══════════════════════════════════════════════════════════
    # OLD: sequential for loop calling _extract_relationships once per batch
    #      → if 5 batches × 2s each = 10s minimum
    # NEW: asyncio.gather fires all batches simultaneously
    #      → wall time = slowest single batch ~2-3s regardless of batch count

    def _extract_relationships_parallel(
        self, jobs: list[tuple[list[dict], str]]
    ) -> list[dict]:
        """Fire all relationship extraction jobs simultaneously."""

        async def _run_all():
            loop     = asyncio.get_event_loop()
            executor = ThreadPoolExecutor(
                max_workers=_REL_ASYNC_WORKERS,
                thread_name_prefix="rel_worker"
            )
            try:
                logger.info(f"[ASYNC] Firing {len(jobs)} relationship extraction jobs simultaneously")

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

    # ══════════════════════════════════════════════════════════
    # OPTIMIZATION 2: batch node creation
    # ══════════════════════════════════════════════════════════
    # OLD: loop → session.run per entity → N round trips to Neo4j
    #      196 entities × ~5ms per trip = ~1s just in network overhead
    # NEW: UNWIND → single Cypher call → 1 round trip
    #      All 196 entities in one shot → ~10ms

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

    # ══════════════════════════════════════════════════════════
    # OPTIMIZATION 3: batch relationship creation
    # ══════════════════════════════════════════════════════════
    # OLD: loop → session.run per relationship → N round trips
    #      150 rels × ~5ms = ~750ms just in network overhead
    # NEW: UNWIND → single Cypher call → 1 round trip ~10ms

    def _create_relationships_batch(self, doc_id: str, relationships: list[dict]) -> int:
        """
        Create all relationships in a single UNWIND Cypher call.
        CHANGES FROM _create_relationships:
        - One session.run instead of N session.run calls
        - UNWIND processes the full list server-side
        - count(r) returns total created
        """
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

    # ══════════════════════════════════════════════════════════
    # OPTIMIZATION 4: single-query clear
    # ══════════════════════════════════════════════════════════
    # OLD: two separate session.run calls (delete rels, then nodes)
    # NEW: DETACH DELETE removes nodes AND all their rels in one call

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