# # from fastapi import FastAPI, UploadFile, File, HTTPException
# # from fastapi.concurrency import run_in_threadpool
# # import os
# # import uuid
# # import logging

# # from app.services.pipeline import run_pipeline
# # from app.models.schema import SummarizationOutput
# # from app.core.config import (
# #     APP_TITLE,
# #     MAX_FILE_SIZE_MB,
# #     ALLOWED_FILE_TYPES,
# #     TEMP_DIR,
# #     TEMP_FILE_PREFIX,
# # )

# # app = FastAPI(title=APP_TITLE)

# # logger = logging.getLogger(__name__)


# # @app.get("/", tags=["Health"])
# # async def health() -> dict:
# #     logger.info("Health check endpoint accessed")
# #     return {"status": "ok"}


# # @app.post(
# #     "/summarize",
# #     response_model=SummarizationOutput,
# #     tags=["Summarization"],
# # )
# # async def summarize_document(
# #     file: UploadFile = File(...)
# # ) -> SummarizationOutput:

# #     logger.info(f"Received summarization request: {file.filename}")

# #     #File type validation
# #     if not file.filename.lower().endswith(ALLOWED_FILE_TYPES):
# #         logger.warning(f"Invalid file type uploaded: {file.filename}")
# #         raise HTTPException(
# #             status_code=400,
# #             detail="Only supported file types allowed."
# #         )

# #     contents = await file.read()
# #     file_size_mb = len(contents) / (1024 * 1024)

# #     logger.info(f"Uploaded file size: {file_size_mb:.2f} MB")

# #     #File size validation
# #     if file_size_mb > MAX_FILE_SIZE_MB:
# #         raise HTTPException(
# #             status_code=400,
# #             detail=f"File too large. Max size allowed is {MAX_FILE_SIZE_MB}MB."
# #         )

# #     # Temp file path from config
# #     temp_filename = (
# #         f"{TEMP_FILE_PREFIX}_"
# #         f"{uuid.uuid4().hex}_{file.filename}"
# #     )

# #     temp_path = TEMP_DIR / temp_filename

# #     try:
# #         with open(temp_path, "wb") as buffer:
# #             buffer.write(contents)

# #         logger.info(f"Temporary file created at {temp_path}")

# #         result = await run_in_threadpool(
# #             run_pipeline,
# #             str(temp_path)
# #         )

# #         logger.info(
# #             f"Summarization completed for {file.filename}"
# #         )

# #         return result

# #     except Exception:
# #         logger.exception("Pipeline execution failed")
# #         raise HTTPException(
# #             status_code=500,
# #             detail="Pipeline execution failed."
# #         )

# #     finally:
# #         if temp_path.exists():
# #             temp_path.unlink()
# #             logger.info(f"Temporary file deleted: {temp_path}")

# """APi adding """

# from fastapi import FastAPI, UploadFile, File, HTTPException, Query
# from fastapi.concurrency import run_in_threadpool
# import uuid
# import logging

# from app.services.pipeline import run_pipeline
# from app.services.entity_extraction import extract_entities
# from app.services.graph import query_graph

# from app.models.schema import SummarizationOutput
# from app.core.config import (
#     APP_TITLE,
#     MAX_FILE_SIZE_MB,
#     ALLOWED_FILE_TYPES,
#     TEMP_DIR,
#     TEMP_FILE_PREFIX,
# )

# app = FastAPI(title=APP_TITLE)

# logger = logging.getLogger(__name__)


# # -----------------------------
# # Health Check
# # -----------------------------
# @app.get("/", tags=["Health"])
# async def health() -> dict:
#     logger.info("Health check endpoint accessed")
#     return {"status": "ok"}


# # -----------------------------
# # Document Summarization
# # -----------------------------
# @app.post(
#     "/summarize",
#     response_model=SummarizationOutput,
#     tags=["Summarization"],
# )
# async def summarize_document(
#     file: UploadFile = File(...)
# ) -> SummarizationOutput:

#     logger.info(f"Received summarization request: {file.filename}")

#     # File type validation
#     if not file.filename.lower().endswith(ALLOWED_FILE_TYPES):
#         logger.warning(f"Invalid file type uploaded: {file.filename}")
#         raise HTTPException(
#             status_code=400,
#             detail="Only supported file types allowed."
#         )

#     contents = await file.read()
#     file_size_mb = len(contents) / (1024 * 1024)

#     logger.info(f"Uploaded file size: {file_size_mb:.2f} MB")

#     # File size validation
#     if file_size_mb > MAX_FILE_SIZE_MB:
#         raise HTTPException(
#             status_code=400,
#             detail=f"File too large. Max size allowed is {MAX_FILE_SIZE_MB}MB."
#         )

#     temp_filename = (
#         f"{TEMP_FILE_PREFIX}_"
#         f"{uuid.uuid4().hex}_{file.filename}"
#     )

#     temp_path = TEMP_DIR / temp_filename

#     try:

#         with open(temp_path, "wb") as buffer:
#             buffer.write(contents)

#         logger.info(f"Temporary file created at {temp_path}")

#         result = await run_in_threadpool(
#             run_pipeline,
#             str(temp_path)
#         )

#         logger.info(
#             f"Summarization completed for {file.filename}"
#         )

#         return result

#     except Exception:
#         logger.exception("Pipeline execution failed")
#         raise HTTPException(
#             status_code=500,
#             detail="Pipeline execution failed."
#         )

#     finally:

#         if temp_path.exists():
#             temp_path.unlink()
#             logger.info(f"Temporary file deleted: {temp_path}")


# # -----------------------------
# # Entity Extraction API
# # -----------------------------
# @app.post(
#     "/extract-entities",
#     tags=["Entity Extraction"]
# )
# async def entity_extraction_api(text: str):

#     logger.info("Entity extraction request received")

#     try:

#         entities = await run_in_threadpool(
#             extract_entities,
#             text
#         )

#         return {
#             "entities": entities
#         }

#     except Exception:

#         logger.exception("Entity extraction failed")

#         raise HTTPException(
#             status_code=500,
#             detail="Entity extraction failed"
#         )


# # -----------------------------
# # Knowledge Graph Query API
# # -----------------------------
# @app.get(
#     "/graph-query",
#     tags=["Knowledge Graph"]
# )
# async def graph_query_api(
#     entity: str = Query(..., description="Entity name")
# ):

#     logger.info(f"Graph query for entity: {entity}")

#     try:

#         relationships = await run_in_threadpool(
#             query_graph,
#             entity
#         )

#         return {
#             "entity": entity,
#             "relationships": relationships
#         }

#     except Exception:

#         logger.exception("Graph query failed")

#         raise HTTPException(
#             status_code=500,
#             detail="Graph query failed"
#         )

"Antropic"
# from fastapi import FastAPI, UploadFile, File, HTTPException, Query
# from fastapi.concurrency import run_in_threadpool
# import uuid
# import logging

# from app.services.pipeline import run_pipeline
# from app.services.extractor import extract_entities_from_chunks
# from app.services.graph_builder import build_graph, query_graph
# from app.services.fact_verifier import verify_facts
# from app.db.mongodb import MongoDB
# from app.db.vector_store import VectorStore
# from app.models.schema import SummarizationOutput
# from app.core.config import (
#     APP_TITLE,
#     MAX_FILE_SIZE_MB,
#     ALLOWED_FILE_TYPES,
#     TEMP_DIR,
#     TEMP_FILE_PREFIX,
# )

# app = FastAPI(title=APP_TITLE)
# logger = logging.getLogger(__name__)
# db = MongoDB()
# vector_store = VectorStore()


# # ─────────────────────────────────────────
# # Health Check
# # ─────────────────────────────────────────
# @app.get("/", tags=["Health"])
# async def health() -> dict:
#     return {"status": "ok"}


# # ─────────────────────────────────────────
# # Summarization
# # ─────────────────────────────────────────
# @app.post("/summarize", tags=["Summarization"])
# async def summarize_document(file: UploadFile = File(...)):

#     logger.info(f"Summarization request: {file.filename}")

#     if not file.filename.lower().endswith(ALLOWED_FILE_TYPES):
#         raise HTTPException(status_code=400, detail="Only supported file types allowed.")

#     contents = await file.read()
#     file_size_mb = len(contents) / (1024 * 1024)

#     if file_size_mb > MAX_FILE_SIZE_MB:
#         raise HTTPException(
#             status_code=400,
#             detail=f"File too large. Max allowed: {MAX_FILE_SIZE_MB}MB."
#         )

#     temp_filename = f"{TEMP_FILE_PREFIX}_{uuid.uuid4().hex}_{file.filename}"
#     temp_path = TEMP_DIR / temp_filename

#     try:
#         with open(temp_path, "wb") as buffer:
#             buffer.write(contents)

#         # FIX 1 — pass contents so cache check works in pipeline
#         result = await run_in_threadpool(
#             run_pipeline,
#             str(temp_path),
#             contents               # ← this was missing before
#         )

#         logger.info(f"Summarization completed for {file.filename}")

#         # result now includes doc_id from pipeline
#         return result

#     except Exception:
#         logger.exception("Pipeline execution failed")
#         raise HTTPException(status_code=500, detail="Pipeline execution failed.")

#     finally:
#         if temp_path.exists():
#             temp_path.unlink()


# # ─────────────────────────────────────────
# # Entity Extraction
# # ─────────────────────────────────────────
# @app.post("/extract-entities", tags=["Entity Extraction"])
# async def entity_extraction_api(doc_id: str):
#     """
#     Extracts entities from already-ingested document.
#     Call /summarize first to get doc_id.
#     """
#     logger.info(f"Entity extraction for doc_id: {doc_id}")

#     # FIX 2 — fetch chunks from MongoDB using doc_id
#     document = db.get_by_doc_id(doc_id)
#     if not document:
#         raise HTTPException(
#             status_code=404,
#             detail="Document not found. Run /summarize first to get doc_id."
#         )

#     try:
#         chunks = document["chunks"]

#         entities = await run_in_threadpool(
#             extract_entities_from_chunks,
#             chunks                 # ← extract from stored chunks, not raw text
#         )

#         # store entities back in MongoDB for graph building
#         db.update_document(doc_id, {"entities": entities})

#         return {
#             "doc_id": doc_id,
#             "entity_count": len(entities),
#             "entities": entities
#         }

#     except Exception:
#         logger.exception("Entity extraction failed")
#         raise HTTPException(status_code=500, detail="Entity extraction failed.")


# # ─────────────────────────────────────────
# # Knowledge Graph Builder
# # ─────────────────────────────────────────
# @app.post("/build-graph", tags=["Knowledge Graph"])
# async def build_graph_api(doc_id: str):
#     """
#     Builds knowledge graph from extracted entities.
#     Call /extract-entities first.
#     """
#     logger.info(f"Building graph for doc_id: {doc_id}")

#     document = db.get_by_doc_id(doc_id)
#     if not document:
#         raise HTTPException(status_code=404, detail="Document not found.")

#     if "entities" not in document:
#         raise HTTPException(
#             status_code=400,
#             detail="Entities not found. Run /extract-entities first."
#         )

#     try:
#         result = await run_in_threadpool(
#             build_graph,
#             doc_id,
#             document["entities"]
#         )

#         return {
#             "doc_id": doc_id,
#             "nodes_created": result["nodes"],
#             "relationships_created": result["relationships"]
#         }

#     except Exception:
#         logger.exception("Graph building failed")
#         raise HTTPException(status_code=500, detail="Graph building failed.")


# # ─────────────────────────────────────────
# # Knowledge Graph Query
# # ─────────────────────────────────────────
# @app.get("/graph-query", tags=["Knowledge Graph"])
# async def graph_query_api(
#     doc_id: str = Query(..., description="Document ID"),
#     entity: str = Query(..., description="Entity name to query")
# ):
#     # FIX 3 — scoped to doc_id, not global
#     logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity}")

#     try:
#         relationships = await run_in_threadpool(
#             query_graph,
#             doc_id,                # ← scope query to this document
#             entity
#         )

#         return {
#             "doc_id": doc_id,
#             "entity": entity,
#             "relationships": relationships
#         }

#     except Exception:
#         logger.exception("Graph query failed")
#         raise HTTPException(status_code=500, detail="Graph query failed.")


# # ─────────────────────────────────────────
# # Fact Verification
# # ─────────────────────────────────────────
# @app.post("/verify-facts", tags=["Fact Verification"])
# async def verify_facts_api(doc_id: str):
#     """
#     Verifies summary claims against source chunks.
#     Call /summarize first.
#     """
#     logger.info(f"Fact verification for doc_id: {doc_id}")

#     document = db.get_by_doc_id(doc_id)
#     if not document:
#         raise HTTPException(status_code=404, detail="Document not found.")

#     try:
#         result = await run_in_threadpool(
#             verify_facts,
#             doc_id,
#             document["summary"],
#             document["chunks"]
#         )

#         return {
#             "doc_id": doc_id,
#             "coverage_score": result["coverage_score"],
#             "flagged_claims": result["flagged_claims"]
#         }

#     except Exception:
#         logger.exception("Fact verification failed")
#         raise HTTPException(status_code=500, detail="Fact verification failed.")


# # ─────────────────────────────────────────
# # Semantic Query
# # ─────────────────────────────────────────
# @app.post("/query", tags=["Query"])
# async def query_api(doc_id: str, query: str):
#     """
#     Semantic search over document chunks using vector store.
#     """
#     logger.info(f"Semantic query | doc_id: {doc_id} | query: {query}")

#     document = db.get_by_doc_id(doc_id)
#     if not document:
#         raise HTTPException(status_code=404, detail="Document not found.")

#     try:
#         results = await run_in_threadpool(
#             vector_store.search,
#             doc_id,
#             query
#         )

#         return {
#             "doc_id": doc_id,
#             "query": query,
#             "results": results
#         }

#     except Exception:
#         logger.exception("Query failed")
#         raise HTTPException(status_code=500, detail="Query failed.")


# ### Correct Flow for API Users
# """
# 1. POST /summarize        → upload PDF → get doc_id back
# 2. POST /extract-entities → send doc_id → get entities
# 3. POST /build-graph      → send doc_id → builds Neo4j graph
# 4. GET  /graph-query      → send doc_id + entity → get relationships
# 5. POST /verify-facts     → send doc_id → get coverage score
# 6. POST /query            → send doc_id + question → semantic answer

# """

"""V2 final"""

# from fastapi import FastAPI, UploadFile, File, HTTPException, Query
# from fastapi.concurrency import run_in_threadpool
# import uuid
# import logging

# from app.services.pipeline import run_pipeline
# from app.services.extractor import extract_entities_from_chunks
# from app.services.graph_builder import build_graph, query_graph
# from app.services.fact_verifier import verify_facts
# from app.db.mongodb import MongoDB
# from app.db.vector_store import VectorStore
# from app.core.config import (
#     APP_TITLE,
#     MAX_FILE_SIZE_MB,
#     ALLOWED_FILE_TYPES,
#     TEMP_DIR,
#     TEMP_FILE_PREFIX,
# )

# app = FastAPI(title=APP_TITLE)
# logger = logging.getLogger(__name__)
# db = MongoDB()
# vector_store = VectorStore()


# # ─────────────────────────────────────────
# # Health Check
# # ─────────────────────────────────────────
# @app.get("/", tags=["Health"])
# async def health() -> dict:
#     return {"status": "ok"}


# # ─────────────────────────────────────────
# # Summarization
# # ─────────────────────────────────────────
# @app.post("/summarize", tags=["Summarization"])
# async def summarize_document(file: UploadFile = File(...)):
#     """
#     Upload a PDF or TXT file to generate a hierarchical summary.
#     Returns doc_id — save this for all other endpoints.
#     """
#     logger.info(f"Summarization request: {file.filename}")

#     if not file.filename.lower().endswith(ALLOWED_FILE_TYPES):
#         raise HTTPException(status_code=400, detail="Only supported file types allowed.")

#     contents = await file.read()
#     file_size_mb = len(contents) / (1024 * 1024)

#     if file_size_mb > MAX_FILE_SIZE_MB:
#         raise HTTPException(
#             status_code=400,
#             detail=f"File too large. Max allowed: {MAX_FILE_SIZE_MB}MB."
#         )

#     temp_filename = f"{TEMP_FILE_PREFIX}_{uuid.uuid4().hex}_{file.filename}"
#     temp_path = TEMP_DIR / temp_filename

#     try:
#         with open(temp_path, "wb") as buffer:
#             buffer.write(contents)

#         result = await run_in_threadpool(
#             run_pipeline,
#             str(temp_path),
#             contents
#         )

#         logger.info(f"Summarization completed for {file.filename}")

#         return result

#     except Exception:
#         logger.exception("Pipeline execution failed")
#         raise HTTPException(status_code=500, detail="Pipeline execution failed.")

#     finally:
#         if temp_path.exists():
#             temp_path.unlink()


# # ─────────────────────────────────────────
# # Entity Extraction
# # ─────────────────────────────────────────
# @app.post("/extract-entities", tags=["Entity Extraction"])
# async def entity_extraction_api(doc_id: str):
#     """
#     Extracts entities from already-ingested document.
#     Requires: /summarize to have been called first.
#     """
#     logger.info(f"Entity extraction for doc_id: {doc_id}")

#     document = db.get_by_doc_id(doc_id)
#     if not document:
#         raise HTTPException(
#             status_code=404,
#             detail="Document not found. Run /summarize first to get doc_id."
#         )

#     try:
#         chunks = document["chunks"]

#         entities = await run_in_threadpool(
#             extract_entities_from_chunks,
#             chunks
#         )

#         # Store entities in MongoDB so /build-graph can use them
#         db.update_document(doc_id, {"entities": entities})

#         return {
#             "doc_id": doc_id,
#             "entity_count": len(entities),
#             "entities": entities
#         }

#     except Exception:
#         logger.exception("Entity extraction failed")
#         raise HTTPException(status_code=500, detail="Entity extraction failed.")


# # ─────────────────────────────────────────
# # Knowledge Graph Builder
# # ─────────────────────────────────────────
# @app.post("/build-graph", tags=["Knowledge Graph"])
# async def build_graph_api(doc_id: str):
#     """
#     Builds knowledge graph in Neo4j from extracted entities.
#     Requires: /extract-entities to have been called first.
#     """
#     logger.info(f"Building graph for doc_id: {doc_id}")

#     document = db.get_by_doc_id(doc_id)
#     if not document:
#         raise HTTPException(status_code=404, detail="Document not found.")

#     if not document.get("entities"):
#         raise HTTPException(
#             status_code=400,
#             detail="Entities not found. Run /extract-entities first."
#         )

#     try:
#         result = await run_in_threadpool(
#             build_graph,
#             doc_id,
#             document["entities"],
#             document["chunks"]      # FIX — was missing, caused TypeError
#         )

#         # Mark graph as built in MongoDB
#         db.update_document(doc_id, {"graph_built": True})

#         return {
#             "doc_id": doc_id,
#             "nodes_created": result["nodes"],
#             "relationships_created": result["relationships"]
#         }

#     except Exception:
#         logger.exception("Graph building failed")
#         raise HTTPException(status_code=500, detail="Graph building failed.")


# # ─────────────────────────────────────────
# # Knowledge Graph Query
# # ─────────────────────────────────────────
# @app.get("/graph-query", tags=["Knowledge Graph"])
# async def graph_query_api(
#     doc_id: str = Query(..., description="Document ID from /summarize"),
#     entity: str = Query(..., description="Entity name to query relationships for")
# ):
#     """
#     Query relationships for a specific entity within a document.
#     Requires: /build-graph to have been called first.
#     """
#     logger.info(f"Graph query | doc_id: {doc_id} | entity: {entity}")

#     document = db.get_by_doc_id(doc_id)
#     if not document:
#         raise HTTPException(status_code=404, detail="Document not found.")

#     if not document.get("graph_built"):
#         raise HTTPException(
#             status_code=400,
#             detail="Graph not built yet. Run /build-graph first."
#         )

#     try:
#         relationships = await run_in_threadpool(
#             query_graph,
#             doc_id,
#             entity
#         )

#         return {
#             "doc_id": doc_id,
#             "entity": entity,
#             "relationships": relationships
#         }

#     except Exception:
#         logger.exception("Graph query failed")
#         raise HTTPException(status_code=500, detail="Graph query failed.")


# # ─────────────────────────────────────────
# # Fact Verification
# # ─────────────────────────────────────────
# @app.post("/verify-facts", tags=["Fact Verification"])
# async def verify_facts_api(doc_id: str):
#     """
#     Verifies summary claims against source chunks using vector search.
#     Returns coverage score and flagged unsupported claims.
#     Requires: /summarize to have been called first.
#     """
#     logger.info(f"Fact verification for doc_id: {doc_id}")

#     document = db.get_by_doc_id(doc_id)
#     if not document:
#         raise HTTPException(status_code=404, detail="Document not found.")

#     try:
#         result = await run_in_threadpool(
#             verify_facts,
#             doc_id,
#             document["summary"],
#             document["chunks"]
#         )

#         # Store verification result in MongoDB
#         db.update_document(doc_id, {
#             "facts_verified": True,
#             "coverage_score": result["coverage_score"]
#         })

#         return {
#             "doc_id": doc_id,
#             "coverage_score": result["coverage_score"],
#             "total_claims": result["total_claims"],
#             "supported_claims": result["supported_claims"],
#             "flagged_claims": result["flagged_claims"],
#             "status": result["status"]
#         }

#     except Exception:
#         logger.exception("Fact verification failed")
#         raise HTTPException(status_code=500, detail="Fact verification failed.")


# # ─────────────────────────────────────────
# # Semantic Query
# # ─────────────────────────────────────────
# @app.post("/query", tags=["Query"])
# async def query_api(doc_id: str, query: str):
#     """
#     Semantic search over document chunks using MongoDB vector search.
#     Returns most relevant chunks for the query.
#     Requires: /summarize to have been called first.
#     """
#     logger.info(f"Semantic query | doc_id: {doc_id} | query: {query}")

#     document = db.get_by_doc_id(doc_id)
#     if not document:
#         raise HTTPException(status_code=404, detail="Document not found.")

#     try:
#         results = await run_in_threadpool(
#             vector_store.search,
#             doc_id,
#             query
#         )

#         return {
#             "doc_id": doc_id,
#             "query": query,
#             "results": results
#         }

#     except Exception:
#         logger.exception("Query failed")
#         raise HTTPException(status_code=500, detail="Query failed.")

"""modular"""

from fastapi import FastAPI
from app.core.config import APP_TITLE
from app.api.routes import summarization, entities, graph, fact_verification, query
from fastapi.middleware.cors import CORSMiddleware



app = FastAPI(title=APP_TITLE)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(summarization.router)
app.include_router(entities.router)
app.include_router(graph.router)
app.include_router(fact_verification.router)
app.include_router(query.router)


@app.get("/", tags=["Health"])
async def health() -> dict:
    return {"status": "ok"}