# import os
# from pathlib import Path
# from dotenv import load_dotenv

# load_dotenv()

# # Base Paths
# BASE_DIR = Path(__file__).resolve().parents[2]

# TEMP_DIR = BASE_DIR / "temp"
# LOG_DIR = BASE_DIR / "logs"

# TEMP_DIR.mkdir(exist_ok=True)
# LOG_DIR.mkdir(exist_ok=True)

# # App Settings

# APP_TITLE = os.getenv("APP_TITLE", "GenAI Document Summarization API")

# DEBUG = os.getenv("DEBUG", "False") == "True"


# # File Upload Settings

# MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", 10))

# ALLOWED_FILE_TYPES = tuple(os.getenv("ALLOWED_FILE_TYPES", ".pdf,.txt").split(","))

# TEMP_FILE_PREFIX = os.getenv("TEMP_FILE_PREFIX", "temp")


# # Logging
# LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
# LOG_FILE = LOG_DIR / "app.log"

# # Chunking Configuration

# TOKEN_ENCODING = os.getenv("TOKEN_ENCODING", "cl100k_base")

# CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 800))

# CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", 50))

# CHUNK_SEPARATORS = os.getenv("CHUNK_SEPARATORS", "\\n\\n,\\n,. , ,").split(",")

# CHUNKING_LOG_TEMPLATE = (
#     "Chunk configuration | chunk_size={chunk_size}, overlap={overlap}"
# )

# # Document Ingestion Configuration

# SUPPORTED_EXTENSIONS = tuple(os.getenv("SUPPORTED_EXTENSIONS", ".pdf,.txt").split(","))

# TEXT_FILE_ENCODING = os.getenv("TEXT_FILE_ENCODING", "utf-8")

# PDF_EXTRACTION_MODE = os.getenv("PDF_EXTRACTION_MODE", "blocks")

# ENABLE_TEXT_CLEANING = os.getenv("ENABLE_TEXT_CLEANING", "True") == "True"

# TEST_SAMPLE_PATH = os.getenv("TEST_SAMPLE_PATH", "C:\\Users\\harsraj\\Downloads\\covid.pdf")

# # AWS Bedrock Configuration (Environment Dependent)

# AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "us.meta.llama3-2-11b-instruct-v1:0")

# # Embeddings + Vector Search
# EMBEDDING_MODEL_ID = "amazon.titan-embed-text-v2:0"
# VECTOR_SEARCH_INDEX_NAME = "chunks_vector_index"
# VECTOR_DIMENSIONS = 1024
# VECTOR_TOP_K = 5

# # LLM Generation Settings (Application Defaults)
# # Can be overridden via ENV if needed


# MODEL_MAX_TOKENS = int(os.getenv("MODEL_MAX_TOKENS", 900))

# EXECUTIVE_MAX_TOKENS = int(os.getenv("EXECUTIVE_MAX_TOKENS", 1500))

# MODEL_TEMPERATURE = float(os.getenv("MODEL_TEMPERATURE", 0))#0.2

# MODEL_TOP_P = float(os.getenv("MODEL_TOP_P", 1))#0.9

# MODEL_RETRIES = int(os.getenv("MODEL_RETRIES", 3))

# RETRY_BASE_DELAY = int(os.getenv("RETRY_BASE_DELAY", 2))


# # Parallel Processing (Performance Tuning)

# MAX_WORKERS = int(os.getenv("MAX_WORKERS", 6)) #try 4 or 6


# # Hierarchical Summarization Logic

# SECTION_GROUP_SIZE = int(os.getenv("SECTION_GROUP_SIZE", 6))

# LOW_COVERAGE_THRESHOLD = float(os.getenv("LOW_COVERAGE_THRESHOLD", 80.0))

# #Mongo db 

# MONGODB_URI = os.getenv("MONGODB_URI")
# MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")

# #Neo4j Config

# NEO4J_URI = os.getenv("NEO4J_URI")
# NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
# NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# # Extractor Configuration
# MIN_ENTITY_NAME_LENGTH = int(os.getenv("MIN_ENTITY_NAME_LENGTH", 3))
# EXTRACTOR_MAX_WORKERS = int(os.getenv("EXTRACTOR_MAX_WORKERS", 2))
# SKIP_ENTITY_TYPES: set = set(os.getenv("SKIP_ENTITY_TYPES", "").split(",")) if os.getenv("SKIP_ENTITY_TYPES") else set()
# CITATION_COUNT_THRESHOLD = int(os.getenv("CITATION_COUNT_THRESHOLD", 10))
# CITATION_KEYWORD_THRESHOLD = int(os.getenv("CITATION_KEYWORD_THRESHOLD", 3))
# REFERENCE_CHUNK_KEYWORDS = [
#     "arXiv preprint", "Proceedings of", "In Proceedings",
#     "Conference on", "arXiv:", "doi:", "arxiv.org",
#     "ACL Anthology", "Neural Information Processing",
#     "International Conference", "Annual Meeting"
# ]

# # Fact Verifier Configuration
# MIN_CLAIM_WORDS = int(os.getenv("MIN_CLAIM_WORDS", 4))
# TABLE_CHUNK_NUMERIC_THRESHOLD = float(os.getenv("TABLE_CHUNK_NUMERIC_THRESHOLD", 0.30))
# FACT_VERIFIER_TOP_K = int(os.getenv("FACT_VERIFIER_TOP_K", 3))
# FACT_VERIFIER_MAX_CLAIMS = int(os.getenv("FACT_VERIFIER_MAX_CLAIMS", 20))
# FACT_VERIFIER_MAX_SOURCE_CHARS = int(os.getenv("FACT_VERIFIER_MAX_SOURCE_CHARS", 3000))
# FACT_VERIFIER_COVERAGE_THRESHOLD = float(os.getenv("FACT_VERIFIER_COVERAGE_THRESHOLD", 0.7))
# QUERY_ENRICHMENT_MIN_WORDS = int(os.getenv("QUERY_ENRICHMENT_MIN_WORDS", 6))
# KEYWORD_FALLBACK_MIN_WORD_LENGTH = int(os.getenv("KEYWORD_FALLBACK_MIN_WORD_LENGTH", 4))

# # Graph Builder Configuration
# GRAPH_ANCHOR_TOP_N = int(os.getenv("GRAPH_ANCHOR_TOP_N", 4))
# GRAPH_CHUNK_BATCH_SIZE = int(os.getenv("GRAPH_CHUNK_BATCH_SIZE", 3))
# GRAPH_ENTITY_BATCH_SIZE = int(os.getenv("GRAPH_ENTITY_BATCH_SIZE", 6))
# GRAPH_MAX_ENTITIES_PER_LLM_CALL = int(os.getenv("GRAPH_MAX_ENTITIES_PER_LLM_CALL", 8))
# GRAPH_CROSS_CLUSTER_THRESHOLD = int(os.getenv("GRAPH_CROSS_CLUSTER_THRESHOLD", 16))
# GRAPH_CONTENT_CHAR_LIMIT = int(os.getenv("GRAPH_CONTENT_CHAR_LIMIT", 1000))
# GRAPH_QUERY_LIMIT = int(os.getenv("GRAPH_QUERY_LIMIT", 50))
# GRAPH_MIN_ENTITY_NAME_LENGTH = int(os.getenv("GRAPH_MIN_ENTITY_NAME_LENGTH", 2))

# LOW_VALUE_ENTITY_TYPES = set(os.getenv("LOW_VALUE_ENTITY_TYPES", "DATE,OTHER").split(","))
# HIGH_VALUE_TYPES = set(os.getenv("HIGH_VALUE_TYPES", "TECHNOLOGY,CONCEPT,ORGANIZATION,EVENT").split(","))
# LOW_VALUE_ENTITY_NAMES = {
#     "email", "gmail", "asu.edu", "hotmail",
#     "table 1", "table 2", "table 3", "table 4", "table a1", "table a2",
#     "table a3", "table a4", "figure 1", "figure a1", "figure a2",
#     "appendix table a1", "appendix table a2", "appendix table a4",
#     "appendix figure a2", "panel a", "panel b",
#     "p-value", "ols", "ols regression", "scale",
#     "oclc", "june", "march", "january", "february", "april", "may",
#     "july", "august", "september", "october", "november", "december",
# }

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Base Paths ─────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parents[2]

TEMP_DIR = BASE_DIR / "temp"
LOG_DIR  = BASE_DIR / "logs"

TEMP_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ── App Settings ───────────────────────────────────────────────────────────────

APP_TITLE = os.getenv("APP_TITLE", "GenAI Document Summarization API")
DEBUG     = os.getenv("DEBUG", "False") == "True"

# ── File Upload Settings ───────────────────────────────────────────────────────

MAX_FILE_SIZE_MB   = int(os.getenv("MAX_FILE_SIZE_MB", 10))
ALLOWED_FILE_TYPES = tuple(os.getenv("ALLOWED_FILE_TYPES", ".pdf,.txt").split(","))
TEMP_FILE_PREFIX   = os.getenv("TEMP_FILE_PREFIX", "temp")

# ── Logging ────────────────────────────────────────────────────────────────────

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE  = LOG_DIR / "app.log"

# ── Chunking Configuration ─────────────────────────────────────────────────────

TOKEN_ENCODING   = os.getenv("TOKEN_ENCODING", "cl100k_base")
CHUNK_SIZE       = int(os.getenv("CHUNK_SIZE", 800))
CHUNK_OVERLAP    = int(os.getenv("CHUNK_OVERLAP", 50))
CHUNK_SEPARATORS = os.getenv("CHUNK_SEPARATORS", "\\n\\n,\\n,. , ,").split(",")
CHUNKING_LOG_TEMPLATE = (
    "Chunk configuration | chunk_size={chunk_size}, overlap={overlap}"
)

# ── Document Ingestion — General ───────────────────────────────────────────────

SUPPORTED_EXTENSIONS = tuple(os.getenv("SUPPORTED_EXTENSIONS", ".pdf,.txt").split(","))
TEXT_FILE_ENCODING   = os.getenv("TEXT_FILE_ENCODING", "utf-8")
PDF_EXTRACTION_MODE  = os.getenv("PDF_EXTRACTION_MODE", "blocks")
ENABLE_TEXT_CLEANING = os.getenv("ENABLE_TEXT_CLEANING", "True") == "True"
TEST_SAMPLE_PATH     = os.getenv("TEST_SAMPLE_PATH", "C:\\Users\\harsraj\\Downloads\\robo.pdf")

# ── Document Ingestion — Workers ───────────────────────────────────────────────

PAGE_WORKERS  = int(os.getenv("PAGE_WORKERS", 10))
TABLE_WORKERS = int(os.getenv("TABLE_WORKERS", 6))

# ── Document Ingestion — OCR ───────────────────────────────────────────────────

MIN_OCR_WIDTH  = int(os.getenv("MIN_OCR_WIDTH", 100))
MIN_OCR_HEIGHT = int(os.getenv("MIN_OCR_HEIGHT", 50))
MIN_OCR_PIXELS = int(os.getenv("MIN_OCR_PIXELS", 10000))
TESSERACT_CMD  = os.getenv(
    "TESSERACT_CMD",
    "C:\\Users\\harsraj\\AppData\\Local\\Programs\\Tesseract-OCR\\tesseract.exe",
)

# ── Document Ingestion — Table Validation Thresholds ──────────────────────────

TABLE_MIN_ROWS             = int(os.getenv("TABLE_MIN_ROWS", 3))
TABLE_MIN_COLS             = int(os.getenv("TABLE_MIN_COLS", 2))
TABLE_MAX_AVG_CELL_LENGTH  = int(os.getenv("TABLE_MAX_AVG_CELL_LENGTH", 40))
TABLE_MIN_NUMERIC_RATIO    = float(os.getenv("TABLE_MIN_NUMERIC_RATIO", 0.25))
TABLE_MAX_SINGLE_COL_RATIO = float(os.getenv("TABLE_MAX_SINGLE_COL_RATIO", 0.6))
TABLE_MIN_MULTI_COL_ROWS   = int(os.getenv("TABLE_MIN_MULTI_COL_ROWS", 3))
TABLE_MAX_MID_WORD_RATIO   = float(os.getenv("TABLE_MAX_MID_WORD_RATIO", 0.4))

# ── AWS Bedrock Configuration ──────────────────────────────────────────────────

AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "us.meta.llama3-2-11b-instruct-v1:0")

# ── Embeddings + Vector Search ─────────────────────────────────────────────────

EMBEDDING_MODEL_ID       = "amazon.titan-embed-text-v2:0"
VECTOR_SEARCH_INDEX_NAME = "chunks_vector_index"
VECTOR_DIMENSIONS        = 1024
VECTOR_TOP_K             = 5

# ── LLM Generation Settings ────────────────────────────────────────────────────

MODEL_MAX_TOKENS     = int(os.getenv("MODEL_MAX_TOKENS", 900))
EXECUTIVE_MAX_TOKENS = int(os.getenv("EXECUTIVE_MAX_TOKENS", 1500))
MODEL_TEMPERATURE    = float(os.getenv("MODEL_TEMPERATURE", 0))
MODEL_TOP_P          = float(os.getenv("MODEL_TOP_P", 1))
MODEL_RETRIES        = int(os.getenv("MODEL_RETRIES", 3))
RETRY_BASE_DELAY     = int(os.getenv("RETRY_BASE_DELAY", 2))

# ── Parallel Processing ────────────────────────────────────────────────────────

MAX_WORKERS = int(os.getenv("MAX_WORKERS", 6))

# ── Hierarchical Summarization ─────────────────────────────────────────────────

SECTION_GROUP_SIZE     = int(os.getenv("SECTION_GROUP_SIZE", 6))
LOW_COVERAGE_THRESHOLD = float(os.getenv("LOW_COVERAGE_THRESHOLD", 80.0))

# ── MongoDB ────────────────────────────────────────────────────────────────────

MONGODB_URI     = os.getenv("MONGODB_URI")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")

# ── Neo4j ──────────────────────────────────────────────────────────────────────

NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# ── Extractor Configuration ────────────────────────────────────────────────────

MIN_ENTITY_NAME_LENGTH = int(os.getenv("MIN_ENTITY_NAME_LENGTH", 3))
EXTRACTOR_MAX_WORKERS  = int(os.getenv("EXTRACTOR_MAX_WORKERS", 2))
SKIP_ENTITY_TYPES: set = (
    set(os.getenv("SKIP_ENTITY_TYPES", "").split(","))
    if os.getenv("SKIP_ENTITY_TYPES") else set()
)
CITATION_COUNT_THRESHOLD   = int(os.getenv("CITATION_COUNT_THRESHOLD", 10))
CITATION_KEYWORD_THRESHOLD = int(os.getenv("CITATION_KEYWORD_THRESHOLD", 3))
REFERENCE_CHUNK_KEYWORDS   = [
    "arXiv preprint", "Proceedings of", "In Proceedings",
    "Conference on", "arXiv:", "doi:", "arxiv.org",
    "ACL Anthology", "Neural Information Processing",
    "International Conference", "Annual Meeting",
]

# ── Fact Verifier Configuration ────────────────────────────────────────────────

MIN_CLAIM_WORDS                  = int(os.getenv("MIN_CLAIM_WORDS", 4))
TABLE_CHUNK_NUMERIC_THRESHOLD    = float(os.getenv("TABLE_CHUNK_NUMERIC_THRESHOLD", 0.30))
FACT_VERIFIER_TOP_K              = int(os.getenv("FACT_VERIFIER_TOP_K", 3))
FACT_VERIFIER_MAX_CLAIMS         = int(os.getenv("FACT_VERIFIER_MAX_CLAIMS", 20))
FACT_VERIFIER_MAX_SOURCE_CHARS   = int(os.getenv("FACT_VERIFIER_MAX_SOURCE_CHARS", 3000))
FACT_VERIFIER_COVERAGE_THRESHOLD = float(os.getenv("FACT_VERIFIER_COVERAGE_THRESHOLD", 0.7))
QUERY_ENRICHMENT_MIN_WORDS       = int(os.getenv("QUERY_ENRICHMENT_MIN_WORDS", 6))
KEYWORD_FALLBACK_MIN_WORD_LENGTH = int(os.getenv("KEYWORD_FALLBACK_MIN_WORD_LENGTH", 4))

# ── Graph Builder Configuration ────────────────────────────────────────────────

GRAPH_ANCHOR_TOP_N              = int(os.getenv("GRAPH_ANCHOR_TOP_N", 4))
GRAPH_CHUNK_BATCH_SIZE          = int(os.getenv("GRAPH_CHUNK_BATCH_SIZE", 3))
GRAPH_ENTITY_BATCH_SIZE         = int(os.getenv("GRAPH_ENTITY_BATCH_SIZE", 6))
GRAPH_MAX_ENTITIES_PER_LLM_CALL = int(os.getenv("GRAPH_MAX_ENTITIES_PER_LLM_CALL", 8))
GRAPH_CROSS_CLUSTER_THRESHOLD   = int(os.getenv("GRAPH_CROSS_CLUSTER_THRESHOLD", 16))
GRAPH_CONTENT_CHAR_LIMIT        = int(os.getenv("GRAPH_CONTENT_CHAR_LIMIT", 1000))
GRAPH_QUERY_LIMIT               = int(os.getenv("GRAPH_QUERY_LIMIT", 50))
GRAPH_MIN_ENTITY_NAME_LENGTH    = int(os.getenv("GRAPH_MIN_ENTITY_NAME_LENGTH", 2))

LOW_VALUE_ENTITY_TYPES = set(os.getenv("LOW_VALUE_ENTITY_TYPES", "DATE,OTHER").split(","))
HIGH_VALUE_TYPES       = set(os.getenv("HIGH_VALUE_TYPES", "TECHNOLOGY,CONCEPT,ORGANIZATION,EVENT").split(","))
LOW_VALUE_ENTITY_NAMES = {
    "email", "gmail", "asu.edu", "hotmail",
    "table 1", "table 2", "table 3", "table 4", "table a1", "table a2",
    "table a3", "table a4", "figure 1", "figure a1", "figure a2",
    "appendix table a1", "appendix table a2", "appendix table a4",
    "appendix figure a2", "panel a", "panel b",
    "p-value", "ols", "ols regression", "scale",
    "oclc", "june", "march", "january", "february", "april", "may",
    "july", "august", "september", "october", "november", "december",
}