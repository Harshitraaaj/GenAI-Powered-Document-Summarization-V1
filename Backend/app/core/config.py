
from pathlib import Path
from functools import lru_cache
from pydantic import computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


CHUNKING_LOG_TEMPLATE = "Chunk configuration | chunk_size={chunk_size}, overlap={overlap}"

REFERENCE_CHUNK_KEYWORDS = [
    "arXiv preprint", "Proceedings of", "In Proceedings",
    "Conference on", "arXiv:", "doi:", "arxiv.org",
    "ACL Anthology", "Neural Information Processing",
    "International Conference", "Annual Meeting",
]

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


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


    # --- CORS ---
    CORS_ALLOW_ORIGINS:     list[str] = ["http://localhost:3000"]
    CORS_ALLOW_CREDENTIALS: bool      = True
    CORS_ALLOW_METHODS:     list[str] = ["*"]
    CORS_ALLOW_HEADERS:     list[str] = ["*"]

    # --- Application ---
    APP_TITLE:                 str = "GenAI Document Summarization API"
    DEBUG:                    bool = False
    LOG_LEVEL:                 str = "INFO"
    LOG_ERROR_TRUNCATE_LENGTH: int = 100

    # --- File Handling ---
    MAX_FILE_SIZE_MB:     int             = 10
    ALLOWED_FILE_TYPES:   tuple[str, ...] = (".pdf", ".txt")
    SUPPORTED_EXTENSIONS: tuple[str, ...] = (".pdf", ".txt")
    TEMP_FILE_PREFIX:     str             = "temp"
    BYTES_PER_MB: int = 1024 * 1024

    # --- Text Processing ---
    TOKEN_ENCODING:   str       = "cl100k_base"
    CHUNK_SIZE:       int       = 800
    CHUNK_OVERLAP:    int       = 50
    CHUNK_SEPARATORS: list[str] = ["\\n\\n", "\\n", ". ", " ", ""]

    TEXT_FILE_ENCODING:   str  = "utf-8"
    PDF_EXTRACTION_MODE:  str  = "text"
    ENABLE_TEXT_CLEANING: bool = True
    TEST_SAMPLE_PATH:     str  = ""

    TEXT_CLEAN_PATTERNS: list[tuple[str, str]] = [
    (r"-\s*\n\s*", ""),
    (r"\n+", " "),
    (r"\bPage\s*\d+\b", ""),
    (r"\s+", " "),
    ]

    # --- Ingestion Workers ---
    PAGE_WORKERS:              int = 10
    TABLE_WORKERS:             int = 6
    INGEST_POOL_MIN_WORKERS:   int = 8
    INGEST_THREAD_NAME_PREFIX: str = "ingest"
    INGEST_DEV_OUTPUT_FILE:    str = "extracted_output.txt"

    # --- OCR ---
    MIN_OCR_WIDTH:           int   = 100
    MIN_OCR_HEIGHT:          int   = 50
    MIN_OCR_PIXELS:          int   = 10_000
    TESSERACT_CMD:           str   = ""
    OCR_PIXEL_SAMPLE_STEP:   int   = 8
    OCR_PIXEL_SAMPLE_THRESH: int   = 8000
    OCR_MIN_VARIANCE:        int   = 300
    OCR_MAX_ASPECT:          float = 8.0
    OCR_MIN_UNIQUE_COLORS:   int   = 10
    OCR_UPSCALE_FACTOR:      int   = 2
    OCR_PSM_MODE:            int   = 11
    OCR_TEXT_OPEN_TAG:       str   = "[IMAGE OCR TEXT]"
    OCR_CONFIG_TEMPLATE: str = "--psm {psm}"
    OCR_RESAMPLE_FILTER: str = "LANCZOS"

    # --- Image Classification ---
    IMAGE_DOMINANT_MIN_IMAGES:     int = 2
    IMAGE_DOMINANT_MAX_TEXT_CHARS: int = 400

    # --- Table Detection ---
    TABLE_MIN_ROWS:             int   = 3
    TABLE_MIN_COLS:             int   = 2
    TABLE_MAX_AVG_CELL_LENGTH:  int   = 40
    TABLE_MIN_NUMERIC_RATIO:    float = 0.25
    TABLE_MAX_SINGLE_COL_RATIO: float = 0.6
    TABLE_MIN_MULTI_COL_ROWS:   int   = 3
    TABLE_MAX_MID_WORD_RATIO:   float = 0.4
    TABLE_ROTATED_TEXT_RATIO:   float = 0.3
    TABLE_CELL_SEPARATOR:       str   = " | "
    TABLE_OPEN_TAG:             str   = "[TABLE]"
    TABLE_CLOSE_TAG:            str   = "[/TABLE]"

    TABLE_MID_WORD_AVG_LEN_MIN:       int   = 30
    TABLE_MID_WORD_NUMERIC_RATIO_MAX: float = 0.25
    TABLE_CONCAT_WORDS_RATIO_MAX:     float = 0.30
    TABLE_NUMERIC_RATIO_FLOOR:        float = 0.30

     # NEW: Table strategies
    TABLE_STRATEGIES: list[dict] = [
        {
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "snap_tolerance": 3,
            "join_tolerance": 3,
            "edge_min_length": 3,
            "min_words_vertical": 1,
            "min_words_horizontal": 1,
        },
        {
            "vertical_strategy": "lines_strict",
            "horizontal_strategy": "lines_strict",
            "snap_tolerance": 3,
            "join_tolerance": 3,
            "edge_min_length": 3,
            "min_words_vertical": 1,
            "min_words_horizontal": 1,
        },
        {
            "vertical_strategy": "text",
            "horizontal_strategy": "lines",
            "snap_tolerance": 5,
            "join_tolerance": 5,
            "edge_min_length": 3,
            "min_words_vertical": 2,
            "min_words_horizontal": 1,
        },
        {
            "vertical_strategy": "lines",
            "horizontal_strategy": "text",
            "snap_tolerance": 5,
            "join_tolerance": 5,
            "edge_min_length": 3,
            "min_words_vertical": 1,
            "min_words_horizontal": 2,
        },
        {
            "vertical_strategy": "text",
            "horizontal_strategy": "text",
            "snap_tolerance": 10,
            "join_tolerance": 10,
            "edge_min_length": 3,
            "min_words_vertical": 1,
            "min_words_horizontal": 1,
        },
    ]

    # --- AWS / Bedrock ---
    AWS_REGION:          str = "us-east-1"
    AWS_ACCESS_KEY_ID:     str | None = None   # loaded from .env
    AWS_SECRET_ACCESS_KEY: str | None = None   # loaded from .env
    AWS_SESSION_TOKEN:     str | None = None   
    BEDROCK_MODEL_ID:    str = "us.meta.llama3-2-11b-instruct-v1:0"
    BEDROCK_RETRY_MODE:  str = "adaptive"
    BEDROCK_WARMUP_TEXT: str = "warmup"


    # --- Embeddings & Vector Search ---
    EMBEDDING_MODEL_ID:           str = "amazon.titan-embed-text-v2:0"
    EMBEDDINGS_COLLECTION_NAME:   str = "chunk_embeddings"
    VECTOR_SEARCH_INDEX_NAME:     str = "chunks_vector_index"
    VECTOR_DIMENSIONS:            int = 1024
    VECTOR_TOP_K:                 int = 5
    VECTOR_CANDIDATES_MULTIPLIER: int = 10
    EMBED_INPUT_MAX_CHARS:        int = 8000
    EMBED_WORKERS:                int = 5
    EMBED_POOL_CONNECTIONS:       int = 15

    # --- LLM Inference ---
    MODEL_MAX_TOKENS:     int   = 900
    EXECUTIVE_MAX_TOKENS: int   = 1500
    SECTION_MAX_TOKENS:   int   = 900
    MODEL_TEMPERATURE:    float = 0.0
    MODEL_TOP_P:          float = 1.0
    MODEL_RETRIES:        int   = 3
    RETRY_BASE_DELAY:     int   = 2

    # --- Background Embedding ---
    MAX_WORKERS:                    int   = 6
    EMBED_START_DELAY_SECONDS:      int   = 8
    EMBED_THREAD_NAME_PREFIX:       str   = "bg_embed"
    SECTION_FALLBACK_SUMMARY_LIMIT: int   = 500
    EMBED_WAIT_WARN_THRESHOLD:      float = 1.0
    EMBED_DELAY_REDUCTION_HINT:     int   = 2

    # --- Summarization ---
    SECTION_GROUP_SIZE:     int   = 6
    LOW_COVERAGE_THRESHOLD: float = 80.0

    # --- MongoDB ---
    MONGODB_URI:     str | None = None
    MONGODB_DB_NAME: str | None = None

    MONGO_SERVER_SELECTION_TIMEOUT_MS: int = 30000
    MONGO_CONNECT_TIMEOUT_MS:          int = 20000
    MONGO_SOCKET_TIMEOUT_MS:           int = 20000

    MONGO_DOCUMENTS_COLLECTION:  str = "documents"
    MONGO_CACHE_META_COLLECTION: str = "cache_meta"

    MONGO_IDX_PDF_HASH:        str = "pdf_hash_unique"
    MONGO_IDX_DOC_ID:          str = "doc_id_unique"
    MONGO_IDX_CACHE_META_HASH: str = "cache_meta_hash_unique"

    # --- Neo4j ---
    NEO4J_URI:      str | None = None
    NEO4J_USERNAME: str | None = None
    NEO4J_PASSWORD: str | None = None

    # --- Entity Extraction ---
    MIN_ENTITY_NAME_LENGTH:         int      = 2
    EXTRACTOR_MAX_WORKERS:          int      = 2
    EXTRACTOR_ASYNC_WORKERS:        int      = 8
    CHUNK_TIMEOUT_SECONDS:          int      = 30
    RETRY_TIMEOUT_SECONDS:          int      = 15
    SKIP_ENTITY_TYPES:              set[str] = set()
    CITATION_COUNT_THRESHOLD:       int      = 10
    CITATION_KEYWORD_THRESHOLD:     int      = 3
    EXTRACTOR_CONTENT_CHAR_LIMIT:   int      = 4000
    EXTRACTOR_REFERENCE_MAX_TOKENS: int      = 500
    EXTRACTOR_REGULAR_MAX_TOKENS:   int      = 1000
    EXTRACTOR_RETRY_MAX_TOKENS:     int      = 300
    EXTRACTOR_RETRY_CONTENT_LIMIT:  int      = 1000

    # --- Fact Verification ---
    FACT_VERIFIER_WORKERS:                 int   = 5
    MIN_CLAIM_WORDS:                       int   = 4
    TABLE_CHUNK_NUMERIC_THRESHOLD:         float = 0.30
    FACT_VERIFIER_TOP_K:                   int   = 3
    FACT_VERIFIER_MAX_CLAIMS:              int   = 20
    FACT_VERIFIER_MAX_SOURCE_CHARS:        int   = 3000
    FACT_VERIFIER_COVERAGE_THRESHOLD:      float = 0.7
    QUERY_ENRICHMENT_MIN_WORDS:            int   = 6
    KEYWORD_FALLBACK_MIN_WORD_LENGTH:      int   = 4
    FACT_VERIFIER_THREAD_NAME_PREFIX:      str   = "verify_worker"
    FACT_VERIFIER_MAX_TOKENS:              int   = 300
    FACT_VERIFIER_KEYWORD_SCORE_TOP_N:     int   = 5
    FACT_VERIFIER_KEYWORD_RETURN_TOP_N:    int   = 3
    FACT_VERIFIER_TABLE_SCORE_PENALTY:     float = 0.5
    FACT_VERIFIER_QUERY_ENRICHMENT_SUFFIX: str   = "impact study findings outcomes"

    # --- Graph Relationship Extraction ---
    GRAPH_REL_ASYNC_WORKERS:         int      = 8
    GRAPH_ANCHOR_TOP_N:              int      = 4
    GRAPH_CHUNK_BATCH_SIZE:          int      = 3
    GRAPH_ENTITY_BATCH_SIZE:         int      = 6
    GRAPH_MAX_ENTITIES_PER_LLM_CALL: int      = 12
    GRAPH_CROSS_CLUSTER_THRESHOLD:   int      = 16
    GRAPH_CONTENT_CHAR_LIMIT:        int      = 1000
    GRAPH_QUERY_LIMIT:               int      = 50
    GRAPH_MIN_ENTITY_NAME_LENGTH:    int      = 2
    GRAPH_MIN_ROTATING_BATCH_SIZE:   int      = 3
    GRAPH_DEFAULT_RELATIONSHIP_TYPE: str      = "RELATED_TO"
    GRAPH_REL_THREAD_NAME_PREFIX:    str      = "rel_worker"
    GRAPH_REL_MAX_TOKENS:            int      = 1200
    LOW_VALUE_ENTITY_TYPES:          set[str] = {"DATE", "OTHER"}
    HIGH_VALUE_TYPES:                set[str] = {"TECHNOLOGY", "CONCEPT", "ORGANIZATION", "EVENT"}

    @computed_field
    @property
    def BASE_DIR(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @computed_field
    @property
    def TEMP_DIR(self) -> Path:
        path = self.BASE_DIR / "temp"
        path.mkdir(exist_ok=True)
        return path

    @computed_field
    @property
    def LOG_DIR(self) -> Path:
        path = self.BASE_DIR / "logs"
        path.mkdir(exist_ok=True)
        return path

    @computed_field
    @property
    def LOG_FILE(self) -> Path:
        return self.LOG_DIR / "app.log"

    @field_validator("ALLOWED_FILE_TYPES", "SUPPORTED_EXTENSIONS", mode="before")
    @classmethod
    def _parse_tuple(cls, v: object) -> tuple[str, ...]:
        if isinstance(v, str):
            return tuple(v.split(","))
        return v  # type: ignore[return-value]

    @field_validator("CHUNK_SEPARATORS", mode="before")
    @classmethod
    def _parse_list(cls, v: object) -> list[str]:
        if isinstance(v, str):
            return v.split(",")
        return v  # type: ignore[return-value]

    @field_validator("SKIP_ENTITY_TYPES", "LOW_VALUE_ENTITY_TYPES", "HIGH_VALUE_TYPES", mode="before")
    @classmethod
    def _parse_set(cls, v: object) -> set[str]:
        if isinstance(v, str):
            return set(v.split(",")) if v else set()
        return v  # type: ignore[return-value]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

