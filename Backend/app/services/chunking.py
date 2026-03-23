from typing import List, Dict
from langchain_text_splitters import RecursiveCharacterTextSplitter
import logging

from app.core.config import (
    TOKEN_ENCODING,
    CHUNK_SIZE,
    CHUNK_OVERLAP,
    CHUNK_SEPARATORS,
    CHUNKING_LOG_TEMPLATE,
)

logger = logging.getLogger(__name__)


def chunk_document(
    text: str,
    chunk_size: int = CHUNK_SIZE,
    chunk_overlap: int = CHUNK_OVERLAP,
) -> List[Dict]:

    logger.info("Starting document chunking")

    if not text or not text.strip():
        logger.warning("Empty document received for chunking")
        return []

    logger.info(
        CHUNKING_LOG_TEMPLATE.format(
            chunk_size=chunk_size,
            overlap=chunk_overlap,
        )
    )

    try:
        splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            encoding_name=TOKEN_ENCODING,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=CHUNK_SEPARATORS,
        )

        raw_chunks = splitter.split_text(text)

        chunks = [
            {
                "chunk_id": idx + 1,
                "content": chunk.strip(),
                "char_length": len(chunk.strip()),
            }
            for idx, chunk in enumerate(raw_chunks)
            if chunk.strip()
        ]

        logger.info(
            f"Chunking completed successfully | "
            f"Total chunks: {len(chunks)}"
        )

        return chunks

    except Exception:
        logger.exception("Chunking failed")
        raise