
import logging
from langchain_text_splitters import RecursiveCharacterTextSplitter

from app.core.config import settings, CHUNKING_LOG_TEMPLATE

logger = logging.getLogger(__name__)


def chunk_document(
    text: str,
    chunk_size: int = settings.CHUNK_SIZE,
    chunk_overlap: int = settings.CHUNK_OVERLAP,
) -> list[dict]:

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
            encoding_name=settings.TOKEN_ENCODING,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=settings.CHUNK_SEPARATORS,
        )

        raw_chunks = splitter.split_text(text)

        chunks = [
            {
                "chunk_id":   idx + 1,
                "content":    chunk.strip(),
                "char_length": len(chunk.strip()),
            }
            for idx, chunk in enumerate(raw_chunks)
            if chunk.strip()
        ]

        logger.info(f"Chunking completed successfully | Total chunks: {len(chunks)}")
        return chunks

    except Exception:
        logger.exception("Chunking failed")
        raise