import hashlib
import logging

logger = logging.getLogger(__name__)


def get_pdf_hash(file_bytes: bytes) -> str:
    """
    Generate an MD5 hash from PDF bytes for deduplication/caching.
    """

    if not file_bytes:
        logger.warning("get_pdf_hash called with empty bytes")
        return ""

    pdf_hash = hashlib.md5(file_bytes).hexdigest()

    logger.debug(f"PDF hash generated: {pdf_hash}")

    return pdf_hash


def get_sha256_hash(file_bytes: bytes) -> str:
    """
    Generate a SHA-256 hash (use when stronger hashing is needed).
    """
    if not file_bytes:
        return ""

    return hashlib.sha256(file_bytes).hexdigest()