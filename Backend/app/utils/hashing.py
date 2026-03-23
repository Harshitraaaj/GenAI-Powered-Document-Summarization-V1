import hashlib
import logging

logger = logging.getLogger(__name__)


def get_pdf_hash(file_bytes: bytes) -> str:
    """
    Generate a deterministic MD5 hash from raw PDF bytes.

    Same PDF uploaded multiple times will always produce
    the same hash — this is the cache key in MongoDB.

    MD5 is used here for speed (not cryptographic security).
    For security-sensitive use cases, switch to sha256.

    Args:
        file_bytes: raw bytes of the uploaded file

    Returns:
        32-character hex string e.g. 'd41d8cd98f00b204e9800998ecf8427e'
    """
    if not file_bytes:
        logger.warning("get_pdf_hash called with empty bytes")
        return ""

    pdf_hash = hashlib.md5(file_bytes).hexdigest()

    logger.debug(f"PDF hash generated: {pdf_hash}")

    return pdf_hash


def get_sha256_hash(file_bytes: bytes) -> str:
    """
    SHA-256 variant — use this if security matters more than speed.
    Produces a 64-character hex string.
    """
    if not file_bytes:
        return ""

    return hashlib.sha256(file_bytes).hexdigest()