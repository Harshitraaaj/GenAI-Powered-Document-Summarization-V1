import uuid
import logging

logger = logging.getLogger(__name__)


def generate_doc_id() -> str:
    """
    Generate a unique document ID for each ingested document.

    Format: 'doc_<uuid4_hex>'
    Example: 'doc_3f2504e04f8911d39a0c0305e82c3301'

    The 'doc_' prefix makes it immediately recognisable in logs,
    MongoDB queries, and API responses — easy to distinguish from
    other IDs in the system (e.g. chunk_ids, entity_ids).

    UUID4 is random — no timestamp, no machine info encoded.
    Collision probability is astronomically low (2^122 space).
    """
    doc_id = f"doc_{uuid.uuid4().hex}"

    logger.debug(f"Generated doc_id: {doc_id}")

    return doc_id


def generate_chunk_id(doc_id: str, index: int) -> str:
    """
    Generate a deterministic chunk ID from doc_id + chunk index.

    Format: '<doc_id>_chunk_<index>'
    Example: 'doc_3f2504e04f89_chunk_0'

    Deterministic — same doc + same index always produces same chunk_id.
    Useful for referencing specific chunks in entity extraction
    and fact verification results.
    """
    return f"{doc_id}_chunk_{index}"


def generate_entity_id(doc_id: str, entity_name: str, entity_type: str) -> str:
    """
    Generate a deterministic entity ID from doc + entity details.

    Format: '<doc_id>_entity_<md5_of_name_type>'
    Ensures the same entity in the same document always gets the same ID —
    important for deduplication and Neo4j node creation.
    """
    import hashlib

    key = f"{entity_name.lower().strip()}_{entity_type.lower().strip()}"
    entity_hash = hashlib.md5(key.encode()).hexdigest()[:12]

    return f"{doc_id}_entity_{entity_hash}"