import uuid
import logging

logger = logging.getLogger(__name__)


def generate_doc_id() -> str:

    doc_id = f"doc_{uuid.uuid4().hex}"

    logger.debug(f"Generated doc_id: {doc_id}")

    return doc_id


def generate_chunk_id(doc_id: str, index: int) -> str:

    return f"{doc_id}_chunk_{index}"


def generate_entity_id(doc_id: str, entity_name: str, entity_type: str) -> str:
 
    import hashlib

    key = f"{entity_name.lower().strip()}_{entity_type.lower().strip()}"
    entity_hash = hashlib.md5(key.encode()).hexdigest()[:12]

    return f"{doc_id}_entity_{entity_hash}"