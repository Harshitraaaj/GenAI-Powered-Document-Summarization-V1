ENTITY_EXTRACTION_PROMPT = """
You are an expert entity extractor. Given the following text, extract all named entities.

For each entity extract:
- name: the entity name as it appears in text
- type: one of [PERSON, ORGANIZATION, LOCATION, DATE, CONCEPT, TECHNOLOGY, EVENT, PRODUCT, OTHER]
- context: a brief one-line description of how it appears in the text

Rules:
- Skip single letters, abbreviations with no clear meaning, and obvious typos
- Only extract entities that are meaningful and clearly identifiable
- Do NOT include page numbers, bullet symbols, or formatting artifacts

Return ONLY a valid JSON object in this exact format, no preamble:
{{
    "entities": [
        {{
            "name": "entity name",
            "type": "ENTITY_TYPE",
            "context": "brief context"
        }}
    ]
}}

Text:
{content}
"""

REFERENCE_CHUNK_ENTITY_PROMPT = """
You are an expert entity extractor. Given the following reference list, extract ONLY relevant non-person entities.

Focus on extracting:
- ORGANIZATION
- TECHNOLOGY
- MODEL
- DATASET

Do NOT extract:
- person names (e.g., "Fei et al.")
- citation-only references
- URLs, arXiv IDs, or noisy tokens
- section/table/figure references

Rules:
- Only extract meaningful and clearly identifiable entities
- Avoid duplicates
- Ignore formatting artifacts

Return ONLY a valid JSON object in this exact format, no preamble:
{{
    "entities": [
        {{
            "name": "entity name",
            "type": "ENTITY_TYPE",
            "context": "brief context"
        }}
    ]
}}

Text:
{content}
"""