

ENTITY_EXTRACTION_PROMPT = """
You are an expert entity extractor. Given the following text, extract all named entities.

For each entity extract:
- name: the entity name as it appears in text
- type: one of [PERSON, ORGANIZATION, LOCATION, DATE, CONCEPT, TECHNOLOGY, EVENT, PRODUCT, MODEL, DATASET]
- context: a brief one-line description of how it appears in the text

Type guidelines:
- PERSON: human names only — never email addresses or usernames
- ORGANIZATION: companies, institutions, universities, journals, conferences, agencies
- LOCATION: countries, cities, regions, geographical places
- DATE: specific dates, years, time periods
- CONCEPT: abstract ideas, theories, methodologies, domain-specific terms
- TECHNOLOGY: tools, frameworks, protocols, hardware, sensors, programming languages, software libraries
- EVENT: named events, conferences, competitions, incidents
- PRODUCT: named products, services, platforms
- MODEL: named algorithms, AI/ML models, statistical models, computational methods
- DATASET: named datasets, benchmarks, corpora

Rules:
- ONLY extract entities explicitly mentioned in the text — never infer or assume
- If an entity is not clearly present in the text, do NOT include it
- Skip email addresses, URLs, domain names, and formatting artifacts
- Skip single letters, pure numbers, and abbreviations with no clear meaning
- context must describe how the entity appears IN THE TEXT — never write "not mentioned"
- If you cannot write a meaningful context from the text, skip the entity entirely

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
- ORGANIZATION: publishers, journals, conferences, institutions
- TECHNOLOGY: tools, frameworks, protocols, hardware, software
- MODEL: named algorithms, AI/ML models, statistical or computational methods
- DATASET: named datasets, benchmarks, corpora

Do NOT extract:
- Person names or author citations (e.g. "Lewis et al.")
- URLs, arXiv IDs, DOIs, or domain names
- Conference names or paper titles as DATASET
- Hardware or sensors as MODEL
- Programming languages or frameworks as MODEL
- Email addresses or usernames

Rules:
- ONLY extract entities explicitly mentioned in the reference text — never infer or assume
- context must describe how the entity appears in the text — never write "not mentioned"
- If you cannot write a meaningful context from the text, skip the entity entirely
- Avoid duplicates and formatting artifacts

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