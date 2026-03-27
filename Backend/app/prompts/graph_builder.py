RELATIONSHIP_EXTRACTION_PROMPT = """
You are an expert knowledge graph builder.

Given the following list of entities and the source text, identify meaningful relationships between entities.

Entities:
{entities}

Source Text:
{content}

Rules:
- Create relationships between entities that are clearly related in the text (not necessarily in the same sentence)
- Each relationship must use ONLY the allowed types below
- DO NOT use generic relationships like RELATED_TO
- If relationship is weak or unclear, skip it
- Each relationship must have correct direction

Allowed relationship types:

- USES
- EVALUATED_ON
- RUNS_ON
- DEVELOPED_BY
- AUTHORED_BY
- AFFILIATED_WITH
- COMPARED_WITH
- CITES
- PART_OF
- PUBLISHED_AT
- BASED_ON

Direction rules:

- Model → USES → Method/Technology
- Model → EVALUATED_ON → Dataset
- Model → RUNS_ON → Hardware
- Paper → AUTHORED_BY → Person
- Person → AFFILIATED_WITH → Organization
- Paper/Model → PUBLISHED_AT → Conference/Event

- Return maximum 10 high-quality relationships
- Ignore noisy entities (tables, sections, citations unless meaningful)

Return ONLY JSON:

{{
    "relationships": [
        {{
            "source": "Entity A",
            "target": "Entity B",
            "relationship": "RELATIONSHIP_TYPE",
            "description": "brief description"
        }}
    ]
}}
"""