RELATIONSHIP_EXTRACTION_PROMPT = """
You are an expert knowledge graph builder.

Given the following list of entities and the source text, identify meaningful relationships between entities.

Entities:
{entities}

Source Text:
{content}

Rules:
- Only create relationships between entities that are explicitly mentioned together in the text
- Use clear, meaningful relationship labels in UPPERCASE_SNAKE_CASE
- Each relationship must have a source AND target that exist in the entities list above
- Return a maximum of 8 relationships
- Do NOT create relationships based on examples, analogies, or hypothetical scenarios in the text
- Do NOT create relationships between fictional characters or illustrative examples used in the paper
- Only create relationships that describe real, factual connections between the actual entities of the document
- Do NOT create relationships based on proximity alone (entities mentioned in the same paragraph)
- Do NOT create relationships between email addresses, dates, or table/figure references

For each relationship return:
- source: exact name of the first entity (must match entity list)
- target: exact name of the second entity (must match entity list)
- relationship: UPPERCASE_SNAKE_CASE label (e.g. WORKS_FOR, PART_OF, USED_BY, RELATED_TO, DEVELOPED_BY)
- description: one-line description of the relationship

You MUST return ONLY a raw JSON object. No explanation, no markdown, no code blocks.
Start your response with {{ and end with }}

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