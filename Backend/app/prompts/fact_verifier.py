
FACT_VERIFICATION_PROMPT = """You are a fact verification expert.

Your job is to check whether a given claim is supported by the provided source text.

Claim:
{claim}

Source Text:
{source_text}

Instructions:
- A claim is SUPPORTED if the source text mentions the same MEANING or FACT,
  even if the exact wording is different.
- A claim is SUPPORTED if the source text contains numbers or statistics that
  confirm the claim, even partially.
- A claim is FLAGGED only if:
  (a) the source text explicitly contradicts the claim, OR
  (b) the source text has absolutely no relevant information about the claim topic.
- Do NOT flag a claim just because the exact wording is not in the source text.
- Do NOT flag a claim just because it is a short phrase — check if the concept is present.

Confidence rules — THIS IS IMPORTANT:
- "confidence" represents how certain you are in YOUR VERDICT, not how strongly the claim is supported.
- If supported=true  and confidence=0.9 → you are 90% sure the source backs this claim.
- If supported=false and confidence=0.9 → you are 90% sure this claim is absent from the source.
- NEVER return confidence=0.0 unless the source text is completely unreadable or empty.
- A flagged claim should still have a HIGH confidence (e.g. 0.8–0.95) if you are sure it is not in the source.

Respond ONLY with valid JSON, no preamble, no markdown fences:
{{
    "supported": true or false,
    "confidence": 0.0 to 1.0,
    "reason": "one-line explanation of why the claim is or is not supported"
}}
"""