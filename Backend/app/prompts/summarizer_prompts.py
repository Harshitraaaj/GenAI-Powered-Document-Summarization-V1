
SYSTEM_PROMPT = """
You are a factual document summarization assistant.
Return strictly valid JSON.
Do not include explanations or markdown.
"""

CHUNK_SUMMARY_PROMPT = """
Return EXACTLY one valid JSON object.

Rules:
- Be strictly grounded in the provided text.
- Do NOT invent facts or introduce external information.
- Only include risks that are EXPLICITLY stated or described in the text.
- Do NOT imply, infer, or generalize risks beyond what is written.
- Only include action_items that are EXPLICITLY recommended in the text.
- If no risks are explicitly stated in the text, return an empty list [].
- If no action items are explicitly stated, return an empty list [].
- If the text is only a list of references or citations, return empty lists for all fields.
- Keep the summary analytical and precise.

{{
  "summary": "<3-5 sentence factual summary>",
  "tldr": "<One-line TL;DR>",
  "key_points": ["<main insights from the text>"],
  "risks": ["<only risks EXPLICITLY mentioned in the text>"],
  "action_items": ["<only actions EXPLICITLY recommended in the text>"]
}}

TEXT:
\"\"\"{content}\"\"\"
"""

SECTION_SUMMARY_PROMPT = """
Return EXACTLY one valid JSON object synthesizing the chunk summaries below.

Rules:
- Only use information from the chunk summaries provided.
- Do NOT add new information not present in the chunks.
- Do NOT use placeholder text. Write real content in every field.
- Never write "unknown", "None mentioned", or any placeholder text.

CRITICAL — For the risks field:
- A risk is a DANGER, DOWNSIDE, LIMITATION, or NEGATIVE CONSEQUENCE explicitly mentioned.
- NOT a risk: achievements, results, capabilities, or methods described positively.
- Go through each chunk summary one by one and collect every item from each chunk's risks list.
- Include ALL of them without filtering.
- If after checking all chunks you find no risks, return "risks": []

CRITICAL — For the action_items field:
- An action_item MUST be a concrete recommendation or policy action.
- An action_item MUST start with a verb like: "Address", "Implement", "Develop",
  "Evaluate", "Investigate", "Policy makers should...", "Mitigate", "Conduct".
- NEVER copy outcomes or consequences into action_items.
- For example: "delayed graduation" is a RISK not an action_item.
- For example: "job loss" is a RISK not an action_item.
- For example: "lower GPA" is a RISK not an action_item.
- Go through each chunk summary one by one and collect every item from each chunk's
  action_items list. Include only real recommendations.
- If after checking all chunks you find no real action items, return "action_items": []

{{
  "summary": "<synthesized summary from the chunks>",
  "tldr": "<one line summary>",
  "key_points": ["<key points from the chunks>"],
  "risks": ["<every item from risks lists across all chunk summaries>"],
  "action_items": ["<only concrete recommendations from action_items lists — never outcomes>"]
}}

CHUNK SUMMARIES:
{content}
"""

EXECUTIVE_SUMMARY_PROMPT = """
Return EXACTLY one valid JSON object as the executive summary.

Rules:
- Provide a strategic synthesis structured as: (1) Problem context, (2) Proposed solution, (3) Strategic impact.
- Only use information from the section summaries provided below.
- Do NOT add new information not present in the sections.
- Do NOT use placeholder text. Write real content in every field.
- Never write "unknown", "None mentioned", or any placeholder text.
- Keep it analytical and strategic, not descriptive.

CRITICAL — For the risks field:
- A risk is a DANGER, DOWNSIDE, LIMITATION, or NEGATIVE CONSEQUENCE.
- NOT a risk: positive results, capabilities, or methods.
- Step 1: Find the risks list in section 1. Copy every item from it.
- Step 2: Find the risks list in section 2. Copy every item from it.
- Step 3: Find the risks list in section 3. Copy every item from it.
- Step 4: Combine all items from steps 1, 2, 3 into one risks list.
- Do NOT skip any section. Do NOT filter out any risk.
- Do NOT copy items from key_points or action_items into risks.

CRITICAL — For the action_items field:
- An action_item MUST be a concrete recommendation, policy action, or strategic next step.
- An action_item MUST start with a verb like: "Address", "Implement", "Develop",
  "Evaluate", "Investigate", "Policy makers should...", "Mitigate", "Conduct".
- NEVER copy outcomes, risks, or consequences into action_items.
- For example: "delayed graduation" is a RISK not an action_item.
- For example: "job loss" is a RISK not an action_item.
- Step 1: Find the action_items list in section 1. Copy every REAL recommendation.
- Step 2: Find the action_items list in section 2. Copy every REAL recommendation.
- Step 3: Find the action_items list in section 3. Copy every REAL recommendation.
- Step 4: Combine all items into one action_items list.
- Do NOT copy items from key_points or risks into action_items.
- If no real action items exist, return "action_items": []

{{
  "tldr": "<1-line strategic takeaway>",
  "summary": "<4-6 sentence synthesis covering problem, solution, and strategic impact>",
  "key_points": ["<strategic insights>"],
  "risks": ["<combined risks from ALL section summaries — every item>"],
  "action_items": ["<only concrete recommendations — never outcomes or risks>"]
}}

SECTION SUMMARIES:
{content}
"""