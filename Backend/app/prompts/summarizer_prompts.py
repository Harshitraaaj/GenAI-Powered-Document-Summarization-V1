# SYSTEM_PROMPT = """
# You are a factual document summarization assistant.
# Return strictly valid JSON.
# Do not include explanations or markdown.
# """

# CHUNK_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object.

# Rules:
# - Be strictly grounded in the provided text.
# - Do NOT invent facts or introduce external information.
# - Identify operational, technical, financial, security, scalability,
#   interoperability, or adoption challenges as risks if mentioned or implied.
# - If challenges or limitations are described, interpret them as risks.
# - For every meaningful risk identified, provide a realistic mitigation
#   strategy as an action_item.
# - If no risks are reasonably identifiable, return an empty list.
# - Keep the summary analytical and precise.

# {{
#   "summary": "<3-5 sentence factual summary>",
#   "tldr": "<One-line TL;DR>",
#   "key_points": ["<main insights>"],
#   "risks": ["<systemic or operational risks mentioned or implied>"],
#   "action_items": ["<clear mitigation aligned to each risk>"]
# }}

# TEXT:
# \"\"\"{content}\"\"\"
# """

# SECTION_SUMMARY_PROMPT = """
# Return EXACTLY one JSON object.

# {{
#   "summary": "<Section synthesis>",
#   "tldr": "<One-line summary>",
#   "key_points": ["<points>"],
#   "risks": ["<risks>"],
#   "action_items": ["<actions>"]
# }}

# CHUNK SUMMARIES:
# {content}
# """

# EXECUTIVE_SUMMARY_PROMPT = """
# Return EXACTLY one JSON object.

# Rules:
# - Provide a strategic, high-level synthesis of the entire document.
# - Structure the executive summary clearly as:
#   1) Problem context
#   2) Proposed solution / architecture
#   3) Strategic impact / long-term benefits
# - Stay strictly grounded in the section summaries.
# - Do NOT introduce new information.
# - Do NOT hallucinate.
# - Highlight the main challenge discussed.
# - Keep it analytical, not descriptive.

# {{
#   "tldr": "<1-line strategic takeaway>",
#   "summary": "<4-6 sentence synthesis: Problem + Solution + Impact>",
#   "key_points": ["<strategic insights>"],
#   "risks": ["<major systemic risks>"],
#   "action_items": ["<high-level recommendations>"]
# }}

# SECTION SUMMARIES:
# {content}
# """


"""v2"""

# SYSTEM_PROMPT = """
# You are a factual document summarization assistant.
# Return strictly valid JSON.
# Do not include explanations or markdown.
# """

# CHUNK_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object.

# Rules:
# - Be strictly grounded in the provided text.
# - Do NOT invent facts or introduce external information.
# - Only include risks that are EXPLICITLY stated or described in the text.
# - Do NOT imply, infer, or generalize risks beyond what is written.
# - Only include action_items that are EXPLICITLY recommended in the text.
# - If no risks are explicitly stated in the text, return an empty list [].
# - If no action items are explicitly stated, return an empty list [].
# - Keep the summary analytical and precise.

# {{
#   "summary": "<3-5 sentence factual summary>",
#   "tldr": "<One-line TL;DR>",
#   "key_points": ["<main insights from the text>"],
#   "risks": ["<only risks EXPLICITLY mentioned in the text>"],
#   "action_items": ["<only actions EXPLICITLY recommended in the text>"]
# }}

# TEXT:
# \"\"\"{content}\"\"\"
# """

# SECTION_SUMMARY_PROMPT = """
# Return EXACTLY one JSON object.

# Rules:
# - Only synthesize information present in the chunk summaries below.
# - Only carry forward risks that were EXPLICITLY in the chunks.
# - Do NOT add new risks or generalize beyond what the chunks state.
# - If no risks exist in the chunks, return empty list [].

# {{
#   "summary": "<Section synthesis>",
#   "tldr": "<One-line summary>",
#   "key_points": ["<points from the chunks>"],
#   "risks": ["<only risks carried from the chunks>"],
#   "action_items": ["<only actions carried from the chunks>"]
# }}

# CHUNK SUMMARIES:
# {content}
# """

# EXECUTIVE_SUMMARY_PROMPT = """
# Return EXACTLY one JSON object.

# Rules:
# - Provide a strategic, high-level synthesis of the entire document.
# - Structure the executive summary clearly as:
#   1) Problem context
#   2) Proposed solution / architecture
#   3) Strategic impact / long-term benefits
# - Stay strictly grounded in the section summaries provided.
# - Do NOT introduce new information not present in the section summaries.
# - Do NOT hallucinate or infer risks beyond what is stated.
# - Only include risks that appear in the section summaries below.
# - Keep it analytical, not descriptive.

# {{
#   "tldr": "<1-line strategic takeaway>",
#   "summary": "<4-6 sentence synthesis: Problem + Solution + Impact>",
#   "key_points": ["<strategic insights from sections>"],
#   "risks": ["<only risks present in the section summaries>"],
#   "action_items": ["<only recommendations present in the section summaries>"]
# }}

# SECTION SUMMARIES:
# {content}
# """

"""v3 correct slightly worng"""
# # Optimized for Meta Llama 3.2 11B on Amazon Bedrock

# SYSTEM_PROMPT = """
# You are a factual document summarization assistant.
# Return strictly valid JSON.
# Do not include explanations or markdown.
# """

# CHUNK_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object.

# Rules:
# - Be strictly grounded in the provided text.
# - Do NOT invent facts or introduce external information.
# - Only include risks that are EXPLICITLY stated or described in the text.
# - Do NOT imply, infer, or generalize risks beyond what is written.
# - Only include action_items that are EXPLICITLY recommended in the text.
# - If no risks are explicitly stated in the text, return an empty list [].
# - If no action items are explicitly stated, return an empty list [].
# - Keep the summary analytical and precise.

# {{
#   "summary": "<3-5 sentence factual summary>",
#   "tldr": "<One-line TL;DR>",
#   "key_points": ["<main insights from the text>"],
#   "risks": ["<only risks EXPLICITLY mentioned in the text>"],
#   "action_items": ["<only actions EXPLICITLY recommended in the text>"]
# }}

# TEXT:
# \"\"\"{content}\"\"\"
# """

# SECTION_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object synthesizing the chunk summaries below.

# Rules:
# - Only use information from the chunk summaries provided.
# - Do NOT add new information not present in the chunks.
# - Do NOT use placeholder text. Write real content in every field.
# - For the risks field: go through each chunk summary one by one and collect every risk string you find. Include all of them.
# - For the action_items field: go through each chunk summary one by one and collect every action item string you find. Include all of them.
# - If after checking all chunks you find no risks, return "risks": []
# - If after checking all chunks you find no action items, return "action_items": []
# - Never write "unknown", "None mentioned", or any placeholder text.

# {{
#   "summary": "<synthesized summary from the chunks>",
#   "tldr": "<one line summary>",
#   "key_points": ["<key points from the chunks>"],
#   "risks": ["<every risk found across all chunk summaries>"],
#   "action_items": ["<every action item found across all chunk summaries>"]
# }}

# CHUNK SUMMARIES:
# {content}
# """

# EXECUTIVE_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object as the executive summary.

# Rules:
# - Provide a strategic synthesis structured as: (1) Problem context, (2) Proposed solution, (3) Strategic impact.
# - Only use information from the section summaries provided below.
# - Do NOT add new information not present in the sections.
# - Do NOT use placeholder text. Write real content in every field.
# - For the risks field: go through each section summary one by one and collect every risk string you find. Include all of them.
# - For the action_items field: go through each section summary one by one and collect every action item string you find. Include all of them.
# - If after checking all sections you find no risks, return "risks": []
# - Never write "unknown", "None mentioned", or any placeholder text.
# - Keep it analytical and strategic, not descriptive.

# {{
#   "tldr": "<1-line strategic takeaway>",
#   "summary": "<4-6 sentence synthesis covering problem, solution, and strategic impact>",
#   "key_points": ["<strategic insights>"],
#   "risks": ["<every risk found across all section summaries>"],
#   "action_items": ["<every action item found across all section summaries>"]
# }}

# SECTION SUMMARIES:
# {content}
# """

"""v4 close to perfect"""
# Optimized for Meta Llama 3.2 11B on Amazon Bedrock

# SYSTEM_PROMPT = """
# You are a factual document summarization assistant.
# Return strictly valid JSON.
# Do not include explanations or markdown.
# """

# CHUNK_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object.

# Rules:
# - Be strictly grounded in the provided text.
# - Do NOT invent facts or introduce external information.
# - Only include risks that are EXPLICITLY stated or described in the text.
# - Do NOT imply, infer, or generalize risks beyond what is written.
# - Only include action_items that are EXPLICITLY recommended in the text.
# - If no risks are explicitly stated in the text, return an empty list [].
# - If no action items are explicitly stated, return an empty list [].
# - If the text is only a list of references or citations, return empty lists for all fields.
# - Keep the summary analytical and precise.

# {{
#   "summary": "<3-5 sentence factual summary>",
#   "tldr": "<One-line TL;DR>",
#   "key_points": ["<main insights from the text>"],
#   "risks": ["<only risks EXPLICITLY mentioned in the text>"],
#   "action_items": ["<only actions EXPLICITLY recommended in the text>"]
# }}

# TEXT:
# \"\"\"{content}\"\"\"
# """

# SECTION_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object synthesizing the chunk summaries below.

# Rules:
# - Only use information from the chunk summaries provided.
# - Do NOT add new information not present in the chunks.
# - Do NOT use placeholder text. Write real content in every field.
# - For the risks field: go through each chunk summary one by one and collect every risk string you find. Include all of them.
# - For the action_items field: go through each chunk summary one by one and collect every action item string you find. Include all of them.
# - If after checking all chunks you find no risks, return "risks": []
# - If after checking all chunks you find no action items, return "action_items": []
# - Never write "unknown", "None mentioned", or any placeholder text.

# {{
#   "summary": "<synthesized summary from the chunks>",
#   "tldr": "<one line summary>",
#   "key_points": ["<key points from the chunks>"],
#   "risks": ["<every risk found across all chunk summaries>"],
#   "action_items": ["<every action item found across all chunk summaries>"]
# }}

# CHUNK SUMMARIES:
# {content}
# """

# EXECUTIVE_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object as the executive summary.

# Rules:
# - Provide a strategic synthesis structured as: (1) Problem context, (2) Proposed solution, (3) Strategic impact.
# - Only use information from the section summaries provided below.
# - Do NOT add new information not present in the sections.
# - Do NOT use placeholder text. Write real content in every field.
# - Never write "unknown", "None mentioned", or any placeholder text.
# - Keep it analytical and strategic, not descriptive.

# IMPORTANT — For the risks field:
# - A risk is a DANGER, DOWNSIDE, LIMITATION, or NEGATIVE CONSEQUENCE.
# - Examples of risks: "use of biased knowledge sources", "automation of jobs", "retrieval collapse"
# - NOT a risk: achievements, results, methods, or action items.
# - Go through each section summary risks list one by one and copy every item into the risks field.
# - Do NOT copy items from key_points or action_items into risks.

# IMPORTANT — For the action_items field:
# - Go through each section summary action_items list one by one and copy every item.
# - Do NOT copy items from key_points or risks into action_items.

# {{
#   "tldr": "<1-line strategic takeaway>",
#   "summary": "<4-6 sentence synthesis covering problem, solution, and strategic impact>",
#   "key_points": ["<strategic insights>"],
#   "risks": ["<copy ONLY from risks lists in section summaries — dangers and downsides only>"],
#   "action_items": ["<copy ONLY from action_items lists in section summaries>"]
# }}

# SECTION SUMMARIES:
# {content}
# """

"""V5 working"""
# Optimized for Meta Llama 3.2 11B on Amazon Bedrock

# SYSTEM_PROMPT = """
# You are a factual document summarization assistant.
# Return strictly valid JSON.
# Do not include explanations or markdown.
# """

# CHUNK_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object.

# Rules:
# - Be strictly grounded in the provided text.
# - Do NOT invent facts or introduce external information.
# - Only include risks that are EXPLICITLY stated or described in the text.
# - Do NOT imply, infer, or generalize risks beyond what is written.
# - Only include action_items that are EXPLICITLY recommended in the text.
# - If no risks are explicitly stated in the text, return an empty list [].
# - If no action items are explicitly stated, return an empty list [].
# - If the text is only a list of references or citations, return empty lists for all fields.
# - Keep the summary analytical and precise.

# {{
#   "summary": "<3-5 sentence factual summary>",
#   "tldr": "<One-line TL;DR>",
#   "key_points": ["<main insights from the text>"],
#   "risks": ["<only risks EXPLICITLY mentioned in the text>"],
#   "action_items": ["<only actions EXPLICITLY recommended in the text>"]
# }}

# TEXT:
# \"\"\"{content}\"\"\"
# """

# SECTION_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object synthesizing the chunk summaries below.

# Rules:
# - Only use information from the chunk summaries provided.
# - Do NOT add new information not present in the chunks.
# - Do NOT use placeholder text. Write real content in every field.
# - A risk is a DANGER, DOWNSIDE, LIMITATION, or NEGATIVE CONSEQUENCE explicitly mentioned.
# - NOT a risk: achievements, results, capabilities, or methods described positively.
# - For the risks field: go through each chunk summary one by one and collect every item from each chunk's risks list. Include all of them.
# - For the action_items field: go through each chunk summary one by one and collect every item from each chunk's action_items list. Include all of them.
# - If after checking all chunks you find no risks, return "risks": []
# - If after checking all chunks you find no action items, return "action_items": []
# - Never write "unknown", "None mentioned", or any placeholder text.

# {{
#   "summary": "<synthesized summary from the chunks>",
#   "tldr": "<one line summary>",
#   "key_points": ["<key points from the chunks>"],
#   "risks": ["<every item from risks lists across all chunk summaries>"],
#   "action_items": ["<every item from action_items lists across all chunk summaries>"]
# }}

# CHUNK SUMMARIES:
# {content}
# """

# EXECUTIVE_SUMMARY_PROMPT = """
# Return EXACTLY one valid JSON object as the executive summary.

# Rules:
# - Provide a strategic synthesis structured as: (1) Problem context, (2) Proposed solution, (3) Strategic impact.
# - Only use information from the section summaries provided below.
# - Do NOT add new information not present in the sections.
# - Do NOT use placeholder text. Write real content in every field.
# - Never write "unknown", "None mentioned", or any placeholder text.
# - Keep it analytical and strategic, not descriptive.

# CRITICAL — For the risks field:
# - A risk is a DANGER, DOWNSIDE, LIMITATION, or NEGATIVE CONSEQUENCE.
# - Examples of risks: "use of biased knowledge sources", "automation of jobs", "retrieval collapse", "generation of misleading content"
# - NOT a risk: positive results, capabilities, or methods.
# - Step 1: Find the risks list in section 1. Copy every item from it.
# - Step 2: Find the risks list in section 2. Copy every item from it.
# - Step 3: Find the risks list in section 3. Copy every item from it.
# - Step 4: Combine all items from steps 1, 2, 3 into one risks list.
# - Do NOT skip any section. Do NOT filter out any risk.
# - Do NOT copy items from key_points or action_items into risks.

# CRITICAL — For the action_items field:
# - Step 1: Find the action_items list in section 1. Copy every item.
# - Step 2: Find the action_items list in section 2. Copy every item.
# - Step 3: Find the action_items list in section 3. Copy every item.
# - Step 4: Combine all items from steps 1, 2, 3 into one action_items list.
# - Do NOT copy items from key_points or risks into action_items.

# {{
#   "tldr": "<1-line strategic takeaway>",
#   "summary": "<4-6 sentence synthesis covering problem, solution, and strategic impact>",
#   "key_points": ["<strategic insights>"],
#   "risks": ["<combined risks from ALL section summaries — every item>"],
#   "action_items": ["<combined action_items from ALL section summaries — every item>"]
# }}

# SECTION SUMMARIES:
# {content}
# """

"""v6 cahcign bug fix"""
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