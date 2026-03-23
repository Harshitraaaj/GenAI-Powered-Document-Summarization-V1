# import json
# import time
# import boto3
# import logging
# from typing import Dict, Any, List
# from botocore.exceptions import ClientError
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from app.prompts.summarizer_prompts import (
#     SYSTEM_PROMPT,
#     CHUNK_SUMMARY_PROMPT,
#     SECTION_SUMMARY_PROMPT,
#     EXECUTIVE_SUMMARY_PROMPT,
# )

# from app.core.config import (
#     AWS_REGION,
#     BEDROCK_MODEL_ID,
#     MODEL_MAX_TOKENS,
#     EXECUTIVE_MAX_TOKENS,
#     MODEL_TEMPERATURE,
#     MODEL_TOP_P,
#     MODEL_RETRIES,
#     RETRY_BASE_DELAY,
#     MAX_WORKERS,
#     SECTION_GROUP_SIZE,
#     LOW_COVERAGE_THRESHOLD,
# )

# logger = logging.getLogger(__name__)


# bedrock_client = boto3.client(service_name="bedrock-runtime", region_name=AWS_REGION)


# # utilities
# def clean_model_output(text: str) -> str:
#     text = text.strip()

#     if text.startswith("```"):
#         text = text.replace("```json", "").replace("```", "").strip()

#     return text


# def safe_json_parse(text: str) -> Dict[str, Any]:

#     if not text:
#         logger.warning("Empty model output received")
#         return {"error": "Empty model output"}

#     try:
#         return json.loads(text)

#     except json.JSONDecodeError:
#         logger.error("Invalid JSON returned by model")
#         return {
#             "error": "Invalid JSON returned by model",
#             "raw_output": text,
#         }


# def normalize_summary_fields(parsed: Dict[str, Any]) -> Dict[str, Any]:

#     required_fields = {
#         "summary": "",
#         "tldr": "",
#         "key_points": [],
#         "risks": [],
#         "action_items": [],
#     }

#     for field, default in required_fields.items():
#         parsed.setdefault(field, default)

#     return parsed


# # Model Invocation
# def invoke_model(
#     prompt: str,
#     max_tokens: int = MODEL_MAX_TOKENS,
#     temperature: float = MODEL_TEMPERATURE,
#     retries: int = MODEL_RETRIES,
# ) -> str:

#     last_exception = None

#     logger.info("Invoking Bedrock model")

#     formatted_prompt = f"""
# <|begin_of_text|>
# <|start_header_id|>system<|end_header_id|>
# {SYSTEM_PROMPT}
# <|eot_id|>
# <|start_header_id|>user<|end_header_id|>
# {prompt}
# <|eot_id|>
# <|start_header_id|>assistant<|end_header_id|>
# """

#     body = {
#         "prompt": formatted_prompt,
#         "max_gen_len": max_tokens,
#         "temperature": temperature,
#         "top_p": MODEL_TOP_P,
#     }

#     start_time = time.time()

#     for attempt in range(retries + 1):
#         try:
#             response = bedrock_client.invoke_model(
#                 modelId=BEDROCK_MODEL_ID,
#                 body=json.dumps(body),
#                 contentType="application/json",
#                 accept="application/json",
#             )

#             latency = round(time.time() - start_time, 2)
#             logger.info(f"Bedrock response received | latency={latency}s")

#             response_body = json.loads(response["body"].read())
#             content = response_body.get("generation", "")

#             if not content:
#                 raise RuntimeError("Empty model response")

#             return clean_model_output(content)

#         except ClientError as e:
#             last_exception = e
#             logger.warning(f"Bedrock ClientError: {e}")

#             error_code = e.response["Error"]["Code"]

#             if error_code in (
#                 "ThrottlingException",
#                 "TooManyRequestsException",
#             ):
#                 time.sleep(RETRY_BASE_DELAY**attempt)
#                 continue

#             time.sleep(RETRY_BASE_DELAY)

#         except Exception as e:
#             last_exception = e
#             logger.exception("Model invocation failed")
#             time.sleep(RETRY_BASE_DELAY)

#     logger.error("Bedrock invocation failed after retries")
#     raise RuntimeError(f"Bedrock invocation failed: {last_exception}")


# # Chunk Level
# def summarize_chunk(chunk: Dict) -> Dict:

#     logger.info(f"Summarizing chunk {chunk['chunk_id']}")

#     prompt = CHUNK_SUMMARY_PROMPT.format(content=chunk["content"])

#     output = invoke_model(prompt)

#     parsed = normalize_summary_fields(safe_json_parse(output))

#     parsed["chunk_id"] = chunk["chunk_id"]

#     return parsed


# # Section Level
# def summarize_section(
#     section_id: int,
#     chunk_summaries: List[Dict],
# ) -> Dict:

#     logger.info(f"Generating section summary {section_id}")

#     combined_text = ""
#     source_chunks = []

#     for c in chunk_summaries:
#         if c.get("summary"):
#             combined_text += f"\nChunk {c['chunk_id']}:\n{c['summary']}\n"
#             source_chunks.append(c["chunk_id"])

#     if not combined_text.strip():
#         logger.warning(f"Section {section_id} empty")

#         return {
#             "section_id": section_id,
#             "summary": "",
#             "tldr": "",
#             "key_points": [],
#             "risks": [],
#             "action_items": [],
#             "source_chunks": [],
#         }

#     prompt = SECTION_SUMMARY_PROMPT.format(content=combined_text)

#     output = invoke_model(prompt)

#     parsed = normalize_summary_fields(safe_json_parse(output))

#     parsed["section_id"] = section_id
#     parsed["source_chunks"] = source_chunks

#     return parsed


# # Executive Summary
# def summarize_executive(
#     section_summaries: List[Dict],
# ) -> Dict:

#     logger.info("Generating executive summary")

#     combined_text = ""
#     source_sections = []

#     for s in section_summaries:
#         if s.get("summary"):
#             combined_text += f"\nSection {s['section_id']}:\n{s['summary']}\n"
#             source_sections.append(s["section_id"])

#     if not combined_text.strip():
#         logger.warning("Executive summary skipped")

#         return normalize_summary_fields({"source_sections": []})

#     prompt = EXECUTIVE_SUMMARY_PROMPT.format(content=combined_text)

#     output = invoke_model(
#         prompt,
#         max_tokens=EXECUTIVE_MAX_TOKENS,
#     )

#     parsed = normalize_summary_fields(safe_json_parse(output))

#     parsed["source_sections"] = source_sections

#     return parsed


# # Parallel Chunk Processing
# def parallel_chunk_summarization(
#     chunks: List[Dict],
# ) -> List[Dict]:

#     logger.info(f"Parallel chunk summarization | workers={MAX_WORKERS}")

#     results = []

#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(summarize_chunk, chunk): chunk["chunk_id"]
#             for chunk in chunks
#         }

#         for future in as_completed(futures):
#             results.append(future.result())

#     results.sort(key=lambda x: x["chunk_id"])

#     return results


# # Section Grouping
# def group_into_sections(
#     chunk_summaries: List[Dict],
#     group_size: int = SECTION_GROUP_SIZE,
# ):

#     return [
#         chunk_summaries[i : i + group_size]
#         for i in range(
#             0,
#             len(chunk_summaries),
#             group_size,
#         )
#     ]


# # Hierarchical Pipeline
# def run_hierarchical_summarization(
#     chunks: List[Dict],
# ) -> Dict:

#     logger.info("Hierarchical summarization started")

#     if not chunks:
#         raise ValueError("No chunks provided")

#     total_chunks = len(chunks)

#     chunk_summaries = parallel_chunk_summarization(chunks)
#     valid_chunks = [c for c in chunk_summaries if c.get("summary")]

#     grouped = group_into_sections(valid_chunks)

#     section_summaries = [summarize_section(i + 1, sec) for i, sec in enumerate(grouped)]

#     valid_sections = [s for s in section_summaries if s.get("summary")]

#     executive_summary = summarize_executive(valid_sections)

#     covered_chunk_ids = list(
#         {cid for s in section_summaries for cid in s.get("source_chunks", [])}
#     )

#     synthesis_coverage_percent = round(
#         (len(covered_chunk_ids) / total_chunks) * 100,
#         2,
#     )

#     missing_sections = len(grouped) - len(valid_sections)

#     status = "ok"

#     if synthesis_coverage_percent < LOW_COVERAGE_THRESHOLD:
#         status = "low_coverage_warning"

#     if missing_sections > 0:
#         status = "section_missing_warning"

#     metadata = {
#         "total_chunks": total_chunks,
#         "valid_chunks": len(valid_chunks),
#         "coverage_percent": synthesis_coverage_percent,
#         "missing_sections": missing_sections,
#         "status": status,
#         "coverage_details": {
#             "covered_chunk_ids": covered_chunk_ids,
#             "missing_chunk_ids": list(
#                 set(c["chunk_id"] for c in chunks) - set(covered_chunk_ids)
#             ),
#         },
#     }

#     return {
#         "metadata": metadata,
#         "chunk_summaries": chunk_summaries,
#         "section_summaries": section_summaries,
#         "executive_summary": executive_summary,
#     }

# """v1 Antropic"""
# import json
# import time
# import boto3
# import logging
# from typing import Dict, Any, List
# from botocore.exceptions import ClientError
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from app.prompts.summarizer_prompts import (
#     SYSTEM_PROMPT,
#     CHUNK_SUMMARY_PROMPT,
#     SECTION_SUMMARY_PROMPT,
#     EXECUTIVE_SUMMARY_PROMPT,
# )

# from app.core.config import (
#     AWS_REGION,
#     BEDROCK_MODEL_ID,
#     MODEL_MAX_TOKENS,
#     EXECUTIVE_MAX_TOKENS,
#     MODEL_TEMPERATURE,
#     MODEL_TOP_P,
#     MODEL_RETRIES,
#     RETRY_BASE_DELAY,
#     MAX_WORKERS,
#     SECTION_GROUP_SIZE,
#     LOW_COVERAGE_THRESHOLD,
# )

# logger = logging.getLogger(__name__)


# bedrock_client = boto3.client(service_name="bedrock-runtime", region_name=AWS_REGION)


# # utilities
# def clean_model_output(text: str) -> str:
#     text = text.strip()

#     if text.startswith("```"):
#         text = text.replace("```json", "").replace("```", "").strip()

#     return text


# def safe_json_parse(text: str) -> Dict[str, Any]:

#     if not text:
#         logger.warning("Empty model output received")
#         return {"error": "Empty model output"}

#     try:
#         return json.loads(text)

#     except json.JSONDecodeError:
#         logger.error("Invalid JSON returned by model")
#         return {
#             "error": "Invalid JSON returned by model",
#             "raw_output": text,
#         }


# def normalize_summary_fields(parsed: Dict[str, Any]) -> Dict[str, Any]:

#     required_fields = {
#         "summary": "",
#         "tldr": "",
#         "key_points": [],
#         "risks": [],
#         "action_items": [],
#     }

#     for field, default in required_fields.items():
#         parsed.setdefault(field, default)

#     return parsed


# # Model Invocation
# def invoke_model(
#     prompt: str,
#     max_tokens: int = MODEL_MAX_TOKENS,
#     temperature: float = MODEL_TEMPERATURE,
#     retries: int = MODEL_RETRIES,
# ) -> str:

#     last_exception = None

#     logger.info("Invoking Bedrock model")

#     formatted_prompt = f"""
# <|begin_of_text|>
# <|start_header_id|>system<|end_header_id|>
# {SYSTEM_PROMPT}
# <|eot_id|>
# <|start_header_id|>user<|end_header_id|>
# {prompt}
# <|eot_id|>
# <|start_header_id|>assistant<|end_header_id|>
# """

#     body = {
#         "prompt": formatted_prompt,
#         "max_gen_len": max_tokens,
#         "temperature": temperature,
#         "top_p": MODEL_TOP_P,
#     }

#     start_time = time.time()

#     for attempt in range(retries + 1):
#         try:
#             response = bedrock_client.invoke_model(
#                 modelId=BEDROCK_MODEL_ID,
#                 body=json.dumps(body),
#                 contentType="application/json",
#                 accept="application/json",
#             )

#             latency = round(time.time() - start_time, 2)
#             logger.info(f"Bedrock response received | latency={latency}s")

#             response_body = json.loads(response["body"].read())
#             content = response_body.get("generation", "")

#             if not content:
#                 raise RuntimeError("Empty model response")

#             return clean_model_output(content)

#         except ClientError as e:
#             last_exception = e
#             logger.warning(f"Bedrock ClientError: {e}")

#             error_code = e.response["Error"]["Code"]

#             if error_code in (
#                 "ThrottlingException",
#                 "TooManyRequestsException",
#             ):
#                 time.sleep(RETRY_BASE_DELAY**attempt)
#                 continue

#             time.sleep(RETRY_BASE_DELAY)

#         except Exception as e:
#             last_exception = e
#             logger.exception("Model invocation failed")
#             time.sleep(RETRY_BASE_DELAY)

#     logger.error("Bedrock invocation failed after retries")
#     raise RuntimeError(f"Bedrock invocation failed: {last_exception}")


# # Chunk Level
# def summarize_chunk(chunk: Dict) -> Dict:

#     logger.info(f"Summarizing chunk {chunk['chunk_id']}")

#     prompt = CHUNK_SUMMARY_PROMPT.format(content=chunk["content"])

#     output = invoke_model(prompt)

#     parsed = normalize_summary_fields(safe_json_parse(output))

#     parsed["chunk_id"] = chunk["chunk_id"]

#     return parsed


# # Section Level
# def summarize_section(
#     section_id: int,
#     chunk_summaries: List[Dict],
# ) -> Dict:

#     logger.info(f"Generating section summary {section_id}")

#     combined_text = ""
#     source_chunks = []

#     for c in chunk_summaries:
#         if c.get("summary"):
#             combined_text += f"\nChunk {c['chunk_id']}:\n{c['summary']}\n"
#             source_chunks.append(c["chunk_id"])

#     if not combined_text.strip():
#         logger.warning(f"Section {section_id} empty")

#         return {
#             "section_id": section_id,
#             "summary": "",
#             "tldr": "",
#             "key_points": [],
#             "risks": [],
#             "action_items": [],
#             "source_chunks": [],
#         }

#     prompt = SECTION_SUMMARY_PROMPT.format(content=combined_text)

#     # Use higher token limit for sections — prevents truncation on data-heavy chunks
#     output = invoke_model(prompt, max_tokens=900)

#     parsed = normalize_summary_fields(safe_json_parse(output))

#     # FIX 1 — retry if section summary came back empty
#     if not parsed.get("summary") and combined_text.strip():
#         logger.warning(f"Section {section_id} returned empty summary — retrying")
#         output = invoke_model(prompt, max_tokens=900)
#         parsed = normalize_summary_fields(safe_json_parse(output))

#         if not parsed.get("summary"):
#             logger.error(f"Section {section_id} retry also failed — using fallback")
#             # Fallback — build minimal summary from chunk tldr fields
#             fallback_summary = " ".join(
#                 c.get("tldr", "") for c in chunk_summaries if c.get("tldr")
#             )
#             parsed["summary"] = fallback_summary[:500] if fallback_summary else ""

#     parsed["section_id"] = section_id
#     parsed["source_chunks"] = source_chunks

#     return parsed


# # Executive Summary
# def summarize_executive(
#     section_summaries: List[Dict],
# ) -> Dict:

#     logger.info("Generating executive summary")

#     combined_text = ""
#     source_sections = []

#     for s in section_summaries:
#         if s.get("summary"):
#             combined_text += f"\nSection {s['section_id']}:\n{s['summary']}\n"

#             # FIX 2 — use bullet list format for risks/action_items
#             # Prevents Llama from splitting items on commas
#             if s.get("risks"):
#                 risks_text = "\n".join(f"- {r}" for r in s["risks"])
#                 combined_text += f"Risks:\n{risks_text}\n"

#             if s.get("action_items"):
#                 actions_text = "\n".join(f"- {a}" for a in s["action_items"])
#                 combined_text += f"Action items:\n{actions_text}\n"

#             source_sections.append(s["section_id"])

#     if not combined_text.strip():
#         logger.warning("Executive summary skipped")

#         return normalize_summary_fields({"source_sections": []})

#     prompt = EXECUTIVE_SUMMARY_PROMPT.format(content=combined_text)

#     output = invoke_model(
#         prompt,
#         max_tokens=EXECUTIVE_MAX_TOKENS,
#     )

#     parsed = normalize_summary_fields(safe_json_parse(output))

#     parsed["source_sections"] = source_sections

#     return parsed


# # Parallel Chunk Processing
# def parallel_chunk_summarization(
#     chunks: List[Dict],
# ) -> List[Dict]:

#     logger.info(f"Parallel chunk summarization | workers={MAX_WORKERS}")

#     results = []

#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(summarize_chunk, chunk): chunk["chunk_id"]
#             for chunk in chunks
#         }

#         for future in as_completed(futures):
#             results.append(future.result())

#     results.sort(key=lambda x: x["chunk_id"])

#     return results


# # Section Grouping
# def group_into_sections(
#     chunk_summaries: List[Dict],
#     group_size: int = SECTION_GROUP_SIZE,
# ):

#     return [
#         chunk_summaries[i : i + group_size]
#         for i in range(
#             0,
#             len(chunk_summaries),
#             group_size,
#         )
#     ]


# # Hierarchical Pipeline
# def run_hierarchical_summarization(
#     chunks: List[Dict],
# ) -> Dict:

#     logger.info("Hierarchical summarization started")

#     if not chunks:
#         raise ValueError("No chunks provided")

#     total_chunks = len(chunks)

#     chunk_summaries = parallel_chunk_summarization(chunks)
#     valid_chunks = [c for c in chunk_summaries if c.get("summary")]

#     grouped = group_into_sections(valid_chunks)

#     # FIX 3 — always append all sections (including empty ones after retry)
#     # so missing_sections count is accurate
#     section_summaries = []
#     for i, sec in enumerate(grouped):
#         result = summarize_section(i + 1, sec)
#         section_summaries.append(result)

#     valid_sections = [s for s in section_summaries if s.get("summary")]

#     executive_summary = summarize_executive(valid_sections)

#     covered_chunk_ids = list(
#         {cid for s in section_summaries for cid in s.get("source_chunks", [])}
#     )

#     synthesis_coverage_percent = round(
#         (len(covered_chunk_ids) / total_chunks) * 100,
#         2,
#     )

#     missing_sections = len(grouped) - len(valid_sections)

#     status = "ok"

#     if synthesis_coverage_percent < LOW_COVERAGE_THRESHOLD:
#         status = "low_coverage_warning"

#     if missing_sections > 0:
#         status = "section_missing_warning"

#     metadata = {
#         "total_chunks": total_chunks,
#         "valid_chunks": len(valid_chunks),
#         "coverage_percent": synthesis_coverage_percent,
#         "missing_sections": missing_sections,
#         "status": status,
#         "coverage_details": {
#             "covered_chunk_ids": covered_chunk_ids,
#             "missing_chunk_ids": list(
#                 set(c["chunk_id"] for c in chunks) - set(covered_chunk_ids)
#             ),
#         },
#     }

#     return {
#         "metadata": metadata,
#         "chunk_summaries": chunk_summaries,
#         "section_summaries": section_summaries,
#         "executive_summary": executive_summary,
#     }

"""speed"""
"""v2 — parallel section summaries"""
# import json
# import time
# import boto3
# import logging
# from typing import Dict, Any, List
# from botocore.exceptions import ClientError
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from app.prompts.summarizer_prompts import (
#     SYSTEM_PROMPT,
#     CHUNK_SUMMARY_PROMPT,
#     SECTION_SUMMARY_PROMPT,
#     EXECUTIVE_SUMMARY_PROMPT,
# )

# from app.core.config import (
#     AWS_REGION,
#     BEDROCK_MODEL_ID,
#     MODEL_MAX_TOKENS,
#     EXECUTIVE_MAX_TOKENS,
#     MODEL_TEMPERATURE,
#     MODEL_TOP_P,
#     MODEL_RETRIES,
#     RETRY_BASE_DELAY,
#     MAX_WORKERS,
#     SECTION_GROUP_SIZE,
#     LOW_COVERAGE_THRESHOLD,
# )

# logger = logging.getLogger(__name__)

# bedrock_client = boto3.client(service_name="bedrock-runtime", region_name=AWS_REGION,)


# def clean_model_output(text: str) -> str:
#     text = text.strip()
#     if text.startswith("```"):
#         text = text.replace("```json", "").replace("```", "").strip()
#     return text


# def safe_json_parse(text: str) -> Dict[str, Any]:
#     if not text:
#         logger.warning("Empty model output received")
#         return {"error": "Empty model output"}
#     try:
#         return json.loads(text)
#     except json.JSONDecodeError:
#         logger.error("Invalid JSON returned by model")
#         return {
#             "error": "Invalid JSON returned by model",
#             "raw_output": text,
#         }


# def normalize_summary_fields(parsed: Dict[str, Any]) -> Dict[str, Any]:
#     required_fields = {
#         "summary": "",
#         "tldr": "",
#         "key_points": [],
#         "risks": [],
#         "action_items": [],
#     }
#     for field, default in required_fields.items():
#         parsed.setdefault(field, default)
#     return parsed


# def invoke_model(
#     prompt: str,
#     max_tokens: int = MODEL_MAX_TOKENS,
#     temperature: float = MODEL_TEMPERATURE,
#     retries: int = MODEL_RETRIES,
# ) -> str:
#     last_exception = None
#     logger.info("Invoking Bedrock model")

#     formatted_prompt = f"""
# <|begin_of_text|>
# <|start_header_id|>system<|end_header_id|>
# {SYSTEM_PROMPT}
# <|eot_id|>
# <|start_header_id|>user<|end_header_id|>
# {prompt}
# <|eot_id|>
# <|start_header_id|>assistant<|end_header_id|>
# """

#     body = {
#         "prompt": formatted_prompt,
#         "max_gen_len": max_tokens,
#         "temperature": temperature,
#         "top_p": MODEL_TOP_P,
#     }

#     start_time = time.time()

#     for attempt in range(retries + 1):
#         try:
#             response = bedrock_client.invoke_model(
#                 modelId=BEDROCK_MODEL_ID,
#                 body=json.dumps(body),
#                 contentType="application/json",
#                 accept="application/json",
#             )

#             latency = round(time.time() - start_time, 2)
#             logger.info(f"Bedrock response received | latency={latency}s")

#             response_body = json.loads(response["body"].read())
#             content = response_body.get("generation", "")

#             if not content:
#                 raise RuntimeError("Empty model response")

#             return clean_model_output(content)

#         except ClientError as e:
#             last_exception = e
#             logger.warning(f"Bedrock ClientError: {e}")
#             error_code = e.response["Error"]["Code"]
#             if error_code in ("ThrottlingException", "TooManyRequestsException"):
#                 time.sleep(RETRY_BASE_DELAY ** attempt)
#                 continue
#             time.sleep(RETRY_BASE_DELAY)

#         except Exception as e:
#             last_exception = e
#             logger.exception("Model invocation failed")
#             time.sleep(RETRY_BASE_DELAY)

#     logger.error("Bedrock invocation failed after retries")
#     raise RuntimeError(f"Bedrock invocation failed: {last_exception}")


# def summarize_chunk(chunk: Dict) -> Dict:
#     logger.info(f"Summarizing chunk {chunk['chunk_id']}")
#     prompt = CHUNK_SUMMARY_PROMPT.format(content=chunk["content"])
#     output = invoke_model(prompt)
#     parsed = normalize_summary_fields(safe_json_parse(output))
#     parsed["chunk_id"] = chunk["chunk_id"]
#     return parsed


# def summarize_section(section_id: int, chunk_summaries: List[Dict]) -> Dict:
#     logger.info(f"Generating section summary {section_id}")

#     combined_text = ""
#     source_chunks = []

#     for c in chunk_summaries:
#         if c.get("summary"):
#             combined_text += f"\nChunk {c['chunk_id']}:\n{c['summary']}\n"
#             source_chunks.append(c["chunk_id"])

#     if not combined_text.strip():
#         logger.warning(f"Section {section_id} empty")
#         return {
#             "section_id": section_id,
#             "summary": "",
#             "tldr": "",
#             "key_points": [],
#             "risks": [],
#             "action_items": [],
#             "source_chunks": [],
#         }

#     prompt = SECTION_SUMMARY_PROMPT.format(content=combined_text)
#     output = invoke_model(prompt, max_tokens=900)
#     parsed = normalize_summary_fields(safe_json_parse(output))

#     if not parsed.get("summary") and combined_text.strip():
#         logger.warning(f"Section {section_id} returned empty summary — retrying")
#         output = invoke_model(prompt, max_tokens=900)
#         parsed = normalize_summary_fields(safe_json_parse(output))

#         if not parsed.get("summary"):
#             logger.error(f"Section {section_id} retry also failed — using fallback")
#             fallback_summary = " ".join(
#                 c.get("tldr", "") for c in chunk_summaries if c.get("tldr")
#             )
#             parsed["summary"] = fallback_summary[:500] if fallback_summary else ""

#     parsed["section_id"] = section_id
#     parsed["source_chunks"] = source_chunks
#     return parsed


# def summarize_executive(section_summaries: List[Dict]) -> Dict:
#     logger.info("Generating executive summary")

#     combined_text = ""
#     source_sections = []

#     for s in section_summaries:
#         if s.get("summary"):
#             combined_text += f"\nSection {s['section_id']}:\n{s['summary']}\n"
#             if s.get("risks"):
#                 risks_text = "\n".join(f"- {r}" for r in s["risks"])
#                 combined_text += f"Risks:\n{risks_text}\n"
#             if s.get("action_items"):
#                 actions_text = "\n".join(f"- {a}" for a in s["action_items"])
#                 combined_text += f"Action items:\n{actions_text}\n"
#             source_sections.append(s["section_id"])

#     if not combined_text.strip():
#         logger.warning("Executive summary skipped")
#         return normalize_summary_fields({"source_sections": []})

#     prompt = EXECUTIVE_SUMMARY_PROMPT.format(content=combined_text)
#     output = invoke_model(prompt, max_tokens=EXECUTIVE_MAX_TOKENS)
#     parsed = normalize_summary_fields(safe_json_parse(output))
#     parsed["source_sections"] = source_sections
#     return parsed


# def parallel_chunk_summarization(chunks: List[Dict]) -> List[Dict]:
#     logger.info(f"Parallel chunk summarization | workers={MAX_WORKERS}")

#     results = []
#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(summarize_chunk, chunk): chunk["chunk_id"]
#             for chunk in chunks
#         }
#         for future in as_completed(futures):
#             results.append(future.result())

#     results.sort(key=lambda x: x["chunk_id"])
#     return results


# def parallel_section_summarization(grouped: List[List[Dict]]) -> List[Dict]:
#     """
#     Run all section summaries in parallel — same pattern as chunk summarization.
#     Each section is independent so they can all fire simultaneously.

#     Before: sections 1→2→3→...→8 sequential = sum of all latencies
#     After:  all 8 fire at t=0 = slowest single section latency
#     """
#     logger.info(f"Parallel section summarization | sections={len(grouped)} | workers={MAX_WORKERS}")

#     results = {}
#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(summarize_section, i + 1, sec): i
#             for i, sec in enumerate(grouped)
#         }
#         for future in as_completed(futures):
#             i = futures[future]
#             results[i] = future.result()

#     # Reassemble in correct section order
#     return [results[i] for i in sorted(results.keys())]


# def group_into_sections(
#     chunk_summaries: List[Dict],
#     group_size: int = SECTION_GROUP_SIZE,
# ):
#     return [
#         chunk_summaries[i: i + group_size]
#         for i in range(0, len(chunk_summaries), group_size)
#     ]


# def run_hierarchical_summarization(chunks: List[Dict]) -> Dict:
#     logger.info("Hierarchical summarization started")

#     if not chunks:
#         raise ValueError("No chunks provided")

#     total_chunks = len(chunks)

#     chunk_summaries = parallel_chunk_summarization(chunks)
#     valid_chunks = [c for c in chunk_summaries if c.get("summary")]

#     grouped = group_into_sections(valid_chunks)

#     # ── PARALLEL section summaries — all fire simultaneously ──
#     section_summaries = parallel_section_summarization(grouped)
#     # ─────────────────────────────────────────────────────────

#     valid_sections = [s for s in section_summaries if s.get("summary")]

#     executive_summary = summarize_executive(valid_sections)

#     covered_chunk_ids = list(
#         {cid for s in section_summaries for cid in s.get("source_chunks", [])}
#     )

#     synthesis_coverage_percent = round(
#         (len(covered_chunk_ids) / total_chunks) * 100, 2
#     )

#     missing_sections = len(grouped) - len(valid_sections)
#     status = "ok"

#     if synthesis_coverage_percent < LOW_COVERAGE_THRESHOLD:
#         status = "low_coverage_warning"
#     if missing_sections > 0:
#         status = "section_missing_warning"

#     metadata = {
#         "total_chunks": total_chunks,
#         "valid_chunks": len(valid_chunks),
#         "coverage_percent": synthesis_coverage_percent,
#         "missing_sections": missing_sections,
#         "status": status,
#         "coverage_details": {
#             "covered_chunk_ids": covered_chunk_ids,
#             "missing_chunk_ids": list(
#                 set(c["chunk_id"] for c in chunks) - set(covered_chunk_ids)
#             ),
#         },
#     }

#     return {
#         "metadata": metadata,
#         "chunk_summaries": chunk_summaries,
#         "section_summaries": section_summaries,
#         "executive_summary": executive_summary,
#     }

"""testign success"""

# import json
# import time
# import boto3
# import logging
# from typing import Dict, Any, List, Optional
# from botocore.exceptions import ClientError
# from concurrent.futures import ThreadPoolExecutor, Future, as_completed

# from app.prompts.summarizer_prompts import (
#     SYSTEM_PROMPT,
#     CHUNK_SUMMARY_PROMPT,
#     SECTION_SUMMARY_PROMPT,
#     EXECUTIVE_SUMMARY_PROMPT,
# )

# from app.core.config import (
#     AWS_REGION,
#     BEDROCK_MODEL_ID,
#     MODEL_MAX_TOKENS,
#     EXECUTIVE_MAX_TOKENS,
#     MODEL_TEMPERATURE,
#     MODEL_TOP_P,
#     MODEL_RETRIES,
#     RETRY_BASE_DELAY,
#     MAX_WORKERS,
#     SECTION_GROUP_SIZE,
#     LOW_COVERAGE_THRESHOLD,
# )

# logger = logging.getLogger(__name__)

# bedrock_client = boto3.client(
#     service_name="bedrock-runtime",
#     region_name=AWS_REGION,
# )

# # How long to wait before firing embeddings after Llama starts.
# # Gives Llama time to establish its TCP connection to Bedrock so both
# # models aren't competing for AWS network bandwidth at the same cold start.
# # 8s = enough for Llama cold start to resolve, after which Titan starts warm.
# _EMBED_START_DELAY_SECONDS = 8


# # ── Helpers (unchanged) ───────────────────────────────────────────────────────

# def clean_model_output(text: str) -> str:
#     text = text.strip()
#     if text.startswith("```"):
#         text = text.replace("```json", "").replace("```", "").strip()
#     return text


# def safe_json_parse(text: str) -> Dict[str, Any]:
#     if not text:
#         logger.warning("Empty model output received")
#         return {"error": "Empty model output"}
#     try:
#         return json.loads(text)
#     except json.JSONDecodeError:
#         logger.error("Invalid JSON returned by model")
#         return {"error": "Invalid JSON returned by model", "raw_output": text}


# def normalize_summary_fields(parsed: Dict[str, Any]) -> Dict[str, Any]:
#     required_fields = {
#         "summary": "",
#         "tldr": "",
#         "key_points": [],
#         "risks": [],
#         "action_items": [],
#     }
#     for field, default in required_fields.items():
#         parsed.setdefault(field, default)
#     return parsed


# def invoke_model(
#     prompt: str,
#     max_tokens: int = MODEL_MAX_TOKENS,
#     temperature: float = MODEL_TEMPERATURE,
#     retries: int = MODEL_RETRIES,
# ) -> str:
#     last_exception = None
#     logger.info("Invoking Bedrock model")

#     formatted_prompt = f"""
# <|begin_of_text|>
# <|start_header_id|>system<|end_header_id|>
# {SYSTEM_PROMPT}
# <|eot_id|>
# <|start_header_id|>user<|end_header_id|>
# {prompt}
# <|eot_id|>
# <|start_header_id|>assistant<|end_header_id|>
# """

#     body = {
#         "prompt": formatted_prompt,
#         "max_gen_len": max_tokens,
#         "temperature": temperature,
#         "top_p": MODEL_TOP_P,
#     }

#     start_time = time.time()

#     for attempt in range(retries + 1):
#         try:
#             response = bedrock_client.invoke_model(
#                 modelId=BEDROCK_MODEL_ID,
#                 body=json.dumps(body),
#                 contentType="application/json",
#                 accept="application/json",
#             )

#             latency = round(time.time() - start_time, 2)
#             logger.info(f"Bedrock response received | latency={latency}s")

#             response_body = json.loads(response["body"].read())
#             content = response_body.get("generation", "")

#             if not content:
#                 raise RuntimeError("Empty model response")

#             return clean_model_output(content)

#         except ClientError as e:
#             last_exception = e
#             logger.warning(f"Bedrock ClientError: {e}")
#             error_code = e.response["Error"]["Code"]
#             if error_code in ("ThrottlingException", "TooManyRequestsException"):
#                 time.sleep(RETRY_BASE_DELAY ** attempt)
#                 continue
#             time.sleep(RETRY_BASE_DELAY)

#         except Exception as e:
#             last_exception = e
#             logger.exception("Model invocation failed")
#             time.sleep(RETRY_BASE_DELAY)

#     logger.error("Bedrock invocation failed after retries")
#     raise RuntimeError(f"Bedrock invocation failed: {last_exception}")


# # ── Summarization functions (unchanged) ──────────────────────────────────────

# def summarize_chunk(chunk: Dict) -> Dict:
#     logger.info(f"Summarizing chunk {chunk['chunk_id']}")
#     prompt = CHUNK_SUMMARY_PROMPT.format(content=chunk["content"])
#     output = invoke_model(prompt)
#     parsed = normalize_summary_fields(safe_json_parse(output))
#     parsed["chunk_id"] = chunk["chunk_id"]
#     return parsed


# def summarize_section(section_id: int, chunk_summaries: List[Dict]) -> Dict:
#     logger.info(f"Generating section summary {section_id}")

#     combined_text = ""
#     source_chunks = []

#     for c in chunk_summaries:
#         if c.get("summary"):
#             combined_text += f"\nChunk {c['chunk_id']}:\n{c['summary']}\n"
#             source_chunks.append(c["chunk_id"])

#     if not combined_text.strip():
#         logger.warning(f"Section {section_id} empty")
#         return {
#             "section_id": section_id,
#             "summary": "",
#             "tldr": "",
#             "key_points": [],
#             "risks": [],
#             "action_items": [],
#             "source_chunks": [],
#         }

#     prompt = SECTION_SUMMARY_PROMPT.format(content=combined_text)
#     output = invoke_model(prompt, max_tokens=900)
#     parsed = normalize_summary_fields(safe_json_parse(output))

#     if not parsed.get("summary") and combined_text.strip():
#         logger.warning(f"Section {section_id} returned empty summary — retrying")
#         output = invoke_model(prompt, max_tokens=900)
#         parsed = normalize_summary_fields(safe_json_parse(output))

#         if not parsed.get("summary"):
#             logger.error(f"Section {section_id} retry also failed — using fallback")
#             fallback_summary = " ".join(
#                 c.get("tldr", "") for c in chunk_summaries if c.get("tldr")
#             )
#             parsed["summary"] = fallback_summary[:500] if fallback_summary else ""

#     parsed["section_id"] = section_id
#     parsed["source_chunks"] = source_chunks
#     return parsed


# def summarize_executive(section_summaries: List[Dict]) -> Dict:
#     logger.info("Generating executive summary")

#     combined_text = ""
#     source_sections = []

#     for s in section_summaries:
#         if s.get("summary"):
#             combined_text += f"\nSection {s['section_id']}:\n{s['summary']}\n"
#             if s.get("risks"):
#                 risks_text = "\n".join(f"- {r}" for r in s["risks"])
#                 combined_text += f"Risks:\n{risks_text}\n"
#             if s.get("action_items"):
#                 actions_text = "\n".join(f"- {a}" for a in s["action_items"])
#                 combined_text += f"Action items:\n{actions_text}\n"
#             source_sections.append(s["section_id"])

#     if not combined_text.strip():
#         logger.warning("Executive summary skipped")
#         return normalize_summary_fields({"source_sections": []})

#     prompt = EXECUTIVE_SUMMARY_PROMPT.format(content=combined_text)
#     output = invoke_model(prompt, max_tokens=EXECUTIVE_MAX_TOKENS)
#     parsed = normalize_summary_fields(safe_json_parse(output))
#     parsed["source_sections"] = source_sections
#     return parsed


# def parallel_chunk_summarization(chunks: List[Dict]) -> List[Dict]:
#     logger.info(f"Parallel chunk summarization | workers={MAX_WORKERS}")

#     results = []
#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(summarize_chunk, chunk): chunk["chunk_id"]
#             for chunk in chunks
#         }
#         for future in as_completed(futures):
#             results.append(future.result())

#     results.sort(key=lambda x: x["chunk_id"])
#     return results


# def parallel_section_summarization(grouped: List[List[Dict]]) -> List[Dict]:
#     logger.info(
#         f"Parallel section summarization | sections={len(grouped)} | workers={MAX_WORKERS}"
#     )

#     results = {}
#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(summarize_section, i + 1, sec): i
#             for i, sec in enumerate(grouped)
#         }
#         for future in as_completed(futures):
#             i = futures[future]
#             results[i] = future.result()

#     return [results[i] for i in sorted(results.keys())]


# def group_into_sections(
#     chunk_summaries: List[Dict],
#     group_size: int = SECTION_GROUP_SIZE,
# ):
#     return [
#         chunk_summaries[i: i + group_size]
#         for i in range(0, len(chunk_summaries), group_size)
#     ]


# def _delayed_store_embeddings(vector_store, doc_id: str, chunks: List[Dict], delay: float) -> bool:
#     """
#     Wait `delay` seconds then run store_embeddings.

#     Why the delay?
#     When Llama and Titan both cold-start at t=0 simultaneously, they compete
#     for AWS network bandwidth — both end up taking ~20s instead of ~9s each.
#     Waiting ~8s lets Llama's TCP connection establish first. By the time Titan
#     starts, Llama is already warm and not competing for the same resources.
#     Titan then gets its full ~9s cold start instead of ~20s.

#     Net result: Llama cold start = 8-9s, Titan cold start = 8-9s but starts
#     at t=8, so Titan finishes at t=17 — still well inside Llama's ~30s
#     chunk summarization window.
#     """
#     logger.info(
#         "[PARALLEL] Delaying embedding start by %.1fs to avoid competing cold starts",
#         delay
#     )
#     time.sleep(delay)
#     logger.info("[PARALLEL] Delay done — firing store_embeddings now")
#     return vector_store.store_embeddings(doc_id, chunks)


# # ── Main entry point ──────────────────────────────────────────────────────────

# def run_hierarchical_summarization(
#     chunks: List[Dict],
#     doc_id: Optional[str] = None,
#     vector_store=None,
# ) -> Dict:
#     """
#     Hierarchical summarization with staggered parallel embedding.

#     Embedding fires in a background thread with an 8s delay so Llama's
#     TCP cold start resolves before Titan attempts its own connection.
#     Both complete well within the chunk summarization window (~30s).

#     Timeline:
#         t=0s   Llama chunk summ starts  ████████████████████████████ ~30s
#         t=0s   Llama cold start         ████████ ~8s then warm
#         t=8s   Titan embedding starts        ████████████████ ~17s (done t=25)
#         t=30s  Section summarization              ████ ~5s
#         t=35s  Executive summary                      ██ ~3s
#         t=38s  embed_future.result() → already done at t=25, wait=0s
#         Total: ~38s  vs previous ~55s
#     """
#     logger.info("Hierarchical summarization started")

#     if not chunks:
#         raise ValueError("No chunks provided")

#     total_chunks = len(chunks)
#     embed_future: Optional[Future] = None
#     embed_executor = None

#     # ── Step 1: Fire embeddings in background with stagger delay ──────────────
#     if doc_id and vector_store:
#         logger.info(
#             "[PARALLEL] Scheduling store_embeddings with %ss delay | doc_id=%s",
#             _EMBED_START_DELAY_SECONDS, doc_id
#         )
#         embed_executor = ThreadPoolExecutor(
#             max_workers=1, thread_name_prefix="bg_embed"
#         )
#         embed_future = embed_executor.submit(
#             _delayed_store_embeddings,
#             vector_store, doc_id, chunks, _EMBED_START_DELAY_SECONDS
#         )
#     else:
#         logger.info("[PARALLEL] No doc_id/vector_store — embedding skipped")

#     # ── Step 2: Chunk summarization (~30s, Llama cold start at t=0) ──────────
#     t0 = time.time()
#     chunk_summaries = parallel_chunk_summarization(chunks)
#     logger.info("[PARALLEL] Chunk summarization done | %.2fs", time.time() - t0)

#     valid_chunks = [c for c in chunk_summaries if c.get("summary")]
#     grouped = group_into_sections(valid_chunks)

#     # ── Step 3: Section summarization (~5s, Llama warm by now) ───────────────
#     t0 = time.time()
#     section_summaries = parallel_section_summarization(grouped)
#     logger.info("[PARALLEL] Section summarization done | %.2fs", time.time() - t0)

#     valid_sections = [s for s in section_summaries if s.get("summary")]

#     # ── Step 4: Executive summary (~3s) ──────────────────────────────────────
#     t0 = time.time()
#     executive_summary = summarize_executive(valid_sections)
#     logger.info("[PARALLEL] Executive summary done | %.2fs", time.time() - t0)

#     # ── Step 5: Wait for embedding (should already be done) ──────────────────
#     if embed_future is not None:
#         t0 = time.time()
#         try:
#             embed_success = embed_future.result()
#             wait_time = round(time.time() - t0, 3)
#             if wait_time > 1.0:
#                 logger.warning(
#                     "[PARALLEL] Waited %.3fs for embedding to finish — "
#                     "consider reducing _EMBED_START_DELAY_SECONDS from %s to %s",
#                     wait_time,
#                     _EMBED_START_DELAY_SECONDS,
#                     max(0, _EMBED_START_DELAY_SECONDS - 2)
#                 )
#             else:
#                 logger.info(
#                     "[PARALLEL] Embedding already done | waited=%.3fs | success=%s",
#                     wait_time, embed_success
#                 )
#         except Exception:
#             logger.exception(
#                 "[PARALLEL] Background embedding failed — "
#                 "summary returned without vector search for this doc."
#             )
#         finally:
#             embed_executor.shutdown(wait=False)

#     # ── Step 6: Build result ──────────────────────────────────────────────────
#     covered_chunk_ids = list(
#         {cid for s in section_summaries for cid in s.get("source_chunks", [])}
#     )
#     synthesis_coverage_percent = round(
#         (len(covered_chunk_ids) / total_chunks) * 100, 2
#     )
#     missing_sections = len(grouped) - len(valid_sections)

#     status = "ok"
#     if synthesis_coverage_percent < LOW_COVERAGE_THRESHOLD:
#         status = "low_coverage_warning"
#     if missing_sections > 0:
#         status = "section_missing_warning"

#     metadata = {
#         "total_chunks": total_chunks,
#         "valid_chunks": len(valid_chunks),
#         "coverage_percent": synthesis_coverage_percent,
#         "missing_sections": missing_sections,
#         "status": status,
#         "coverage_details": {
#             "covered_chunk_ids": covered_chunk_ids,
#             "missing_chunk_ids": list(
#                 set(c["chunk_id"] for c in chunks) - set(covered_chunk_ids)
#             ),
#         },
#     }

#     return {
#         "metadata": metadata,
#         "chunk_summaries": chunk_summaries,
#         "section_summaries": section_summaries,
#         "executive_summary": executive_summary,
#     }

"""missign sectionn """
import json
import time
import boto3
import logging
from typing import Dict, Any, List, Optional
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

from app.prompts.summarizer_prompts import (
    SYSTEM_PROMPT,
    CHUNK_SUMMARY_PROMPT,
    SECTION_SUMMARY_PROMPT,
    EXECUTIVE_SUMMARY_PROMPT,
)

from app.core.config import (
    AWS_REGION,
    BEDROCK_MODEL_ID,
    MODEL_MAX_TOKENS,
    EXECUTIVE_MAX_TOKENS,
    MODEL_TEMPERATURE,
    MODEL_TOP_P,
    MODEL_RETRIES,
    RETRY_BASE_DELAY,
    MAX_WORKERS,
    SECTION_GROUP_SIZE,
    LOW_COVERAGE_THRESHOLD,
)

logger = logging.getLogger(__name__)

bedrock_client = boto3.client(
    service_name="bedrock-runtime",
    region_name=AWS_REGION,
)

# How long to wait before firing embeddings after Llama starts.
# Gives Llama time to establish its TCP connection to Bedrock so both
# models aren't competing for AWS network bandwidth at the same cold start.
# 8s = enough for Llama cold start to resolve, after which Titan starts warm.
_EMBED_START_DELAY_SECONDS = 8


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_model_output(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()
    return text


def safe_json_parse(text: str) -> Dict[str, Any]:
    if not text:
        logger.warning("Empty model output received")
        return {"error": "Empty model output"}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.error("Invalid JSON returned by model")
        return {"error": "Invalid JSON returned by model", "raw_output": text}


def normalize_summary_fields(parsed: Dict[str, Any]) -> Dict[str, Any]:
    required_fields = {
        "summary": "",
        "tldr": "",
        "key_points": [],
        "risks": [],
        "action_items": [],
    }
    for field, default in required_fields.items():
        parsed.setdefault(field, default)
    return parsed


def invoke_model(
    prompt: str,
    max_tokens: int = MODEL_MAX_TOKENS,
    temperature: float = MODEL_TEMPERATURE,
    retries: int = MODEL_RETRIES,
) -> str:
    last_exception = None
    logger.info("Invoking Bedrock model")

    formatted_prompt = f"""
<|begin_of_text|>
<|start_header_id|>system<|end_header_id|>
{SYSTEM_PROMPT}
<|eot_id|>
<|start_header_id|>user<|end_header_id|>
{prompt}
<|eot_id|>
<|start_header_id|>assistant<|end_header_id|>
"""

    body = {
        "prompt": formatted_prompt,
        "max_gen_len": max_tokens,
        "temperature": temperature,
        "top_p": MODEL_TOP_P,
    }

    start_time = time.time()

    for attempt in range(retries + 1):
        try:
            response = bedrock_client.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json",
            )

            latency = round(time.time() - start_time, 2)
            logger.info(f"Bedrock response received | latency={latency}s")

            response_body = json.loads(response["body"].read())
            content = response_body.get("generation", "")

            if not content:
                raise RuntimeError("Empty model response")

            return clean_model_output(content)

        except ClientError as e:
            last_exception = e
            logger.warning(f"Bedrock ClientError: {e}")
            error_code = e.response["Error"]["Code"]
            if error_code in ("ThrottlingException", "TooManyRequestsException"):
                time.sleep(RETRY_BASE_DELAY ** attempt)
                continue
            time.sleep(RETRY_BASE_DELAY)

        except Exception as e:
            last_exception = e
            logger.exception("Model invocation failed")
            time.sleep(RETRY_BASE_DELAY)

    logger.error("Bedrock invocation failed after retries")
    raise RuntimeError(f"Bedrock invocation failed: {last_exception}")


# ── Summarization functions ───────────────────────────────────────────────────

def summarize_chunk(chunk: Dict) -> Dict:
    logger.info(f"Summarizing chunk {chunk['chunk_id']}")
    prompt = CHUNK_SUMMARY_PROMPT.format(content=chunk["content"])
    output = invoke_model(prompt)
    parsed = normalize_summary_fields(safe_json_parse(output))
    parsed["chunk_id"] = chunk["chunk_id"]
    return parsed


def summarize_section(section_id: int, chunk_summaries: List[Dict]) -> Dict:
    logger.info(f"Generating section summary {section_id}")

    combined_text = ""
    source_chunks = []

    for c in chunk_summaries:
        if c.get("summary"):
            combined_text += f"\nChunk {c['chunk_id']}:\n{c['summary']}\n"
            source_chunks.append(c["chunk_id"])

    if not combined_text.strip():
        logger.warning(f"Section {section_id} empty")
        return {
            "section_id":   section_id,
            "summary":      "",
            "tldr":         "",
            "key_points":   [],
            "risks":        [],
            "action_items": [],
            "source_chunks": [],
        }

    prompt = SECTION_SUMMARY_PROMPT.format(content=combined_text)
    output = invoke_model(prompt, max_tokens=900)
    parsed = normalize_summary_fields(safe_json_parse(output))

    if not parsed.get("summary") and combined_text.strip():
        logger.warning(f"Section {section_id} returned empty summary — retrying")
        output = invoke_model(prompt, max_tokens=900)
        parsed = normalize_summary_fields(safe_json_parse(output))

        if not parsed.get("summary"):
            logger.error(f"Section {section_id} retry also failed — using fallback")
            fallback_summary = " ".join(
                c.get("tldr", "") for c in chunk_summaries if c.get("tldr")
            )
            parsed["summary"] = fallback_summary[:500] if fallback_summary else ""

    parsed["section_id"]    = section_id
    parsed["source_chunks"] = source_chunks
    return parsed


def summarize_executive(section_summaries: List[Dict]) -> Dict:
    logger.info("Generating executive summary")

    combined_text   = ""
    source_sections = []

    for s in section_summaries:
        if s.get("summary"):
            combined_text += f"\nSection {s['section_id']}:\n{s['summary']}\n"
            if s.get("risks"):
                risks_text     = "\n".join(f"- {r}" for r in s["risks"])
                combined_text += f"Risks:\n{risks_text}\n"
            if s.get("action_items"):
                actions_text   = "\n".join(f"- {a}" for a in s["action_items"])
                combined_text += f"Action items:\n{actions_text}\n"
            source_sections.append(s["section_id"])

    if not combined_text.strip():
        logger.warning("Executive summary skipped")
        return normalize_summary_fields({"source_sections": []})

    prompt = EXECUTIVE_SUMMARY_PROMPT.format(content=combined_text)
    output = invoke_model(prompt, max_tokens=EXECUTIVE_MAX_TOKENS)
    parsed = normalize_summary_fields(safe_json_parse(output))
    parsed["source_sections"] = source_sections
    return parsed


def parallel_chunk_summarization(chunks: List[Dict]) -> List[Dict]:
    logger.info(f"Parallel chunk summarization | workers={MAX_WORKERS}")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(summarize_chunk, chunk): chunk["chunk_id"]
            for chunk in chunks
        }
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda x: x["chunk_id"])
    return results


def parallel_section_summarization(grouped: List[List[Dict]]) -> List[Dict]:
    logger.info(
        f"Parallel section summarization | sections={len(grouped)} | workers={MAX_WORKERS}"
    )

    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(summarize_section, i + 1, sec): i
            for i, sec in enumerate(grouped)
        }
        for future in as_completed(futures):
            i          = futures[future]
            results[i] = future.result()

    return [results[i] for i in sorted(results.keys())]


def group_into_sections(
    chunk_summaries: List[Dict],
    group_size: int = SECTION_GROUP_SIZE,
):
    return [
        chunk_summaries[i: i + group_size]
        for i in range(0, len(chunk_summaries), group_size)
    ]


def _delayed_store_embeddings(
    vector_store, doc_id: str, chunks: List[Dict], delay: float
) -> bool:
    """
    Wait `delay` seconds then run store_embeddings.

    Why the delay?
    When Llama and Titan both cold-start at t=0 simultaneously, they compete
    for AWS network bandwidth — both end up taking ~20s instead of ~9s each.
    Waiting ~8s lets Llama's TCP connection establish first. By the time Titan
    starts, Llama is already warm and not competing for the same resources.
    Titan then gets its full ~9s cold start instead of ~20s.
    """
    logger.info(
        "[PARALLEL] Delaying embedding start by %.1fs to avoid competing cold starts",
        delay
    )
    time.sleep(delay)
    logger.info("[PARALLEL] Delay done — firing store_embeddings now")
    return vector_store.store_embeddings(doc_id, chunks)


# ── Main entry point ──────────────────────────────────────────────────────────

def run_hierarchical_summarization(
    chunks: List[Dict],
    doc_id: Optional[str] = None,
    vector_store=None,
) -> Dict:
    """
    Hierarchical summarization with staggered parallel embedding.

    Embedding fires in a background thread with an 8s delay so Llama's
    TCP cold start resolves before Titan attempts its own connection.
    Both complete well within the chunk summarization window (~30s).

    Timeline:
        t=0s   Llama chunk summ starts  ████████████████████████████ ~30s
        t=0s   Llama cold start         ████████ ~8s then warm
        t=8s   Titan embedding starts        ████████████████ ~17s (done t=25)
        t=30s  Section summarization              ████ ~5s
        t=35s  Executive summary                      ██ ~3s
        t=38s  embed_future.result() → already done at t=25, wait=0s
        Total: ~38s  vs previous ~55s
    """
    logger.info("Hierarchical summarization started")

    if not chunks:
        raise ValueError("No chunks provided")

    total_chunks  = len(chunks)
    embed_future: Optional[Future] = None
    embed_executor = None

    # ── Step 1: Fire embeddings in background with stagger delay ─────────────
    if doc_id and vector_store:
        logger.info(
            "[PARALLEL] Scheduling store_embeddings with %ss delay | doc_id=%s",
            _EMBED_START_DELAY_SECONDS, doc_id
        )
        embed_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="bg_embed"
        )
        embed_future = embed_executor.submit(
            _delayed_store_embeddings,
            vector_store, doc_id, chunks, _EMBED_START_DELAY_SECONDS
        )
    else:
        logger.info("[PARALLEL] No doc_id/vector_store — embedding skipped")

    # ── Step 2: Chunk summarization (~30s, Llama cold start at t=0) ──────────
    t0             = time.time()
    chunk_summaries = parallel_chunk_summarization(chunks)
    logger.info("[PARALLEL] Chunk summarization done | %.2fs", time.time() - t0)

    valid_chunks = [c for c in chunk_summaries if c.get("summary")]
    grouped      = group_into_sections(valid_chunks)

    # ── Step 3: Section summarization (~5s, Llama warm by now) ───────────────
    t0               = time.time()
    section_summaries = parallel_section_summarization(grouped)
    logger.info("[PARALLEL] Section summarization done | %.2fs", time.time() - t0)

    valid_sections = [s for s in section_summaries if s.get("summary")]

    # ── Step 4: Executive summary (~3s) ──────────────────────────────────────
    t0                = time.time()
    executive_summary = summarize_executive(valid_sections)
    logger.info("[PARALLEL] Executive summary done | %.2fs", time.time() - t0)

    # ── Step 5: Wait for embedding (should already be done) ──────────────────
    if embed_future is not None:
        t0 = time.time()
        try:
            embed_success = embed_future.result()
            wait_time     = round(time.time() - t0, 3)
            if wait_time > 1.0:
                logger.warning(
                    "[PARALLEL] Waited %.3fs for embedding to finish — "
                    "consider reducing _EMBED_START_DELAY_SECONDS from %s to %s",
                    wait_time,
                    _EMBED_START_DELAY_SECONDS,
                    max(0, _EMBED_START_DELAY_SECONDS - 2)
                )
            else:
                logger.info(
                    "[PARALLEL] Embedding already done | waited=%.3fs | success=%s",
                    wait_time, embed_success
                )
        except Exception:
            logger.exception(
                "[PARALLEL] Background embedding failed — "
                "summary returned without vector search for this doc."
            )
        finally:
            embed_executor.shutdown(wait=False)

    # ── Step 6: Build result ──────────────────────────────────────────────────
    #
    # FIX: missing_sections and missing_chunk_ids were previously calculated
    # incorrectly:
    #
    # OLD missing_sections = len(grouped) - len(valid_sections)
    #   → counted sections that existed but had empty summary after retries
    #   → if all 7 sections were generated (even with some chunks missing),
    #     this always returned 0 even when chunks were skipped
    #
    # OLD missing_chunk_ids = all_chunk_ids - covered_chunk_ids
    #   → covered_chunk_ids came from section source_chunks lists
    #   → a chunk could be in source_chunks but still have empty summary=""
    #   → chunk 22 (summary="") was IN source_chunks of section 4,
    #     so it appeared covered when it was actually empty
    #
    # NEW: derive everything from chunk_summaries directly
    #   → summarized_ids  = chunks that have a non-empty summary string
    #   → missing_chunk_ids = chunks processed but returned empty summary
    #   → missing_sections  = sections whose summary is still empty after retry
    #   → covered_chunk_ids = chunks that appear in any section's source list
    #     (kept for backward compatibility — shows section coverage)

    all_chunk_ids    = {c["chunk_id"] for c in chunks}
    summarized_ids   = {c["chunk_id"] for c in chunk_summaries if c.get("summary")}

    # Chunks that were processed but returned empty summary (like chunk 22)
    missing_chunk_ids = sorted(all_chunk_ids - summarized_ids)

    # Sections whose summary is empty even after retry
    missing_sections  = len([s for s in section_summaries if not s.get("summary")])

    # Chunks that appear in at least one section's source list
    covered_chunk_ids = sorted(
        {cid for s in section_summaries for cid in s.get("source_chunks", [])}
    )

    # Coverage based on chunks with real summaries, not just section coverage
    synthesis_coverage_percent = round(
        (len(summarized_ids) / total_chunks) * 100, 2
    )

    # Status — most severe condition wins
    status = "ok"
    if synthesis_coverage_percent < LOW_COVERAGE_THRESHOLD:
        status = "low_coverage_warning"
    if missing_chunk_ids:
        status = "chunks_missing_warning"
        logger.warning(
            "Missing chunk summaries | chunk_ids=%s", missing_chunk_ids
        )
    if missing_sections > 0:
        status = "section_missing_warning"
        logger.warning(
            "Missing section summaries | count=%d", missing_sections
        )

    metadata = {
        "total_chunks":     total_chunks,
        "valid_chunks":     len(summarized_ids),
        "coverage_percent": synthesis_coverage_percent,
        "missing_sections": missing_sections,
        "status":           status,
        "coverage_details": {
            "covered_chunk_ids": covered_chunk_ids,
            "missing_chunk_ids": missing_chunk_ids,
        },
    }

    if missing_chunk_ids:
        logger.info(
            "Coverage | total=%d | summarized=%d | missing_chunks=%s | missing_sections=%d",
            total_chunks, len(summarized_ids), missing_chunk_ids, missing_sections
        )

    return {
        "metadata":          metadata,
        "chunk_summaries":   chunk_summaries,
        "section_summaries": section_summaries,
        "executive_summary": executive_summary,
    }