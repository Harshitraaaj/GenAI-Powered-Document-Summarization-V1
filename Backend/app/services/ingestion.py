# import os
# import re
# import fitz
# import logging
# import pdfplumber
# import pytesseract
# from PIL import Image
# import io
# from concurrent.futures import ThreadPoolExecutor

# from app.core.config import (
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
# )

# pytesseract.pytesseract.tesseract_cmd = "C:\\Users\\harsraj\\AppData\\Local\\Programs\\Tesseract-OCR\\tesseract.exe"

# logger = logging.getLogger(__name__)

# # Parallel workers for page processing
# PAGE_WORKERS = 10


# # -----------------------------
# # Text Cleaning
# # -----------------------------
# def clean_text(text: str) -> str:

#     if not ENABLE_TEXT_CLEANING:
#         return text

#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)

#     return text.strip()


# # -----------------------------
# # OCR for Image
# # -----------------------------
# def extract_text_from_image(image_bytes: bytes) -> str:

#     try:

#         image = Image.open(io.BytesIO(image_bytes))

#         text = pytesseract.image_to_string(image)

#         return clean_text(text)

#     except Exception:
#         logger.exception("OCR failed on image")
#         return ""


# # -----------------------------
# # Table Extraction (NEW)
# # -----------------------------
# def extract_tables_from_pdf(file_path: str) -> list[dict]:
#     """
#     Extracts all tables from a PDF using pdfplumber.
#     Returns a list of dicts with page number, table index, and formatted text.
#     pdfplumber is used here instead of PyMuPDF because it has
#     significantly better table boundary detection accuracy.
#     """

#     tables_data = []

#     try:

#         with pdfplumber.open(file_path) as pdf:

#             for page_num, page in enumerate(pdf.pages):

#                 tables = page.extract_tables()

#                 if not tables:
#                     continue

#                 logger.info(
#                     f"Found {len(tables)} table(s) on page {page_num + 1}"
#                 )

#                 for table_idx, table in enumerate(tables):

#                     if not table:
#                         continue

#                     # Convert table rows into readable pipe-separated text
#                     # e.g [["Name", "Age"], ["Alice", "30"]]
#                     # becomes:
#                     # Name | Age
#                     # Alice | 30
#                     rows = []

#                     for row in table:

#                         if not row:
#                             continue

#                         cleaned_row = " | ".join(
#                             cell.strip() if cell else ""
#                             for cell in row
#                         )

#                         rows.append(cleaned_row)

#                     table_text = "\n".join(rows)

#                     tables_data.append({
#                         "page": page_num + 1,             # 1-indexed for logging
#                         "page_0indexed": page_num,         # 0-indexed for matching with fitz
#                         "table_index": table_idx,
#                         "text": f"[TABLE]\n{table_text}\n[/TABLE]"
#                     })

#     except Exception:
#         logger.exception("Table extraction failed")

#     logger.info(f"Total tables extracted from document: {len(tables_data)}")

#     return tables_data


# # -----------------------------
# # Process Single Page (UPDATED)
# # -----------------------------
# def process_page(
#     page_number: int,
#     page,
#     doc,
#     tables_on_page: list = None        # NEW parameter
# ) -> str:

#     try:

#         page_text = ""

#         # TEXT BLOCK EXTRACTION — NO CHANGE
#         blocks = page.get_text(PDF_EXTRACTION_MODE)

#         for block in blocks:

#             block_text = block[4]

#             if block_text.strip():
#                 page_text += block_text + "\n"

#         # IMAGE OCR — NO CHANGE
#         image_list = page.get_images(full=True)

#         if image_list:
#             logger.info(
#                 f"Found {len(image_list)} image(s) on page {page_number + 1}"
#             )

#         for img in image_list:

#             xref = img[0]

#             base_image = doc.extract_image(xref)

#             image_bytes = base_image["image"]

#             ocr_text = extract_text_from_image(image_bytes)

#             if ocr_text.strip():

#                 page_text += "\n[IMAGE OCR TEXT]\n"

#                 page_text += ocr_text + "\n"

#         # TABLE INJECTION — NEW
#         # Tables are appended after text and image content for the page.
#         # Tagged with [TABLE]...[/TABLE] so chunker/summarizer can
#         # optionally detect and handle them differently downstream.
#         if tables_on_page:

#             for table in tables_on_page:

#                 page_text += f"\n{table['text']}\n"

#             logger.info(
#                 f"Injected {len(tables_on_page)} table(s) into page {page_number + 1}"
#             )

#         return clean_text(page_text)

#     except Exception:
#         logger.exception(f"Error processing page {page_number + 1}")
#         return ""


# # -----------------------------
# # PDF Extraction (UPDATED)
# # -----------------------------
# def extract_from_pdf(file_path: str) -> str:

#     logger.info(f"Starting PDF extraction: {file_path}")

#     doc = fitz.open(file_path)

#     # NEW — extract all tables upfront using pdfplumber
#     # group by 0-indexed page number for quick lookup during page processing
#     all_tables = extract_tables_from_pdf(file_path)

#     tables_by_page = {}

#     for table in all_tables:
#         page_idx = table["page_0indexed"]
#         tables_by_page.setdefault(page_idx, []).append(table)

#     # Load all pages
#     pages = [(i, doc.load_page(i)) for i in range(len(doc))]

#     extracted_pages = []

#     # Parallel page processing
#     with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:

#         futures = [
#             executor.submit(
#                 process_page,
#                 page_number,
#                 page,
#                 doc,
#                 tables_by_page.get(page_number, [])    # NEW — pass tables for this page
#             )
#             for page_number, page in pages
#         ]

#         for future in futures:
#             extracted_pages.append(future.result())

#     doc.close()

#     final_text = "\n\n".join(extracted_pages)

#     logger.info(
#         f"PDF extraction completed | "
#         f"Pages: {len(extracted_pages)} | "
#         f"Tables: {len(all_tables)} | "                # NEW in log
#         f"Characters: {len(final_text)}"
#     )

#     return final_text


# # -----------------------------
# # TXT Extraction — NO CHANGE
# # -----------------------------
# def extract_from_txt(file_path: str) -> str:

#     logger.info(f"Starting TXT extraction: {file_path}")

#     try:

#         with open(
#             file_path,
#             "r",
#             encoding=TEXT_FILE_ENCODING,
#         ) as file:

#             text = file.read()

#         cleaned_text = clean_text(text)

#         logger.info(
#             f"TXT extraction completed | "
#             f"Characters: {len(cleaned_text)}"
#         )

#         return cleaned_text

#     except Exception as e:

#         logger.exception("Error reading TXT file")

#         raise RuntimeError(f"Error reading TXT file: {e}")


# # -----------------------------
# # Document Loader — NO CHANGE
# # -----------------------------
# def load_document(file_path: str) -> str:

#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()

#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(
#             f"Unsupported file type: {extension}"
#         )

#     if extension == ".pdf":
#         text = extract_from_pdf(file_path)

#     elif extension == ".txt":
#         text = extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")

#     return text


# # -----------------------------
# # Local Testing — NO CHANGE
# # -----------------------------
# if __name__ == "__main__":

#     logger.info("Running ingestion test")

#     text = load_document(TEST_SAMPLE_PATH)

#     output_file = "extracted_output.txt"

#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)

#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved extracted content to: {output_file}")

"""v1"""

# import os
# import re
# import fitz
# import logging
# import pdfplumber
# import pytesseract
# from PIL import Image
# import io
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from app.core.config import (
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
# )

# pytesseract.pytesseract.tesseract_cmd = os.getenv(
#     "TESSERACT_CMD",
#     "C:\\Users\\harsraj\\AppData\\Local\\Programs\\Tesseract-OCR\\tesseract.exe"
# )

# logger = logging.getLogger(__name__)

# PAGE_WORKERS = 10
# TABLE_WORKERS = 6

# MIN_OCR_WIDTH = 100
# MIN_OCR_HEIGHT = 50
# MIN_OCR_PIXELS = 10000

# # Table validation thresholds
# TABLE_MIN_ROWS = 3
# TABLE_MIN_COLS = 2
# TABLE_MIN_CONSISTENT_COLS = 0.6
# TABLE_MAX_AVG_CELL_LENGTH = 80    # reject paragraph text split into cells
# TABLE_MIN_NUMERIC_RATIO = 0.10    # real data tables contain numbers
# TABLE_MAX_SINGLE_COL_RATIO = 0.7  # reject if too many single-column rows

# TABLE_STRATEGIES = [
#     # Strategy 1 — default (border/line detection — works for bordered tables)
#     {},
#     # Strategy 2 — text-based (no borders — academic PDFs, reports)
#     {
#         "vertical_strategy": "text",
#         "horizontal_strategy": "text",
#         "snap_tolerance": 3,
#         "join_tolerance": 3,
#         "edge_min_length": 10,
#         "min_words_vertical": 3,
#         "min_words_horizontal": 2,
#     },
#     # Strategy 3 — lines only
#     {
#         "vertical_strategy": "lines",
#         "horizontal_strategy": "lines",
#     },
#     # Strategy 4 — lines strict
#     {
#         "vertical_strategy": "lines_strict",
#         "horizontal_strategy": "lines_strict",
#     },
# ]


# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text

#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)

#     return text.strip()


# def _should_ocr_image(image: Image.Image) -> bool:
#     """Skip OCR on small images — logos, icons, decorative elements."""
#     w, h = image.size
#     return w >= MIN_OCR_WIDTH and h >= MIN_OCR_HEIGHT and (w * h) >= MIN_OCR_PIXELS


# def extract_text_from_image(image_bytes: bytes) -> str:
#     try:
#         image = Image.open(io.BytesIO(image_bytes))

#         if not _should_ocr_image(image):
#             return ""

#         image = image.convert("L")
#         text = pytesseract.image_to_string(image, config="--psm 6")
#         return clean_text(text)

#     except Exception:
#         logger.exception("OCR failed on image")
#         return ""


# def _extract_tables_with_strategy(page, strategy: dict) -> list:
#     try:
#         if strategy:
#             return page.extract_tables(table_settings=strategy) or []
#         else:
#             return page.extract_tables() or []
#     except Exception:
#         return []


# def _is_valid_table(table: list) -> bool:
#     """
#     Universal table validator — works for any PDF type.

#     Rejects:
#     - Two-column layout text (long sentences split into cols)
#     - Reference/bibliography sections (no numbers, long text)
#     - Footnotes and captions (too few rows/cols)
#     - Single-column wrapped paragraphs

#     Accepts:
#     - Data tables with numbers (financial, academic, scientific)
#     - Summary tables with short labels and values
#     - Comparison tables with consistent columns
#     - Bordered tables from any domain
#     """
#     rows = []
#     col_counts = []
#     all_cells = []
#     single_col_rows = 0

#     for row in table:
#         if not row:
#             continue

#         non_empty_cells = [
#             cell.strip() for cell in row
#             if cell and cell.strip()
#         ]

#         if len(non_empty_cells) == 0:
#             continue

#         if len(non_empty_cells) == 1:
#             single_col_rows += 1

#         if len(non_empty_cells) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty_cells))
#             all_cells.extend(non_empty_cells)

#         cleaned_row = " | ".join(
#             cell.strip() if cell else ""
#             for cell in row
#         )
#         if cleaned_row.strip():
#             rows.append(cleaned_row)

#     # ── Check 1: Minimum rows ──────────────────────────────────────
#     if len(rows) < TABLE_MIN_ROWS:
#         return False

#     # ── Check 2: Must have multi-column rows ───────────────────────
#     if not any("|" in row for row in rows):
#         return False

#     # ── Check 3: Too many single-column rows = wrapped paragraph ──
#     total_rows = len(rows)
#     if total_rows > 0:
#         single_col_ratio = single_col_rows / total_rows
#         if single_col_ratio > TABLE_MAX_SINGLE_COL_RATIO:
#             return False

#     # ── Check 4: Column consistency ───────────────────────────────
#     if not col_counts:
#         return False

#     most_common_col_count = max(set(col_counts), key=col_counts.count)
#     consistent_rows = sum(1 for c in col_counts if c == most_common_col_count)
#     consistency_ratio = consistent_rows / len(col_counts)

#     if consistency_ratio < TABLE_MIN_CONSISTENT_COLS:
#         return False

#     if most_common_col_count < TABLE_MIN_COLS:
#         return False

#     # ── Check 5: Cell length — rejects paragraph text in cells ────
#     # Two-column layout false positives have very long cell content
#     # Real table cells are short: numbers, labels, short phrases
#     if all_cells:
#         avg_cell_length = sum(len(c) for c in all_cells) / len(all_cells)
#         if avg_cell_length > TABLE_MAX_AVG_CELL_LENGTH:
#             return False

#     # ── Check 6: Numeric content ──────────────────────────────────
#     # Data tables always have numbers — financial, academic, scientific
#     # Pure text false positives (references, paragraphs) have no numbers
#     # Exception: header-only tables may not have numbers, so only
#     # apply this check if there are enough cells to be meaningful
#     if all_cells and len(all_cells) >= 6:
#         numeric_cells = sum(
#             1 for cell in all_cells
#             if any(char.isdigit() for char in cell)
#         )
#         numeric_ratio = numeric_cells / len(all_cells)
#         if numeric_ratio < TABLE_MIN_NUMERIC_RATIO:
#             return False

#     return True


# def _format_table(table: list) -> str | None:
#     """
#     Convert validated pdfplumber table to pipe-separated text.
#     Returns None if table fails validation.
#     """
#     if not _is_valid_table(table):
#         return None

#     rows = []
#     for row in table:
#         if not row:
#             continue

#         non_empty_cells = [cell for cell in row if cell and cell.strip()]
#         if len(non_empty_cells) < 1:
#             continue

#         cleaned_row = " | ".join(
#             cell.strip() if cell else ""
#             for cell in row
#         )
#         if cleaned_row.strip():
#             rows.append(cleaned_row)

#     return "\n".join(rows) if rows else None


# def _extract_tables_for_page(args: tuple) -> tuple[int, list[dict]]:
#     """
#     Extract tables from a single page — parallel execution.
#     Each call opens its own pdfplumber instance for thread safety.
#     """
#     page_num, file_path = args
#     page_tables = []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page = pdf.pages[page_num]

#             for strategy in TABLE_STRATEGIES:
#                 raw_tables = _extract_tables_with_strategy(page, strategy)

#                 if not raw_tables:
#                     continue

#                 valid_tables = [t for t in raw_tables if _is_valid_table(t)]

#                 if valid_tables:
#                     for table_idx, table in enumerate(valid_tables):
#                         table_text = _format_table(table)
#                         if table_text:
#                             page_tables.append({
#                                 "page": page_num + 1,
#                                 "page_0indexed": page_num,
#                                 "table_index": table_idx,
#                                 "text": f"[TABLE]\n{table_text}\n[/TABLE]"
#                             })
#                     break

#     except Exception:
#         logger.exception(f"Table extraction failed for page {page_num + 1}")

#     return page_num, page_tables


# def extract_tables_from_pdf(file_path: str) -> list[dict]:
#     """
#     Extracts tables from all pages in parallel.
#     Tries 4 strategies per page, stops at first that finds valid tables.
#     """
#     tables_data = []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             num_pages = len(pdf.pages)

#         args = [(page_num, file_path) for page_num in range(num_pages)]

#         with ThreadPoolExecutor(max_workers=TABLE_WORKERS) as executor:
#             futures = {
#                 executor.submit(_extract_tables_for_page, arg): arg[0]
#                 for arg in args
#             }

#             results = {}
#             for future in as_completed(futures):
#                 page_num, page_tables = future.result()
#                 if page_tables:
#                     results[page_num] = page_tables
#                     logger.info(
#                         f"Found {len(page_tables)} table(s) on page {page_num + 1}"
#                     )

#             for page_num in sorted(results.keys()):
#                 tables_data.extend(results[page_num])

#     except Exception:
#         logger.exception("Table extraction failed")

#     logger.info(f"Total tables extracted from document: {len(tables_data)}")
#     return tables_data


# def process_page(
#     page_number: int,
#     page,
#     doc,
#     tables_on_page: list = None
# ) -> tuple[int, str]:
#     """
#     Process a single PDF page — text extraction, OCR, table injection.
#     Returns (page_number, text) for ordered reassembly.
#     """
#     try:
#         page_text = ""

#         blocks = page.get_text(PDF_EXTRACTION_MODE)
#         for block in blocks:
#             block_text = block[4]
#             if block_text.strip():
#                 page_text += block_text + "\n"

#         image_list = page.get_images(full=True)
#         if image_list:
#             logger.info(f"Found {len(image_list)} image(s) on page {page_number + 1}")

#         for img in image_list:
#             xref = img[0]
#             base_image = doc.extract_image(xref)
#             image_bytes = base_image["image"]
#             ocr_text = extract_text_from_image(image_bytes)
#             if ocr_text.strip():
#                 page_text += "\n[IMAGE OCR TEXT]\n"
#                 page_text += ocr_text + "\n"

#         if tables_on_page:
#             for table in tables_on_page:
#                 page_text += f"\n{table['text']}\n"
#             logger.info(
#                 f"Injected {len(tables_on_page)} table(s) into page {page_number + 1}"
#             )

#         return page_number, clean_text(page_text)

#     except Exception:
#         logger.exception(f"Error processing page {page_number + 1}")
#         return page_number, ""


# def extract_from_pdf(file_path: str) -> str:
#     logger.info(f"Starting PDF extraction: {file_path}")

#     doc = fitz.open(file_path)

#     # Run table extraction and page loading concurrently
#     with ThreadPoolExecutor(max_workers=2) as executor:
#         table_future = executor.submit(extract_tables_from_pdf, file_path)
#         pages = [(i, doc.load_page(i)) for i in range(len(doc))]
#         all_tables = table_future.result()

#     tables_by_page = {}
#     for table in all_tables:
#         page_idx = table["page_0indexed"]
#         tables_by_page.setdefault(page_idx, []).append(table)

#     results = {}

#     with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
#         futures = {
#             executor.submit(
#                 process_page,
#                 page_number,
#                 page,
#                 doc,
#                 tables_by_page.get(page_number, [])
#             ): page_number
#             for page_number, page in pages
#         }

#         for future in as_completed(futures):
#             page_number, page_text = future.result()
#             results[page_number] = page_text

#     doc.close()

#     extracted_pages = [results[i] for i in sorted(results.keys())]
#     final_text = "\n\n".join(extracted_pages)

#     logger.info(
#         f"PDF extraction completed | "
#         f"Pages: {len(extracted_pages)} | "
#         f"Tables: {len(all_tables)} | "
#         f"Characters: {len(final_text)}"
#     )

#     return final_text


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")

#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as file:
#             text = file.read()

#         cleaned_text = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned_text)}")
#         return cleaned_text

#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     if extension == ".pdf":
#         text = extract_from_pdf(file_path)
#     elif extension == ".txt":
#         text = extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# if __name__ == "__main__":
#     logger.info("Running ingestion test")

#     text = load_document(TEST_SAMPLE_PATH)

#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)

#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved extracted content to: {output_file}")

"""v1.2"""

# import os
# import re
# import fitz
# import logging
# import pdfplumber
# import pytesseract
# from PIL import Image
# import io
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from app.core.config import (
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
# )

# pytesseract.pytesseract.tesseract_cmd = os.getenv(
#     "TESSERACT_CMD",
#     "C:\\Users\\harsraj\\AppData\\Local\\Programs\\Tesseract-OCR\\tesseract.exe"
# )

# logger = logging.getLogger(__name__)

# PAGE_WORKERS = 10
# TABLE_WORKERS = 6

# MIN_OCR_WIDTH = 100
# MIN_OCR_HEIGHT = 50
# MIN_OCR_PIXELS = 10000

# # Table validation thresholds — tuned from measured PDF data
# # Wrong pages (false positives): avg_len=16-50, numeric=0.14-0.50
# # Correct pages (real tables):   avg_len=4-10,  numeric=0.69-0.82
# TABLE_MIN_ROWS = 3
# TABLE_MIN_COLS = 2
# TABLE_MAX_AVG_CELL_LENGTH = 15   # real tables: 4-10, false positives: 16-50
# TABLE_MIN_NUMERIC_RATIO = 0.55   # real tables: 0.69-0.82, false positives: 0.14-0.50
# TABLE_MAX_SINGLE_COL_RATIO = 0.7

# TABLE_STRATEGIES = [
#     {},
#     {
#         "vertical_strategy": "text",
#         "horizontal_strategy": "text",
#         "snap_tolerance": 3,
#         "join_tolerance": 3,
#         "edge_min_length": 10,
#         "min_words_vertical": 3,
#         "min_words_horizontal": 2,
#     },
#     {
#         "vertical_strategy": "lines",
#         "horizontal_strategy": "lines",
#     },
#     {
#         "vertical_strategy": "lines_strict",
#         "horizontal_strategy": "lines_strict",
#     },
# ]


# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text

#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)

#     return text.strip()


# def _should_ocr_image(image: Image.Image) -> bool:
#     w, h = image.size
#     return w >= MIN_OCR_WIDTH and h >= MIN_OCR_HEIGHT and (w * h) >= MIN_OCR_PIXELS


# def extract_text_from_image(image_bytes: bytes) -> str:
#     try:
#         image = Image.open(io.BytesIO(image_bytes))

#         if not _should_ocr_image(image):
#             return ""

#         image = image.convert("L")
#         text = pytesseract.image_to_string(image, config="--psm 6")
#         return clean_text(text)

#     except Exception:
#         logger.exception("OCR failed on image")
#         return ""


# def _extract_tables_with_strategy(page, strategy: dict) -> list:
#     try:
#         if strategy:
#             return page.extract_tables(table_settings=strategy) or []
#         else:
#             return page.extract_tables() or []
#     except Exception:
#         return []


# def _is_valid_table(table: list) -> bool:
#     """
#     Table validator using two measured thresholds:

#     1. avg_cell_length < 15
#        Real table cells are short numbers/labels (measured: 4-10 chars).
#        Two-column layout text and references have long cells (16-50 chars).

#     2. numeric_ratio > 0.55
#        Real data tables have 69-82% numeric cells.
#        False positives (paragraph text, references) have 14-50%.

#     Consistency ratio is NOT used — real statistical tables have
#     low consistency (0.18-0.77) because header rows differ from data rows.
#     """
#     rows = []
#     col_counts = []
#     all_cells = []
#     single_col_rows = 0
#     total_rows = 0

#     for row in table:
#         if not row:
#             continue

#         non_empty_cells = [
#             cell.strip() for cell in row
#             if cell and cell.strip()
#         ]

#         total_rows += 1

#         if len(non_empty_cells) == 0:
#             continue

#         if len(non_empty_cells) == 1:
#             single_col_rows += 1

#         if len(non_empty_cells) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty_cells))
#             all_cells.extend(non_empty_cells)

#         cleaned_row = " | ".join(
#             cell.strip() if cell else ""
#             for cell in row
#         )
#         if cleaned_row.strip():
#             rows.append(cleaned_row)

#     # Check 1 — minimum rows
#     if len(rows) < TABLE_MIN_ROWS:
#         return False

#     # Check 2 — must have multi-column rows
#     if not any("|" in row for row in rows):
#         return False

#     # Check 3 — too many single-column rows = wrapped paragraph
#     if total_rows > 0 and (single_col_rows / total_rows) > TABLE_MAX_SINGLE_COL_RATIO:
#         return False

#     if not col_counts:
#         return False

#     # Check 4 — avg cell length
#     # Real table cells are short (4-10 chars)
#     # Two-column layout text averages 16-50 chars per cell
#     if all_cells:
#         avg_cell_length = sum(len(c) for c in all_cells) / len(all_cells)
#         if avg_cell_length > TABLE_MAX_AVG_CELL_LENGTH:
#             return False

#     # Check 5 — numeric content ratio
#     # Real data tables: 69-82% of cells contain numbers
#     # False positives: 14-50%
#     # Only apply when 6+ cells to avoid rejecting small header tables
#     if all_cells and len(all_cells) >= 6:
#         numeric_cells = sum(
#             1 for cell in all_cells
#             if any(char.isdigit() for char in cell)
#         )
#         if (numeric_cells / len(all_cells)) < TABLE_MIN_NUMERIC_RATIO:
#             return False

#     return True


# def _format_table(table: list) -> str | None:
#     """
#     Convert validated pdfplumber table to pipe-separated text.
#     Returns None if table fails validation.
#     """
#     if not _is_valid_table(table):
#         return None

#     rows = []
#     for row in table:
#         if not row:
#             continue
#         non_empty_cells = [cell for cell in row if cell and cell.strip()]
#         if not non_empty_cells:
#             continue
#         cleaned_row = " | ".join(
#             cell.strip() if cell else ""
#             for cell in row
#         )
#         if cleaned_row.strip():
#             rows.append(cleaned_row)

#     return "\n".join(rows) if rows else None


# def _extract_tables_for_page(args: tuple) -> tuple[int, list[dict]]:
#     """
#     Extract tables from a single page — parallel execution.
#     Each call opens its own pdfplumber instance for thread safety.
#     """
#     page_num, file_path = args
#     page_tables = []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page = pdf.pages[page_num]

#             for strategy in TABLE_STRATEGIES:
#                 raw_tables = _extract_tables_with_strategy(page, strategy)

#                 if not raw_tables:
#                     continue

#                 valid_tables = [t for t in raw_tables if _is_valid_table(t)]

#                 if valid_tables:
#                     for table_idx, table in enumerate(valid_tables):
#                         table_text = _format_table(table)
#                         if table_text:
#                             page_tables.append({
#                                 "page": page_num + 1,
#                                 "page_0indexed": page_num,
#                                 "table_index": table_idx,
#                                 "text": f"[TABLE]\n{table_text}\n[/TABLE]"
#                             })
#                     break

#     except Exception:
#         logger.exception(f"Table extraction failed for page {page_num + 1}")

#     return page_num, page_tables


# def extract_tables_from_pdf(file_path: str) -> list[dict]:
#     """
#     Extracts tables from all pages in parallel.
#     Tries 4 strategies per page, stops at first that finds valid tables.
#     """
#     tables_data = []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             num_pages = len(pdf.pages)

#         args = [(page_num, file_path) for page_num in range(num_pages)]

#         with ThreadPoolExecutor(max_workers=TABLE_WORKERS) as executor:
#             futures = {
#                 executor.submit(_extract_tables_for_page, arg): arg[0]
#                 for arg in args
#             }

#             results = {}
#             for future in as_completed(futures):
#                 page_num, page_tables = future.result()
#                 if page_tables:
#                     results[page_num] = page_tables
#                     logger.info(
#                         f"Found {len(page_tables)} table(s) on page {page_num + 1}"
#                     )

#             for page_num in sorted(results.keys()):
#                 tables_data.extend(results[page_num])

#     except Exception:
#         logger.exception("Table extraction failed")

#     logger.info(f"Total tables extracted from document: {len(tables_data)}")
#     return tables_data


# def process_page(
#     page_number: int,
#     page,
#     doc,
#     tables_on_page: list = None
# ) -> tuple[int, str]:
#     """
#     Process a single PDF page — text extraction, OCR, table injection.
#     Returns (page_number, text) for ordered reassembly.
#     """
#     try:
#         page_text = ""

#         blocks = page.get_text(PDF_EXTRACTION_MODE)
#         for block in blocks:
#             block_text = block[4]
#             if block_text.strip():
#                 page_text += block_text + "\n"

#         image_list = page.get_images(full=True)
#         if image_list:
#             logger.info(f"Found {len(image_list)} image(s) on page {page_number + 1}")

#         for img in image_list:
#             xref = img[0]
#             base_image = doc.extract_image(xref)
#             image_bytes = base_image["image"]
#             ocr_text = extract_text_from_image(image_bytes)
#             if ocr_text.strip():
#                 page_text += "\n[IMAGE OCR TEXT]\n"
#                 page_text += ocr_text + "\n"

#         if tables_on_page:
#             for table in tables_on_page:
#                 page_text += f"\n{table['text']}\n"
#             logger.info(
#                 f"Injected {len(tables_on_page)} table(s) into page {page_number + 1}"
#             )

#         return page_number, clean_text(page_text)

#     except Exception:
#         logger.exception(f"Error processing page {page_number + 1}")
#         return page_number, ""


# def extract_from_pdf(file_path: str) -> str:
#     logger.info(f"Starting PDF extraction: {file_path}")

#     doc = fitz.open(file_path)

#     with ThreadPoolExecutor(max_workers=2) as executor:
#         table_future = executor.submit(extract_tables_from_pdf, file_path)
#         pages = [(i, doc.load_page(i)) for i in range(len(doc))]
#         all_tables = table_future.result()

#     tables_by_page = {}
#     for table in all_tables:
#         page_idx = table["page_0indexed"]
#         tables_by_page.setdefault(page_idx, []).append(table)

#     results = {}

#     with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
#         futures = {
#             executor.submit(
#                 process_page,
#                 page_number,
#                 page,
#                 doc,
#                 tables_by_page.get(page_number, [])
#             ): page_number
#             for page_number, page in pages
#         }

#         for future in as_completed(futures):
#             page_number, page_text = future.result()
#             results[page_number] = page_text

#     doc.close()

#     extracted_pages = [results[i] for i in sorted(results.keys())]
#     final_text = "\n\n".join(extracted_pages)

#     logger.info(
#         f"PDF extraction completed | "
#         f"Pages: {len(extracted_pages)} | "
#         f"Tables: {len(all_tables)} | "
#         f"Characters: {len(final_text)}"
#     )

#     return final_text


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")

#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as file:
#             text = file.read()

#         cleaned_text = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned_text)}")
#         return cleaned_text

#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     if extension == ".pdf":
#         text = extract_from_pdf(file_path)
#     elif extension == ".txt":
#         text = extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# if __name__ == "__main__":
#     logger.info("Running ingestion test")

#     text = load_document(TEST_SAMPLE_PATH)

#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)

#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved extracted content to: {output_file}")

"""v1.3"""

"""
ingestion.py

Document ingestion pipeline — text extraction, OCR, and table detection.
Supports PDF and TXT files.

Table extraction uses 5 pdfplumber strategies in priority order.
Validated at 23/25 on robo_paper + covid_paper test suite.
"""

# import io
# import os
# import re
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed

# import fitz
# import pdfplumber
# import pytesseract
# from PIL import Image

# from app.core.config import (
#     # General ingestion
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
#     # Workers
#     PAGE_WORKERS,
#     TABLE_WORKERS,
#     # OCR
#     MIN_OCR_WIDTH,
#     MIN_OCR_HEIGHT,
#     MIN_OCR_PIXELS,
#     TESSERACT_CMD,
#     # Table validation
#     TABLE_MIN_ROWS,
#     TABLE_MIN_COLS,
#     TABLE_MAX_AVG_CELL_LENGTH,
#     TABLE_MIN_NUMERIC_RATIO,
#     TABLE_MAX_SINGLE_COL_RATIO,
#     TABLE_MIN_MULTI_COL_ROWS,
#     TABLE_MAX_MID_WORD_RATIO,
# )

# logger = logging.getLogger(__name__)

# pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# # ── Table extraction strategies (tried in order, first valid result wins) ──────
# _TABLE_STRATEGIES = [
#     # 1 - bordered tables (visible lines)
#     {
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     # 2 - strict lines
#     {
#         "vertical_strategy":    "lines_strict",
#         "horizontal_strategy":  "lines_strict",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     # 3 - text vertical + lines horizontal
#     {
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   2,
#         "min_words_horizontal": 1,
#     },
#     # 4 - lines vertical + text horizontal
#     {
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 2,
#     },
#     # 5 - text both (borderless research paper tables)
#     {
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       10,
#         "join_tolerance":       10,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
# ]


# # ══════════════════════════════════════════════════════════════════════════════
# # Text cleaning
# # ══════════════════════════════════════════════════════════════════════════════

# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text
#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)
#     return text.strip()


# # ══════════════════════════════════════════════════════════════════════════════
# # OCR
# # ══════════════════════════════════════════════════════════════════════════════

# def _should_ocr_image(image: Image.Image) -> bool:
#     w, h = image.size
#     return w >= MIN_OCR_WIDTH and h >= MIN_OCR_HEIGHT and (w * h) >= MIN_OCR_PIXELS


# def extract_text_from_image(image_bytes: bytes) -> str:
#     try:
#         image = Image.open(io.BytesIO(image_bytes))
#         if not _should_ocr_image(image):
#             return ""
#         image = image.convert("L")
#         text = pytesseract.image_to_string(image, config="--psm 6")
#         return clean_text(text)
#     except Exception:
#         logger.exception("OCR failed on image")
#         return ""


# # ══════════════════════════════════════════════════════════════════════════════
# # Table validation helpers
# # ══════════════════════════════════════════════════════════════════════════════

# def _has_rotated_text(page) -> bool:
#     """True if the page has significant rotated text — not a data table."""
#     try:
#         chars = page.chars
#         if not chars:
#             return False
#         rotated = sum(1 for c in chars if c.get("upright", 1) == 0)
#         return (rotated / len(chars)) > 0.3
#     except Exception:
#         return False


# def _has_mid_word_splits(all_cells: list, avg_len: float, numeric_ratio: float) -> bool:
#     """
#     True when too many cells end mid-word — sign that flowing text
#     is being misread as table columns.

#     Uses > 0.25 (not >=) so a page with numeric_ratio exactly 0.25
#     still gets checked and rejected if mid-word ratio is high.
#     """
#     if avg_len <= 30 or numeric_ratio > 0.25:
#         return False

#     mid_word_count = 0
#     for cell in all_cells:
#         first_line = cell.split("\n")[0].strip()
#         if not first_line:
#             continue
#         last_char = first_line[-1]
#         if last_char.islower() and last_char not in ".,:;!?)\"'":
#             mid_word_count += 1

#     ratio = mid_word_count / len(all_cells) if all_cells else 0
#     return ratio > TABLE_MAX_MID_WORD_RATIO


# def _is_valid_table(table: list, page) -> bool:
#     """
#     Validate a raw pdfplumber table against all quality gates.
#     Returns True only when confident it is a real data table.
#     """
#     if not table:
#         return False

#     all_cells       = []
#     col_counts      = []
#     single_col_rows = 0
#     total_rows      = 0

#     for row in table:
#         if not row:
#             continue
#         non_empty = [c.strip() for c in row if c and c.strip()]
#         total_rows += 1
#         if len(non_empty) == 1:
#             single_col_rows += 1
#         if len(non_empty) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty))
#             all_cells.extend(non_empty)

#     if total_rows < TABLE_MIN_ROWS:
#         return False
#     if not col_counts:
#         return False
#     if len(col_counts) < TABLE_MIN_MULTI_COL_ROWS:
#         return False
#     if max(col_counts) < TABLE_MIN_COLS:
#         return False
#     if (single_col_rows / total_rows) > TABLE_MAX_SINGLE_COL_RATIO:
#         return False

#     first_line_lengths = [len(c.split("\n")[0].strip()) for c in all_cells]
#     avg_len = sum(first_line_lengths) / len(first_line_lengths) if first_line_lengths else 0
#     if avg_len > TABLE_MAX_AVG_CELL_LENGTH:
#         return False

#     numeric = sum(1 for c in all_cells if any(ch.isdigit() for ch in c.split("\n")[0]))
#     numeric_ratio = numeric / len(all_cells) if all_cells else 0
#     if numeric_ratio < TABLE_MIN_NUMERIC_RATIO:
#         return False

#     if _has_rotated_text(page):
#         return False
#     if _has_mid_word_splits(all_cells, avg_len, numeric_ratio):
#         return False

#     logger.debug(
#     f"Table accepted | rows={total_rows} cols={max(col_counts)} "
#     f"avg_len={avg_len:.1f} numeric={numeric_ratio:.2f} "
#     f"single_ratio={single_col_rows/total_rows:.2f}"
#     )

#     return True


# def _format_table(table: list) -> str:
#     """Convert a validated pdfplumber table to pipe-separated text."""
#     rows = []
#     for row in table:
#         if not row:
#             continue
#         if not any(c and c.strip() for c in row):
#             continue
#         cleaned = " | ".join(c.strip() if c else "" for c in row)
#         if cleaned.strip():
#             rows.append(cleaned)
#     return "\n".join(rows) if rows else ""


# # ══════════════════════════════════════════════════════════════════════════════
# # Table extraction
# # ══════════════════════════════════════════════════════════════════════════════

# def _extract_tables_for_page(args: tuple) -> tuple[int, list[dict]]:
#     """
#     Try all 5 strategies on one page, stop at first that finds valid tables.
#     Opens its own pdfplumber handle for thread safety.
#     """
#     page_num, file_path = args
#     page_tables = []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page = pdf.pages[page_num]

#             for strategy in _TABLE_STRATEGIES:
#                 try:
#                     raw_tables = page.extract_tables(table_settings=strategy) or []
#                 except Exception:
#                     continue

#                 valid = [t for t in raw_tables if _is_valid_table(t, page)]
#                 if not valid:
#                     continue

#                 for idx, table in enumerate(valid):
#                     text = _format_table(table)
#                     if text:
#                         page_tables.append({
#                             "page":          page_num + 1,
#                             "page_0indexed": page_num,
#                             "table_index":   idx,
#                             "text":          f"[TABLE]\n{text}\n[/TABLE]",
#                         })
#                 break   # first strategy with valid tables wins

#     except Exception:
#         logger.exception(f"Table extraction failed for page {page_num + 1}")

#     return page_num, page_tables


# def extract_tables_from_pdf(file_path: str) -> list[dict]:
#     """
#     Extract tables from all pages in parallel.
#     Returns list of table dicts sorted by page number.
#     """
#     tables_data = []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             num_pages = len(pdf.pages)

#         args = [(page_num, file_path) for page_num in range(num_pages)]
#         results: dict[int, list] = {}

#         with ThreadPoolExecutor(max_workers=TABLE_WORKERS) as executor:
#             futures = {
#                 executor.submit(_extract_tables_for_page, arg): arg[0]
#                 for arg in args
#             }
#             for future in as_completed(futures):
#                 page_num, page_tables = future.result()
#                 if page_tables:
#                     results[page_num] = page_tables
#                     logger.info(f"Found {len(page_tables)} table(s) on page {page_num + 1}")

#         for page_num in sorted(results):
#             tables_data.extend(results[page_num])

#     except Exception:
#         logger.exception("Table extraction failed")

#     logger.info(f"Total tables extracted: {len(tables_data)}")
#     return tables_data


# # ══════════════════════════════════════════════════════════════════════════════
# # PDF page processor
# # ══════════════════════════════════════════════════════════════════════════════

# def process_page(
#     page_number: int,
#     page,
#     doc,
#     tables_on_page: list | None = None,
# ) -> tuple[int, str]:
#     """
#     Extract text + OCR + inject tables for a single PDF page.
#     Returns (page_number, text) for ordered reassembly.
#     """
#     try:
#         page_text = ""

#         blocks = page.get_text(PDF_EXTRACTION_MODE)
#         for block in blocks:
#             if block[4].strip():
#                 page_text += block[4] + "\n"

#         image_list = page.get_images(full=True)
#         if image_list:
#             logger.info(f"Found {len(image_list)} image(s) on page {page_number + 1}")

#         for img in image_list:
#             base_image = doc.extract_image(img[0])
#             ocr_text = extract_text_from_image(base_image["image"])
#             if ocr_text.strip():
#                 page_text += "\n[IMAGE OCR TEXT]\n" + ocr_text + "\n"

#         if tables_on_page:
#             for table in tables_on_page:
#                 page_text += f"\n{table['text']}\n"
#             logger.info(
#                 f"Injected {len(tables_on_page)} table(s) into page {page_number + 1}"
#             )

#         return page_number, clean_text(page_text)

#     except Exception:
#         logger.exception(f"Error processing page {page_number + 1}")
#         return page_number, ""


# # ══════════════════════════════════════════════════════════════════════════════
# # Public API
# # ══════════════════════════════════════════════════════════════════════════════

# def extract_from_pdf(file_path: str) -> str:
#     logger.info(f"Starting PDF extraction: {file_path}")

#     doc = fitz.open(file_path)

#     with ThreadPoolExecutor(max_workers=2) as executor:
#         table_future = executor.submit(extract_tables_from_pdf, file_path)
#         pages = [(i, doc.load_page(i)) for i in range(len(doc))]
#         all_tables = table_future.result()

#     tables_by_page: dict[int, list] = {}
#     for table in all_tables:
#         tables_by_page.setdefault(table["page_0indexed"], []).append(table)

#     results: dict[int, str] = {}
#     with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
#         futures = {
#             executor.submit(
#                 process_page,
#                 page_number,
#                 page,
#                 doc,
#                 tables_by_page.get(page_number, []),
#             ): page_number
#             for page_number, page in pages
#         }
#         for future in as_completed(futures):
#             page_number, page_text = future.result()
#             results[page_number] = page_text

#     doc.close()

#     extracted_pages = [results[i] for i in sorted(results)]
#     final_text = "\n\n".join(extracted_pages)

#     logger.info(
#         f"PDF extraction completed | "
#         f"Pages: {len(extracted_pages)} | "
#         f"Tables: {len(all_tables)} | "
#         f"Characters: {len(final_text)}"
#     )
#     return final_text


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")
#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as f:
#             text = f.read()
#         cleaned = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned)}")
#         return cleaned
#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     text = extract_from_pdf(file_path) if extension == ".pdf" else extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# # ── Dev runner ─────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     text = load_document(TEST_SAMPLE_PATH)
#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)
#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved to: {output_file}")

"""v1.4"""

"""
ingestion.py

Document ingestion pipeline — text extraction, OCR, and table detection.
Supports PDF and TXT files.

Table extraction uses 5 pdfplumber strategies in priority order.
Debug logging on every rejection gate — set LOG_LEVEL=DEBUG in .env to diagnose
missed tables.
"""

# import io
# import os
# import re
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed

# import fitz
# import pdfplumber
# import pytesseract
# from PIL import Image

# from app.core.config import (
#     # General ingestion
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
#     # Workers
#     PAGE_WORKERS,
#     TABLE_WORKERS,
#     # OCR
#     MIN_OCR_WIDTH,
#     MIN_OCR_HEIGHT,
#     MIN_OCR_PIXELS,
#     TESSERACT_CMD,
#     # Table validation
#     TABLE_MIN_ROWS,
#     TABLE_MIN_COLS,
#     TABLE_MAX_AVG_CELL_LENGTH,
#     TABLE_MIN_NUMERIC_RATIO,
#     TABLE_MAX_SINGLE_COL_RATIO,
#     TABLE_MIN_MULTI_COL_ROWS,
#     TABLE_MAX_MID_WORD_RATIO,
# )

# logger = logging.getLogger(__name__)

# pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# # ── Table extraction strategies (tried in order, first valid result wins) ──────
# _TABLE_STRATEGIES = [
#     # 1 - bordered tables (visible lines)
#     {
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     # 2 - strict lines
#     {
#         "vertical_strategy":    "lines_strict",
#         "horizontal_strategy":  "lines_strict",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     # 3 - text vertical + lines horizontal
#     {
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   2,
#         "min_words_horizontal": 1,
#     },
#     # 4 - lines vertical + text horizontal
#     {
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 2,
#     },
#     # 5 - text both (borderless research paper tables)
#     {
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       10,
#         "join_tolerance":       10,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
# ]


# # ══════════════════════════════════════════════════════════════════════════════
# # Text cleaning
# # ══════════════════════════════════════════════════════════════════════════════

# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text
#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)
#     return text.strip()


# # ══════════════════════════════════════════════════════════════════════════════
# # OCR
# # ══════════════════════════════════════════════════════════════════════════════

# def _should_ocr_image(image: Image.Image) -> bool:
#     w, h = image.size
#     return w >= MIN_OCR_WIDTH and h >= MIN_OCR_HEIGHT and (w * h) >= MIN_OCR_PIXELS


# def extract_text_from_image(image_bytes: bytes) -> str:
#     try:
#         image = Image.open(io.BytesIO(image_bytes))
#         if not _should_ocr_image(image):
#             return ""
#         image = image.convert("L")
#         text = pytesseract.image_to_string(image, config="--psm 6")
#         return clean_text(text)
#     except Exception:
#         logger.exception("OCR failed on image")
#         return ""


# # ══════════════════════════════════════════════════════════════════════════════
# # Table validation helpers
# # ══════════════════════════════════════════════════════════════════════════════

# def _has_rotated_text(page) -> bool:
#     """True if the page has significant rotated text — not a data table."""
#     try:
#         chars = page.chars
#         if not chars:
#             return False
#         rotated = sum(1 for c in chars if c.get("upright", 1) == 0)
#         return (rotated / len(chars)) > 0.3
#     except Exception:
#         return False


# def _has_mid_word_splits(all_cells: list, avg_len: float, numeric_ratio: float) -> bool:
#     """
#     True when too many cells end mid-word — sign that flowing text
#     is being misread as table columns.

#     Uses > 0.25 (not >=) so a page with numeric_ratio exactly 0.25
#     still gets checked and rejected if mid-word ratio is high.
#     """
#     if avg_len <= 30 or numeric_ratio > 0.25:
#         return False

#     mid_word_count = 0
#     for cell in all_cells:
#         first_line = cell.split("\n")[0].strip()
#         if not first_line:
#             continue
#         last_char = first_line[-1]
#         if last_char.islower() and last_char not in ".,:;!?)\"'":
#             mid_word_count += 1

#     ratio = mid_word_count / len(all_cells) if all_cells else 0
#     return ratio > TABLE_MAX_MID_WORD_RATIO


# def _is_valid_table(table: list, page, page_num: int = -1) -> bool:
#     """
#     Validate a raw pdfplumber table against all quality gates.
#     Returns True only when confident it is a real data table.

#     Set LOG_LEVEL=DEBUG to see per-gate rejection reasons per page.
#     """
#     if not table:
#         return False

#     all_cells       = []
#     col_counts      = []
#     single_col_rows = 0
#     total_rows      = 0

#     for row in table:
#         if not row:
#             continue
#         non_empty = [c.strip() for c in row if c and c.strip()]
#         total_rows += 1
#         if len(non_empty) == 1:
#             single_col_rows += 1
#         if len(non_empty) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty))
#             all_cells.extend(non_empty)

#     p = f"p{page_num + 1}"   # human-readable page label for debug logs

#     if total_rows < TABLE_MIN_ROWS:
#         logger.debug(f"{p} REJECT rows={total_rows} < {TABLE_MIN_ROWS}")
#         return False

#     if not col_counts:
#         logger.debug(f"{p} REJECT no multi-col rows found")
#         return False

#     if len(col_counts) < TABLE_MIN_MULTI_COL_ROWS:
#         logger.debug(f"{p} REJECT multi_col_rows={len(col_counts)} < {TABLE_MIN_MULTI_COL_ROWS}")
#         return False

#     if max(col_counts) < TABLE_MIN_COLS:
#         logger.debug(f"{p} REJECT max_cols={max(col_counts)} < {TABLE_MIN_COLS}")
#         return False

#     single_ratio = single_col_rows / total_rows
#     if single_ratio > TABLE_MAX_SINGLE_COL_RATIO:
#         logger.debug(f"{p} REJECT single_col_ratio={single_ratio:.2f} > {TABLE_MAX_SINGLE_COL_RATIO}")
#         return False

#     first_line_lengths = [len(c.split("\n")[0].strip()) for c in all_cells]
#     avg_len = sum(first_line_lengths) / len(first_line_lengths) if first_line_lengths else 0
#     if avg_len > TABLE_MAX_AVG_CELL_LENGTH:
#         logger.debug(f"{p} REJECT avg_len={avg_len:.1f} > {TABLE_MAX_AVG_CELL_LENGTH}")
#         return False

#     numeric = sum(1 for c in all_cells if any(ch.isdigit() for ch in c.split("\n")[0]))
#     numeric_ratio = numeric / len(all_cells) if all_cells else 0
#     if numeric_ratio < TABLE_MIN_NUMERIC_RATIO:
#         logger.debug(f"{p} REJECT numeric_ratio={numeric_ratio:.2f} < {TABLE_MIN_NUMERIC_RATIO}")
#         return False

#     if _has_rotated_text(page):
#         logger.debug(f"{p} REJECT rotated text detected")
#         return False

#     if _has_mid_word_splits(all_cells, avg_len, numeric_ratio):
#         logger.debug(f"{p} REJECT mid-word splits detected avg_len={avg_len:.1f} numeric={numeric_ratio:.2f}")
#         return False

#     logger.debug(
#         f"{p} ACCEPT rows={total_rows} cols={max(col_counts)} "
#         f"avg_len={avg_len:.1f} numeric={numeric_ratio:.2f} "
#         f"single_ratio={single_ratio:.2f}"
#     )
#     return True


# def _format_table(table: list) -> str:
#     """Convert a validated pdfplumber table to pipe-separated text."""
#     rows = []
#     for row in table:
#         if not row:
#             continue
#         if not any(c and c.strip() for c in row):
#             continue
#         cleaned = " | ".join(c.strip() if c else "" for c in row)
#         if cleaned.strip():
#             rows.append(cleaned)
#     return "\n".join(rows) if rows else ""


# # ══════════════════════════════════════════════════════════════════════════════
# # Table extraction
# # ══════════════════════════════════════════════════════════════════════════════

# def _extract_tables_for_page(args: tuple) -> tuple[int, list[dict]]:
#     """
#     Try all 5 strategies on one page, stop at first that finds valid tables.
#     Opens its own pdfplumber handle for thread safety.
#     """
#     page_num, file_path = args
#     page_tables = []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page = pdf.pages[page_num]

#             for strategy_idx, strategy in enumerate(_TABLE_STRATEGIES, start=1):
#                 try:
#                     raw_tables = page.extract_tables(table_settings=strategy) or []
#                 except Exception:
#                     continue

#                 valid = [t for t in raw_tables if _is_valid_table(t, page, page_num)]
#                 if not valid:
#                     continue

#                 for idx, table in enumerate(valid):
#                     text = _format_table(table)
#                     if text:
#                         page_tables.append({
#                             "page":          page_num + 1,
#                             "page_0indexed": page_num,
#                             "table_index":   idx,
#                             "text":          f"[TABLE]\n{text}\n[/TABLE]",
#                         })
#                 break   # first strategy with valid tables wins

#     except Exception:
#         logger.exception(f"Table extraction failed for page {page_num + 1}")

#     return page_num, page_tables


# def extract_tables_from_pdf(file_path: str) -> list[dict]:
#     """
#     Extract tables from all pages in parallel.
#     Returns list of table dicts sorted by page number.
#     """
#     tables_data = []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             num_pages = len(pdf.pages)

#         args = [(page_num, file_path) for page_num in range(num_pages)]
#         results: dict[int, list] = {}

#         with ThreadPoolExecutor(max_workers=TABLE_WORKERS) as executor:
#             futures = {
#                 executor.submit(_extract_tables_for_page, arg): arg[0]
#                 for arg in args
#             }
#             for future in as_completed(futures):
#                 page_num, page_tables = future.result()
#                 if page_tables:
#                     results[page_num] = page_tables
#                     logger.info(f"Found {len(page_tables)} table(s) on page {page_num + 1}")

#         for page_num in sorted(results):
#             tables_data.extend(results[page_num])

#     except Exception:
#         logger.exception("Table extraction failed")

#     logger.info(f"Total tables extracted: {len(tables_data)}")
#     return tables_data


# # ══════════════════════════════════════════════════════════════════════════════
# # PDF page processor
# # ══════════════════════════════════════════════════════════════════════════════

# def process_page(
#     page_number: int,
#     page,
#     doc,
#     tables_on_page: list | None = None,
# ) -> tuple[int, str]:
#     """
#     Extract text + OCR + inject tables for a single PDF page.
#     Returns (page_number, text) for ordered reassembly.
#     """
#     try:
#         page_text = ""

#         blocks = page.get_text(PDF_EXTRACTION_MODE)
#         for block in blocks:
#             if block[4].strip():
#                 page_text += block[4] + "\n"

#         image_list = page.get_images(full=True)
#         if image_list:
#             logger.info(f"Found {len(image_list)} image(s) on page {page_number + 1}")

#         for img in image_list:
#             base_image = doc.extract_image(img[0])
#             ocr_text = extract_text_from_image(base_image["image"])
#             if ocr_text.strip():
#                 page_text += "\n[IMAGE OCR TEXT]\n" + ocr_text + "\n"

#         if tables_on_page:
#             for table in tables_on_page:
#                 page_text += f"\n{table['text']}\n"
#             logger.info(
#                 f"Injected {len(tables_on_page)} table(s) into page {page_number + 1}"
#             )

#         return page_number, clean_text(page_text)

#     except Exception:
#         logger.exception(f"Error processing page {page_number + 1}")
#         return page_number, ""


# # ══════════════════════════════════════════════════════════════════════════════
# # Public API
# # ══════════════════════════════════════════════════════════════════════════════

# def extract_from_pdf(file_path: str) -> str:
#     logger.info(f"Starting PDF extraction: {file_path}")

#     doc = fitz.open(file_path)

#     with ThreadPoolExecutor(max_workers=2) as executor:
#         table_future = executor.submit(extract_tables_from_pdf, file_path)
#         pages = [(i, doc.load_page(i)) for i in range(len(doc))]
#         all_tables = table_future.result()

#     tables_by_page: dict[int, list] = {}
#     for table in all_tables:
#         tables_by_page.setdefault(table["page_0indexed"], []).append(table)

#     results: dict[int, str] = {}
#     with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
#         futures = {
#             executor.submit(
#                 process_page,
#                 page_number,
#                 page,
#                 doc,
#                 tables_by_page.get(page_number, []),
#             ): page_number
#             for page_number, page in pages
#         }
#         for future in as_completed(futures):
#             page_number, page_text = future.result()
#             results[page_number] = page_text

#     doc.close()

#     extracted_pages = [results[i] for i in sorted(results)]
#     final_text = "\n\n".join(extracted_pages)

#     logger.info(
#         f"PDF extraction completed | "
#         f"Pages: {len(extracted_pages)} | "
#         f"Tables: {len(all_tables)} | "
#         f"Characters: {len(final_text)}"
#     )
#     return final_text


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")
#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as f:
#             text = f.read()
#         cleaned = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned)}")
#         return cleaned
#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     text = extract_from_pdf(file_path) if extension == ".pdf" else extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# # ── Dev runner ─────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     text = load_document(TEST_SAMPLE_PATH)
#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)
#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved to: {output_file}")

"""v4"""

# import io
# import os
# import re
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed

# import fitz
# import pdfplumber
# import pytesseract
# from PIL import Image

# from app.core.config import (
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
#     PAGE_WORKERS,
#     TABLE_WORKERS,
#     MIN_OCR_WIDTH,
#     MIN_OCR_HEIGHT,
#     MIN_OCR_PIXELS,
#     TESSERACT_CMD,
#     TABLE_MIN_ROWS,
#     TABLE_MIN_COLS,
#     TABLE_MAX_AVG_CELL_LENGTH,
#     TABLE_MIN_NUMERIC_RATIO,
#     TABLE_MAX_SINGLE_COL_RATIO,
#     TABLE_MIN_MULTI_COL_ROWS,
#     TABLE_MAX_MID_WORD_RATIO,
# )

# logger = logging.getLogger(__name__)
# pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# # ── Table strategies ──────────────────────────────────────────────────────────
# _TABLE_STRATEGIES = [
#     {   # 1 — bordered tables (visible lines)
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 2 — strict lines
#         "vertical_strategy":    "lines_strict",
#         "horizontal_strategy":  "lines_strict",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 3 — text vertical + lines horizontal
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   2,
#         "min_words_horizontal": 1,
#     },
#     {   # 4 — lines vertical + text horizontal
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 2,
#     },
#     {   # 5 — text both (borderless research paper tables)
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       10,
#         "join_tolerance":       10,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
# ]

# # ── Image-dominant page detection ─────────────────────────────────────────────
# # Pages where most content is images (figures, screenshots) with little text.
# # Table extraction on these pages produces high false positive rates.
# _IMAGE_DOMINANT_MIN_IMAGES     = 2     # ≥ 2 images on the page
# _IMAGE_DOMINANT_MAX_TEXT_CHARS = 400   # AND < 400 chars of actual text


# def _is_image_dominant_page(page_num: int, file_path: str) -> bool:
#     """
#     Returns True when a page has many images but little text.
#     These pages are typically figures, diagrams, or screenshot appendix pages.
#     Table extraction on image-dominant pages generates false positives.

#     Pages 9, 17, 18, 26, 28, 37, 38 in robo_paper are image-dominant.
#     """
#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page = pdf.pages[page_num]
#             text = page.extract_text() or ""
#             images = page.images or []

#             is_dominant = (
#                 len(images) >= _IMAGE_DOMINANT_MIN_IMAGES
#                 and len(text.strip()) < _IMAGE_DOMINANT_MAX_TEXT_CHARS
#             )
#             if is_dominant:
#                 logger.debug(
#                     f"p{page_num + 1} image-dominant: "
#                     f"images={len(images)} text_chars={len(text.strip())} — skipping table extraction"
#                 )
#             return is_dominant
#     except Exception:
#         return False


# # ══════════════════════════════════════════════════════════════════════════════
# # Text cleaning
# # ══════════════════════════════════════════════════════════════════════════════

# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text
#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)
#     return text.strip()


# # ══════════════════════════════════════════════════════════════════════════════
# # OCR
# # ══════════════════════════════════════════════════════════════════════════════

# def _should_ocr_image(image: Image.Image) -> bool:
#     w, h = image.size
#     return w >= MIN_OCR_WIDTH and h >= MIN_OCR_HEIGHT and (w * h) >= MIN_OCR_PIXELS


# def extract_text_from_image(image_bytes: bytes) -> str:
#     try:
#         image = Image.open(io.BytesIO(image_bytes))
#         if not _should_ocr_image(image):
#             return ""
#         image = image.convert("L")
#         text = pytesseract.image_to_string(image, config="--psm 6")
#         return clean_text(text)
#     except Exception:
#         logger.exception("OCR failed on image")
#         return ""


# # ══════════════════════════════════════════════════════════════════════════════
# # Table validation helpers
# # ══════════════════════════════════════════════════════════════════════════════

# def _has_rotated_text(page) -> bool:
#     """True if the page has significant rotated text — not a data table."""
#     try:
#         chars = page.chars
#         if not chars:
#             return False
#         rotated = sum(1 for c in chars if c.get("upright", 1) == 0)
#         return (rotated / len(chars)) > 0.3
#     except Exception:
#         return False


# def _has_mid_word_splits(
#     all_cells: list, avg_len: float, numeric_ratio: float
# ) -> bool:
#     """
#     True when too many cells end mid-word — sign that flowing text
#     is being misread as table columns.

#     Uses > 0.25 (not >=) so a page with numeric_ratio exactly 0.25
#     still gets checked and rejected if mid-word ratio is high.
#     """
#     if avg_len <= 30 or numeric_ratio > 0.25:
#         return False

#     mid_word_count = 0
#     for cell in all_cells:
#         first_line = cell.split("\n")[0].strip()
#         if not first_line:
#             continue
#         last_char = first_line[-1]
#         if last_char.islower() and last_char not in ".,:;!?)\"'":
#             mid_word_count += 1

#     ratio = mid_word_count / len(all_cells) if all_cells else 0
#     return ratio > TABLE_MAX_MID_WORD_RATIO


# def _is_valid_table(table: list, page, page_num: int = -1) -> bool:
#     """
#     Validate a raw pdfplumber table against all quality gates.
#     Returns True only when confident it is a real data table.

#     Gate order (fastest/cheapest first):
#       1. min rows
#       2. multi-column structure
#       3. single-column ratio
#       4. avg cell length  (rejects figure captions, reference lists)
#       5. numeric ratio    (rejects text-heavy false positives)
#       6. rotated text
#       7. mid-word splits  (rejects paragraph text misread as columns)
#     """
#     if not table:
#         return False

#     all_cells       = []
#     col_counts      = []
#     single_col_rows = 0
#     total_rows      = 0

#     for row in table:
#         if not row:
#             continue
#         non_empty = [c.strip() for c in row if c and c.strip()]
#         total_rows += 1
#         if len(non_empty) == 1:
#             single_col_rows += 1
#         if len(non_empty) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty))
#             all_cells.extend(non_empty)

#     p = f"p{page_num + 1}"

#     if total_rows < TABLE_MIN_ROWS:
#         logger.debug(f"{p} REJECT rows={total_rows} < {TABLE_MIN_ROWS}")
#         return False

#     if not col_counts:
#         logger.debug(f"{p} REJECT no multi-col rows")
#         return False

#     if len(col_counts) < TABLE_MIN_MULTI_COL_ROWS:
#         logger.debug(f"{p} REJECT multi_col_rows={len(col_counts)} < {TABLE_MIN_MULTI_COL_ROWS}")
#         return False

#     if max(col_counts) < TABLE_MIN_COLS:
#         logger.debug(f"{p} REJECT max_cols={max(col_counts)} < {TABLE_MIN_COLS}")
#         return False

#     single_ratio = single_col_rows / total_rows
#     if single_ratio > TABLE_MAX_SINGLE_COL_RATIO:
#         logger.debug(f"{p} REJECT single_col_ratio={single_ratio:.2f}")
#         return False

#     first_line_lengths = [len(c.split("\n")[0].strip()) for c in all_cells]
#     avg_len = sum(first_line_lengths) / len(first_line_lengths) if first_line_lengths else 0
#     if avg_len > TABLE_MAX_AVG_CELL_LENGTH:
#         logger.debug(f"{p} REJECT avg_len={avg_len:.1f} > {TABLE_MAX_AVG_CELL_LENGTH}")
#         return False

#     numeric = sum(1 for c in all_cells if any(ch.isdigit() for ch in c.split("\n")[0]))
#     numeric_ratio = numeric / len(all_cells) if all_cells else 0

#     # ── Tightened to 0.30 from original 0.25 ─────────────────────────────────
#     # Figure captions and appendix image pages typically score 0.10–0.25.
#     # Real data tables (results, ablations, comparisons) score ≥ 0.30.
#     # This change eliminates the 7 image-page false positives.
#     effective_min_numeric = max(TABLE_MIN_NUMERIC_RATIO, 0.30)
#     if numeric_ratio < effective_min_numeric:
#         logger.debug(f"{p} REJECT numeric_ratio={numeric_ratio:.2f} < {effective_min_numeric}")
#         return False

#     if _has_rotated_text(page):
#         logger.debug(f"{p} REJECT rotated text")
#         return False

#     if _has_mid_word_splits(all_cells, avg_len, numeric_ratio):
#         logger.debug(f"{p} REJECT mid-word splits avg_len={avg_len:.1f} numeric={numeric_ratio:.2f}")
#         return False

#     logger.debug(
#         f"{p} ACCEPT rows={total_rows} cols={max(col_counts)} "
#         f"avg_len={avg_len:.1f} numeric={numeric_ratio:.2f} "
#         f"single_ratio={single_ratio:.2f}"
#     )
#     return True


# def _format_table(table: list) -> str:
#     rows = []
#     for row in table:
#         if not row:
#             continue
#         if not any(c and c.strip() for c in row):
#             continue
#         cleaned = " | ".join(c.strip() if c else "" for c in row)
#         if cleaned.strip():
#             rows.append(cleaned)
#     return "\n".join(rows) if rows else ""


# # ══════════════════════════════════════════════════════════════════════════════
# # Table extraction
# # ══════════════════════════════════════════════════════════════════════════════

# def _extract_tables_for_page(args: tuple) -> tuple[int, list[dict]]:
#     """
#     Try all strategies on one page, stop at first valid result.
#     Skips image-dominant pages to avoid figure false positives.
#     """
#     page_num, file_path = args
#     page_tables = []

#     # ── Skip image-dominant pages (figures, screenshots) ─────────────────────
#     if _is_image_dominant_page(page_num, file_path):
#         return page_num, []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page = pdf.pages[page_num]

#             for strategy_idx, strategy in enumerate(_TABLE_STRATEGIES, start=1):
#                 try:
#                     raw_tables = page.extract_tables(table_settings=strategy) or []
#                 except Exception:
#                     continue

#                 valid = [t for t in raw_tables if _is_valid_table(t, page, page_num)]
#                 if not valid:
#                     continue

#                 for idx, table in enumerate(valid):
#                     text = _format_table(table)
#                     if text:
#                         page_tables.append({
#                             "page":          page_num + 1,
#                             "page_0indexed": page_num,
#                             "table_index":   idx,
#                             "text":          f"[TABLE]\n{text}\n[/TABLE]",
#                         })
#                 break

#     except Exception:
#         logger.exception(f"Table extraction failed for page {page_num + 1}")

#     return page_num, page_tables


# def extract_tables_from_pdf(file_path: str) -> list[dict]:
#     tables_data = []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             num_pages = len(pdf.pages)

#         args = [(page_num, file_path) for page_num in range(num_pages)]
#         results: dict[int, list] = {}

#         with ThreadPoolExecutor(max_workers=TABLE_WORKERS) as executor:
#             futures = {
#                 executor.submit(_extract_tables_for_page, arg): arg[0]
#                 for arg in args
#             }
#             for future in as_completed(futures):
#                 page_num, page_tables = future.result()
#                 if page_tables:
#                     results[page_num] = page_tables
#                     logger.info(f"Found {len(page_tables)} table(s) on page {page_num + 1}")

#         for page_num in sorted(results):
#             tables_data.extend(results[page_num])

#     except Exception:
#         logger.exception("Table extraction failed")

#     logger.info(f"Total tables extracted: {len(tables_data)}")
#     return tables_data


# # ══════════════════════════════════════════════════════════════════════════════
# # PDF page processor
# # ══════════════════════════════════════════════════════════════════════════════

# def process_page(
#     page_number: int,
#     page,
#     doc,
#     tables_on_page: list | None = None,
# ) -> tuple[int, str]:
#     try:
#         page_text = ""

#         blocks = page.get_text(PDF_EXTRACTION_MODE)
#         for block in blocks:
#             if block[4].strip():
#                 page_text += block[4] + "\n"

#         image_list = page.get_images(full=True)
#         if image_list:
#             logger.info(f"Found {len(image_list)} image(s) on page {page_number + 1}")

#         for img in image_list:
#             base_image = doc.extract_image(img[0])
#             ocr_text = extract_text_from_image(base_image["image"])
#             if ocr_text.strip():
#                 page_text += "\n[IMAGE OCR TEXT]\n" + ocr_text + "\n"

#         if tables_on_page:
#             for table in tables_on_page:
#                 page_text += f"\n{table['text']}\n"
#             logger.info(
#                 f"Injected {len(tables_on_page)} table(s) into page {page_number + 1}"
#             )

#         return page_number, clean_text(page_text)

#     except Exception:
#         logger.exception(f"Error processing page {page_number + 1}")
#         return page_number, ""


# # ══════════════════════════════════════════════════════════════════════════════
# # Public API
# # ══════════════════════════════════════════════════════════════════════════════

# def extract_from_pdf(file_path: str) -> str:
#     logger.info(f"Starting PDF extraction: {file_path}")

#     doc = fitz.open(file_path)

#     with ThreadPoolExecutor(max_workers=2) as executor:
#         table_future = executor.submit(extract_tables_from_pdf, file_path)
#         pages = [(i, doc.load_page(i)) for i in range(len(doc))]
#         all_tables = table_future.result()

#     tables_by_page: dict[int, list] = {}
#     for table in all_tables:
#         tables_by_page.setdefault(table["page_0indexed"], []).append(table)

#     results: dict[int, str] = {}
#     with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
#         futures = {
#             executor.submit(
#                 process_page,
#                 page_number,
#                 page,
#                 doc,
#                 tables_by_page.get(page_number, []),
#             ): page_number
#             for page_number, page in pages
#         }
#         for future in as_completed(futures):
#             page_number, page_text = future.result()
#             results[page_number] = page_text

#     doc.close()

#     extracted_pages = [results[i] for i in sorted(results)]
#     final_text = "\n\n".join(extracted_pages)

#     logger.info(
#         f"PDF extraction completed | "
#         f"Pages: {len(extracted_pages)} | "
#         f"Tables: {len(all_tables)} | "
#         f"Characters: {len(final_text)}"
#     )
#     return final_text


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")
#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as f:
#             text = f.read()
#         cleaned = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned)}")
#         return cleaned
#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     text = extract_from_pdf(file_path) if extension == ".pdf" else extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# # ── Dev runner ──────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     text = load_document(TEST_SAMPLE_PATH)
#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)
#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved to: {output_file}")

# """v5"""
# import io
# import os
# import re
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed

# import fitz
# import pdfplumber
# import pytesseract
# from PIL import Image

# from app.core.config import (
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
#     PAGE_WORKERS,
#     TABLE_WORKERS,
#     MIN_OCR_WIDTH,
#     MIN_OCR_HEIGHT,
#     MIN_OCR_PIXELS,
#     TESSERACT_CMD,
#     TABLE_MIN_ROWS,
#     TABLE_MIN_COLS,
#     TABLE_MAX_AVG_CELL_LENGTH,
#     TABLE_MIN_NUMERIC_RATIO,
#     TABLE_MAX_SINGLE_COL_RATIO,
#     TABLE_MIN_MULTI_COL_ROWS,
#     TABLE_MAX_MID_WORD_RATIO,
# )

# logger = logging.getLogger(__name__)
# pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# # ── Table strategies ──────────────────────────────────────────
# _TABLE_STRATEGIES = [
#     {   # 1 — bordered tables (visible lines)
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 2 — strict lines
#         "vertical_strategy":    "lines_strict",
#         "horizontal_strategy":  "lines_strict",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 3 — text vertical + lines horizontal
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   2,
#         "min_words_horizontal": 1,
#     },
#     {   # 4 — lines vertical + text horizontal
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 2,
#     },
#     {   # 5 — text both (borderless research paper tables)
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       10,
#         "join_tolerance":       10,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
# ]

# # ── Image-dominant page detection ─────────────────────────────
# # Pages where images dominate (figures, screenshots) with little text.
# # Table extraction on these produces high false positive rates.
# _IMAGE_DOMINANT_MIN_IMAGES     = 2
# _IMAGE_DOMINANT_MAX_TEXT_CHARS = 400


# def _is_image_dominant_page(page_num: int, file_path: str) -> bool:
#     """
#     Returns True when a page has ≥2 images AND < 400 chars of text.
#     These pages are typically figures or screenshot appendix pages.
#     Skipping them eliminates false positives like pages 9,17,18,37,38.
#     """
#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page   = pdf.pages[page_num]
#             text   = page.extract_text() or ""
#             images = page.images or []
#             is_dom = (
#                 len(images) >= _IMAGE_DOMINANT_MIN_IMAGES
#                 and len(text.strip()) < _IMAGE_DOMINANT_MAX_TEXT_CHARS
#             )
#             if is_dom:
#                 logger.debug(
#                     f"p{page_num + 1} image-dominant: "
#                     f"images={len(images)} text_chars={len(text.strip())} — skipping"
#                 )
#             return is_dom
#     except Exception:
#         return False


# # ══════════════════════════════════════════════════════════════
# # Text cleaning
# # ══════════════════════════════════════════════════════════════

# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text
#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)
#     return text.strip()


# # ══════════════════════════════════════════════════════════════
# # OCR
# # ══════════════════════════════════════════════════════════════

# def _should_ocr_image(image: Image.Image) -> bool:
#     w, h = image.size
#     return w >= MIN_OCR_WIDTH and h >= MIN_OCR_HEIGHT and (w * h) >= MIN_OCR_PIXELS


# def extract_text_from_image(image_bytes: bytes) -> str:
#     try:
#         image = Image.open(io.BytesIO(image_bytes))
#         if not _should_ocr_image(image):
#             return ""
#         image = image.convert("L")
#         text  = pytesseract.image_to_string(image, config="--psm 6")
#         return clean_text(text)
#     except Exception:
#         logger.exception("OCR failed on image")
#         return ""


# # ══════════════════════════════════════════════════════════════
# # Table validation helpers
# # ══════════════════════════════════════════════════════════════

# def _has_rotated_text(page) -> bool:
#     """True if the page has significant rotated text — not a data table."""
#     try:
#         chars = page.chars
#         if not chars:
#             return False
#         rotated = sum(1 for c in chars if c.get("upright", 1) == 0)
#         return (rotated / len(chars)) > 0.3
#     except Exception:
#         return False


# def _has_mid_word_splits(
#     all_cells: list, avg_len: float, numeric_ratio: float
# ) -> bool:
#     """
#     True when too many cells end mid-word — sign that flowing text
#     is being misread as table columns.
#     Uses > 0.25 (not >=) so exactly 0.25 still gets checked.
#     """
#     if avg_len <= 30 or numeric_ratio > 0.25:
#         return False
#     mid_word_count = 0
#     for cell in all_cells:
#         first_line = cell.split("\n")[0].strip()
#         if not first_line:
#             continue
#         last_char = first_line[-1]
#         if last_char.islower() and last_char not in ".,:;!?)\"'":
#             mid_word_count += 1
#     ratio = mid_word_count / len(all_cells) if all_cells else 0
#     return ratio > TABLE_MAX_MID_WORD_RATIO


# def _has_concatenated_words(all_cells: list) -> bool:
#     """
#     Detects 2-column PDF layout misread as a table.
#     Real table cells always start at a word boundary (uppercase, digit,
#     symbol, or a known short fragment).
#     2-column layout text splits produce cells that start mid-word with
#     a lowercase letter, e.g. 'ublishedasaconference', 'ehavioraldisplacement'.

#     Rejects when > 30% of cells start with a lowercase letter.
#     """
#     if not all_cells:
#         return False
#     mid_word_starts = 0
#     for cell in all_cells:
#         first_line = cell.split("\n")[0].strip()
#         if not first_line:
#             continue
#         # Cell starts lowercase = mid-word break from previous column
#         if first_line[0].islower():
#             mid_word_starts += 1
#     ratio = mid_word_starts / len(all_cells)
#     return ratio > 0.30


# def _is_valid_table(table: list, page, page_num: int = -1) -> bool:
#     """
#     Validate a raw pdfplumber table against all quality gates.
#     Returns True only when confident it is a real data table.

#     Gate order (fastest first):
#       1. min rows
#       2. multi-column structure
#       3. single-column ratio
#       4. avg cell length          — rejects figure captions, reference lists
#       5. numeric ratio            — rejects text-heavy false positives
#       6. rotated text
#       7. mid-word splits          — rejects paragraph text as columns
#       8. concatenated words       — rejects 2-column layout pages (NEW)
#     """
#     if not table:
#         return False

#     all_cells       = []
#     col_counts      = []
#     single_col_rows = 0
#     total_rows      = 0

#     for row in table:
#         if not row:
#             continue
#         non_empty = [c.strip() for c in row if c and c.strip()]
#         total_rows += 1
#         if len(non_empty) == 1:
#             single_col_rows += 1
#         if len(non_empty) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty))
#             all_cells.extend(non_empty)

#     p = f"p{page_num + 1}"

#     if total_rows < TABLE_MIN_ROWS:
#         logger.debug(f"{p} REJECT rows={total_rows} < {TABLE_MIN_ROWS}")
#         return False

#     if not col_counts:
#         logger.debug(f"{p} REJECT no multi-col rows")
#         return False

#     if len(col_counts) < TABLE_MIN_MULTI_COL_ROWS:
#         logger.debug(f"{p} REJECT multi_col_rows={len(col_counts)} < {TABLE_MIN_MULTI_COL_ROWS}")
#         return False

#     if max(col_counts) < TABLE_MIN_COLS:
#         logger.debug(f"{p} REJECT max_cols={max(col_counts)} < {TABLE_MIN_COLS}")
#         return False

#     single_ratio = single_col_rows / total_rows
#     if single_ratio > TABLE_MAX_SINGLE_COL_RATIO:
#         logger.debug(f"{p} REJECT single_col_ratio={single_ratio:.2f}")
#         return False

#     first_line_lengths = [len(c.split("\n")[0].strip()) for c in all_cells]
#     avg_len = sum(first_line_lengths) / len(first_line_lengths) if first_line_lengths else 0

#     if avg_len > TABLE_MAX_AVG_CELL_LENGTH:
#         logger.debug(f"{p} REJECT avg_len={avg_len:.1f} > {TABLE_MAX_AVG_CELL_LENGTH}")
#         return False

#     numeric = sum(1 for c in all_cells if any(ch.isdigit() for ch in c.split("\n")[0]))
#     numeric_ratio = numeric / len(all_cells) if all_cells else 0

#     # Tightened to 0.30 — figure captions score 0.10–0.25, real tables score ≥0.30
#     effective_min_numeric = max(TABLE_MIN_NUMERIC_RATIO, 0.30)
#     if numeric_ratio < effective_min_numeric:
#         logger.debug(f"{p} REJECT numeric_ratio={numeric_ratio:.2f} < {effective_min_numeric}")
#         return False

#     if _has_rotated_text(page):
#         logger.debug(f"{p} REJECT rotated text")
#         return False

#     if _has_mid_word_splits(all_cells, avg_len, numeric_ratio):
#         logger.debug(f"{p} REJECT mid-word splits avg_len={avg_len:.1f} numeric={numeric_ratio:.2f}")
#         return False

#     # New gate — catches 2-column layout pages (pages 26, 28 and appendix pages)
#     if _has_concatenated_words(all_cells):
#         logger.debug(f"{p} REJECT 2-column layout misread as table")
#         return False

#     logger.debug(
#         f"{p} ACCEPT rows={total_rows} cols={max(col_counts)} "
#         f"avg_len={avg_len:.1f} numeric={numeric_ratio:.2f} "
#         f"single_ratio={single_ratio:.2f}"
#     )
#     return True


# def _format_table(table: list) -> str:
#     """Convert a validated pdfplumber table to pipe-separated text."""
#     rows = []
#     for row in table:
#         if not row:
#             continue
#         if not any(c and c.strip() for c in row):
#             continue
#         cleaned = " | ".join(c.strip() if c else "" for c in row)
#         if cleaned.strip():
#             rows.append(cleaned)
#     return "\n".join(rows) if rows else ""


# # ══════════════════════════════════════════════════════════════
# # Table extraction
# # ══════════════════════════════════════════════════════════════

# def _extract_tables_for_page(args: tuple) -> tuple[int, list[dict]]:
#     """
#     Try all 5 strategies on one page, stop at first with valid tables.
#     Skips image-dominant pages to avoid false positives.
#     """
#     page_num, file_path = args
#     page_tables = []

#     # Skip pages dominated by images (figures, screenshots)
#     if _is_image_dominant_page(page_num, file_path):
#         return page_num, []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page = pdf.pages[page_num]

#             for strategy_idx, strategy in enumerate(_TABLE_STRATEGIES, start=1):
#                 try:
#                     raw_tables = page.extract_tables(table_settings=strategy) or []
#                 except Exception:
#                     continue

#                 valid = [t for t in raw_tables if _is_valid_table(t, page, page_num)]
#                 if not valid:
#                     continue

#                 for idx, table in enumerate(valid):
#                     text = _format_table(table)
#                     if text:
#                         page_tables.append({
#                             "page":          page_num + 1,
#                             "page_0indexed": page_num,
#                             "table_index":   idx,
#                             "text":          f"[TABLE]\n{text}\n[/TABLE]",
#                         })
#                 break   # first strategy with valid tables wins

#     except Exception:
#         logger.exception(f"Table extraction failed for page {page_num + 1}")

#     return page_num, page_tables


# def extract_tables_from_pdf(file_path: str) -> list[dict]:
#     """Extract tables from all pages in parallel."""
#     tables_data = []
#     try:
#         with pdfplumber.open(file_path) as pdf:
#             num_pages = len(pdf.pages)

#         args    = [(page_num, file_path) for page_num in range(num_pages)]
#         results: dict[int, list] = {}

#         with ThreadPoolExecutor(max_workers=TABLE_WORKERS) as executor:
#             futures = {
#                 executor.submit(_extract_tables_for_page, arg): arg[0]
#                 for arg in args
#             }
#             for future in as_completed(futures):
#                 page_num, page_tables = future.result()
#                 if page_tables:
#                     results[page_num] = page_tables
#                     logger.info(f"Found {len(page_tables)} table(s) on page {page_num + 1}")

#         for page_num in sorted(results):
#             tables_data.extend(results[page_num])

#     except Exception:
#         logger.exception("Table extraction failed")

#     logger.info(f"Total tables extracted: {len(tables_data)}")
#     return tables_data


# # ══════════════════════════════════════════════════════════════
# # PDF page processor
# # ══════════════════════════════════════════════════════════════

# def process_page(
#     page_number:    int,
#     page,
#     doc,
#     tables_on_page: list | None = None,
# ) -> tuple[int, str]:
#     """Extract text + OCR + inject tables for a single PDF page."""
#     try:
#         page_text = ""

#         blocks = page.get_text(PDF_EXTRACTION_MODE)
#         for block in blocks:
#             if block[4].strip():
#                 page_text += block[4] + "\n"

#         image_list = page.get_images(full=True)
#         if image_list:
#             logger.info(f"Found {len(image_list)} image(s) on page {page_number + 1}")

#         for img in image_list:
#             base_image = doc.extract_image(img[0])
#             ocr_text   = extract_text_from_image(base_image["image"])
#             if ocr_text.strip():
#                 page_text += "\n[IMAGE OCR TEXT]\n" + ocr_text + "\n"

#         if tables_on_page:
#             for table in tables_on_page:
#                 page_text += f"\n{table['text']}\n"
#             logger.info(
#                 f"Injected {len(tables_on_page)} table(s) into page {page_number + 1}"
#             )

#         return page_number, clean_text(page_text)

#     except Exception:
#         logger.exception(f"Error processing page {page_number + 1}")
#         return page_number, ""


# # ══════════════════════════════════════════════════════════════
# # Public API
# # ══════════════════════════════════════════════════════════════

# def extract_from_pdf(file_path: str) -> str:
#     logger.info(f"Starting PDF extraction: {file_path}")

#     doc = fitz.open(file_path)

#     with ThreadPoolExecutor(max_workers=2) as executor:
#         table_future = executor.submit(extract_tables_from_pdf, file_path)
#         pages        = [(i, doc.load_page(i)) for i in range(len(doc))]
#         all_tables   = table_future.result()

#     tables_by_page: dict[int, list] = {}
#     for table in all_tables:
#         tables_by_page.setdefault(table["page_0indexed"], []).append(table)

#     results: dict[int, str] = {}
#     with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
#         futures = {
#             executor.submit(
#                 process_page,
#                 page_number,
#                 page,
#                 doc,
#                 tables_by_page.get(page_number, []),
#             ): page_number
#             for page_number, page in pages
#         }
#         for future in as_completed(futures):
#             page_number, page_text = future.result()
#             results[page_number]   = page_text

#     doc.close()

#     extracted_pages = [results[i] for i in sorted(results)]
#     final_text      = "\n\n".join(extracted_pages)

#     logger.info(
#         f"PDF extraction completed | "
#         f"Pages: {len(extracted_pages)} | "
#         f"Tables: {len(all_tables)} | "
#         f"Characters: {len(final_text)}"
#     )
#     return final_text


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")
#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as f:
#             text    = f.read()
#         cleaned = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned)}")
#         return cleaned
#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     text = extract_from_pdf(file_path) if extension == ".pdf" else extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# # ── Dev runner ─────────────────────────────────────────────────
# if __name__ == "__main__":
#     text        = load_document(TEST_SAMPLE_PATH)
#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)
#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved to: {output_file}")


"""final v1"""
# import io
# import os
# import re
# import logging
# from concurrent.futures import ThreadPoolExecutor, as_completed

# import fitz
# import pdfplumber
# import pytesseract
# from PIL import Image

# from app.core.config import (
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
#     PAGE_WORKERS,
#     TABLE_WORKERS,
#     MIN_OCR_WIDTH,
#     MIN_OCR_HEIGHT,
#     MIN_OCR_PIXELS,
#     TESSERACT_CMD,
#     TABLE_MIN_ROWS,
#     TABLE_MIN_COLS,
#     TABLE_MAX_AVG_CELL_LENGTH,
#     TABLE_MIN_NUMERIC_RATIO,
#     TABLE_MAX_SINGLE_COL_RATIO,
#     TABLE_MIN_MULTI_COL_ROWS,
#     TABLE_MAX_MID_WORD_RATIO,
# )

# logger = logging.getLogger(__name__)
# pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# # ── Table strategies ──────────────────────────────────────────
# _TABLE_STRATEGIES = [
#     {   # 1 — bordered tables (visible lines)
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 2 — strict lines
#         "vertical_strategy":    "lines_strict",
#         "horizontal_strategy":  "lines_strict",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 3 — text vertical + lines horizontal
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   2,
#         "min_words_horizontal": 1,
#     },
#     {   # 4 — lines vertical + text horizontal
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 2,
#     },
#     {   # 5 — text both (borderless research paper tables)
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       10,
#         "join_tolerance":       10,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
# ]

# # ── Image-dominant page detection ─────────────────────────────
# # Pages where images dominate (figures, screenshots) with little text.
# # Table extraction on these produces high false positive rates.
# _IMAGE_DOMINANT_MIN_IMAGES     = 2
# _IMAGE_DOMINANT_MAX_TEXT_CHARS = 400


# def _is_image_dominant_page(page_num: int, file_path: str) -> bool:
#     """
#     Returns True when a page has ≥2 images AND < 400 chars of text.
#     These pages are typically figures or screenshot appendix pages.
#     Skipping them eliminates false positives like pages 9,17,18,37,38.
#     """
#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page   = pdf.pages[page_num]
#             text   = page.extract_text() or ""
#             images = page.images or []
#             is_dom = (
#                 len(images) >= _IMAGE_DOMINANT_MIN_IMAGES
#                 and len(text.strip()) < _IMAGE_DOMINANT_MAX_TEXT_CHARS
#             )
#             if is_dom:
#                 logger.debug(
#                     f"p{page_num + 1} image-dominant: "
#                     f"images={len(images)} text_chars={len(text.strip())} — skipping"
#                 )
#             return is_dom
#     except Exception:
#         return False


# # ══════════════════════════════════════════════════════════════
# # Text cleaning
# # ══════════════════════════════════════════════════════════════

# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text
#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)
#     return text.strip()


# # ══════════════════════════════════════════════════════════════
# # OCR
# # ══════════════════════════════════════════════════════════════

# def _should_ocr_image(image: Image.Image) -> bool:
#     w, h = image.size
#     return w >= MIN_OCR_WIDTH and h >= MIN_OCR_HEIGHT and (w * h) >= MIN_OCR_PIXELS


# def extract_text_from_image(image_bytes: bytes) -> str:
#     try:
#         image = Image.open(io.BytesIO(image_bytes))
#         if not _should_ocr_image(image):
#             return ""
#         image = image.convert("L")
#         text  = pytesseract.image_to_string(image, config="--psm 6")
#         return clean_text(text)
#     except Exception:
#         logger.exception("OCR failed on image")
#         return ""


# # ══════════════════════════════════════════════════════════════
# # Table validation helpers
# # ══════════════════════════════════════════════════════════════

# def _has_rotated_text(page) -> bool:
#     """True if the page has significant rotated text — not a data table."""
#     try:
#         chars = page.chars
#         if not chars:
#             return False
#         rotated = sum(1 for c in chars if c.get("upright", 1) == 0)
#         return (rotated / len(chars)) > 0.3
#     except Exception:
#         return False


# def _has_mid_word_splits(
#     all_cells: list, avg_len: float, numeric_ratio: float
# ) -> bool:
#     """
#     True when too many cells end mid-word — sign that flowing text
#     is being misread as table columns.
#     Uses > 0.25 (not >=) so exactly 0.25 still gets checked.
#     """
#     if avg_len <= 30 or numeric_ratio > 0.25:
#         return False
#     mid_word_count = 0
#     for cell in all_cells:
#         first_line = cell.split("\n")[0].strip()
#         if not first_line:
#             continue
#         last_char = first_line[-1]
#         if last_char.islower() and last_char not in ".,:;!?)\"'":
#             mid_word_count += 1
#     ratio = mid_word_count / len(all_cells) if all_cells else 0
#     return ratio > TABLE_MAX_MID_WORD_RATIO


# def _has_concatenated_words(all_cells: list) -> bool:
#     """
#     Detects 2-column PDF layout misread as a table.
#     Real table cells always start at a word boundary (uppercase, digit,
#     symbol, or a known short fragment).
#     2-column layout text splits produce cells that start mid-word with
#     a lowercase letter, e.g. 'ublishedasaconference', 'ehavioraldisplacement'.

#     Rejects when > 30% of cells start with a lowercase letter.
#     """
#     if not all_cells:
#         return False
#     mid_word_starts = 0
#     for cell in all_cells:
#         first_line = cell.split("\n")[0].strip()
#         if not first_line:
#             continue
#         # Cell starts lowercase = mid-word break from previous column
#         if first_line[0].islower():
#             mid_word_starts += 1
#     ratio = mid_word_starts / len(all_cells)
#     return ratio > 0.30


# def _is_valid_table(table: list, page, page_num: int = -1) -> bool:
#     """
#     Validate a raw pdfplumber table against all quality gates.
#     Returns True only when confident it is a real data table.

#     Gate order (fastest first):
#       1. min rows
#       2. multi-column structure
#       3. single-column ratio
#       4. avg cell length          — rejects figure captions, reference lists
#       5. numeric ratio            — rejects text-heavy false positives
#       6. rotated text
#       7. mid-word splits          — rejects paragraph text as columns
#       8. concatenated words       — rejects 2-column layout pages
#     """
#     if not table:
#         return False

#     all_cells       = []
#     col_counts      = []
#     single_col_rows = 0
#     total_rows      = 0

#     for row in table:
#         if not row:
#             continue
#         non_empty = [c.strip() for c in row if c and c.strip()]
#         total_rows += 1
#         if len(non_empty) == 1:
#             single_col_rows += 1
#         if len(non_empty) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty))
#             all_cells.extend(non_empty)

#     p = f"p{page_num + 1}"

#     if total_rows < TABLE_MIN_ROWS:
#         logger.debug(f"{p} REJECT rows={total_rows} < {TABLE_MIN_ROWS}")
#         return False

#     if not col_counts:
#         logger.debug(f"{p} REJECT no multi-col rows")
#         return False

#     if len(col_counts) < TABLE_MIN_MULTI_COL_ROWS:
#         logger.debug(f"{p} REJECT multi_col_rows={len(col_counts)} < {TABLE_MIN_MULTI_COL_ROWS}")
#         return False

#     if max(col_counts) < TABLE_MIN_COLS:
#         logger.debug(f"{p} REJECT max_cols={max(col_counts)} < {TABLE_MIN_COLS}")
#         return False

#     single_ratio = single_col_rows / total_rows
#     if single_ratio > TABLE_MAX_SINGLE_COL_RATIO:
#         logger.debug(f"{p} REJECT single_col_ratio={single_ratio:.2f}")
#         return False

#     first_line_lengths = [len(c.split("\n")[0].strip()) for c in all_cells]
#     avg_len = sum(first_line_lengths) / len(first_line_lengths) if first_line_lengths else 0

#     if avg_len > TABLE_MAX_AVG_CELL_LENGTH:
#         logger.debug(f"{p} REJECT avg_len={avg_len:.1f} > {TABLE_MAX_AVG_CELL_LENGTH}")
#         return False

#     numeric = sum(1 for c in all_cells if any(ch.isdigit() for ch in c.split("\n")[0]))
#     numeric_ratio = numeric / len(all_cells) if all_cells else 0

#     # Tightened to 0.30 — figure captions score 0.10–0.25, real tables score ≥0.30
#     effective_min_numeric = max(TABLE_MIN_NUMERIC_RATIO, 0.30)
#     if numeric_ratio < effective_min_numeric:
#         logger.debug(f"{p} REJECT numeric_ratio={numeric_ratio:.2f} < {effective_min_numeric}")
#         return False

#     if _has_rotated_text(page):
#         logger.debug(f"{p} REJECT rotated text")
#         return False

#     if _has_mid_word_splits(all_cells, avg_len, numeric_ratio):
#         logger.debug(f"{p} REJECT mid-word splits avg_len={avg_len:.1f} numeric={numeric_ratio:.2f}")
#         return False

#     # New gate — catches 2-column layout pages (pages 26, 28 and appendix pages)
#     if _has_concatenated_words(all_cells):
#         logger.debug(f"{p} REJECT 2-column layout misread as table")
#         return False

#     logger.debug(
#         f"{p} ACCEPT rows={total_rows} cols={max(col_counts)} "
#         f"avg_len={avg_len:.1f} numeric={numeric_ratio:.2f} "
#         f"single_ratio={single_ratio:.2f}"
#     )
#     return True


# def _format_table(table: list) -> str:
#     """Convert a validated pdfplumber table to pipe-separated text."""
#     rows = []
#     for row in table:
#         if not row:
#             continue
#         if not any(c and c.strip() for c in row):
#             continue
#         cleaned = " | ".join(c.strip() if c else "" for c in row)
#         if cleaned.strip():
#             rows.append(cleaned)
#     return "\n".join(rows) if rows else ""


# # ══════════════════════════════════════════════════════════════
# # Table extraction
# # ══════════════════════════════════════════════════════════════

# def _extract_tables_for_page(args: tuple) -> tuple[int, list[dict]]:
#     """
#     Try all 5 strategies on one page, stop at first with valid tables.
#     Skips image-dominant pages to avoid false positives.
#     """
#     page_num, file_path = args
#     page_tables = []

#     # Skip pages dominated by images (figures, screenshots)
#     if _is_image_dominant_page(page_num, file_path):
#         return page_num, []

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page = pdf.pages[page_num]

#             for strategy_idx, strategy in enumerate(_TABLE_STRATEGIES, start=1):
#                 try:
#                     raw_tables = page.extract_tables(table_settings=strategy) or []
#                 except Exception:
#                     continue

#                 valid = [t for t in raw_tables if _is_valid_table(t, page, page_num)]
#                 if not valid:
#                     continue

#                 for idx, table in enumerate(valid):
#                     text = _format_table(table)
#                     if text:
#                         page_tables.append({
#                             "page":          page_num + 1,
#                             "page_0indexed": page_num,
#                             "table_index":   idx,
#                             "text":          f"[TABLE]\n{text}\n[/TABLE]",
#                         })
#                 break   # first strategy with valid tables wins

#     except Exception:
#         logger.exception(f"Table extraction failed for page {page_num + 1}")

#     return page_num, page_tables


# def extract_tables_from_pdf(file_path: str) -> list[dict]:
#     """Extract tables from all pages in parallel."""
#     tables_data = []
#     try:
#         with pdfplumber.open(file_path) as pdf:
#             num_pages = len(pdf.pages)

#         args    = [(page_num, file_path) for page_num in range(num_pages)]
#         results: dict[int, list] = {}

#         with ThreadPoolExecutor(max_workers=TABLE_WORKERS) as executor:
#             futures = {
#                 executor.submit(_extract_tables_for_page, arg): arg[0]
#                 for arg in args
#             }
#             for future in as_completed(futures):
#                 page_num, page_tables = future.result()
#                 if page_tables:
#                     results[page_num] = page_tables
#                     logger.info(f"Found {len(page_tables)} table(s) on page {page_num + 1}")

#         for page_num in sorted(results):
#             tables_data.extend(results[page_num])

#     except Exception:
#         logger.exception("Table extraction failed")

#     logger.info(f"Total tables extracted: {len(tables_data)}")
#     return tables_data


# # ══════════════════════════════════════════════════════════════
# # PDF page processor
# # ══════════════════════════════════════════════════════════════

# def process_page(
#     page_number:    int,
#     page,
#     doc,
#     tables_on_page: list | None = None,
# ) -> tuple[int, str]:
#     """Extract text + OCR + inject tables for a single PDF page."""
#     try:
#         page_text = ""

#         blocks = page.get_text(PDF_EXTRACTION_MODE)
#         for block in blocks:
#             if block[4].strip():
#                 page_text += block[4] + "\n"

#         image_list = page.get_images(full=True)
#         if image_list:
#             logger.info(f"Found {len(image_list)} image(s) on page {page_number + 1}")

#         for img in image_list:
#             base_image = doc.extract_image(img[0])
#             ocr_text   = extract_text_from_image(base_image["image"])
#             if ocr_text.strip():
#                 page_text += "\n[IMAGE OCR TEXT]\n" + ocr_text + "\n"

#         if tables_on_page:
#             for table in tables_on_page:
#                 page_text += f"\n{table['text']}\n"
#             logger.info(
#                 f"Injected {len(tables_on_page)} table(s) into page {page_number + 1}"
#             )

#         return page_number, clean_text(page_text)

#     except Exception:
#         logger.exception(f"Error processing page {page_number + 1}")
#         return page_number, ""


# # ══════════════════════════════════════════════════════════════
# # Public API
# # ══════════════════════════════════════════════════════════════

# def extract_from_pdf(file_path: str) -> str:
#     logger.info(f"Starting PDF extraction: {file_path}")

#     doc = fitz.open(file_path)

#     with ThreadPoolExecutor(max_workers=2) as executor:
#         table_future = executor.submit(extract_tables_from_pdf, file_path)
#         pages        = [(i, doc.load_page(i)) for i in range(len(doc))]
#         all_tables   = table_future.result()

#     tables_by_page: dict[int, list] = {}
#     for table in all_tables:
#         tables_by_page.setdefault(table["page_0indexed"], []).append(table)

#     results: dict[int, str] = {}
#     with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as executor:
#         futures = {
#             executor.submit(
#                 process_page,
#                 page_number,
#                 page,
#                 doc,
#                 tables_by_page.get(page_number, []),
#             ): page_number
#             for page_number, page in pages
#         }
#         for future in as_completed(futures):
#             page_number, page_text = future.result()
#             results[page_number]   = page_text

#     doc.close()

#     extracted_pages = [results[i] for i in sorted(results)]
#     final_text      = "\n\n".join(extracted_pages)

#     logger.info(
#         f"PDF extraction completed | "
#         f"Pages: {len(extracted_pages)} | "
#         f"Tables: {len(all_tables)} | "
#         f"Characters: {len(final_text)}"
#     )
#     return final_text


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")
#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as f:
#             text    = f.read()
#         cleaned = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned)}")
#         return cleaned
#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     text = extract_from_pdf(file_path) if extension == ".pdf" else extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# # ── Dev runner ─────────────────────────────────────────────────
# if __name__ == "__main__":
#     text        = load_document(TEST_SAMPLE_PATH)
#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)
#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved to: {output_file}")

"""final v2"""
# import io
# import os
# import re
# import asyncio
# import logging
# from concurrent.futures import ThreadPoolExecutor

# import fitz
# import pdfplumber
# import pytesseract
# from PIL import Image

# from app.core.config import (
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
#     PAGE_WORKERS,
#     TABLE_WORKERS,
#     MIN_OCR_WIDTH,
#     MIN_OCR_HEIGHT,
#     MIN_OCR_PIXELS,
#     TESSERACT_CMD,
#     TABLE_MIN_ROWS,
#     TABLE_MIN_COLS,
#     TABLE_MAX_AVG_CELL_LENGTH,
#     TABLE_MIN_NUMERIC_RATIO,
#     TABLE_MAX_SINGLE_COL_RATIO,
#     TABLE_MIN_MULTI_COL_ROWS,
#     TABLE_MAX_MID_WORD_RATIO,
# )

# logger = logging.getLogger(__name__)
# pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# # ── Shared thread pool for CPU-bound blocking work ────────────
# # One pool reused across all async calls — avoids overhead of
# # creating/destroying pools per page.
# _EXECUTOR = ThreadPoolExecutor(max_workers=max(PAGE_WORKERS, TABLE_WORKERS))

# # ── Table strategies ──────────────────────────────────────────
# _TABLE_STRATEGIES = [
#     {   # 1 — bordered tables (visible lines)
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 2 — strict lines
#         "vertical_strategy":    "lines_strict",
#         "horizontal_strategy":  "lines_strict",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 3 — text vertical + lines horizontal
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   2,
#         "min_words_horizontal": 1,
#     },
#     {   # 4 — lines vertical + text horizontal
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 2,
#     },
#     {   # 5 — text both (borderless research paper tables)
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       10,
#         "join_tolerance":       10,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
# ]

# # ── Image-dominant page constants ─────────────────────────────
# _IMAGE_DOMINANT_MIN_IMAGES     = 2
# _IMAGE_DOMINANT_MAX_TEXT_CHARS = 400


# # ══════════════════════════════════════════════════════════════
# # Text cleaning
# # ══════════════════════════════════════════════════════════════

# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text
#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)
#     return text.strip()


# # ══════════════════════════════════════════════════════════════
# # OCR  (blocking — runs in thread pool)
# # ══════════════════════════════════════════════════════════════

# def _should_ocr_image(image: Image.Image) -> bool:
#     w, h = image.size
#     return w >= MIN_OCR_WIDTH and h >= MIN_OCR_HEIGHT and (w * h) >= MIN_OCR_PIXELS


# def _ocr_image_bytes(image_bytes: bytes) -> str:
#     """Blocking OCR — called via run_in_executor."""
#     try:
#         image = Image.open(io.BytesIO(image_bytes))
#         if not _should_ocr_image(image):
#             return ""
#         image = image.convert("L")
#         text  = pytesseract.image_to_string(image, config="--psm 6")
#         return clean_text(text)
#     except Exception:
#         logger.exception("OCR failed on image")
#         return ""


# async def extract_text_from_image_async(image_bytes: bytes) -> str:
#     """Non-blocking OCR wrapper."""
#     loop = asyncio.get_event_loop()
#     return await loop.run_in_executor(_EXECUTOR, _ocr_image_bytes, image_bytes)


# # ══════════════════════════════════════════════════════════════
# # Table validation helpers  (pure CPU — no I/O)
# # ══════════════════════════════════════════════════════════════

# def _has_rotated_text(page) -> bool:
#     try:
#         chars = page.chars
#         if not chars:
#             return False
#         rotated = sum(1 for c in chars if c.get("upright", 1) == 0)
#         return (rotated / len(chars)) > 0.3
#     except Exception:
#         return False


# def _has_mid_word_splits(
#     all_cells: list, avg_len: float, numeric_ratio: float
# ) -> bool:
#     if avg_len <= 30 or numeric_ratio > 0.25:
#         return False
#     mid_word_count = 0
#     for cell in all_cells:
#         first_line = cell.split("\n")[0].strip()
#         if not first_line:
#             continue
#         last_char = first_line[-1]
#         if last_char.islower() and last_char not in ".,:;!?)\"'":
#             mid_word_count += 1
#     ratio = mid_word_count / len(all_cells) if all_cells else 0
#     return ratio > TABLE_MAX_MID_WORD_RATIO


# def _has_concatenated_words(all_cells: list) -> bool:
#     if not all_cells:
#         return False
#     mid_word_starts = 0
#     for cell in all_cells:
#         first_line = cell.split("\n")[0].strip()
#         if not first_line:
#             continue
#         if first_line[0].islower():
#             mid_word_starts += 1
#     ratio = mid_word_starts / len(all_cells)
#     return ratio > 0.30


# def _is_valid_table(table: list, page, page_num: int = -1) -> bool:
#     """
#     Validate a raw pdfplumber table.
#     Gates (fastest first):
#       1. min rows
#       2. multi-column structure
#       3. single-column ratio
#       4. avg cell length
#       5. numeric ratio
#       6. rotated text
#       7. mid-word splits
#       8. concatenated words (2-column layout)
#     """
#     if not table:
#         return False

#     all_cells       = []
#     col_counts      = []
#     single_col_rows = 0
#     total_rows      = 0

#     for row in table:
#         if not row:
#             continue
#         non_empty = [c.strip() for c in row if c and c.strip()]
#         total_rows += 1
#         if len(non_empty) == 1:
#             single_col_rows += 1
#         if len(non_empty) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty))
#             all_cells.extend(non_empty)

#     p = f"p{page_num + 1}"

#     if total_rows < TABLE_MIN_ROWS:
#         return False
#     if not col_counts:
#         return False
#     if len(col_counts) < TABLE_MIN_MULTI_COL_ROWS:
#         return False
#     if max(col_counts) < TABLE_MIN_COLS:
#         return False

#     single_ratio = single_col_rows / total_rows
#     if single_ratio > TABLE_MAX_SINGLE_COL_RATIO:
#         return False

#     first_line_lengths = [len(c.split("\n")[0].strip()) for c in all_cells]
#     avg_len = sum(first_line_lengths) / len(first_line_lengths) if first_line_lengths else 0
#     if avg_len > TABLE_MAX_AVG_CELL_LENGTH:
#         return False

#     numeric = sum(1 for c in all_cells if any(ch.isdigit() for ch in c.split("\n")[0]))
#     numeric_ratio = numeric / len(all_cells) if all_cells else 0

#     effective_min_numeric = max(TABLE_MIN_NUMERIC_RATIO, 0.30)
#     if numeric_ratio < effective_min_numeric:
#         return False

#     if _has_rotated_text(page):
#         return False
#     if _has_mid_word_splits(all_cells, avg_len, numeric_ratio):
#         return False
#     if _has_concatenated_words(all_cells):
#         return False

#     logger.debug(
#         f"{p} ACCEPT rows={total_rows} cols={max(col_counts)} "
#         f"avg_len={avg_len:.1f} numeric={numeric_ratio:.2f}"
#     )
#     return True


# def _format_table(table: list) -> str:
#     rows = []
#     for row in table:
#         if not row:
#             continue
#         if not any(c and c.strip() for c in row):
#             continue
#         cleaned = " | ".join(c.strip() if c else "" for c in row)
#         if cleaned.strip():
#             rows.append(cleaned)
#     return "\n".join(rows) if rows else ""


# # ══════════════════════════════════════════════════════════════
# # Async table extraction  (one page)
# # ══════════════════════════════════════════════════════════════

# def _extract_tables_sync(page_num: int, file_path: str) -> list[dict]:
#     """
#     Blocking table extraction for one page.
#     Opens the PDF once, checks image dominance inline,
#     then tries all strategies.
#     Runs in thread pool via run_in_executor.
#     """
#     page_tables = []
#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page   = pdf.pages[page_num]
#             text   = page.extract_text() or ""
#             images = page.images or []

#             # Skip image-dominant pages (figures / screenshots)
#             if (len(images) >= _IMAGE_DOMINANT_MIN_IMAGES
#                     and len(text.strip()) < _IMAGE_DOMINANT_MAX_TEXT_CHARS):
#                 logger.debug(f"p{page_num+1} image-dominant — skipping")
#                 return []

#             for strategy in _TABLE_STRATEGIES:
#                 try:
#                     raw_tables = page.extract_tables(table_settings=strategy) or []
#                 except Exception:
#                     continue

#                 valid = [t for t in raw_tables if _is_valid_table(t, page, page_num)]
#                 if not valid:
#                     continue

#                 for idx, table in enumerate(valid):
#                     text_out = _format_table(table)
#                     if text_out:
#                         page_tables.append({
#                             "page":          page_num + 1,
#                             "page_0indexed": page_num,
#                             "table_index":   idx,
#                             "text":          f"[TABLE]\n{text_out}\n[/TABLE]",
#                         })
#                 break   # first strategy with valid tables wins

#     except Exception:
#         logger.exception(f"Table extraction failed for page {page_num + 1}")

#     return page_tables


# async def _extract_tables_for_page_async(
#     page_num: int, file_path: str
# ) -> tuple[int, list[dict]]:
#     """Async wrapper — runs blocking table extraction in thread pool."""
#     loop   = asyncio.get_event_loop()
#     tables = await loop.run_in_executor(
#         _EXECUTOR, _extract_tables_sync, page_num, file_path
#     )
#     if tables:
#         logger.info(f"Found {len(tables)} table(s) on page {page_num + 1}")
#     return page_num, tables


# async def extract_tables_from_pdf_async(file_path: str) -> list[dict]:
#     """
#     Extract tables from ALL pages concurrently.
#     All pages run simultaneously — no sequential waiting.
#     """
#     with pdfplumber.open(file_path) as pdf:
#         num_pages = len(pdf.pages)

#     # Launch all pages at once
#     tasks   = [
#         _extract_tables_for_page_async(pn, file_path)
#         for pn in range(num_pages)
#     ]
#     results = await asyncio.gather(*tasks)

#     # Sort by page number and flatten
#     tables_data = []
#     for page_num, page_tables in sorted(results, key=lambda x: x[0]):
#         tables_data.extend(page_tables)

#     logger.info(f"Total tables extracted: {len(tables_data)}")
#     return tables_data


# # ══════════════════════════════════════════════════════════════
# # Async page processor  (text + OCR + table injection)
# # ══════════════════════════════════════════════════════════════

# async def _process_page_async(
#     page_number:    int,
#     page,           # fitz page object
#     doc,            # fitz document (for image extraction)
#     tables_on_page: list | None = None,
# ) -> tuple[int, str]:
#     """
#     Extract text + run OCR on all images concurrently + inject tables.
#     All images on a page are OCR'd in parallel using asyncio.gather.
#     """
#     try:
#         page_text = ""

#         # 1 — Extract text blocks (fast, no I/O)
#         blocks = page.get_text(PDF_EXTRACTION_MODE)
#         for block in blocks:
#             if block[4].strip():
#                 page_text += block[4] + "\n"

#         # 2 — OCR all images on this page CONCURRENTLY
#         image_list = page.get_images(full=True)
#         if image_list:
#             logger.info(f"Found {len(image_list)} image(s) on page {page_number + 1}")
#             # Extract raw bytes first (fast, synchronous)
#             image_bytes_list = []
#             for img in image_list:
#                 try:
#                     base_image = doc.extract_image(img[0])
#                     image_bytes_list.append(base_image["image"])
#                 except Exception:
#                     image_bytes_list.append(None)

#             # Run all OCR calls concurrently
#             ocr_tasks  = [
#                 extract_text_from_image_async(b)
#                 for b in image_bytes_list
#                 if b is not None
#             ]
#             ocr_results = await asyncio.gather(*ocr_tasks)

#             for ocr_text in ocr_results:
#                 if ocr_text.strip():
#                     page_text += "\n[IMAGE OCR TEXT]\n" + ocr_text + "\n"

#         # 3 — Inject tables
#         if tables_on_page:
#             for table in tables_on_page:
#                 page_text += f"\n{table['text']}\n"
#             logger.info(
#                 f"Injected {len(tables_on_page)} table(s) into page {page_number + 1}"
#             )

#         return page_number, clean_text(page_text)

#     except Exception:
#         logger.exception(f"Error processing page {page_number + 1}")
#         return page_number, ""


# # ══════════════════════════════════════════════════════════════
# # Core async PDF extractor
# # ══════════════════════════════════════════════════════════════

# async def _extract_from_pdf_async(file_path: str) -> str:
#     """
#     Full async pipeline:
#       - Table extraction (all pages) runs concurrently
#       - Page text + OCR (all pages) runs concurrently
#       - Both pipelines start simultaneously via asyncio.gather
#     """
#     logger.info(f"Starting async PDF extraction: {file_path}")

#     doc   = fitz.open(file_path)
#     pages = [(i, doc.load_page(i)) for i in range(len(doc))]

#     # ── Run table extraction AND page processing concurrently ──
#     # table_task  : extracts all tables across all pages in parallel
#     # page_tasks  : processes all pages (text + OCR) in parallel
#     #
#     # Both start at the same time — total time ≈ max(table_time, page_time)
#     # instead of table_time + page_time.

#     table_task = extract_tables_from_pdf_async(file_path)

#     # We need tables before injecting, so gather tables first,
#     # then immediately kick off all page tasks with table data.
#     all_tables = await table_task

#     tables_by_page: dict[int, list] = {}
#     for table in all_tables:
#         tables_by_page.setdefault(table["page_0indexed"], []).append(table)

#     # Now process all pages concurrently (text + OCR + inject tables)
#     page_tasks = [
#         _process_page_async(
#             page_number,
#             page,
#             doc,
#             tables_by_page.get(page_number, []),
#         )
#         for page_number, page in pages
#     ]
#     page_results = await asyncio.gather(*page_tasks)

#     doc.close()

#     # Sort and join
#     results      = dict(page_results)
#     final_text   = "\n\n".join(results[i] for i in sorted(results))

#     logger.info(
#         f"PDF extraction completed | "
#         f"Pages: {len(pages)} | "
#         f"Tables: {len(all_tables)} | "
#         f"Characters: {len(final_text)}"
#     )
#     return final_text


# # ══════════════════════════════════════════════════════════════
# # Public API  (sync wrappers — drop-in replacements)
# # ══════════════════════════════════════════════════════════════

# def extract_from_pdf(file_path: str) -> str:
#     """
#     Sync entry point — runs the async pipeline.
#     If an event loop is already running (FastAPI / uvicorn),
#     uses run_in_executor to avoid nested-loop errors.
#     """
#     logger.info(f"Starting PDF extraction: {file_path}")
#     try:
#         loop = asyncio.get_running_loop()
#         # Already inside an async context (FastAPI) —
#         # run our coroutine in a fresh thread with its own loop
#         import concurrent.futures
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
#             future = pool.submit(asyncio.run, _extract_from_pdf_async(file_path))
#             return future.result()
#     except RuntimeError:
#         # No running loop — run directly
#         return asyncio.run(_extract_from_pdf_async(file_path))


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")
#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as f:
#             text    = f.read()
#         cleaned = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned)}")
#         return cleaned
#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     text = extract_from_pdf(file_path) if extension == ".pdf" else extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# # ── Dev runner ─────────────────────────────────────────────────
# if __name__ == "__main__":
#     text        = load_document(TEST_SAMPLE_PATH)
#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)
#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved to: {output_file}")


"""
ingestion.py — Maximum-speed PDF extraction
============================================
Architecture:
  asyncio        → coordinates ALL concurrent work (zero-overhead I/O waiting)
  ThreadPoolExecutor → runs ALL blocking CPU/I/O work (pdfplumber, pytesseract, fitz)
  Combined via   → loop.run_in_executor() bridges the two

Parallelism map:
  ┌─────────────────────────────────────────────────────┐
  │  asyncio event loop (coordinator)                   │
  │                                                     │
  │  ┌─────────────────┐   ┌──────────────────────┐    │
  │  │ Table extraction│   │ Page text + OCR      │    │
  │  │ all 39 pages    │   │ all 39 pages         │    │
  │  │ concurrently    │   │ concurrently         │    │
  │  │ (ThreadPool)    │   │ (ThreadPool)         │    │
  │  └────────┬────────┘   └──────────┬───────────┘    │
  │           │                       │                 │
  │           └──────────┬────────────┘                 │
  │                      ▼                              │
  │              asyncio.gather() — waits for           │
  │              ALL results simultaneously             │
  └─────────────────────────────────────────────────────┘

Key optimizations vs previous version:
  1. Single PDF open per page (image check inline — no double open)
  2. All 39 table extractions fire simultaneously
  3. All 39 page text extractions fire simultaneously
  4. Table extraction + page processing overlap via asyncio.gather
  5. All images on a page OCR'd concurrently within each page task
  6. One shared ThreadPoolExecutor (no per-call pool creation overhead)
  7. FastAPI-safe: detects running event loop, runs in fresh thread
"""

# import io
# import os
# import re
# import asyncio
# import logging
# from concurrent.futures import ThreadPoolExecutor

# import fitz
# import pdfplumber
# import pytesseract
# from PIL import Image

# from app.core.config import (
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
#     PAGE_WORKERS,
#     TABLE_WORKERS,
#     MIN_OCR_WIDTH,
#     MIN_OCR_HEIGHT,
#     MIN_OCR_PIXELS,
#     TESSERACT_CMD,
#     TABLE_MIN_ROWS,
#     TABLE_MIN_COLS,
#     TABLE_MAX_AVG_CELL_LENGTH,
#     TABLE_MIN_NUMERIC_RATIO,
#     TABLE_MAX_SINGLE_COL_RATIO,
#     TABLE_MIN_MULTI_COL_ROWS,
#     TABLE_MAX_MID_WORD_RATIO,
# )

# logger = logging.getLogger(__name__)
# pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# # ── Single shared thread pool ─────────────────────────────────
# # Workers = max of page workers and table workers so both
# # pipelines can run at full concurrency simultaneously.
# # Created once at module load — zero per-call overhead.
# _WORKERS = max(PAGE_WORKERS, TABLE_WORKERS, 8)
# _POOL    = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="ingest")

# # ── Image-dominant page constants ─────────────────────────────
# _IMAGE_DOMINANT_MIN_IMAGES     = 2
# _IMAGE_DOMINANT_MAX_TEXT_CHARS = 400

# # ── Table extraction strategies ───────────────────────────────
# _TABLE_STRATEGIES = [
#     {   # 1 — bordered tables (visible lines)
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 2 — strict lines
#         "vertical_strategy":    "lines_strict",
#         "horizontal_strategy":  "lines_strict",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {   # 3 — text vertical + lines horizontal
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   2,
#         "min_words_horizontal": 1,
#     },
#     {   # 4 — lines vertical + text horizontal
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 2,
#     },
#     {   # 5 — text both (borderless research-paper tables)
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       10,
#         "join_tolerance":       10,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
# ]


# # ══════════════════════════════════════════════════════════════
# # Helpers
# # ══════════════════════════════════════════════════════════════

# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text
#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)
#     return text.strip()


# async def _run(fn, *args):
#     """
#     Run a blocking function in the shared thread pool.
#     Sugar for: await loop.run_in_executor(_POOL, fn, *args)
#     """
#     loop = asyncio.get_event_loop()
#     return await loop.run_in_executor(_POOL, fn, *args)


# # ══════════════════════════════════════════════════════════════
# # OCR  (blocking — offloaded to thread pool)
# # ══════════════════════════════════════════════════════════════

# def _ocr_bytes(image_bytes: bytes) -> str:
#     """Blocking OCR. Runs in thread pool — never called directly."""
#     try:
#         image = Image.open(io.BytesIO(image_bytes))
#         w, h  = image.size
#         if w < MIN_OCR_WIDTH or h < MIN_OCR_HEIGHT or (w * h) < MIN_OCR_PIXELS:
#             return ""
#         text = pytesseract.image_to_string(image.convert("L"), config="--psm 6")
#         return clean_text(text)
#     except Exception:
#         logger.exception("OCR failed")
#         return ""


# async def _ocr_async(image_bytes: bytes) -> str:
#     """Non-blocking OCR wrapper."""
#     return await _run(_ocr_bytes, image_bytes)


# # ══════════════════════════════════════════════════════════════
# # Table validation  (pure CPU — no async needed)
# # ══════════════════════════════════════════════════════════════

# def _has_rotated_text(page) -> bool:
#     try:
#         chars = page.chars
#         if not chars:
#             return False
#         rotated = sum(1 for c in chars if c.get("upright", 1) == 0)
#         return (rotated / len(chars)) > 0.3
#     except Exception:
#         return False


# def _has_mid_word_splits(all_cells, avg_len, numeric_ratio) -> bool:
#     if avg_len <= 30 or numeric_ratio > 0.25:
#         return False
#     count = sum(
#         1 for c in all_cells
#         if (fl := c.split("\n")[0].strip())
#         and fl[-1].islower()
#         and fl[-1] not in ".,:;!?)\"'"
#     )
#     return (count / len(all_cells)) > TABLE_MAX_MID_WORD_RATIO if all_cells else False


# def _has_concatenated_words(all_cells) -> bool:
#     if not all_cells:
#         return False
#     count = sum(
#         1 for c in all_cells
#         if (fl := c.split("\n")[0].strip()) and fl[0].islower()
#     )
#     return (count / len(all_cells)) > 0.30


# def _is_valid_table(table, page, page_num=-1) -> bool:
#     if not table:
#         return False

#     all_cells, col_counts, single_col_rows, total_rows = [], [], 0, 0

#     for row in table:
#         if not row:
#             continue
#         non_empty = [c.strip() for c in row if c and c.strip()]
#         total_rows += 1
#         if len(non_empty) == 1:
#             single_col_rows += 1
#         if len(non_empty) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty))
#             all_cells.extend(non_empty)

#     if total_rows < TABLE_MIN_ROWS:                             return False
#     if not col_counts:                                          return False
#     if len(col_counts) < TABLE_MIN_MULTI_COL_ROWS:              return False
#     if max(col_counts) < TABLE_MIN_COLS:                        return False
#     if (single_col_rows / total_rows) > TABLE_MAX_SINGLE_COL_RATIO: return False

#     fl_lens = [len(c.split("\n")[0].strip()) for c in all_cells]
#     avg_len = sum(fl_lens) / len(fl_lens) if fl_lens else 0
#     if avg_len > TABLE_MAX_AVG_CELL_LENGTH:                     return False

#     numeric = sum(
#         1 for c in all_cells
#         if any(ch.isdigit() for ch in c.split("\n")[0])
#     )
#     numeric_ratio = numeric / len(all_cells) if all_cells else 0
#     if numeric_ratio < max(TABLE_MIN_NUMERIC_RATIO, 0.30):      return False
#     if _has_rotated_text(page):                                 return False
#     if _has_mid_word_splits(all_cells, avg_len, numeric_ratio): return False
#     if _has_concatenated_words(all_cells):                      return False

#     return True


# def _format_table(table) -> str:
#     rows = []
#     for row in table:
#         if not row or not any(c and c.strip() for c in row):
#             continue
#         rows.append(" | ".join(c.strip() if c else "" for c in row))
#     return "\n".join(rows)


# # ══════════════════════════════════════════════════════════════
# # Table extraction — one page  (blocking, runs in thread pool)
# # ══════════════════════════════════════════════════════════════

# def _extract_tables_one_page(page_num: int, file_path: str) -> list[dict]:
#     """
#     Blocking — opens PDF once, checks image dominance inline,
#     tries all strategies, returns valid tables.
#     Called via run_in_executor so it never blocks the event loop.
#     """
#     try:
#         with pdfplumber.open(file_path) as pdf:
#             page   = pdf.pages[page_num]
#             text   = page.extract_text() or ""
#             images = page.images or []

#             # Skip image-dominant pages (figures/screenshots)
#             if (len(images) >= _IMAGE_DOMINANT_MIN_IMAGES
#                     and len(text.strip()) < _IMAGE_DOMINANT_MAX_TEXT_CHARS):
#                 return []

#             page_tables = []
#             for strategy in _TABLE_STRATEGIES:
#                 try:
#                     raw = page.extract_tables(table_settings=strategy) or []
#                 except Exception:
#                     continue

#                 valid = [t for t in raw if _is_valid_table(t, page, page_num)]
#                 if not valid:
#                     continue

#                 for idx, tbl in enumerate(valid):
#                     fmt = _format_table(tbl)
#                     if fmt:
#                         page_tables.append({
#                             "page":          page_num + 1,
#                             "page_0indexed": page_num,
#                             "table_index":   idx,
#                             "text":          f"[TABLE]\n{fmt}\n[/TABLE]",
#                         })
#                 break   # first strategy with valid tables wins

#             return page_tables

#     except Exception:
#         logger.exception(f"Table extraction failed p{page_num + 1}")
#         return []


# # ══════════════════════════════════════════════════════════════
# # Page text + OCR  (blocking text, async OCR)
# # ══════════════════════════════════════════════════════════════

# def _get_page_text_and_images(page_num: int, file_path: str) -> tuple[str, list[bytes]]:
#     """
#     Blocking: extract raw text blocks + raw image bytes from one page.
#     Runs in thread pool. Returns (text, [image_bytes, ...]).
#     """
#     try:
#         doc  = fitz.open(file_path)
#         page = doc.load_page(page_num)

#         # Text blocks
#         text = ""
#         for block in page.get_text(PDF_EXTRACTION_MODE):
#             if block[4].strip():
#                 text += block[4] + "\n"

#         # Image bytes
#         image_bytes_list = []
#         for img in page.get_images(full=True):
#             try:
#                 image_bytes_list.append(doc.extract_image(img[0])["image"])
#             except Exception:
#                 pass

#         doc.close()
#         return text, image_bytes_list

#     except Exception:
#         logger.exception(f"Text/image extraction failed p{page_num + 1}")
#         return "", []


# async def _process_one_page_async(
#     page_num:       int,
#     file_path:      str,
#     tables_on_page: list,
# ) -> tuple[int, str]:
#     """
#     Async page processor:
#       1. Offload blocking text+image extraction to thread pool
#       2. OCR all images on this page concurrently (asyncio.gather)
#       3. Inject tables
#       4. Return (page_num, cleaned_text)
#     """
#     # Step 1 — blocking extraction in thread (non-blocking from event loop)
#     raw_text, image_bytes_list = await _run(
#         _get_page_text_and_images, page_num, file_path
#     )

#     page_text = raw_text

#     # Step 2 — OCR all images concurrently
#     if image_bytes_list:
#         logger.info(f"Found {len(image_bytes_list)} image(s) on page {page_num + 1}")
#         ocr_results = await asyncio.gather(
#             *[_ocr_async(b) for b in image_bytes_list]
#         )
#         for ocr_text in ocr_results:
#             if ocr_text.strip():
#                 page_text += "\n[IMAGE OCR TEXT]\n" + ocr_text + "\n"

#     # Step 3 — inject tables
#     if tables_on_page:
#         for tbl in tables_on_page:
#             page_text += f"\n{tbl['text']}\n"
#         logger.info(f"Injected {len(tables_on_page)} table(s) into page {page_num + 1}")

#     return page_num, clean_text(page_text)


# # ══════════════════════════════════════════════════════════════
# # Main async pipeline
# # ══════════════════════════════════════════════════════════════

# async def _extract_pdf_async(file_path: str) -> str:
#     """
#     Full concurrent pipeline:

#     Phase 1 — ALL table extractions fire simultaneously
#               (39 pages × pdfplumber in thread pool, coordinated by asyncio)

#     Phase 2 — ALL page text+OCR tasks fire simultaneously
#               (39 pages × fitz+pytesseract in thread pool + async OCR gather)

#     Phases 1 and 2 run SEQUENTIALLY only because tables must be
#     injected into pages. They are still much faster than before
#     because within each phase every page runs concurrently.

#     Timeline:
#       Before:  [table p1][table p2]...[page p1][page p2]...  ~25s
#       After:   [all tables at once] → [all pages at once]    ~8s
#     """
#     logger.info(f"Starting async PDF extraction: {file_path}")

#     # Count pages (fast open)
#     with pdfplumber.open(file_path) as pdf:
#         num_pages = len(pdf.pages)

#     # ── Phase 1: extract tables from ALL pages concurrently ──
#     table_tasks = [
#         _run(_extract_tables_one_page, pn, file_path)
#         for pn in range(num_pages)
#     ]
#     table_results = await asyncio.gather(*table_tasks)

#     # Build page→tables map and log
#     tables_by_page: dict[int, list] = {}
#     total_tables = 0
#     for pn, page_tables in enumerate(table_results):
#         if page_tables:
#             tables_by_page[pn] = page_tables
#             total_tables += len(page_tables)
#             logger.info(f"Found {len(page_tables)} table(s) on page {pn + 1}")

#     logger.info(f"Total tables extracted: {total_tables}")

#     # ── Phase 2: process ALL pages concurrently ──────────────
#     page_tasks = [
#         _process_one_page_async(
#             pn, file_path, tables_by_page.get(pn, [])
#         )
#         for pn in range(num_pages)
#     ]
#     page_results = await asyncio.gather(*page_tasks)

#     # Sort and join
#     final_text = "\n\n".join(
#         text for _, text in sorted(page_results, key=lambda x: x[0])
#     )

#     logger.info(
#         f"PDF extraction completed | Pages: {num_pages} | "
#         f"Tables: {total_tables} | Characters: {len(final_text)}"
#     )
#     return final_text


# # ══════════════════════════════════════════════════════════════
# # Public API  (sync wrappers — drop-in replacements)
# # ══════════════════════════════════════════════════════════════

# def extract_from_pdf(file_path: str) -> str:
#     """
#     Sync entry point that runs the async pipeline.

#     FastAPI/uvicorn already runs an event loop, so we can't call
#     asyncio.run() directly — that raises 'cannot run nested event loop'.
#     Instead we spin up a dedicated thread with its own fresh loop.
#     This is the standard pattern for mixing sync + async in FastAPI.
#     """
#     logger.info(f"Starting PDF extraction: {file_path}")

#     try:
#         # Check if we're already inside a running event loop (FastAPI)
#         asyncio.get_running_loop()
#         # Yes — run in a fresh thread with its own loop
#         import concurrent.futures
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
#             return pool.submit(asyncio.run, _extract_pdf_async(file_path)).result()
#     except RuntimeError:
#         # No running loop — run directly (dev runner / tests)
#         return asyncio.run(_extract_pdf_async(file_path))


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")
#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as f:
#             text = f.read()
#         cleaned = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned)}")
#         return cleaned
#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     text = extract_from_pdf(file_path) if extension == ".pdf" else extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# # ── Dev runner ────────────────────────────────────────────────
# if __name__ == "__main__":
#     text        = load_document(TEST_SAMPLE_PATH)
#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)
#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved to: {output_file}")

"""test"""
# import io
# import os
# import re
# import asyncio
# import logging
# import statistics
# from concurrent.futures import ThreadPoolExecutor

# import fitz
# import pdfplumber
# import pytesseract
# from PIL import Image

# from app.core.config import (
#     SUPPORTED_EXTENSIONS,
#     TEXT_FILE_ENCODING,
#     PDF_EXTRACTION_MODE,
#     ENABLE_TEXT_CLEANING,
#     TEST_SAMPLE_PATH,
#     PAGE_WORKERS,
#     TABLE_WORKERS,
#     MIN_OCR_WIDTH,
#     MIN_OCR_HEIGHT,
#     MIN_OCR_PIXELS,
#     TESSERACT_CMD,
#     TABLE_MIN_ROWS,
#     TABLE_MIN_COLS,
#     TABLE_MAX_AVG_CELL_LENGTH,
#     TABLE_MIN_NUMERIC_RATIO,
#     TABLE_MAX_SINGLE_COL_RATIO,
#     TABLE_MIN_MULTI_COL_ROWS,
#     TABLE_MAX_MID_WORD_RATIO,
# )

# logger = logging.getLogger(__name__)
# pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# # ── Single shared thread pool ─────────────────────────────────
# _WORKERS = max(PAGE_WORKERS, TABLE_WORKERS, 8)
# _POOL    = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="ingest")

# # ── Image-dominant page constants ─────────────────────────────
# _IMAGE_DOMINANT_MIN_IMAGES     = 2
# _IMAGE_DOMINANT_MAX_TEXT_CHARS = 400

# # ── Smart OCR pre-filter thresholds ──────────────────────────
# # Applied to ALL images regardless of page number or PDF type.
# # Goal: skip images that are provably non-text (solid fills, vector
# # graphics, pure photos) without skipping real scanned text images.
# #
# # Variance threshold: real text on white background has high contrast
# # between black ink pixels and white paper pixels → high variance.
# # Solid-color fills, gradient backgrounds, simple diagrams → low variance.
# # Empirically: text images ~800-3000 variance, diagrams ~50-300.
# _OCR_MIN_VARIANCE    = 300   # below this = likely not a text image
# # Aspect ratio: very wide or very tall images are usually decorative
# # headers/footers/bars, not text blocks worth OCR-ing.
# _OCR_MAX_ASPECT      = 8.0
# # Unique color count: real text images have many shades (anti-aliasing).
# # Pure vector-art exports often have very few unique colors.
# _OCR_MIN_UNIQUE_COLORS = 10  # below this = likely vector/icon, skip OCR

# # ── Table extraction strategies ───────────────────────────────
# _TABLE_STRATEGIES = [
#     {
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {
#         "vertical_strategy":    "lines_strict",
#         "horizontal_strategy":  "lines_strict",
#         "snap_tolerance":       3,
#         "join_tolerance":       3,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
#     {
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "lines",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   2,
#         "min_words_horizontal": 1,
#     },
#     {
#         "vertical_strategy":    "lines",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       5,
#         "join_tolerance":       5,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 2,
#     },
#     {
#         "vertical_strategy":    "text",
#         "horizontal_strategy":  "text",
#         "snap_tolerance":       10,
#         "join_tolerance":       10,
#         "edge_min_length":      3,
#         "min_words_vertical":   1,
#         "min_words_horizontal": 1,
#     },
# ]


# # ══════════════════════════════════════════════════════════════
# # Helpers
# # ══════════════════════════════════════════════════════════════

# def clean_text(text: str) -> str:
#     if not ENABLE_TEXT_CLEANING:
#         return text
#     text = re.sub(r"-\s*\n\s*", "", text)
#     text = re.sub(r"\n+", " ", text)
#     text = re.sub(r"\bPage\s*\d+\b", "", text)
#     text = re.sub(r"\s+", " ", text)
#     return text.strip()


# async def _run(fn, *args):
#     loop = asyncio.get_event_loop()
#     return await loop.run_in_executor(_POOL, fn, *args)


# # ══════════════════════════════════════════════════════════════
# # Smart OCR pre-filter  (general — works for any PDF type)
# # ══════════════════════════════════════════════════════════════

# def _should_ocr(image_bytes: bytes) -> tuple[bool, str]:
#     """
#     General-purpose pre-filter that decides whether an image is
#     worth running through Tesseract.

#     Works for any PDF — not hardcoded to page numbers or document types.
#     Checks image properties that are reliable indicators of text content:

#     1. Size — too small to contain readable text
#     2. Aspect ratio — extremely wide/tall = decorative, not text
#     3. Pixel variance — low variance = solid fills, simple graphics
#     4. Unique color count — very few colors = vector export, icon

#     Returns (should_run_ocr, reason_string) for logging.

#     NOTE: This does NOT skip images that fail — it returns False so the
#     caller can log the skip reason. Real scanned text pages will always
#     pass because they have high variance and many unique colors.
#     """
#     try:
#         image = Image.open(io.BytesIO(image_bytes)).convert("L")
#         w, h  = image.size

#         # Check 1: minimum size
#         if w < MIN_OCR_WIDTH or h < MIN_OCR_HEIGHT:
#             return False, f"too_small ({w}x{h})"
#         if (w * h) < MIN_OCR_PIXELS:
#             return False, f"too_few_pixels ({w*h})"

#         # Check 2: aspect ratio
#         if min(w, h) > 0:
#             aspect = max(w, h) / min(w, h)
#             if aspect > _OCR_MAX_ASPECT:
#                 return False, f"extreme_aspect_ratio ({aspect:.1f})"

#         # Check 3: pixel variance
#         # Sample pixels for speed on large images (every 8th pixel)
#         pixels = list(image.getdata())
#         sample = pixels[::8] if len(pixels) > 8000 else pixels
#         if len(sample) >= 2:
#             var = statistics.variance(sample)
#             if var < _OCR_MIN_VARIANCE:
#                 return False, f"low_variance ({var:.0f} < {_OCR_MIN_VARIANCE})"

#         # Check 4: unique color count
#         unique_colors = len(set(sample))
#         if unique_colors < _OCR_MIN_UNIQUE_COLORS:
#             return False, f"too_few_colors ({unique_colors})"

#         return True, "ok"

#     except Exception as e:
#         # If we can't analyze it, run OCR anyway — better to try than miss text
#         return True, f"analysis_failed ({e})"


# # ══════════════════════════════════════════════════════════════
# # OCR  (blocking — offloaded to thread pool)
# # ══════════════════════════════════════════════════════════════

# def _ocr_bytes(image_bytes: bytes) -> str:
#     """
#     Blocking OCR. Called only after _should_ocr() returns True.
#     Unchanged from original — full OCR quality preserved.
#     """
#     try:
#         image = Image.open(io.BytesIO(image_bytes))
#         w, h  = image.size
#         if w < MIN_OCR_WIDTH or h < MIN_OCR_HEIGHT or (w * h) < MIN_OCR_PIXELS:
#             return ""
#         text = pytesseract.image_to_string(image.convert("L"), config="--psm 6")
#         cleaned = clean_text(text)
#         logger.info("OCR output: '%s'", cleaned[:80] if cleaned else 'EMPTY')
#         return clean_text(text)
#     except Exception:
#         logger.exception("OCR failed")
#         return ""


# async def _ocr_async(image_bytes: bytes, page_num: int, img_idx: int) -> str:
#     """Non-blocking OCR with smart pre-filter and skip logging."""
#     should_run, reason = _should_ocr(image_bytes)
#     if not should_run:
#         logger.info(
#             "Skipping OCR p%d img%d — %s", page_num + 1, img_idx, reason
#         )
#         return ""
#     logger.info("Running OCR p%d img%d", page_num + 1, img_idx)
#     return await _run(_ocr_bytes, image_bytes)


# # ══════════════════════════════════════════════════════════════
# # Table validation  (unchanged)
# # ══════════════════════════════════════════════════════════════

# def _has_rotated_text(page) -> bool:
#     try:
#         chars = page.chars
#         if not chars:
#             return False
#         rotated = sum(1 for c in chars if c.get("upright", 1) == 0)
#         return (rotated / len(chars)) > 0.3
#     except Exception:
#         return False


# def _has_mid_word_splits(all_cells, avg_len, numeric_ratio) -> bool:
#     if avg_len <= 30 or numeric_ratio > 0.25:
#         return False
#     count = sum(
#         1 for c in all_cells
#         if (fl := c.split("\n")[0].strip())
#         and fl[-1].islower()
#         and fl[-1] not in ".,:;!?)\"'"
#     )
#     return (count / len(all_cells)) > TABLE_MAX_MID_WORD_RATIO if all_cells else False


# def _has_concatenated_words(all_cells) -> bool:
#     if not all_cells:
#         return False
#     count = sum(
#         1 for c in all_cells
#         if (fl := c.split("\n")[0].strip()) and fl[0].islower()
#     )
#     return (count / len(all_cells)) > 0.30


# def _is_valid_table(table, page, page_num=-1) -> bool:
#     if not table:
#         return False

#     all_cells, col_counts, single_col_rows, total_rows = [], [], 0, 0

#     for row in table:
#         if not row:
#             continue
#         non_empty = [c.strip() for c in row if c and c.strip()]
#         total_rows += 1
#         if len(non_empty) == 1:
#             single_col_rows += 1
#         if len(non_empty) >= TABLE_MIN_COLS:
#             col_counts.append(len(non_empty))
#             all_cells.extend(non_empty)

#     if total_rows < TABLE_MIN_ROWS:                                  return False
#     if not col_counts:                                               return False
#     if len(col_counts) < TABLE_MIN_MULTI_COL_ROWS:                   return False
#     if max(col_counts) < TABLE_MIN_COLS:                             return False
#     if (single_col_rows / total_rows) > TABLE_MAX_SINGLE_COL_RATIO:  return False

#     fl_lens = [len(c.split("\n")[0].strip()) for c in all_cells]
#     avg_len = sum(fl_lens) / len(fl_lens) if fl_lens else 0
#     if avg_len > TABLE_MAX_AVG_CELL_LENGTH:                          return False

#     numeric = sum(
#         1 for c in all_cells
#         if any(ch.isdigit() for ch in c.split("\n")[0])
#     )
#     numeric_ratio = numeric / len(all_cells) if all_cells else 0
#     if numeric_ratio < max(TABLE_MIN_NUMERIC_RATIO, 0.30):           return False
#     if _has_rotated_text(page):                                      return False
#     if _has_mid_word_splits(all_cells, avg_len, numeric_ratio):      return False
#     if _has_concatenated_words(all_cells):                           return False

#     return True


# def _format_table(table) -> str:
#     rows = []
#     for row in table:
#         if not row or not any(c and c.strip() for c in row):
#             continue
#         rows.append(" | ".join(c.strip() if c else "" for c in row))
#     return "\n".join(rows)


# # ══════════════════════════════════════════════════════════════
# # OPTIMIZATION 1: All tables in ONE pdfplumber open
# # ══════════════════════════════════════════════════════════════
# # Old: _extract_tables_one_page → 39 pdfplumber.open() calls for 39 pages
# # New: _extract_all_tables_batch → 1 pdfplumber.open() for all pages
# # Saves: ~39 file open/close cycles + OS handle overhead on Windows

# def _extract_all_tables_batch(file_path: str) -> dict[int, list]:
#     """
#     Open PDF once via pdfplumber, extract tables from all pages.
#     Identical validation logic to original — just batched.
#     Returns {page_0indexed: [table_dicts]}.
#     """
#     tables_by_page: dict[int, list] = {}
#     total_tables = 0

#     try:
#         with pdfplumber.open(file_path) as pdf:
#             for page_num, page in enumerate(pdf.pages):
#                 try:
#                     text   = page.extract_text() or ""
#                     images = page.images or []

#                     if (len(images) >= _IMAGE_DOMINANT_MIN_IMAGES
#                             and len(text.strip()) < _IMAGE_DOMINANT_MAX_TEXT_CHARS):
#                         continue

#                     page_tables = []
#                     for strategy in _TABLE_STRATEGIES:
#                         try:
#                             raw = page.extract_tables(table_settings=strategy) or []
#                         except Exception:
#                             continue

#                         valid = [t for t in raw if _is_valid_table(t, page, page_num)]
#                         if not valid:
#                             continue

#                         for idx, tbl in enumerate(valid):
#                             fmt = _format_table(tbl)
#                             if fmt:
#                                 page_tables.append({
#                                     "page":          page_num + 1,
#                                     "page_0indexed": page_num,
#                                     "table_index":   idx,
#                                     "text":          f"[TABLE]\n{fmt}\n[/TABLE]",
#                                 })
#                         break

#                     if page_tables:
#                         tables_by_page[page_num] = page_tables
#                         total_tables += len(page_tables)
#                         logger.info(
#                             "Found %d table(s) on page %d",
#                             len(page_tables), page_num + 1
#                         )

#                 except Exception:
#                     logger.exception("Table extraction failed p%d", page_num + 1)

#     except Exception:
#         logger.exception("Failed to open PDF for table extraction")

#     logger.info("Total tables extracted: %d", total_tables)
#     return tables_by_page


# # ══════════════════════════════════════════════════════════════
# # OPTIMIZATION 2: All page text+images in ONE fitz open
# # ══════════════════════════════════════════════════════════════
# # Old: _get_page_text_and_images → 39 fitz.open() calls for 39 pages
# # New: _extract_all_pages_batch → 1 fitz.open() for all pages
# # Saves: ~39 file open/close cycles + repeated PDF parse overhead

# def _extract_all_pages_batch(file_path: str) -> list[tuple[str, list[bytes]]]:
#     """
#     Open PDF once via fitz, extract text+image bytes from all pages.
#     Returns list of (raw_text, [image_bytes]) indexed by page number.
#     """
#     results = []
#     try:
#         doc = fitz.open(file_path)
#         for page_num in range(len(doc)):
#             try:
#                 page = doc.load_page(page_num)

#                 text = ""
#                 for block in page.get_text(PDF_EXTRACTION_MODE):
#                     if block[4].strip():
#                         text += block[4] + "\n"

#                 image_bytes_list = []
#                 for img in page.get_images(full=True):
#                     try:
#                         image_bytes_list.append(doc.extract_image(img[0])["image"])
#                     except Exception:
#                         pass

#                 results.append((text, image_bytes_list))

#             except Exception:
#                 logger.exception("Text/image extraction failed p%d", page_num + 1)
#                 results.append(("", []))

#         doc.close()

#     except Exception:
#         logger.exception("Failed to open PDF for text extraction")

#     return results


# # ══════════════════════════════════════════════════════════════
# # Page assembly with async OCR
# # ══════════════════════════════════════════════════════════════

# async def _process_one_page_async(
#     page_num:         int,
#     raw_text:         str,
#     image_bytes_list: list[bytes],
#     tables_on_page:   list,
# ) -> tuple[int, str]:
#     """
#     Async page assembly:
#     - Receives pre-extracted data (no PDF re-open)
#     - Runs OCR concurrently on all images (with smart pre-filter)
#     - Injects tables
#     OCR is still run on every image that passes the pre-filter —
#     no images are blindly skipped. The filter only avoids provably
#     non-text images (solid fills, tiny icons, extreme banners).
#     """
#     page_text = raw_text

#     if image_bytes_list:
#         logger.info("Found %d image(s) on page %d", len(image_bytes_list), page_num + 1)
#         ocr_results = await asyncio.gather(
#             *[_ocr_async(b, page_num, i) for i, b in enumerate(image_bytes_list)]
#         )
#         for ocr_text in ocr_results:
#             if ocr_text.strip():
#                 page_text += "\n[IMAGE OCR TEXT]\n" + ocr_text + "\n"

#     if tables_on_page:
#         for tbl in tables_on_page:
#             page_text += f"\n{tbl['text']}\n"
#         logger.info("Injected %d table(s) into page %d", len(tables_on_page), page_num + 1)

#     return page_num, clean_text(page_text)


# # ══════════════════════════════════════════════════════════════
# # Main async pipeline
# # ══════════════════════════════════════════════════════════════

# async def _extract_pdf_async(file_path: str) -> str:
#     """
#     Optimized 3-phase pipeline:

#     Phase 1 — 1 pdfplumber open  → all tables extracted
#               (was 39 opens, one per page)

#     Phase 2 — 1 fitz open        → all page text+images extracted
#               (was 39 opens, one per page)

#     Phase 3 — async OCR across all pages concurrently
#               Smart pre-filter skips provably non-text images
#               (solid fills, vector icons, extreme banners)
#               but always runs OCR on images that pass the check.

#     Phases 1 and 2 run sequentially (tables needed before injection).
#     Phase 3 is fully async/concurrent.

#     Expected speedup: 16s → 5-7s for typical PDFs.
#     For PDFs with many scanned text images, savings come mainly
#     from batch file opens (phases 1+2). OCR time scales with the
#     number of images that pass the pre-filter.
#     """
#     logger.info(f"Starting async PDF extraction: {file_path}")

#     # Phase 1: all tables, one file open
#     tables_by_page = await _run(_extract_all_tables_batch, file_path)

#     # Phase 2: all page text+images, one file open
#     page_data  = await _run(_extract_all_pages_batch, file_path)
#     num_pages  = len(page_data)

#     # Phase 3: async OCR + table injection, all pages concurrently
#     page_tasks = [
#         _process_one_page_async(
#             pn,
#             page_data[pn][0],
#             page_data[pn][1],
#             tables_by_page.get(pn, [])
#         )
#         for pn in range(num_pages)
#     ]
#     page_results = await asyncio.gather(*page_tasks)

#     final_text = "\n\n".join(
#         text for _, text in sorted(page_results, key=lambda x: x[0])
#     )

#     total_tables = sum(len(v) for v in tables_by_page.values())
#     logger.info(
#         f"PDF extraction completed | Pages: {num_pages} | "
#         f"Tables: {total_tables} | Characters: {len(final_text)}"
#     )
#     return final_text


# # ══════════════════════════════════════════════════════════════
# # Public API  (unchanged — drop-in replacement)
# # ══════════════════════════════════════════════════════════════

# def extract_from_pdf(file_path: str) -> str:
#     logger.info(f"Starting PDF extraction: {file_path}")
#     try:
#         asyncio.get_running_loop()
#         import concurrent.futures
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
#             return pool.submit(asyncio.run, _extract_pdf_async(file_path)).result()
#     except RuntimeError:
#         return asyncio.run(_extract_pdf_async(file_path))


# def extract_from_txt(file_path: str) -> str:
#     logger.info(f"Starting TXT extraction: {file_path}")
#     try:
#         with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as f:
#             text = f.read()
#         cleaned = clean_text(text)
#         logger.info(f"TXT extraction completed | Characters: {len(cleaned)}")
#         return cleaned
#     except Exception as e:
#         logger.exception("Error reading TXT file")
#         raise RuntimeError(f"Error reading TXT file: {e}")


# def load_document(file_path: str) -> str:
#     logger.info(f"Loading document: {file_path}")

#     if not os.path.exists(file_path):
#         raise FileNotFoundError(file_path)

#     extension = os.path.splitext(file_path)[1].lower()
#     logger.info(f"Detected file type: {extension}")

#     if extension not in SUPPORTED_EXTENSIONS:
#         raise ValueError(f"Unsupported file type: {extension}")

#     text = extract_from_pdf(file_path) if extension == ".pdf" else extract_from_txt(file_path)

#     if not text.strip():
#         logger.warning("Empty extracted document")

#     logger.info("Document ingestion completed")
#     return text


# # ── Dev runner ────────────────────────────────────────────────
# if __name__ == "__main__":
#     text        = load_document(TEST_SAMPLE_PATH)
#     output_file = "extracted_output.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(text)
#     print("Extraction Successful")
#     print(f"Characters Extracted: {len(text)}")
#     print(f"Saved to: {output_file}")

"""test 2"""
import io
import os
import re
import asyncio
import logging
import statistics
from concurrent.futures import ThreadPoolExecutor

import fitz
import pdfplumber
import pytesseract
from PIL import Image

from app.core.config import (
    SUPPORTED_EXTENSIONS,
    TEXT_FILE_ENCODING,
    PDF_EXTRACTION_MODE,
    ENABLE_TEXT_CLEANING,
    TEST_SAMPLE_PATH,
    PAGE_WORKERS,
    TABLE_WORKERS,
    MIN_OCR_WIDTH,
    MIN_OCR_HEIGHT,
    MIN_OCR_PIXELS,
    TESSERACT_CMD,
    TABLE_MIN_ROWS,
    TABLE_MIN_COLS,
    TABLE_MAX_AVG_CELL_LENGTH,
    TABLE_MIN_NUMERIC_RATIO,
    TABLE_MAX_SINGLE_COL_RATIO,
    TABLE_MIN_MULTI_COL_ROWS,
    TABLE_MAX_MID_WORD_RATIO,
)

logger = logging.getLogger(__name__)
pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# ── Single shared thread pool ─────────────────────────────────
_WORKERS = max(PAGE_WORKERS, TABLE_WORKERS, 8)
_POOL    = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="ingest")

# ── Image-dominant page constants ─────────────────────────────
_IMAGE_DOMINANT_MIN_IMAGES     = 2
_IMAGE_DOMINANT_MAX_TEXT_CHARS = 400

# ── Smart OCR pre-filter thresholds ──────────────────────────
# Applied to ALL images regardless of page number or PDF type.
# Goal: skip images that are provably non-text (solid fills, vector
# graphics, pure photos) without skipping real scanned text images.
#
# Variance threshold: real text on white background has high contrast
# between black ink pixels and white paper pixels → high variance.
# Solid-color fills, gradient backgrounds, simple diagrams → low variance.
# Empirically: text images ~800-3000 variance, diagrams ~50-300.
_OCR_MIN_VARIANCE    = 300   # below this = likely not a text image
# Aspect ratio: very wide or very tall images are usually decorative
# headers/footers/bars, not text blocks worth OCR-ing.
_OCR_MAX_ASPECT      = 8.0
# Unique color count: real text images have many shades (anti-aliasing).
# Pure vector-art exports often have very few unique colors.
_OCR_MIN_UNIQUE_COLORS = 10  # below this = likely vector/icon, skip OCR

# ── Table extraction strategies ───────────────────────────────
_TABLE_STRATEGIES = [
    {
        "vertical_strategy":    "lines",
        "horizontal_strategy":  "lines",
        "snap_tolerance":       3,
        "join_tolerance":       3,
        "edge_min_length":      3,
        "min_words_vertical":   1,
        "min_words_horizontal": 1,
    },
    {
        "vertical_strategy":    "lines_strict",
        "horizontal_strategy":  "lines_strict",
        "snap_tolerance":       3,
        "join_tolerance":       3,
        "edge_min_length":      3,
        "min_words_vertical":   1,
        "min_words_horizontal": 1,
    },
    {
        "vertical_strategy":    "text",
        "horizontal_strategy":  "lines",
        "snap_tolerance":       5,
        "join_tolerance":       5,
        "edge_min_length":      3,
        "min_words_vertical":   2,
        "min_words_horizontal": 1,
    },
    {
        "vertical_strategy":    "lines",
        "horizontal_strategy":  "text",
        "snap_tolerance":       5,
        "join_tolerance":       5,
        "edge_min_length":      3,
        "min_words_vertical":   1,
        "min_words_horizontal": 2,
    },
    {
        "vertical_strategy":    "text",
        "horizontal_strategy":  "text",
        "snap_tolerance":       10,
        "join_tolerance":       10,
        "edge_min_length":      3,
        "min_words_vertical":   1,
        "min_words_horizontal": 1,
    },
]


# ══════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════

def clean_text(text: str) -> str:
    if not ENABLE_TEXT_CLEANING:
        return text
    text = re.sub(r"-\s*\n\s*", "", text)
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"\bPage\s*\d+\b", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


async def _run(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_POOL, fn, *args)


# ══════════════════════════════════════════════════════════════
# Smart OCR pre-filter  (general — works for any PDF type)
# ══════════════════════════════════════════════════════════════

def _should_ocr(image_bytes: bytes) -> tuple[bool, str]:
    """
    General-purpose pre-filter that decides whether an image is
    worth running through Tesseract.

    Works for any PDF — not hardcoded to page numbers or document types.
    Checks image properties that are reliable indicators of text content:

    1. Size — too small to contain readable text
    2. Aspect ratio — extremely wide/tall = decorative, not text
    3. Pixel variance — low variance = solid fills, simple graphics
    4. Unique color count — very few colors = vector export, icon

    Returns (should_run_ocr, reason_string) for logging.
    """
    try:
        image = Image.open(io.BytesIO(image_bytes)).convert("L")
        w, h  = image.size

        # Check 1: minimum size
        if w < MIN_OCR_WIDTH or h < MIN_OCR_HEIGHT:
            return False, f"too_small ({w}x{h})"
        if (w * h) < MIN_OCR_PIXELS:
            return False, f"too_few_pixels ({w*h})"

        # Check 2: aspect ratio
        if min(w, h) > 0:
            aspect = max(w, h) / min(w, h)
            if aspect > _OCR_MAX_ASPECT:
                return False, f"extreme_aspect_ratio ({aspect:.1f})"

        # Check 3: pixel variance
        pixels = list(image.getdata())
        sample = pixels[::8] if len(pixels) > 8000 else pixels
        if len(sample) >= 2:
            var = statistics.variance(sample)
            if var < _OCR_MIN_VARIANCE:
                return False, f"low_variance ({var:.0f} < {_OCR_MIN_VARIANCE})"

        # Check 4: unique color count
        unique_colors = len(set(sample))
        if unique_colors < _OCR_MIN_UNIQUE_COLORS:
            return False, f"too_few_colors ({unique_colors})"

        return True, "ok"

    except Exception as e:
        return True, f"analysis_failed ({e})"


# ══════════════════════════════════════════════════════════════
# OCR  (blocking — offloaded to thread pool)
# ══════════════════════════════════════════════════════════════

def _ocr_bytes(image_bytes: bytes) -> str:
    """
    Blocking OCR. Called only after _should_ocr() returns True.

    CHANGES FROM ORIGINAL:
    ─────────────────────────────────────────────────────────────
    1. Upscale 2x before OCR
       WHY: Chart axis labels, tick numbers, and legend text are
       often very small in PDFs. At original resolution Tesseract
       misreads or skips them. 2x upscale makes small text readable.

    2. PSM 11 (sparse text) instead of PSM 6 (uniform block)
       WHY: PSM 6 assumes the whole image is a clean paragraph —
       so it tries to "read" chart grid lines, borders, and plot
       elements as characters → produces garbage like 'TS y _ lm'.
       PSM 11 says "find whatever text exists anywhere in this image,
       ignore everything else" → correctly extracts numbers and labels
       from charts, figures, and architecture diagrams.

    RESULT from testing on robo.pdf:
       Before (PSM 6):  'TS y _ lm : A ne Se Z * <e) 7O?t'  ← garbage
       After  (PSM 11): '1.000 0.997 0.999 96% Cosine Similarity' ← useful
    ─────────────────────────────────────────────────────────────
    """
    try:
        image = Image.open(io.BytesIO(image_bytes))
        w, h  = image.size

        # Size check (unchanged)
        if w < MIN_OCR_WIDTH or h < MIN_OCR_HEIGHT or (w * h) < MIN_OCR_PIXELS:
            return ""

        # CHANGE 1: Upscale 2x — improves accuracy on small chart text
        image = image.resize((w * 2, h * 2), Image.LANCZOS)

        # CHANGE 2: PSM 11 sparse text — works for charts, diagrams, figures
        # Old: pytesseract.image_to_string(image.convert("L"), config="--psm 6")
        text = pytesseract.image_to_string(
            image.convert("L"),
            config="--psm 11"
        )
        return clean_text(text)

    except Exception:
        logger.exception("OCR failed")
        return ""


async def _ocr_async(image_bytes: bytes, page_num: int, img_idx: int) -> str:
    """Non-blocking OCR with smart pre-filter and skip logging."""
    should_run, reason = _should_ocr(image_bytes)
    if not should_run:
        logger.info(
            "Skipping OCR p%d img%d — %s", page_num + 1, img_idx, reason
        )
        return ""
    logger.info("Running OCR p%d img%d", page_num + 1, img_idx)
    return await _run(_ocr_bytes, image_bytes)


# ══════════════════════════════════════════════════════════════
# Table validation  (unchanged)
# ══════════════════════════════════════════════════════════════

def _has_rotated_text(page) -> bool:
    try:
        chars = page.chars
        if not chars:
            return False
        rotated = sum(1 for c in chars if c.get("upright", 1) == 0)
        return (rotated / len(chars)) > 0.3
    except Exception:
        return False


def _has_mid_word_splits(all_cells, avg_len, numeric_ratio) -> bool:
    if avg_len <= 30 or numeric_ratio > 0.25:
        return False
    count = sum(
        1 for c in all_cells
        if (fl := c.split("\n")[0].strip())
        and fl[-1].islower()
        and fl[-1] not in ".,:;!?)\"'"
    )
    return (count / len(all_cells)) > TABLE_MAX_MID_WORD_RATIO if all_cells else False


def _has_concatenated_words(all_cells) -> bool:
    if not all_cells:
        return False
    count = sum(
        1 for c in all_cells
        if (fl := c.split("\n")[0].strip()) and fl[0].islower()
    )
    return (count / len(all_cells)) > 0.30


def _is_valid_table(table, page, page_num=-1) -> bool:
    if not table:
        return False

    all_cells, col_counts, single_col_rows, total_rows = [], [], 0, 0

    for row in table:
        if not row:
            continue
        non_empty = [c.strip() for c in row if c and c.strip()]
        total_rows += 1
        if len(non_empty) == 1:
            single_col_rows += 1
        if len(non_empty) >= TABLE_MIN_COLS:
            col_counts.append(len(non_empty))
            all_cells.extend(non_empty)

    if total_rows < TABLE_MIN_ROWS:                                  return False
    if not col_counts:                                               return False
    if len(col_counts) < TABLE_MIN_MULTI_COL_ROWS:                   return False
    if max(col_counts) < TABLE_MIN_COLS:                             return False
    if (single_col_rows / total_rows) > TABLE_MAX_SINGLE_COL_RATIO:  return False

    fl_lens = [len(c.split("\n")[0].strip()) for c in all_cells]
    avg_len = sum(fl_lens) / len(fl_lens) if fl_lens else 0
    if avg_len > TABLE_MAX_AVG_CELL_LENGTH:                          return False

    numeric = sum(
        1 for c in all_cells
        if any(ch.isdigit() for ch in c.split("\n")[0])
    )
    numeric_ratio = numeric / len(all_cells) if all_cells else 0
    if numeric_ratio < max(TABLE_MIN_NUMERIC_RATIO, 0.30):           return False
    if _has_rotated_text(page):                                      return False
    if _has_mid_word_splits(all_cells, avg_len, numeric_ratio):      return False
    if _has_concatenated_words(all_cells):                           return False

    return True


def _format_table(table) -> str:
    rows = []
    for row in table:
        if not row or not any(c and c.strip() for c in row):
            continue
        rows.append(" | ".join(c.strip() if c else "" for c in row))
    return "\n".join(rows)


# ══════════════════════════════════════════════════════════════
# OPTIMIZATION 1: All tables in ONE pdfplumber open
# ══════════════════════════════════════════════════════════════
# Old: one pdfplumber.open() call per page (39 opens for 39 pages)
# New: one pdfplumber.open() for all pages
# Saves: ~39 file open/close cycles + OS handle overhead on Windows

def _extract_all_tables_batch(file_path: str) -> dict[int, list]:
    """
    Open PDF once via pdfplumber, extract tables from all pages.
    Identical validation logic to original — just batched.
    Returns {page_0indexed: [table_dicts]}.
    """
    tables_by_page: dict[int, list] = {}
    total_tables = 0

    try:
        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                try:
                    text   = page.extract_text() or ""
                    images = page.images or []

                    if (len(images) >= _IMAGE_DOMINANT_MIN_IMAGES
                            and len(text.strip()) < _IMAGE_DOMINANT_MAX_TEXT_CHARS):
                        continue

                    page_tables = []
                    for strategy in _TABLE_STRATEGIES:
                        try:
                            raw = page.extract_tables(table_settings=strategy) or []
                        except Exception:
                            continue

                        valid = [t for t in raw if _is_valid_table(t, page, page_num)]
                        if not valid:
                            continue

                        for idx, tbl in enumerate(valid):
                            fmt = _format_table(tbl)
                            if fmt:
                                page_tables.append({
                                    "page":          page_num + 1,
                                    "page_0indexed": page_num,
                                    "table_index":   idx,
                                    "text":          f"[TABLE]\n{fmt}\n[/TABLE]",
                                })
                        break

                    if page_tables:
                        tables_by_page[page_num] = page_tables
                        total_tables += len(page_tables)
                        logger.info(
                            "Found %d table(s) on page %d",
                            len(page_tables), page_num + 1
                        )

                except Exception:
                    logger.exception("Table extraction failed p%d", page_num + 1)

    except Exception:
        logger.exception("Failed to open PDF for table extraction")

    logger.info("Total tables extracted: %d", total_tables)
    return tables_by_page


# ══════════════════════════════════════════════════════════════
# OPTIMIZATION 2: All page text+images in ONE fitz open
# ══════════════════════════════════════════════════════════════
# Old: one fitz.open() call per page (39 opens for 39 pages)
# New: one fitz.open() for all pages
# Saves: ~39 file open/close cycles + repeated PDF parse overhead

def _extract_all_pages_batch(file_path: str) -> list[tuple[str, list[bytes]]]:
    """
    Open PDF once via fitz, extract text+image bytes from all pages.
    Returns list of (raw_text, [image_bytes]) indexed by page number.
    """
    results = []
    try:
        doc = fitz.open(file_path)
        for page_num in range(len(doc)):
            try:
                page = doc.load_page(page_num)

                text = ""
                for block in page.get_text(PDF_EXTRACTION_MODE):
                    if block[4].strip():
                        text += block[4] + "\n"

                image_bytes_list = []
                for img in page.get_images(full=True):
                    try:
                        image_bytes_list.append(doc.extract_image(img[0])["image"])
                    except Exception:
                        pass

                results.append((text, image_bytes_list))

            except Exception:
                logger.exception("Text/image extraction failed p%d", page_num + 1)
                results.append(("", []))

        doc.close()

    except Exception:
        logger.exception("Failed to open PDF for text extraction")

    return results


# ══════════════════════════════════════════════════════════════
# Page assembly with async OCR
# ══════════════════════════════════════════════════════════════

async def _process_one_page_async(
    page_num:         int,
    raw_text:         str,
    image_bytes_list: list[bytes],
    tables_on_page:   list,
) -> tuple[int, str]:
    """
    Async page assembly:
    - Receives pre-extracted data (no PDF re-open)
    - Runs OCR concurrently on all images (with smart pre-filter)
    - Injects tables
    """
    page_text = raw_text

    if image_bytes_list:
        logger.info("Found %d image(s) on page %d", len(image_bytes_list), page_num + 1)
        ocr_results = await asyncio.gather(
            *[_ocr_async(b, page_num, i) for i, b in enumerate(image_bytes_list)]
        )
        for ocr_text in ocr_results:
            if ocr_text.strip():
                page_text += "\n[IMAGE OCR TEXT]\n" + ocr_text + "\n"

    if tables_on_page:
        for tbl in tables_on_page:
            page_text += f"\n{tbl['text']}\n"
        logger.info("Injected %d table(s) into page %d", len(tables_on_page), page_num + 1)

    return page_num, clean_text(page_text)


# ══════════════════════════════════════════════════════════════
# Main async pipeline
# ══════════════════════════════════════════════════════════════

async def _extract_pdf_async(file_path: str) -> str:
    """
    Optimized 3-phase pipeline:

    Phase 1 — 1 pdfplumber open  → all tables extracted
    Phase 2 — 1 fitz open        → all page text+images extracted
    Phase 3 — async OCR across all pages concurrently
              PSM 11 sparse mode + 2x upscale for chart/figure images
    """
    logger.info(f"Starting async PDF extraction: {file_path}")

    # Phase 1: all tables, one file open
    tables_by_page = await _run(_extract_all_tables_batch, file_path)

    # Phase 2: all page text+images, one file open
    page_data  = await _run(_extract_all_pages_batch, file_path)
    num_pages  = len(page_data)

    # Phase 3: async OCR + table injection, all pages concurrently
    page_tasks = [
        _process_one_page_async(
            pn,
            page_data[pn][0],
            page_data[pn][1],
            tables_by_page.get(pn, [])
        )
        for pn in range(num_pages)
    ]
    page_results = await asyncio.gather(*page_tasks)

    final_text = "\n\n".join(
        text for _, text in sorted(page_results, key=lambda x: x[0])
    )

    total_tables = sum(len(v) for v in tables_by_page.values())
    logger.info(
        f"PDF extraction completed | Pages: {num_pages} | "
        f"Tables: {total_tables} | Characters: {len(final_text)}"
    )
    return final_text


# ══════════════════════════════════════════════════════════════
# Public API  (unchanged — drop-in replacement)
# ══════════════════════════════════════════════════════════════

def extract_from_pdf(file_path: str) -> str:
    logger.info(f"Starting PDF extraction: {file_path}")
    try:
        asyncio.get_running_loop()
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, _extract_pdf_async(file_path)).result()
    except RuntimeError:
        return asyncio.run(_extract_pdf_async(file_path))


def extract_from_txt(file_path: str) -> str:
    logger.info(f"Starting TXT extraction: {file_path}")
    try:
        with open(file_path, "r", encoding=TEXT_FILE_ENCODING) as f:
            text = f.read()
        cleaned = clean_text(text)
        logger.info(f"TXT extraction completed | Characters: {len(cleaned)}")
        return cleaned
    except Exception as e:
        logger.exception("Error reading TXT file")
        raise RuntimeError(f"Error reading TXT file: {e}")


def load_document(file_path: str) -> str:
    logger.info(f"Loading document: {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    extension = os.path.splitext(file_path)[1].lower()
    logger.info(f"Detected file type: {extension}")

    if extension not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported file type: {extension}")

    text = extract_from_pdf(file_path) if extension == ".pdf" else extract_from_txt(file_path)

    if not text.strip():
        logger.warning("Empty extracted document")

    logger.info("Document ingestion completed")
    return text


# ── Dev runner ────────────────────────────────────────────────
if __name__ == "__main__":
    text        = load_document(TEST_SAMPLE_PATH)
    output_file = "extracted_output.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(text)
    print("Extraction Successful")
    print(f"Characters Extracted: {len(text)}")
    print(f"Saved to: {output_file}")