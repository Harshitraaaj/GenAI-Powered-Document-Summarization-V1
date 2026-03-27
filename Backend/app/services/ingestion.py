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
    IMAGE_DOMINANT_MIN_IMAGES,
    IMAGE_DOMINANT_MAX_TEXT_CHARS,
    OCR_MIN_VARIANCE,
    OCR_MAX_ASPECT,
    OCR_MIN_UNIQUE_COLORS,
    OCR_UPSCALE_FACTOR,
    OCR_PSM_MODE,
    INGEST_POOL_MIN_WORKERS,
    TABLE_MID_WORD_AVG_LEN_MIN,
    TABLE_MID_WORD_NUMERIC_RATIO_MAX,
    TABLE_CONCAT_WORDS_RATIO_MAX,
    TABLE_NUMERIC_RATIO_FLOOR,
)

logger = logging.getLogger(__name__)
pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

# ── Single shared thread pool ─────────────────────────────────
_WORKERS = max(PAGE_WORKERS, TABLE_WORKERS, INGEST_POOL_MIN_WORKERS)
_POOL    = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="ingest")

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
            if aspect > OCR_MAX_ASPECT:
                return False, f"extreme_aspect_ratio ({aspect:.1f})"

        # Check 3: pixel variance
        pixels = list(image.getdata())
        sample = pixels[::8] if len(pixels) > 8000 else pixels
        if len(sample) >= 2:
            var = statistics.variance(sample)
            if var < OCR_MIN_VARIANCE:
                return False, f"low_variance ({var:.0f} < {OCR_MIN_VARIANCE})"

        # Check 4: unique color count
        unique_colors = len(set(sample))
        if unique_colors < OCR_MIN_UNIQUE_COLORS:
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
    1. Upscale OCR_UPSCALE_FACTOR x before OCR
       WHY: Chart axis labels, tick numbers, and legend text are
       often very small in PDFs. At original resolution Tesseract
       misreads or skips them. Upscaling makes small text readable.

    2. PSM OCR_PSM_MODE (sparse text) instead of PSM 6 (uniform block)
       WHY: PSM 6 assumes the whole image is a clean paragraph —
       so it tries to "read" chart grid lines, borders, and plot
       elements as characters → produces garbage like 'TS y _ lm'.
       PSM 11 says "find whatever text exists anywhere in this image,
       ignore everything else" → correctly extracts numbers and labels
       from charts, figures, and architecture diagrams.
    ─────────────────────────────────────────────────────────────
    """
    try:
        image = Image.open(io.BytesIO(image_bytes))
        w, h  = image.size

        # Size check
        if w < MIN_OCR_WIDTH or h < MIN_OCR_HEIGHT or (w * h) < MIN_OCR_PIXELS:
            return ""

        # Upscale — improves accuracy on small chart text
        image = image.resize(
            (w * OCR_UPSCALE_FACTOR, h * OCR_UPSCALE_FACTOR),
            Image.LANCZOS,
        )

        # Sparse text PSM — works for charts, diagrams, figures
        text = pytesseract.image_to_string(
            image.convert("L"),
            config=f"--psm {OCR_PSM_MODE}",
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
# Table validation
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
    if avg_len <= TABLE_MID_WORD_AVG_LEN_MIN or numeric_ratio > TABLE_MID_WORD_NUMERIC_RATIO_MAX:
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
    return (count / len(all_cells)) > TABLE_CONCAT_WORDS_RATIO_MAX


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
    if numeric_ratio < max(TABLE_MIN_NUMERIC_RATIO, TABLE_NUMERIC_RATIO_FLOOR): return False
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

                    if (len(images) >= IMAGE_DOMINANT_MIN_IMAGES
                            and len(text.strip()) < IMAGE_DOMINANT_MAX_TEXT_CHARS):
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
              PSM sparse mode + upscale for chart/figure images
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