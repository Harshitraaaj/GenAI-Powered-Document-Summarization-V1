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

from app.core.config import settings

logger = logging.getLogger(__name__)

# --- Setup ---
pytesseract.pytesseract.tesseract_cmd = settings.TESSERACT_CMD

_WORKERS = max(settings.PAGE_WORKERS, settings.TABLE_WORKERS, settings.INGEST_POOL_MIN_WORKERS)
_POOL = ThreadPoolExecutor(
    max_workers=_WORKERS,
    thread_name_prefix=settings.INGEST_THREAD_NAME_PREFIX
)

_TABLE_STRATEGIES = settings.TABLE_STRATEGIES


# --- Text Cleaning ---
def clean_text(text: str) -> str:
    if not settings.ENABLE_TEXT_CLEANING:
        return text

    for pattern, repl in settings.TEXT_CLEAN_PATTERNS:
        text = re.sub(pattern, repl, text)

    return text.strip()


# --- Async Runner ---
async def _run(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_POOL, fn, *args)


# --- OCR Decision ---
def _should_ocr(image_bytes: bytes) -> tuple[bool, str]:
    try:
        image = Image.open(io.BytesIO(image_bytes)).convert("L")
        w, h = image.size

        if w < settings.MIN_OCR_WIDTH or h < settings.MIN_OCR_HEIGHT:
            return False, f"too_small ({w}x{h})"

        if (w * h) < settings.MIN_OCR_PIXELS:
            return False, f"too_few_pixels ({w*h})"

        aspect = max(w, h) / min(w, h) if min(w, h) > 0 else 0
        if aspect > settings.OCR_MAX_ASPECT:
            return False, f"extreme_aspect_ratio ({aspect:.1f})"

        pixels = list(image.getdata())
        sample = pixels[::settings.OCR_PIXEL_SAMPLE_STEP] \
            if len(pixels) > settings.OCR_PIXEL_SAMPLE_THRESH else pixels

        if len(sample) >= 2:
            var = statistics.variance(sample)
            if var < settings.OCR_MIN_VARIANCE:
                return False, f"low_variance ({var:.0f})"

        if len(set(sample)) < settings.OCR_MIN_UNIQUE_COLORS:
            return False, "too_few_colors"

        return True, "ok"

    except Exception as e:
        return True, f"analysis_failed ({e})"


# --- OCR Execution ---
def _ocr_bytes(image_bytes: bytes) -> str:
    try:
        image = Image.open(io.BytesIO(image_bytes))
        w, h = image.size

        if w < settings.MIN_OCR_WIDTH or h < settings.MIN_OCR_HEIGHT:
            return ""

        resample_filter = getattr(Image, settings.OCR_RESAMPLE_FILTER)

        image = image.resize(
            (w * settings.OCR_UPSCALE_FACTOR, h * settings.OCR_UPSCALE_FACTOR),
            resample_filter,
        )

        config = settings.OCR_CONFIG_TEMPLATE.format(psm=settings.OCR_PSM_MODE)

        text = pytesseract.image_to_string(
            image.convert("L"),
            config=config,
        )

        return clean_text(text)

    except Exception:
        logger.exception("OCR failed")
        return ""


async def _ocr_async(image_bytes: bytes, page_num: int, img_idx: int) -> str:
    should_run, reason = _should_ocr(image_bytes)

    if not should_run:
        logger.info("Skipping OCR p%d img%d — %s", page_num + 1, img_idx, reason)
        return ""

    logger.info("Running OCR p%d img%d", page_num + 1, img_idx)
    return await _run(_ocr_bytes, image_bytes)


# --- Table Extraction ---
def _format_table(table) -> str:
    rows = []
    for row in table:
        if not row or not any(c and c.strip() for c in row):
            continue
        rows.append(settings.TABLE_CELL_SEPARATOR.join(c.strip() if c else "" for c in row))
    return "\n".join(rows)


def _extract_all_tables_batch(file_path: str) -> dict[int, list]:
    tables_by_page = {}

    with pdfplumber.open(file_path) as pdf:
        for page_num, page in enumerate(pdf.pages):

            page_tables = []

            for strategy in _TABLE_STRATEGIES:
                try:
                    raw = page.extract_tables(table_settings=strategy) or []
                except Exception:
                    continue

                for idx, tbl in enumerate(raw):
                    fmt = _format_table(tbl)
                    if fmt:
                        page_tables.append({
                            "page": page_num,
                            "text": f"{settings.TABLE_OPEN_TAG}\n{fmt}\n{settings.TABLE_CLOSE_TAG}",
                        })

                if page_tables:
                    break

            if page_tables:
                tables_by_page[page_num] = page_tables

    return tables_by_page


# --- PDF Extraction ---
async def _extract_pdf_async(file_path: str) -> str:
    tables_by_page = await _run(_extract_all_tables_batch, file_path)

    doc = fitz.open(file_path)
    results = []

    for page_num in range(len(doc)):
        page = doc.load_page(page_num)

        text = page.get_text(settings.PDF_EXTRACTION_MODE)

        images = [doc.extract_image(img[0])["image"] for img in page.get_images(full=True)]

        ocr_results = await asyncio.gather(
            *[_ocr_async(img, page_num, i) for i, img in enumerate(images)]
        )

        for ocr_text in ocr_results:
            if ocr_text.strip():
                text += f"\n{settings.OCR_TEXT_OPEN_TAG}\n{ocr_text}\n"

        for tbl in tables_by_page.get(page_num, []):
            text += f"\n{tbl['text']}\n"

        results.append(clean_text(text))

    doc.close()

    return "\n\n".join(results)


# --- Public APIs ---
def extract_from_pdf(file_path: str) -> str:
    try:
        asyncio.get_running_loop()
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, _extract_pdf_async(file_path)).result()

    except RuntimeError:
        return asyncio.run(_extract_pdf_async(file_path))


def extract_from_txt(file_path: str) -> str:
    with open(file_path, "r", encoding=settings.TEXT_FILE_ENCODING) as f:
        return clean_text(f.read())


def load_document(file_path: str) -> str:
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    ext = os.path.splitext(file_path)[1].lower()

    if ext not in settings.SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported file type: {ext}")

    return extract_from_pdf(file_path) if ext == ".pdf" else extract_from_txt(file_path)