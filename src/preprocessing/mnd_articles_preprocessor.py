import os
import re
import json
import argparse
import logging
from typing import Dict, Any, List, Union

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Common navigation and chrome tokens observed on MND pages
NAVIGATION_PATTERNS = [
    r"Newsroom",
    r"Press\s*Releases",
    r"Speeches",
    r"Parliament\s*Matters",
    r"View",
    r"Share\s*this\s*page.*",
    r"Back\s*to\s*top",
    r"Sitemap",
    r"Privacy\s*Statement",
    r"Terms\s*of\s*Use",
    r"Contact\s*Us",
]

# Appendix headers and references
APPENDIX_PATTERNS = [
    r"Appendix\s*[0-9A-Z]+(?:\s*-+)?",  # e.g., "Appendix 1------------"
    r"see\s*Appendix\s*[0-9A-Z]+(?:\s*&\s*Appendix\s*[0-9A-Z]+)?",
]

# Generic disclaimers
DISCLAIMER_PATTERNS = [
    r"NOT\s*FOR\s*DISTRIBUTION.*",
]

FOOTNOTE_PATTERN = r"\[\d+\]"


def remove_navigation_blocks(text: str) -> str:
    """Remove common navigation/menu/footer noise from MND pages."""
    cleaned = text
    # Remove dense nav stretches like "Newsroom ... Press Releases ... View" up to first meaningful token
    cleaned = re.sub(r"Newsroom.*?(Press\s*Releases|Speeches|Parliament\s*Matters|View)", " ", cleaned,
                     flags=re.IGNORECASE | re.DOTALL)
    for pat in NAVIGATION_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    return cleaned


def remove_appendix(text: str) -> str:
    cleaned = text
    for pat in APPENDIX_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    return cleaned


def remove_disclaimers(text: str) -> str:
    cleaned = text
    for pat in DISCLAIMER_PATTERNS:
        cleaned = re.sub(pat + r".*$", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return cleaned


def remove_bracket_footnotes(text: str) -> str:
    return re.sub(FOOTNOTE_PATTERN, "", text)


def normalize_whitespace(text: str) -> str:
    # Replace non-breaking spaces and control chars
    cleaned = text.replace("\u00a0", " ").replace("\u200b", " ")
    cleaned = re.sub(r"[\r\t]", " ", cleaned)
    # Collapse multiple spaces/newlines
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def clean_mnd_text(text: str) -> str:
    """Apply cleaning pipeline to raw MND text."""
    if not text:
        return ""
    cleaned = text
    cleaned = remove_navigation_blocks(cleaned)
    cleaned = remove_appendix(cleaned)
    cleaned = remove_disclaimers(cleaned)
    cleaned = remove_bracket_footnotes(cleaned)
    # Normalize dashes and bullets
    cleaned = cleaned.replace("–", "-").replace("—", "-")
    # Final whitespace normalization
    cleaned = normalize_whitespace(cleaned)
    return cleaned


def read_mnd_json(path: str) -> List[Dict[str, Any]]:
    """Read MND JSON file. Supports either top-level list or {"articles": [...]} shape."""
    with open(path, "r", encoding="utf-8") as f:
        obj: Union[List[Dict[str, Any]], Dict[str, Any]] = json.load(f)
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        if isinstance(obj.get("articles"), list):
            return obj["articles"]
        # Fallbacks if other keys are used
        for key in ("items", "data", "records"):
            if isinstance(obj.get(key), list):
                return obj[key]
    logger.warning("Invalid JSON format: expected a list or an object containing an articles/items list")
    return []


def process_mnd_json(input_path: str, output_path: str) -> str:
    """Process MND JSON and save cleaned CSV to output_path."""
    logger.info(f"Reading input: {input_path}")
    records = read_mnd_json(input_path)
    if not records:
        raise RuntimeError("No valid records found in input JSON")

    processed_rows: List[Dict[str, Any]] = []

    for rec in records:
        text = rec.get("text", "")
        cleaned_text = clean_mnd_text(text)
        meta = (rec.get("metadata", {}) or {})
        processed_rows.append({
            "id": rec.get("id"),
            "source": rec.get("source"),
            "timestamp": rec.get("timestamp"),
            "url": rec.get("url"),
            "language": rec.get("language", "en"),
            "title": meta.get("title"),
            "original_length": len(text or ""),
            "cleaned_length": len(cleaned_text or ""),
            "cleaned_text": cleaned_text,
        })

    df = pandas_safe_dataframe(processed_rows)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Saved processed file: {output_path} ({len(df)} rows)")
    return output_path


def pandas_safe_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """Create DataFrame while safely handling extremely long text entries."""
    # Directly constructing DataFrame is fine; this function exists to mirror style and future-proof
    return pd.DataFrame(rows)


def default_output_from_input(input_path: str, out_dir: str) -> str:
    base = os.path.basename(input_path)
    name, _ = os.path.splitext(base)
    return os.path.join(out_dir, f"{name}_processed.csv")


def main():
    parser = argparse.ArgumentParser(description="Preprocess MND JSON file: clean up text and save CSV")
    parser.add_argument("--input", "-i", required=True, help="Input JSON file path")
    parser.add_argument("--output", "-o", required=False, help="Output CSV file path")
    args = parser.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Default output directory per user request
    output_dir = os.path.join("data", "processed", "mnd")
    output_path = args.output or default_output_from_input(input_path, output_dir)

    process_mnd_json(input_path, output_path)


if __name__ == "__main__":
    main()