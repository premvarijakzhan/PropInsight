import os
import re
import json
import argparse
import logging
from typing import Dict, Any, List

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


NAVIGATION_PATTERNS = [
    r"About\s*Us",
    r"Press\s*Releases",
    r"Select\s*Year",
    r"From\s*To\s*Go",
    r"Read\s*press\s*release",
    r"Share\s*this\s*page.*",
    r"Back\s*to\s*top",
    r"Sitemap",
    r"Privacy\s*Statement",
    r"Terms\s*of\s*Use",
    r"Contact\s*Us",
]

ANNEX_PATTERNS = [
    r"Annex\s*[A-Z]",
    r"Download\s*Annex\s*[A-Z].*",
]

DISCLAIMER_PATTERNS = [
    r"NOT\s*FOR\s*DISTRIBUTION.*",  # Common bond/legal disclaimer blocks
]

FOOTNOTE_PATTERN = r"\[\d+\]"


def remove_navigation_blocks(text: str) -> str:
    """Remove common navigation/menu/footer noise from HDB pages."""
    cleaned = text
    # Remove large nav blocks between "About Us" and "Press Releases" if present
    cleaned = re.sub(r"About\s*Us.*?Press\s*Releases", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    # Remove individual noisy patterns
    for pat in NAVIGATION_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    return cleaned


def remove_annex_and_downloads(text: str) -> str:
    cleaned = text
    for pat in ANNEX_PATTERNS:
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


def clean_hdb_text(text: str) -> str:
    """Apply cleaning pipeline to raw HDB text."""
    if not text:
        return ""
    cleaned = text
    cleaned = remove_navigation_blocks(cleaned)
    cleaned = remove_annex_and_downloads(cleaned)
    cleaned = remove_disclaimers(cleaned)
    cleaned = remove_bracket_footnotes(cleaned)
    # Normalize dashes and bullets
    cleaned = cleaned.replace("–", "-").replace("—", "-")
    # Final whitespace normalization
    cleaned = normalize_whitespace(cleaned)
    return cleaned


def read_hdb_json(path: str) -> List[Dict[str, Any]]:
    """Read HDB JSON file and return list of article records."""
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    articles = obj.get("articles", [])
    if not isinstance(articles, list):
        logger.warning("Invalid format: 'articles' is not a list")
        return []
    return articles


def process_hdb_json(input_path: str, output_path: str) -> str:
    """Process HDB JSON and save cleaned CSV to output_path."""
    logger.info(f"Reading input: {input_path}")
    records = read_hdb_json(input_path)
    if not records:
        raise RuntimeError("No valid records found in input JSON")

    processed_rows: List[Dict[str, Any]] = []

    for rec in records:
        text = rec.get("text", "")
        cleaned_text = clean_hdb_text(text)
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

    df = pd.DataFrame(processed_rows)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Saved processed file: {output_path} ({len(df)} rows)")
    return output_path


def default_output_from_input(input_path: str, out_dir: str) -> str:
    base = os.path.basename(input_path)
    name, _ = os.path.splitext(base)
    return os.path.join(out_dir, f"{name}_processed.csv")


def main():
    parser = argparse.ArgumentParser(description="Preprocess HDB JSON file: clean up text and save CSV")
    parser.add_argument("--input", "-i", required=True, help="Input JSON file path")
    parser.add_argument("--output", "-o", required=False, help="Output CSV file path")
    args = parser.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Default output directory per user request
    output_dir = os.path.join("data", "processed", "hdb")
    output_path = args.output or default_output_from_input(input_path, output_dir)

    process_hdb_json(input_path, output_path)


if __name__ == "__main__":
    main()