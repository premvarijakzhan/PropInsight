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
    r"×!?feedback",
    r"Industry\s*Info",
    r"BuildSG",
    r"Accessibility\s*&\s*Universal\s*Design",
    r"Regulatory\s*Info",
    r"Procurement",
    r"PublicFind\s*out\s*more",
    r"About\s*Us",
    r"News\s*and\s*Publications",
    r"Our\s*Accolades",
    r"Careers",
    r"Corporate\s*Social\s*Responsibility",
    r"Events",
    r"e-Services",
    r"Useful\s*Links",
    r"Circulars",
    r"Publications",
    r"Speeches",
    r"Replies\s*To\s*Forum\s*Letters",
    r"Annual\s*Report",
    r"Publications\s*&\s*Reports",
    r"HomeAbout\s*Us",
    r"Sitemap",
    r"Privacy\s*Statement",
    r"Terms\s*of\s*Use",
    r"FAQS",
    r"Contact\s*Us",
    r"Popular\s*e-Services",
    r"CWRS",
    r"Overseas\s*Testing\s*Management\s*System",
    r"OTMS",
    r"LEAP\s*Portal",
]

ANNEX_PATTERNS = [
    r"Download\s*the\s*PDF\s*version.*",
    r"Download\s*Annex\s*[A-Z].*",
    r"Annex\s*[A-Z]",
    r"Please\s*refer\s*to\s*Annex\s*[A-Z].*",
]

FOOTNOTE_PATTERN = r"\[\d+\]"


def remove_navigation_blocks(text: str) -> str:
    """Remove common navigation/menu/footer noise from BCA pages."""
    cleaned = text
    # Remove large nav block between "Industry Info" and "HomeAbout Us" if present
    cleaned = re.sub(r"Industry\s*Info.*?HomeAbout\s*Us", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    # Remove everything after "Popular e-Services" (footer area) if present
    cleaned = re.sub(r"Popular\s*e-Services.*$", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    # Remove individual noisy patterns
    for pat in NAVIGATION_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    return cleaned


def remove_annex_and_downloads(text: str) -> str:
    cleaned = text
    for pat in ANNEX_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    return cleaned


def normalize_whitespace(text: str) -> str:
    # Replace non-breaking spaces and control chars
    cleaned = text.replace("\u00a0", " ").replace("\u200b", " ")
    cleaned = re.sub(r"[\r\t]", " ", cleaned)
    # Collapse multiple spaces/newlines
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def remove_bracket_footnotes(text: str) -> str:
    return re.sub(FOOTNOTE_PATTERN, "", text)


def clean_bca_text(text: str) -> str:
    """Apply cleaning pipeline to raw BCA text."""
    if not text:
        return ""
    cleaned = text
    cleaned = remove_navigation_blocks(cleaned)
    cleaned = remove_annex_and_downloads(cleaned)
    cleaned = remove_bracket_footnotes(cleaned)
    # Normalize dashes and bullets
    cleaned = cleaned.replace("–", "-").replace("—", "-")
    # Final whitespace normalization
    cleaned = normalize_whitespace(cleaned)
    return cleaned


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                items.append(obj)
            except json.JSONDecodeError:
                logger.warning("Skipping invalid JSONL line")
    return items


def process_bca_jsonl(input_path: str, output_path: str) -> str:
    """Process BCA JSONL and save cleaned CSV to output_path."""
    logger.info(f"Reading input: {input_path}")
    records = read_jsonl(input_path)
    if not records:
        raise RuntimeError("No valid records found in input JSONL")

    processed_rows: List[Dict[str, Any]] = []

    for rec in records:
        text = rec.get("text", "")
        cleaned_text = clean_bca_text(text)
        processed_rows.append({
            "id": rec.get("id"),
            "source": rec.get("source"),
            "timestamp": rec.get("timestamp"),
            "url": rec.get("url"),
            "language": rec.get("language", "en"),
            "title": (rec.get("metadata", {}) or {}).get("title"),
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
    parser = argparse.ArgumentParser(description="Preprocess BCA JSONL file: clean up text and save CSV")
    parser.add_argument("--input", "-i", required=True, help="Input JSONL file path")
    parser.add_argument("--output", "-o", required=False, help="Output CSV file path")
    args = parser.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Default output directory per user request
    output_dir = os.path.join("data", "processed", "bca")
    output_path = args.output or default_output_from_input(input_path, output_dir)

    process_bca_jsonl(input_path, output_path)


if __name__ == "__main__":
    main()