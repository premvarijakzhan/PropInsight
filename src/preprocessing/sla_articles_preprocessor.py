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


# Common UI/navigation noise often present on SLA pages
NAVIGATION_PATTERNS = [
    r"A-\s*A\+",  # font size controls
    r"Back\s*to\s*top",
    r"Home",
    r"Search",
    r"Submit",
    r"Skip\s*to\s*main\s*content",
]

# Headers and repeated pressroom/site furniture
HEADER_PATTERNS = [
    r"Home\s*News\s*Press\s*release",
    r"Press\s*release",
]

# Large blocks we want to remove entirely to the end (tables/appendices/annexes)
APPENDIX_BLOCK_PATTERNS = [
    r"Appendix\s*\d+.*$",
    r"Annex\s*[A-Z].*$",
]

FOOTNOTE_PATTERN = r"\[\d+\]"
# Insert SLA-specific pattern constants above function usage
GOV_BANNER_PATTERNS = [
    r"Click\s*to\s*expand\s*masthead.*",
    r"A\s*Singapore\s*Government\s*Agency\s*Website.*",
    r"Official\s*website\s*links\s*end\s*with\s*\\.gov\\.sg.*",
    r"Secure\s*websites\s*use\s*HTTPS.*",
    r"Look\s*for\s*a\s*lock.*",
    r"Share\s*sensitive\s*information\s*only\s*on\s*official.*",
    r"Government\s*officials\s*will\s*never\s*ask\s*you\s*to\s*transfer\s*money.*",
    r"Call\s*the\s*24/7\s*ScamShield\s*Helpline.*",
]
MENU_BLOCK_PATTERN = r"About\s*SLA.*?Feedback"
FOOTER_BLOCK_PATTERN = r"©\s*\d{4}\s*Government\s*of\s*Singapore.*$"
LAST_UPDATED_PATTERN = r"Last\s*updated\s*\d{1,2}\s*[A-Za-z]{3,9}\s*\d{4}"


def remove_navigation_blocks(text: str) -> str:
    cleaned = text
    for pat in NAVIGATION_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return cleaned


def remove_header_furniture(text: str) -> str:
    cleaned = text
    for pat in HEADER_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return cleaned


def remove_appendix_blocks(text: str) -> str:
    cleaned = text
    for pat in APPENDIX_BLOCK_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return cleaned


def remove_bracket_footnotes(text: str) -> str:
    return re.sub(FOOTNOTE_PATTERN, "", text)


def normalize_whitespace(text: str) -> str:
    cleaned = text.replace("\u00a0", " ").replace("\u200b", " ")
    cleaned = re.sub(r"[\r\t]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def remove_government_banner(text: str) -> str:
    cleaned = text
    for pat in GOV_BANNER_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return cleaned


def remove_menu_blocks(text: str) -> str:
    return re.sub(MENU_BLOCK_PATTERN, " ", text, flags=re.IGNORECASE | re.DOTALL)


def remove_footer_block(text: str) -> str:
    return re.sub(FOOTER_BLOCK_PATTERN, " ", text, flags=re.IGNORECASE | re.DOTALL)


def remove_last_updated(text: str) -> str:
    return re.sub(LAST_UPDATED_PATTERN, " ", text, flags=re.IGNORECASE)


def clean_sla_text(text: str) -> str:
    if not text:
        return ""
    cleaned = text
    cleaned = remove_navigation_blocks(cleaned)
    cleaned = remove_header_furniture(cleaned)
    cleaned = remove_government_banner(cleaned)
    cleaned = remove_menu_blocks(cleaned)
    cleaned = remove_appendix_blocks(cleaned)
    cleaned = remove_last_updated(cleaned)
    cleaned = remove_footer_block(cleaned)
    cleaned = remove_bracket_footnotes(cleaned)
    cleaned = cleaned.replace("–", "-").replace("—", "-")
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


def process_sla_jsonl(input_path: str, output_path: str) -> str:
    logger.info(f"Reading input: {input_path}")
    records = read_jsonl(input_path)
    if not records:
        raise RuntimeError("No valid records found in input JSONL")

    processed_rows: List[Dict[str, Any]] = []

    for rec in records:
        text = rec.get("text", "")
        cleaned_text = clean_sla_text(text)
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

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Saved processed file: {output_path} ({len(df)} rows)")
    return output_path


def default_output_from_input(input_path: str, out_dir: str) -> str:
    base = os.path.basename(input_path)
    name, _ = os.path.splitext(base)
    return os.path.join(out_dir, f"{name}_processed.csv")


def main():
    parser = argparse.ArgumentParser(description="Preprocess SLA JSONL file: clean up text and save CSV")
    parser.add_argument("--input", "-i", required=True, help="Input JSONL file path")
    parser.add_argument("--output", "-o", required=False, help="Output CSV file path")
    args = parser.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_dir = os.path.join("data", "processed", "sla")
    output_path = args.output or default_output_from_input(input_path, output_dir)

    process_sla_jsonl(input_path, output_path)


if __name__ == "__main__":
    main()