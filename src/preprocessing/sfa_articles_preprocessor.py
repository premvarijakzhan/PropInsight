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
    r"A-\s*A\+",  # font size controls
    r"Top",
    r"Related\s*Pages.*",  # will be handled in a block removal below
    r"Related\s*Articles.*",  # will be handled in a block removal below
    r"Search",
    r"Close",
    r"Submit",
    r"Next",
]

SURVEY_BLOCK_PATTERNS = [
    r"We\s*welcome\s*your\s*feedback.*?Let's\s*go",  # header into survey start
    r"Is\s*this\s*page\s*helpful\?.*?(Thank\s*you|Try\s*searching|Search)",
    r"Which\s*of\s*the\s*following\s*best\s*describes\s*you\?.*?(Next|Submit)",
    r"What\s*issue\(s\)\s*did\s*you\s*face\s*with\s*this\s*page\?.*?(Next|Submit)",
    r"Please\s*let\s*us\s*know\s*your\s*thoughts.*?(Next|Submit)",
    r"What\s*is\s*your\s*overall\s*satisfaction\s*level\s*with\s*our\s*website\?.*?(Next|Submit)",
    r"How\s*would\s*you\s*rate\s*our\s*website\s*when\s*compared\s*to\s*other\s*government\s*websites\?.*?(Next|Submit)",
]

SECTION_PATTERNS = [
    r"Last\s*Updated\s*\d{1,2}\s*[A-Za-z]{3,9}\s*\d{4}",
    r"Contents\s*in\s*this\s*page",
]

FOOTNOTE_PATTERN = r"\[\d+\]"


def remove_navigation_blocks(text: str) -> str:
    cleaned = text
    # Remove font-size block and other navigation tokens
    for pat in NAVIGATION_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    # Remove large Related Pages/Articles blocks to end of text
    cleaned = re.sub(r"Related\s*Pages.*$", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"Related\s*Articles.*$", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return cleaned


def remove_survey_blocks(text: str) -> str:
    cleaned = text
    for pat in SURVEY_BLOCK_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    return cleaned


def remove_section_headers(text: str) -> str:
    cleaned = text
    for pat in SECTION_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    return cleaned


def remove_bracket_footnotes(text: str) -> str:
    return re.sub(FOOTNOTE_PATTERN, "", text)


def normalize_whitespace(text: str) -> str:
    cleaned = text.replace("\u00a0", " ").replace("\u200b", " ")
    cleaned = re.sub(r"[\r\t]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def clean_sfa_text(text: str) -> str:
    if not text:
        return ""
    cleaned = text
    cleaned = remove_navigation_blocks(cleaned)
    cleaned = remove_survey_blocks(cleaned)
    cleaned = remove_section_headers(cleaned)
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


def process_sfa_jsonl(input_path: str, output_path: str) -> str:
    logger.info(f"Reading input: {input_path}")
    records = read_jsonl(input_path)
    if not records:
        raise RuntimeError("No valid records found in input JSONL")

    processed_rows: List[Dict[str, Any]] = []

    for rec in records:
        text = rec.get("text", "")
        cleaned_text = clean_sfa_text(text)
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
    parser = argparse.ArgumentParser(description="Preprocess SFA JSONL file: clean up text and save CSV")
    parser.add_argument("--input", "-i", required=True, help="Input JSONL file path")
    parser.add_argument("--output", "-o", required=False, help="Output CSV file path")
    args = parser.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_dir = os.path.join("data", "processed", "sfa")
    output_path = args.output or default_output_from_input(input_path, output_dir)

    process_sfa_jsonl(input_path, output_path)


if __name__ == "__main__":
    main()