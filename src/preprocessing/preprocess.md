# Reddit Corpus Preprocessing with Singlish and Property Domain Integration

This document describes the preprocessing pipeline for transforming Reddit posts and comments (2023–2025) into a high-quality, structured, property-focused corpus that's ready for OpenAI-based labeling and topic modeling / NER / sentiment analysis.

## Overview

The preprocessing pipeline integrates two specialized corpus:
- **Singlish Corpus**: Local Singaporean English vocabulary and expressions
- **Property Domain Corpus**: Singapore real estate terminology and concepts

## Processing Stages

| Stage | Purpose | Output |
|-------|---------|--------|
| 1️⃣ Load & Clean | Normalize text, fix encoding, unify schema | `df['body_clean']` clean text |
| 2️⃣ Quality & Noise Removal | Filter spam, low-quality, and irrelevant posts | Clean filtered df |
| 3️⃣ Dedupe & Hash | Remove duplicates, track processed hashes | Unique corpus |
| 4️⃣ Singlish Detection | Identify and tag Singlish terms and expressions | `singlish_terms`, `has_singlish` |
| 5️⃣ Property Term Extraction | Extract Singapore property domain terminology | `property_terms`, `property_categories` |
| 6️⃣ Quality Scoring | Calculate text quality metrics for prioritization | `quality_score` |
| 7️⃣ Corpus Integration | Combine insights from both specialized corpus | Enhanced metadata |
| 8️⃣ Save & Report | Export enriched data & comprehensive summary | CSV + JSON report |

## New Reddit Corpus Preprocessor

### Script: `reddit_corpus_preprocessor.py`

A comprehensive preprocessor that integrates Singlish and property domain corpus for enhanced text processing.

#### Key Features:

1. **Singlish Integration**:
   - Loads vocabulary from `data/corpus/Singlish/lexicon.csv`
   - Processes JSONL vocabulary files
   - Detects Singlish terms with word boundary matching
   - Provides term definitions, examples, and origins

2. **Property Domain Integration**:
   - Loads terminology from `data/corpus/SGPropertyDomain/glossary.csv`
   - Categorizes terms by domain (HDB, Tax&Duty, PropertyType, etc.)
   - Handles aliases and alternative spellings
   - Groups findings by property categories

3. **Enhanced Text Processing**:
   - Advanced quality metrics calculation
   - Spam and low-quality content detection
   - Content deduplication with hash generation
   - Comprehensive text cleaning and normalization

#### Usage:

```bash
python reddit_corpus_preprocessor.py \
  --input "data/processed/reddit_corpus_2023_2025.csv" \
  --output "data/processed/reddit_corpus_enhanced.csv" \
  --singlish-corpus "data/corpus/Singlish" \
  --property-corpus "data/corpus/SGPropertyDomain" \
  --report "results/reddit_preprocessing_report.json"
```

#### Output Columns:

- `body_clean`: Cleaned and normalized text
- `singlish_terms`: Detected Singlish terms (pipe-separated)
- `singlish_count`: Number of Singlish terms found
- `has_singlish`: Boolean flag for Singlish presence
- `property_terms`: Detected property terms (pipe-separated)
- `property_categories`: Property term categories (pipe-separated)
- `property_count`: Number of property terms found
- `has_property_terms`: Boolean flag for property terms presence
- `quality_score`: Text quality score (0-1)
- `is_spam`: Spam detection flag
- `content_hash`: MD5 hash for deduplication
- `word_count`: Number of words
- `char_count`: Number of characters
🧹 1️⃣ DATA LOADING & CLEANING
What happens:

Loads the raw Reddit CSV from Drive or local upload.

Normalizes schemas (title, selftext, comment_body, etc.).

Cleans messy encoding (fixes weird UTF-8 like “â€””, “Â”, etc.).

Removes URLs, HTML tags, [deleted], markdown links, extra spaces.

Combines title + selftext + comment_body → unified text field body.

Converts timestamps → Singapore timezone (Asia/Singapore).

Filters only posts from 2023–2025.

Why it matters:

Ensures every record has a clean, standardized text string and proper timestamp before labeling or downstream modeling.

🚫 2️⃣ NOISE REMOVAL & QUALITY FILTERS
What happens:

Each post gets text quality metrics via calculate_quality_metrics():

word_count, avg_word_length, uppercase_ratio, digit_ratio, unique_word_ratio, etc.
Used to detect spam, repeated patterns, or empty posts.

Then is_spam_or_low_quality() applies rule-based checks:

Too short (< 5 words)

Excessive CAPS

Repeated words (low unique ratio)

Too many digits (price-listing spam)

“Click here”, “telegram”, etc. patterns

Score = 0 and short → discard

Relevance filtering:

check_property_relevance() (now corpus-driven) measures whether a post mentions Singapore property keywords (e.g. HDB, BTO, rent, Tampines, grant).
It outputs a relevance_score and is_relevant flag.

After that:

All obvious spam, too-short, non-alphabetic, or irrelevant rows are dropped.

Why it matters:

Keeps only on-topic, human, Singapore-property-related content for labeling — no junk, no ads.

🔁 3️⃣ DEDUPLICATION
What happens:

Two-level deduplication:

Early dedupe using partial URL + title + selftext signature (_k1).

Hash-based dedupe using MD5(body).

Also checks a global hash checkpoint Parquet file so previously processed posts (from older runs) aren’t repeated.

Why:

Prevents paying API tokens to re-label duplicates or already-seen posts.

🧭 4️⃣ LABELING HELPER FEATURES
What happens:

Adds boolean columns that guide OpenAI labeling later:

has_positive_words, has_negative_words, has_opinion_words, mentions_price, mentions_policy

Each detected via regex.

Combines these + score + relevance_score + word_count into a labeling_priority (0–100 scale).

Why:

High-priority posts (high engagement + strong opinions) are processed first by the labeling model → faster insights, lower cost.

🧩 5️⃣ REGEX ENRICHMENT + SINGLISH DETECTION
Regex enrichment

Reads all patterns from your corpus file
regex_patterns.jsonl → creates columns like rx_ABSD, rx_TDSR, etc.
Each column marks whether a post contains that policy term or pattern.

Singlish detection

Uses your corpus/Singlish/lexicon.csv word list (e.g. lah, leh, lor, aiyo) to:

Extract all Singlish terms in each body

Add a list column singlish_terms

Add boolean has_singlish

Why:

Adds cultural & policy context.
Labeling models (or your custom classifiers later) can use this metadata to interpret tone, context, and demographic patterns.

🧠 6️⃣ ENTITY RULER (Domain NER)
What happens:

Loads your merged EntityRuler patterns from
SGPropertyDomain/spacy_entityruler_patterns.merged.jsonl
(built from all vocab/*.txt and existing JSONL patterns).

Creates a blank spaCy pipeline (spacy.blank("en")), adds the entity_ruler pipe, and loads rules from disk.

Runs nlp.pipe() over all post bodies in batches.

Extracts all detected domain entities into a column entities:

[{"text": "Tampines", "label": "REGION"},
 {"text": "ABSD", "label": "POLICY"}]

Why:

Domain-specific NER identifies property-related entities that general models miss, e.g.:

Locations: “Queenstown”, “Yishun”, “CCR”

Policies: “ABSD”, “SERS”

Property types: “BTO flat”, “Executive Condo”

You can later aggregate mentions by entity type, build frequency maps, or train supervised NER models.

💬 7️⃣ NLP ENRICHMENT (Full spaCy model)
What happens (optional heavy step):

If RUN_NLP_ENRICHMENT=True, runs full spaCy model (en_core_web_sm):

Adds POS tagging, lemmatization, dependency parsing, NER, and noun chunk extraction.

Saves all tokens, lemmas, POS tags, dependency labels, entities, and aspect candidates per document.

Also extracts sentence-level records into a separate Parquet (reddit_common_sentences.parquet).

Example structure:
df_out["tokens"][0]  = ["my", "hdb", "flat", "in", "bishan", "is", "old"]
df_out["lemmas"][0]  = ["my", "hdb", "flat", "in", "bishan", "be", "old"]
df_out["pos"][0]     = ["PRON", "NOUN", "NOUN", "ADP", "PROPN", "AUX", "ADJ"]
df_out["aspect_candidates"][0] = ["hdb flat", "bishan"]

Why:

This provides linguistic features for:

Aspect-based sentiment analysis (which feature is discussed positively/negatively).

Emotion/sentiment model fine-tuning.

Downstream clustering / topic modeling.

Creating word clouds or dependency graphs.

💾 8️⃣ SAVE OUTPUT & REPORT
What’s saved:

Main cleaned dataset → CSV + Parquet
reddit_clean_common_2023_2025.csv

Noise removal report → text summary of all filtering steps.

Preview CSV → top 100 most “important” posts by labeling priority.

NLP outputs

reddit_common_enriched+nlp.parquet

reddit_common_sentences.parquet

Global hash checkpoint updated to avoid reprocessing duplicates later.

📊 Example of final DataFrame columns
Column	Description
hash	Unique MD5 hash of cleaned text
date, year, month, quarter	Temporal metadata
body	Fully cleaned Reddit post/comment text
qm_*	Quality metrics (word count, etc.)
rel_*	Property relevance scores
rx_*	Regex pattern matches (policies, etc.)
has_singlish, singlish_terms	Local slang
entities	Domain-specific NER results
has_positive_words, mentions_price, etc.	Labeling heuristics
labeling_priority	Priority score for labeling
tokens, lemmas, entities_ner, aspect_candidates	Full NLP features
🔍 Summary in one line

The pipeline takes raw noisy Reddit posts, cleans them, filters them, enriches them with linguistic, cultural, and domain knowledge (NER, regex, Singlish, policies), computes labeling and relevance metadata, and produces a final structured corpus that’s both model-ready and human-interpretable.