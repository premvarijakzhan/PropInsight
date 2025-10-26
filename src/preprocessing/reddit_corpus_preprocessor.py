#!/usr/bin/env python3
"""
Reddit Corpus Preprocessor with Singlish and Property Domain Integration

This script processes the Reddit corpus CSV file and enriches it with:
1. Singlish vocabulary detection and normalization
2. Property domain terminology extraction
3. Text cleaning and quality filtering
4. Entity recognition for Singapore property terms
5. Sentiment and topic modeling preparation


"""

import os
import re
import json
import argparse
import logging
import hashlib
import html
from typing import Dict, Any, List, Set, Tuple
from pathlib import Path

import pandas as pd
import numpy as np
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Text cleaning patterns
URL_PATTERN = re.compile(r"https?://\S+|www\.\S+", flags=re.IGNORECASE)
MD_LINK_PATTERN = re.compile(r"\[([^\]]+)\]\((https?:\/\/[^\)]+)\)")
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
EMOJI_PATTERN = re.compile(
    """
    [\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF
     \U0001F1E0-\U0001F1FF\U00002700-\U000027BF\U000024C2-\U0001F251]
    """,
    flags=re.UNICODE,
)

# Navigation and noise patterns
NAVIGATION_PATTERNS = [
    r"About\s*Us",
    r"Press\s*Releases", 
    r"Share\s*this\s*page.*",
    r"Back\s*to\s*top",
    r"Sitemap",
    r"Privacy\s*Statement",
    r"Terms\s*of\s*Use",
    r"Contact\s*Us",
]

class SinglishCorpusLoader:
    """Loads and manages Singlish vocabulary corpus"""
    
    def __init__(self, corpus_path: str):
        self.corpus_path = Path(corpus_path)
        self.singlish_terms = set()
        self.singlish_lexicon = {}
        self.load_singlish_corpus()
    
    def load_singlish_corpus(self):
        """Load Singlish vocabulary from lexicon.csv and vocabulary files"""
        try:
            print("Loading Singlish lexicon...")
            # Load main lexicon
            lexicon_file = self.corpus_path / "lexicon.csv"
            if lexicon_file.exists():
                df = pd.read_csv(lexicon_file)
                for _, row in df.iterrows():
                    word = str(row.get('Word', '')).lower().strip()
                    if word and word != 'nan':
                        self.singlish_terms.add(word)
                        self.singlish_lexicon[word] = {
                            'description': row.get('Description(Final)', ''),
                            'example': row.get('Example(Final)', ''),
                            'pos': row.get('POS', ''),
                            'origin': row.get('Origin', '')
                        }
                logger.info(f"Loaded {len(self.singlish_terms)} terms from Singlish lexicon")
            
            print("Loading Singlish vocabulary files...")
            # Load vocabulary files
            vocab_dir = self.corpus_path / "vocabulary"
            if vocab_dir.exists():
                vocab_files = list(vocab_dir.glob("*.jsonl"))
                for vocab_file in tqdm(vocab_files, desc="Loading vocabulary files"):
                    try:
                        with open(vocab_file, 'r', encoding='utf-8') as f:
                            for line in f:
                                if line.strip():
                                    data = json.loads(line)
                                    term = data.get('term', '').lower().strip()
                                    if term:
                                        self.singlish_terms.add(term)
                    except Exception as e:
                        print(f"Warning: Could not load {vocab_file}: {e}")
            
            print(f"Loaded {len(self.singlish_terms):,} Singlish terms")
                        
        except Exception as e:
            logger.error(f"Error loading Singlish corpus: {e}")
    
    def detect_singlish_terms(self, text: str) -> List[str]:
        """Detect Singlish terms in text"""
        if not text:
            return []
        
        text_lower = text.lower()
        found_terms = []
        
        for term in self.singlish_terms:
            # Use word boundaries for better matching
            pattern = r'\b' + re.escape(term) + r'\b'
            if re.search(pattern, text_lower):
                found_terms.append(term)
        
        return found_terms

class PropertyDomainCorpus:
    """Loads and manages Singapore property domain corpus"""
    
    def __init__(self, corpus_path: str):
        self.corpus_path = Path(corpus_path)
        self.property_terms = {}
        self.property_categories = set()
        self.load_property_corpus()
    
    def load_property_corpus(self):
        """Load property domain vocabulary from glossary.csv"""
        try:
            print("Loading property domain glossary...")
            glossary_file = self.corpus_path / "glossary.csv"
            if glossary_file.exists():
                df = pd.read_csv(glossary_file)
                for _, row in df.iterrows():
                    term = str(row.get('term', '')).lower().strip()
                    category = str(row.get('category', '')).strip()
                    definition = str(row.get('definition', '')).strip()
                    aliases = str(row.get('aliases', '')).strip()
                    
                    if term and term != 'nan':
                        self.property_terms[term] = {
                            'category': category,
                            'definition': definition,
                            'aliases': aliases.split('|') if aliases and aliases != 'nan' else []
                        }
                        self.property_categories.add(category)
                        
                        # Add aliases as separate terms
                        if aliases and aliases != 'nan':
                            for alias in aliases.split('|'):
                                alias = alias.lower().strip()
                                if alias:
                                    self.property_terms[alias] = {
                                        'category': category,
                                        'definition': definition,
                                        'is_alias': True,
                                        'main_term': term
                                    }
            
            print("Loading property domain vocabulary files...")
            vocab_files = list(self.corpus_path.glob("**/*.jsonl"))
            for file_path in tqdm(vocab_files, desc="Loading property vocabulary"):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            try:
                                data = json.loads(line.strip())
                                if 'term' in data:
                                    term = data['term'].lower().strip()
                                    if term:
                                        self.property_terms[term] = {
                                            'category': 'general',
                                            'definition': data.get('definition', ''),
                                            'aliases': []
                                        }
                                        self.property_categories.add('general')
                                elif 'word' in data:
                                    term = data['word'].lower().strip()
                                    if term:
                                        self.property_terms[term] = {
                                            'category': 'general',
                                            'definition': data.get('definition', ''),
                                            'aliases': []
                                        }
                                        self.property_categories.add('general')
                            except json.JSONDecodeError:
                                continue
                except Exception as e:
                    print(f"Warning: Could not load {file_path}: {e}")
            
            print(f"Loaded {len(self.property_terms):,} property domain terms")
            logger.info(f"Loaded {len(self.property_terms)} property terms from {len(self.property_categories)} categories")
                
        except Exception as e:
            logger.error(f"Error loading property corpus: {e}")
    
    def detect_property_terms(self, text: str) -> Dict[str, List[str]]:
        """Detect property terms in text, grouped by category"""
        if not text:
            return {}
        
        text_lower = text.lower()
        found_terms = {}
        
        for term, info in self.property_terms.items():
            pattern = r'\b' + re.escape(term) + r'\b'
            if re.search(pattern, text_lower):
                category = info['category']
                if category not in found_terms:
                    found_terms[category] = []
                found_terms[category].append(term)
        
        return found_terms

class RedditCorpusPreprocessor:
    """Main preprocessor for Reddit corpus with Singlish and property domain integration"""
    
    def __init__(self, singlish_corpus_path: str, property_corpus_path: str):
        self.singlish_loader = SinglishCorpusLoader(singlish_corpus_path)
        self.property_loader = PropertyDomainCorpus(property_corpus_path)
        
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text or pd.isna(text):
            return ""
        
        # Convert to string and decode HTML entities
        text = str(text)
        text = html.unescape(text)
        
        # Remove URLs and markdown links
        text = URL_PATTERN.sub(" ", text)
        text = MD_LINK_PATTERN.sub(r"\1", text)
        
        # Remove HTML tags
        text = HTML_TAG_PATTERN.sub(" ", text)
        
        # Remove navigation patterns
        for pattern in NAVIGATION_PATTERNS:
            text = re.sub(pattern, " ", text, flags=re.IGNORECASE)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def calculate_quality_metrics(self, text: str) -> Dict[str, float]:
        """Calculate text quality metrics"""
        if not text:
            return {
                'word_count': 0,
                'char_count': 0,
                'avg_word_length': 0,
                'uppercase_ratio': 0,
                'digit_ratio': 0,
                'unique_word_ratio': 0
            }
        
        words = text.split()
        word_count = len(words)
        char_count = len(text)
        
        if word_count == 0:
            return {
                'word_count': 0,
                'char_count': char_count,
                'avg_word_length': 0,
                'uppercase_ratio': 0,
                'digit_ratio': 0,
                'unique_word_ratio': 0
            }
        
        avg_word_length = sum(len(word) for word in words) / word_count
        uppercase_ratio = sum(1 for c in text if c.isupper()) / char_count if char_count > 0 else 0
        digit_ratio = sum(1 for c in text if c.isdigit()) / char_count if char_count > 0 else 0
        unique_word_ratio = len(set(words)) / word_count
        
        return {
            'word_count': word_count,
            'char_count': char_count,
            'avg_word_length': avg_word_length,
            'uppercase_ratio': uppercase_ratio,
            'digit_ratio': digit_ratio,
            'unique_word_ratio': unique_word_ratio
        }
    
    def is_spam_or_low_quality(self, text: str, metrics: Dict[str, float]) -> bool:
        """Determine if text is spam or low quality"""
        if metrics['word_count'] < 5:
            return True
        
        if metrics['uppercase_ratio'] > 0.5:
            return True
        
        if metrics['unique_word_ratio'] < 0.3 and metrics['word_count'] > 10:
            return True
        
        # Check for common spam patterns
        spam_patterns = [
            r'click\s+here',
            r'buy\s+now',
            r'limited\s+time',
            r'act\s+fast',
            r'\$\d+.*free',
        ]
        
        text_lower = text.lower()
        for pattern in spam_patterns:
            if re.search(pattern, text_lower):
                return True
        
        return False
    
    def generate_content_hash(self, text: str) -> str:
        """Generate hash for deduplication"""
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def process_corpus(self, input_file: str, output_file: str) -> Dict[str, Any]:
        """Process the Reddit corpus CSV file"""
        logger.info(f"Loading Reddit corpus from {input_file}")
        
        try:
            df = pd.read_csv(input_file)
            logger.info(f"Loaded {len(df):,} records")
            
            logger.info("Loading corpora...")
            logger.info("Initializing text cleaner...")
            
            logger.info("Processing records...")
            
            # Initialize new columns
            df['body_clean'] = ""
            df['singlish_terms'] = ""
            df['singlish_count'] = 0
            df['property_terms'] = ""
            df['property_categories'] = ""
            df['property_count'] = 0
            df['quality_score'] = 0.0
            df['is_spam'] = False
            df['content_hash'] = ""
            df['word_count'] = 0
            df['char_count'] = 0
            df['has_singlish'] = False
            df['has_property_terms'] = False
            
            processed_hashes = set()
            duplicate_count = 0
            spam_count = 0
            
            for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing records"):
                # Combine text fields
                title = str(row.get('title', '')) if pd.notna(row.get('title')) else ""
                selftext = str(row.get('selftext', '')) if pd.notna(row.get('selftext')) else ""
                comment_body = str(row.get('comment_body', '')) if pd.notna(row.get('comment_body')) else ""
                body = str(row.get('body', '')) if pd.notna(row.get('body')) else ""
                
                # Use existing body or combine fields
                if body and body != 'nan':
                    combined_text = body
                else:
                    combined_text = f"{title} {selftext} {comment_body}".strip()
                
                # Clean text
                clean_text = self.clean_text(combined_text)
                df.at[idx, 'body_clean'] = clean_text
                
                # Generate hash for deduplication
                content_hash = self.generate_content_hash(clean_text)
                df.at[idx, 'content_hash'] = content_hash
                
                # Check for duplicates
                if content_hash in processed_hashes:
                    duplicate_count += 1
                    continue
                processed_hashes.add(content_hash)
                
                # Calculate quality metrics
                metrics = self.calculate_quality_metrics(clean_text)
                df.at[idx, 'word_count'] = metrics['word_count']
                df.at[idx, 'char_count'] = metrics['char_count']
                
                # Check for spam/low quality
                is_spam = self.is_spam_or_low_quality(clean_text, metrics)
                df.at[idx, 'is_spam'] = is_spam
                if is_spam:
                    spam_count += 1
                
                # Calculate quality score (0-1)
                quality_score = min(1.0, (
                    metrics['unique_word_ratio'] * 0.3 +
                    min(1.0, metrics['word_count'] / 50) * 0.3 +
                    (1 - metrics['uppercase_ratio']) * 0.2 +
                    min(1.0, metrics['avg_word_length'] / 6) * 0.2
                ))
                df.at[idx, 'quality_score'] = quality_score
                
                # Detect Singlish terms
                singlish_terms = self.singlish_loader.detect_singlish_terms(clean_text)
                df.at[idx, 'singlish_terms'] = "|".join(singlish_terms)
                df.at[idx, 'singlish_count'] = len(singlish_terms)
                df.at[idx, 'has_singlish'] = len(singlish_terms) > 0
                
                # Detect property terms
                property_terms_dict = self.property_loader.detect_property_terms(clean_text)
                all_property_terms = []
                categories = []
                
                for category, terms in property_terms_dict.items():
                    all_property_terms.extend(terms)
                    categories.append(category)
                
                df.at[idx, 'property_terms'] = "|".join(all_property_terms)
                df.at[idx, 'property_categories'] = "|".join(categories)
                df.at[idx, 'property_count'] = len(all_property_terms)
                df.at[idx, 'has_property_terms'] = len(all_property_terms) > 0
                
                if idx % 1000 == 0:
                    logger.info(f"Processed {idx} records...")
            
            # Remove duplicates
            print("Removing duplicates...")
            initial_count = len(df)
            df = df.drop_duplicates(subset=['content_hash'], keep='first')
            duplicates_removed = initial_count - len(df)
            print(f"Removed {duplicates_removed:,} duplicates")
            
            # Remove spam
            print("Filtering spam...")
            initial_count = len(df)
            df_clean = df[~df['is_spam']]
            spam_removed = initial_count - len(df_clean)
            print(f"Removed {spam_removed:,} spam records")
            
            # Sort by quality score and relevance
            df_clean = df_clean.sort_values([
                'has_property_terms', 
                'has_singlish', 
                'quality_score', 
                'property_count', 
                'singlish_count'
            ], ascending=False)
            
            # Save processed data
            print(f"Saving processed data to {output_file}...")
            df_clean.to_csv(output_file, index=False)
            print(f"Saved {len(df_clean):,} processed records")
            
            # Generate and save report
            print("Generating processing report...")
            report = {
                'total_records': len(df),
                'processed_records': len(df_clean),
                'duplicates_removed': duplicates_removed,
                'spam_removed': spam_removed,
                'records_with_singlish': len(df_clean[df_clean['has_singlish']]),
                'records_with_property_terms': len(df_clean[df_clean['has_property_terms']]),
                'avg_quality_score': df_clean['quality_score'].mean(),
                'avg_word_count': df_clean['word_count'].mean(),
                'singlish_terms_found': df_clean['singlish_count'].sum(),
                'property_terms_found': df_clean['property_count'].sum(),
                'top_property_categories': df_clean['property_categories'].str.split('|').explode().value_counts().head(10).to_dict(),
                'top_singlish_terms': df_clean['singlish_terms'].str.split('|').explode().value_counts().head(10).to_dict()
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Error processing corpus: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description="Reddit Corpus Preprocessor with Singlish and Property Domain Integration")
    parser.add_argument("--input", "-i", required=True, help="Input Reddit corpus CSV file")
    parser.add_argument("--output", "-o", required=True, help="Output processed CSV file")
    parser.add_argument("--singlish-corpus", "-s", required=True, help="Path to Singlish corpus directory")
    parser.add_argument("--property-corpus", "-p", required=True, help="Path to property domain corpus directory")
    parser.add_argument("--report", "-r", help="Output report JSON file")
    
    args = parser.parse_args()
    
    # Initialize preprocessor
    preprocessor = RedditCorpusPreprocessor(args.singlish_corpus, args.property_corpus)
    
    # Process corpus
    report = preprocessor.process_corpus(args.input, args.output)
    
    # Save report
    if args.report:
        with open(args.report, 'w') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Report saved to {args.report}")
    
    # Print summary
    logger.info("Processing completed!")
    logger.info(f"Total records processed: {report['processed_records']}")
    logger.info(f"Records with Singlish: {report['records_with_singlish']}")
    logger.info(f"Records with property terms: {report['records_with_property_terms']}")
    logger.info(f"Average quality score: {report['avg_quality_score']:.3f}")

if __name__ == "__main__":
    main()