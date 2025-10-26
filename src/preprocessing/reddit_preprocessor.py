import os
import glob
import json
import argparse
import hashlib
import html
import re
from typing import Any, Dict, List

import pandas as pd

# Regex patterns
URL_PATTERN = re.compile(r"https?://\S+|www\.\S+", flags=re.IGNORECASE)
MD_LINK_PATTERN = re.compile(r"\[([^\]]+)\]\((https?:\/\/[^\)]+)\)")
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")

# Broad emoji ranges (safe across most Python builds)
EMOJI_PATTERN = re.compile(
    """
    [\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF
     \U0001F1E0-\U0001F1FF\U00002700-\U000027BF\U000024C2-\U0001F251]
    """,
    flags=re.UNICODE,
)


def files_from_glob(input_glob: str) -> List[str]:
    paths = glob.glob(input_glob)
    if not paths:
        # Fallback to any raw file that contains 'reddit' in its name
        # Try both relative and absolute paths
        fallback_patterns = [
            os.path.join('data', 'raw', '*reddit*.json*'),
            os.path.join('..', '..', 'data', 'raw', '*reddit*.json*'),
            os.path.join(os.getcwd(), '..', '..', 'data', 'raw', '*reddit*.json*')
        ]
        for pattern in fallback_patterns:
            paths = glob.glob(pattern)
            if paths:
                break
    return paths


def read_records(path: str) -> List[Dict[str, Any]]:
    """Read a JSON or JSONL file containing Reddit exports and return list of post dicts."""
    with open(path, 'r', encoding='utf-8') as f:
        first_char = f.read(1)
        f.seek(0)
        if first_char == '[':
            return json.load(f)
        # JSONL fallback
        records = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                # If it's not JSONL, attempt full JSON load later
                pass
        if records:
            return records
        f.seek(0)
        return json.load(f)


def normalize_posts_comments(posts: List[Dict[str, Any]]) -> pd.DataFrame:
    """Create rows for posts and flattened comments including parent fields."""
    # Post rows
    post_rows = []
    for p in posts:
        post_rows.append({
            'post_id': p.get('post_id'),
            'comment_id': None,
            'comment_body': '',
            'subreddit': p.get('subreddit'),
            'title': p.get('title') or '',
            'selftext': p.get('selftext') or '',
            'author': p.get('author'),
            'score': p.get('score'),
            'created_utc': p.get('created_utc'),
            'permalink': p.get('permalink'),
            'url': p.get('url'),
            'record_type': 'post',
        })
    df_posts = pd.DataFrame(post_rows)

    # Comment rows via pd.json_normalize
    try:
        df_comments = pd.json_normalize(
            posts,
            record_path=['comments'],
            meta=['post_id', 'subreddit', 'title', 'selftext', 'permalink', 'url'],
            errors='ignore',
        )
    except Exception:
        df_comments = pd.DataFrame(columns=['comment_id', 'comment_author', 'comment_body', 'comment_score', 'created_utc', 'post_id', 'subreddit', 'title', 'selftext', 'permalink', 'url'])

    # Standardize column names
    rename_map = {
        'comment_author': 'author',
        'comment_body': 'comment_body',
        'comment_score': 'score',
        # 'created_utc' is already the comment timestamp in most exports
    }
    for src, dst in rename_map.items():
        if src in df_comments.columns:
            df_comments.rename(columns={src: dst}, inplace=True)

    # Ensure necessary columns exist
    for col in ['comment_id', 'comment_body', 'author', 'score', 'created_utc', 'post_id', 'subreddit', 'title', 'selftext', 'permalink', 'url']:
        if col not in df_comments.columns:
            df_comments[col] = None

    for col in ['title', 'selftext', 'comment_body']:
        if col in df_comments.columns:
            df_comments[col] = df_comments[col].fillna('')
        else:
            df_comments[col] = ''

    df_comments['record_type'] = 'comment'
    df_comments = df_comments[[
        'post_id', 'comment_id', 'comment_body', 'subreddit', 'title', 'selftext', 'author', 'score', 'created_utc', 'permalink', 'url', 'record_type'
    ]].copy()

    # Combine
    df = pd.concat([df_posts, df_comments], ignore_index=True, sort=False)
    return df


def parse_date(df: pd.DataFrame) -> pd.DataFrame:
    """Convert created_utc to timezone-aware datetime; derive year, month, quarter."""
    def parse(x):
        if pd.isna(x):
            return pd.NaT
        # Numeric epoch handling
        try:
            xi = float(x)
            if xi > 1e12:  # milliseconds
                return pd.to_datetime(xi, unit='ms', utc=True)
            elif xi > 1e10:  # microseconds
                return pd.to_datetime(xi, unit='us', utc=True)
            else:
                return pd.to_datetime(xi, unit='s', utc=True)
        except Exception:
            # ISO or other string format
            try:
                return pd.to_datetime(x, utc=True)
            except Exception:
                return pd.NaT

    df['date'] = df['created_utc'].apply(parse)
    
    # Only add datetime features if we have valid dates
    if df['date'].notna().any():
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['quarter'] = df['date'].dt.to_period('Q').astype(str)
    else:
        # If no valid dates, create empty columns
        df['year'] = None
        df['month'] = None
        df['quarter'] = None
    
    return df


def clean_text(text: str) -> str:
    if not isinstance(text, str):
        text = '' if text is None else str(text)
    s = html.unescape(text)
    # Keep anchor text from markdown links
    s = MD_LINK_PATTERN.sub(r"\1", s)
    # Remove URLs
    s = URL_PATTERN.sub(' ', s)
    # Remove HTML tags
    s = HTML_TAG_PATTERN.sub(' ', s)
    # Remove deleted/removed markers
    s = re.sub(r"\[(deleted|removed)\]", ' ', s, flags=re.IGNORECASE)
    # Remove emojis (fallback for narrow builds)
    try:
        s = EMOJI_PATTERN.sub('', s)
    except Exception:
        s = re.sub(r"[\ud800-\udfff]", '', s)
    # Normalize repeated punctuation
    s = re.sub(r"([!?.,])\1{2,}", r"\1", s)
    # Strip markdown decorations
    s = re.sub(r"`+|\*+", '', s)
    # Collapse whitespace
    s = re.sub(r"\s+", ' ', s)
    return s.strip()


def build_body(df: pd.DataFrame) -> pd.DataFrame:
    df['comment_body'] = df['comment_body'].fillna('')
    df['title'] = df['title'].fillna('')
    df['selftext'] = df['selftext'].fillna('')
    df['body'] = (df['title'] + "\n\n" + df['selftext'] + "\n\n" + df['comment_body']).str.strip()
    df['body_clean'] = df['body'].apply(clean_text)
    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    def md5(s: str) -> str:
        s = (s or '').lower().strip()
        return hashlib.md5(s.encode('utf-8')).hexdigest()

    df['hash'] = df['body_clean'].apply(md5)
    df = df.drop_duplicates(subset=['hash']).copy()
    return df


def boilerplate_filter(df: pd.DataFrame) -> pd.DataFrame:
    def is_low_signal(s: str) -> bool:
        if not s or len(s.strip()) < 30:
            return True
        alnum = sum(c.isalnum() for c in s)
        if alnum / max(len(s), 1) < 0.2:
            return True
        if not re.search(r"[A-Za-z]", s):
            return True
        return False

    mask = df['body_clean'].apply(lambda x: not is_low_signal(x))
    return df[mask].copy()


def filter_by_date(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    return df[(df['date'] >= start) & (df['date'] <= end)].copy()


def run_pipeline(input_glob: str, start: str, end: str, out_dir: str) -> pd.DataFrame:
    paths = files_from_glob(input_glob)
    if not paths:
        print("No input files found for glob:", input_glob)
    all_posts: List[Dict[str, Any]] = []
    for p in paths:
        try:
            posts = read_records(p)
            all_posts.extend(posts)
        except Exception as e:
            print(f"Failed to read {p}: {e}")

    df = normalize_posts_comments(all_posts)
    df = parse_date(df)

    start_ts = pd.to_datetime(start, utc=True)
    # If end is a date without time, include the full day
    if 'T' in end:
        end_ts = pd.to_datetime(end, utc=True)
    else:
        end_ts = pd.to_datetime(end + ' 23:59:59', utc=True)

    df = filter_by_date(df, start_ts, end_ts)
    df = build_body(df)
    df = deduplicate(df)
    df = boilerplate_filter(df)

    # Final column order
    cols = [
        'post_id', 'comment_id', 'subreddit', 'author', 'score', 'created_utc', 'date', 'year', 'month', 'quarter',
        'title', 'selftext', 'comment_body', 'body', 'body_clean', 'hash', 'permalink', 'url', 'record_type'
    ]
    df = df[cols]

    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, 'reddit_corpus_2023_2025.parquet')
    csv_path = os.path.join(out_dir, 'reddit_corpus_2023_2025.csv.gz')

    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        print(f"Parquet save failed: {e}")
    try:
        df.to_csv(csv_path, index=False, encoding='utf-8', compression='gzip')
    except Exception as e:
        print(f"CSV save failed: {e}")

    print(f"Saved {len(df)} rows to:\n - {parquet_path}\n - {csv_path}")
    post_rows = int((df['record_type'] == 'post').sum())
    comment_rows = int((df['record_type'] == 'comment').sum())
    print(f"Posts (2023–2025): {post_rows}")
    print(f"Comments (2023–2025): {comment_rows}")

    return df


def main():
    parser = argparse.ArgumentParser(description='Preprocess Reddit posts and comments (2023–2025)')
    parser.add_argument('--input-glob', default=os.path.join('data', 'raw', '*reddit*.json*'), help='Glob for input JSON/JSONL files')
    parser.add_argument('--start', default='2023-01-01', help='Start date (inclusive)')
    parser.add_argument('--end', default='2025-12-31', help='End date (inclusive)')
    parser.add_argument('--out-dir', default=os.path.join('data', 'processed', 'reddit'), help='Output directory')
    args = parser.parse_args()

    run_pipeline(args.input_glob, args.start, args.end, args.out_dir)


if __name__ == '__main__':
    main()