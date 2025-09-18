"""
PropInsight Reddit Scraper

This module scrapes property-related discussions from r/singapore using the official Reddit API (PRAW).
We use the official API instead of web scraping to:
1. Comply with Reddit's Terms of Service
2. Get structured, reliable data
3. Avoid rate limiting issues
4. Access comment threads efficiently

Target: 4,000 samples from Reddit discussions about Singapore property market
Time Range: September 2023 - September 2025 (24 months)
"""

import praw
import pandas as pd
import json
import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass
import re
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class RedditPost:
    """Data structure for Reddit posts - ensures consistent data format"""
    id: str
    title: str
    selftext: str
    score: int
    upvote_ratio: float
    num_comments: int
    created_utc: float
    author: str
    subreddit: str
    url: str
    permalink: str
    comments: List[Dict]
    
class RedditScraper:
    """
    Reddit scraper for Singapore property discussions
    
    Uses PRAW (Python Reddit API Wrapper) for official API access
    Focuses on property-related content with Singapore context
    """
    
    def __init__(self, output_dir: str = "d:\\Test\\New folder\\PropInsight\\data\\raw\\reddit"):
        """
        Initialize Reddit scraper
        
        Args:
            output_dir: Directory to save scraped data
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Property-related keywords for Singapore context
        # These keywords capture the full spectrum of Singapore property discussions
        self.property_keywords = [
            # Property types
            'BTO', 'HDB', 'resale flat', 'condo', 'condominium', 'landed property', 
            'EC', 'executive condo', 'private property', 'public housing',
            
            # Transactions and processes
            'property buying', 'house hunting', 'property viewing', 'COV', 
            'cash over valuation', 'property valuation', 'mortgage', 'home loan',
            'property agent', 'property investment',
            
            # Policy and market terms
            'cooling measures', 'ABSD', 'additional buyer stamp duty', 
            'property tax', 'interest rate', 'property market', 'property price',
            'property bubble', 'property crash',
            
            # Locations (major Singapore areas)
            'Tampines', 'Punggol', 'Bishan', 'Jurong', 'Woodlands', 'Bedok',
            'Toa Payoh', 'Ang Mo Kio', 'Clementi', 'Pasir Ris', 'Sengkang',
            
            # Sentiment indicators
            'expensive', 'affordable', 'overpriced', 'worth it', 'regret',
            'satisfied', 'disappointed', 'good deal', 'bad investment'
        ]
        
        # Date range: Last 24 months (Sept 2023 - Sept 2025)
        self.start_date = datetime(2023, 9, 1)
        self.end_date = datetime(2025, 9, 18)  # Current date
        
        # Initialize Reddit API client
        self.reddit = None
        self.setup_reddit_client()
        
    def setup_reddit_client(self):
        """
        Setup Reddit API client using PRAW
        
        Note: Requires Reddit API credentials in environment variables:
        - REDDIT_CLIENT_ID
        - REDDIT_CLIENT_SECRET  
        - REDDIT_USER_AGENT
        """
        try:
            # Use environment variables for API credentials (secure approach)
            self.reddit = praw.Reddit(
                client_id=os.getenv('REDDIT_CLIENT_ID', 'your_client_id'),
                client_secret=os.getenv('REDDIT_CLIENT_SECRET', 'your_client_secret'),
                user_agent=os.getenv('REDDIT_USER_AGENT', 'PropInsight:v1.0 (by /u/propinsight_research)')
            )
            
            # Test API connection
            self.reddit.user.me()
            logger.info("Reddit API client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Reddit API client: {e}")
            logger.info("Please set environment variables: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT")
            # Continue without API for development/testing
            self.reddit = None
    
    def is_property_related(self, text: str) -> bool:
        """
        Check if text contains property-related keywords
        
        Args:
            text: Text to check (title + selftext)
            
        Returns:
            bool: True if property-related
        """
        text_lower = text.lower()
        
        # Check for any property keywords
        for keyword in self.property_keywords:
            if keyword.lower() in text_lower:
                return True
                
        return False
    
    def is_within_date_range(self, created_utc: float) -> bool:
        """
        Check if post is within our target date range
        
        Args:
            created_utc: Unix timestamp from Reddit
            
        Returns:
            bool: True if within range
        """
        post_date = datetime.fromtimestamp(created_utc)
        return self.start_date <= post_date <= self.end_date
    
    def extract_comments(self, submission, max_comments: int = 10) -> List[Dict]:
        """
        Extract top-level comments from a Reddit submission
        
        Args:
            submission: PRAW submission object
            max_comments: Maximum comments to extract per post
            
        Returns:
            List of comment dictionaries
        """
        comments = []
        
        try:
            # Get top-level comments only (avoid deep thread traversal)
            submission.comments.replace_more(limit=0)
            
            for comment in submission.comments[:max_comments]:
                if hasattr(comment, 'body') and comment.body != '[deleted]':
                    comments.append({
                        'id': comment.id,
                        'body': comment.body,
                        'score': comment.score,
                        'created_utc': comment.created_utc,
                        'author': str(comment.author) if comment.author else '[deleted]'
                    })
                    
        except Exception as e:
            logger.warning(f"Error extracting comments: {e}")
            
        return comments
    
    def scrape_subreddit_search(self, subreddit_name: str, query: str, limit: int = 1000) -> List[RedditPost]:
        """
        Search for posts in a subreddit using specific query
        
        Args:
            subreddit_name: Name of subreddit (e.g., 'singapore')
            query: Search query string
            limit: Maximum posts to retrieve
            
        Returns:
            List of RedditPost objects
        """
        posts = []
        
        if not self.reddit:
            logger.error("Reddit API client not initialized")
            return posts
            
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            # Search for posts with property keywords
            # Use 'all' time filter to get posts from our date range
            search_results = subreddit.search(query, sort='relevance', time_filter='all', limit=limit)
            
            for submission in search_results:
                # Check date range
                if not self.is_within_date_range(submission.created_utc):
                    continue
                
                # Check if property-related (double-check beyond search)
                full_text = f"{submission.title} {submission.selftext}"
                if not self.is_property_related(full_text):
                    continue
                
                # Extract comments
                comments = self.extract_comments(submission)
                
                # Create RedditPost object
                post = RedditPost(
                    id=submission.id,
                    title=submission.title,
                    selftext=submission.selftext,
                    score=submission.score,
                    upvote_ratio=submission.upvote_ratio,
                    num_comments=submission.num_comments,
                    created_utc=submission.created_utc,
                    author=str(submission.author) if submission.author else '[deleted]',
                    subreddit=submission.subreddit.display_name,
                    url=submission.url,
                    permalink=submission.permalink,
                    comments=comments
                )
                
                posts.append(post)
                
                # Rate limiting: small delay between requests
                time.sleep(0.1)
                
                if len(posts) % 100 == 0:
                    logger.info(f"Scraped {len(posts)} posts from r/{subreddit_name}")
                    
        except Exception as e:
            logger.error(f"Error scraping r/{subreddit_name}: {e}")
            
        return posts
    
    def scrape_subreddit_hot_new(self, subreddit_name: str, limit: int = 1000) -> List[RedditPost]:
        """
        Scrape hot and new posts from subreddit (broader collection)
        
        Args:
            subreddit_name: Name of subreddit
            limit: Maximum posts to retrieve
            
        Returns:
            List of RedditPost objects
        """
        posts = []
        
        if not self.reddit:
            logger.error("Reddit API client not initialized")
            return posts
            
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            # Get hot posts
            hot_posts = list(subreddit.hot(limit=limit//2))
            # Get new posts  
            new_posts = list(subreddit.new(limit=limit//2))
            
            all_posts = hot_posts + new_posts
            
            for submission in all_posts:
                # Check date range
                if not self.is_within_date_range(submission.created_utc):
                    continue
                
                # Check if property-related
                full_text = f"{submission.title} {submission.selftext}"
                if not self.is_property_related(full_text):
                    continue
                
                # Extract comments
                comments = self.extract_comments(submission)
                
                # Create RedditPost object
                post = RedditPost(
                    id=submission.id,
                    title=submission.title,
                    selftext=submission.selftext,
                    score=submission.score,
                    upvote_ratio=submission.upvote_ratio,
                    num_comments=submission.num_comments,
                    created_utc=submission.created_utc,
                    author=str(submission.author) if submission.author else '[deleted]',
                    subreddit=submission.subreddit.display_name,
                    url=submission.url,
                    permalink=submission.permalink,
                    comments=comments
                )
                
                posts.append(post)
                
                # Rate limiting
                time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error scraping hot/new from r/{subreddit_name}: {e}")
            
        return posts
    
    def save_posts_to_json(self, posts: List[RedditPost], filename: str):
        """
        Save posts to JSON file with proper formatting
        
        Args:
            posts: List of RedditPost objects
            filename: Output filename
        """
        # Convert dataclass objects to dictionaries
        posts_dict = []
        for post in posts:
            post_dict = {
                'id': post.id,
                'title': post.title,
                'selftext': post.selftext,
                'score': post.score,
                'upvote_ratio': post.upvote_ratio,
                'num_comments': post.num_comments,
                'created_utc': post.created_utc,
                'created_date': datetime.fromtimestamp(post.created_utc).isoformat(),
                'author': post.author,
                'subreddit': post.subreddit,
                'url': post.url,
                'permalink': f"https://reddit.com{post.permalink}",
                'comments': post.comments,
                'source': 'reddit',
                'scraped_at': datetime.now().isoformat()
            }
            posts_dict.append(post_dict)
        
        # Save to JSON file
        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(posts_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(posts)} posts to {output_path}")
    
    def run_full_scrape(self) -> Dict[str, int]:
        """
        Run complete Reddit scraping process
        
        Returns:
            Dictionary with scraping statistics
        """
        logger.info("Starting Reddit scraping for PropInsight dataset")
        
        all_posts = []
        stats = {
            'total_posts': 0,
            'singapore_posts': 0,
            'asksingapore_posts': 0,
            'singaporefi_posts': 0,
            'total_comments': 0
        }
        
        # Target subreddits for Singapore property discussions
        subreddits = ['singapore', 'askSingapore', 'singaporefi']
        
        for subreddit_name in subreddits:
            logger.info(f"Scraping r/{subreddit_name}")
            
            # Method 1: Search-based scraping (more targeted)
            search_queries = [
                'BTO OR HDB OR property OR housing',
                'condo OR condominium OR "landed property"',
                'ABSD OR "cooling measures" OR "property market"',
                'mortgage OR "home loan" OR "property investment"'
            ]
            
            subreddit_posts = []
            
            for query in search_queries:
                posts = self.scrape_subreddit_search(subreddit_name, query, limit=500)
                subreddit_posts.extend(posts)
                time.sleep(1)  # Rate limiting between searches
            
            # Method 2: Hot/New posts (broader collection)
            hot_new_posts = self.scrape_subreddit_hot_new(subreddit_name, limit=1000)
            subreddit_posts.extend(hot_new_posts)
            
            # Remove duplicates based on post ID
            unique_posts = {}
            for post in subreddit_posts:
                unique_posts[post.id] = post
            
            subreddit_posts = list(unique_posts.values())
            
            # Update statistics
            stats[f'{subreddit_name.lower()}_posts'] = len(subreddit_posts)
            stats['total_comments'] += sum(len(post.comments) for post in subreddit_posts)
            
            all_posts.extend(subreddit_posts)
            
            # Save subreddit-specific file
            if subreddit_posts:
                self.save_posts_to_json(subreddit_posts, f'reddit_{subreddit_name}_{datetime.now().strftime("%Y%m%d")}.json')
            
            logger.info(f"Collected {len(subreddit_posts)} posts from r/{subreddit_name}")
        
        # Remove duplicates across all subreddits
        unique_all_posts = {}
        for post in all_posts:
            unique_all_posts[post.id] = post
        
        final_posts = list(unique_all_posts.values())
        stats['total_posts'] = len(final_posts)
        
        # Save combined file
        if final_posts:
            self.save_posts_to_json(final_posts, f'reddit_combined_{datetime.now().strftime("%Y%m%d")}.json')
        
        # Save statistics
        stats_path = self.output_dir / f'reddit_stats_{datetime.now().strftime("%Y%m%d")}.json'
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Reddit scraping completed. Total posts: {stats['total_posts']}")
        return stats

def main():
    """Main function to run Reddit scraper"""
    scraper = RedditScraper()
    stats = scraper.run_full_scrape()
    
    print("\n=== Reddit Scraping Results ===")
    print(f"Total posts collected: {stats['total_posts']}")
    print(f"Total comments collected: {stats['total_comments']}")
    print(f"r/singapore posts: {stats['singapore_posts']}")
    print(f"r/askSingapore posts: {stats['asksingapore_posts']}")
    print(f"r/singaporefi posts: {stats['singaporefi_posts']}")

if __name__ == "__main__":
    main()