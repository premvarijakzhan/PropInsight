"""
PropInsight HardwareZone EDMW Scraper

This module scrapes property-related discussions from HardwareZone's EDMW (Eat Drink Man Woman) forum.
EDMW is Singapore's most active online forum with candid property discussions and market sentiment.

Key features:
1. Session management with login simulation
2. Forum thread navigation and pagination
3. Property-focused content filtering
4. Sentiment analysis from forum posts
5. Anti-detection measures for sustained scraping

Target: 5,000 samples from HardwareZone EDMW property discussions
Time Range: September 2023 - September 2025 (24 months)
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HardwareZonePost:
    """Data structure for HardwareZone forum posts"""
    id: str
    thread_title: str
    post_content: str
    author: str
    post_number: int
    date_posted: datetime
    thread_url: str
    post_url: str
    likes: int
    replies_count: int
    sentiment_score: Optional[float]
    sentiment_confidence: str
    
class HardwareZoneScraper:
    """
    HardwareZone EDMW scraper for Singapore property discussions
    
    Scrapes from the popular EDMW forum which contains:
    1. Property investment discussions
    2. Market sentiment and opinions
    3. Personal experiences with property purchases
    4. Policy discussions and reactions
    """
    
    def __init__(self, output_dir: str = "d:\\Test\\New folder\\PropInsight\\data\\raw\\hardwarezone"):
        """
        Initialize HardwareZone scraper
        
        Args:
            output_dir: Directory to save scraped data
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # HardwareZone URLs
        self.base_url = 'https://forums.hardwarezone.com.sg'
        self.edmw_url = f'{self.base_url}/forums/eat-drink-man-woman.16/'
        
        # HardwareZone search URLs for property-related discussions
        # Enhanced to target HomeSeekers forum (node 74) specifically
        self.search_urls = {
            'property': 'https://forums.hardwarezone.com.sg/search/search?keywords=property&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date',
            'bto': 'https://forums.hardwarezone.com.sg/search/search?keywords=BTO&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date',
            'hdb': 'https://forums.hardwarezone.com.sg/search/search?keywords=HDB&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date',
            'condo': 'https://forums.hardwarezone.com.sg/search/search?keywords=condo&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date',
            'resale': 'https://forums.hardwarezone.com.sg/search/search?keywords=resale&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date',
            'rental': 'https://forums.hardwarezone.com.sg/search/search?keywords=rental&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date',
            'mortgage': 'https://forums.hardwarezone.com.sg/search/search?keywords=mortgage&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date',
            'investment': 'https://forums.hardwarezone.com.sg/search/search?keywords=investment&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date',
            'ec': 'https://forums.hardwarezone.com.sg/search/search?keywords=EC&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date',
            'landed': 'https://forums.hardwarezone.com.sg/search/search?keywords=landed&c%5Bchild_nodes%5D=1&c%5Bnodes%5D%5B0%5D=74&o=date'
        }
        
        # Direct HomeSeekers forum URLs for comprehensive scraping
        self.homeseeker_urls = [
            'https://forums.hardwarezone.com.sg/forums/homeseekers-and-homemakers.74/',
            'https://forums.hardwarezone.com.sg/forums/homeseekers-and-homemakers.74/page-2',
            'https://forums.hardwarezone.com.sg/forums/homeseekers-and-homemakers.74/page-3'
        ]
        
        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
        ]
        
        # Property-related keywords for Singapore context
        self.property_keywords = [
            # Property types and transactions
            'BTO', 'HDB', 'resale flat', 'condo', 'condominium', 'landed property',
            'EC', 'executive condo', 'private property', 'DBSS', 'property investment',
            'buy property', 'sell property', 'property market', 'property price',
            
            # Policy and financial terms
            'cooling measures', 'ABSD', 'additional buyer stamp duty', 'TDSR',
            'loan-to-value', 'LTV', 'mortgage', 'home loan', 'interest rate',
            'property tax', 'stamp duty', 'COV', 'cash over valuation',
            
            # Locations (Singapore areas)
            'Tampines', 'Punggol', 'Bishan', 'Jurong', 'Woodlands', 'Bedok',
            'Toa Payoh', 'Ang Mo Kio', 'Clementi', 'Pasir Ris', 'Sengkang',
            'Yishun', 'Hougang', 'Bukit Batok', 'Choa Chu Kang', 'Sembawang',
            
            # Market sentiment terms
            'property bubble', 'property crash', 'overpriced', 'undervalued',
            'good buy', 'bad investment', 'regret buying', 'satisfied with purchase'
        ]
        
        # Sentiment analysis keywords
        self.sentiment_keywords = {
            'very_positive': ['excellent', 'amazing', 'fantastic', 'love it', 'best decision', 'highly recommend'],
            'positive': ['good', 'satisfied', 'happy', 'worth it', 'recommend', 'glad', 'pleased'],
            'neutral': ['okay', 'average', 'decent', 'acceptable', 'not bad', 'fine', 'alright'],
            'negative': ['disappointed', 'regret', 'bad', 'terrible', 'waste of money', 'avoid'],
            'very_negative': ['horrible', 'worst', 'disaster', 'nightmare', 'scam', 'never again']
        }
        
        # Date range
        self.start_date = datetime(2023, 9, 1)
        self.end_date = datetime(2025, 9, 18)
        
        # Setup session
        self.session = requests.Session()
        self.setup_session()
        
        # Selenium driver for JavaScript-heavy pages
        self.driver = None
    
    def setup_session(self):
        """Setup requests session with enhanced anti-bot headers"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Referer': 'https://forums.hardwarezone.com.sg/',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'Sec-CH-UA': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': '"Windows"'
        })
        
        # Set session cookies to appear more legitimate
        self.session.cookies.update({
            'hwz_session': f'hwz_{random.randint(100000, 999999)}',
            'visitor_id': f'vis_{random.randint(100000, 999999)}',
            'timezone': 'Asia/Singapore'
        })
    
    def rotate_user_agent(self):
        """Rotate user agent to avoid detection"""
        self.session.headers['User-Agent'] = random.choice(self.user_agents)
    
    def calculate_sentiment_score(self, text: str) -> Tuple[Optional[float], str]:
        """
        Calculate sentiment score from forum post text
        
        Args:
            text: Post content text
            
        Returns:
            Tuple of (sentiment_score, confidence_level)
        """
        text_lower = text.lower()
        
        # Count sentiment indicators
        very_positive = sum(1 for phrase in self.sentiment_keywords['very_positive'] if phrase in text_lower)
        positive = sum(1 for phrase in self.sentiment_keywords['positive'] if phrase in text_lower)
        neutral = sum(1 for phrase in self.sentiment_keywords['neutral'] if phrase in text_lower)
        negative = sum(1 for phrase in self.sentiment_keywords['negative'] if phrase in text_lower)
        very_negative = sum(1 for phrase in self.sentiment_keywords['very_negative'] if phrase in text_lower)
        
        total_indicators = very_positive + positive + neutral + negative + very_negative
        
        if total_indicators == 0:
            return None, 'none'
        
        # Calculate weighted sentiment score (-1 to +1)
        score = (
            (very_positive * 1.0) + 
            (positive * 0.5) + 
            (neutral * 0.0) + 
            (negative * -0.5) + 
            (very_negative * -1.0)
        ) / total_indicators
        
        # Determine confidence based on number of indicators
        if total_indicators >= 3:
            confidence = 'high'
        elif total_indicators >= 2:
            confidence = 'medium'
        else:
            confidence = 'low'
        
        return round(score, 2), confidence
    
    def is_property_related(self, text: str) -> bool:
        """Check if content is property-related"""
        text_lower = text.lower()
        return any(keyword.lower() in text_lower for keyword in self.property_keywords)
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse HardwareZone date formats
        
        Args:
            date_str: Date string from forum
            
        Returns:
            Parsed datetime object or None
        """
        # Clean up date string
        date_str = re.sub(r'\s+', ' ', date_str.strip())
        
        # Handle relative dates
        if 'ago' in date_str.lower():
            if 'minute' in date_str:
                minutes = int(re.search(r'(\d+)', date_str).group(1))
                return datetime.now() - timedelta(minutes=minutes)
            elif 'hour' in date_str:
                hours = int(re.search(r'(\d+)', date_str).group(1))
                return datetime.now() - timedelta(hours=hours)
            elif 'day' in date_str:
                days = int(re.search(r'(\d+)', date_str).group(1))
                return datetime.now() - timedelta(days=days)
            elif 'week' in date_str:
                weeks = int(re.search(r'(\d+)', date_str).group(1))
                return datetime.now() - timedelta(weeks=weeks)
        
        # Standard date formats
        date_formats = [
            '%d/%m/%Y, %I:%M %p',  # 15/09/2024, 2:30 PM
            '%d %b %Y at %I:%M %p',  # 15 Sep 2024 at 2:30 PM
            '%Y-%m-%d %H:%M:%S',  # 2024-09-15 14:30:00
            '%d/%m/%Y',  # 15/09/2024
            '%d %b %Y'   # 15 Sep 2024
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def get_thread_urls(self, search_keyword: str, max_pages: int = 10) -> List[str]:
        """
        Get URLs of property-related threads from search results
        
        Args:
            search_keyword: Keyword to search for
            max_pages: Maximum search result pages to process
            
        Returns:
            List of thread URLs
        """
        thread_urls = []
        
        try:
            search_url = self.search_urls.get(search_keyword, self.search_urls['property'])
            
            for page in range(1, max_pages + 1):
                logger.info(f"Getting thread URLs for '{search_keyword}' - page {page}")
                
                # Rotate user agent and add delay
                self.rotate_user_agent()
                time.sleep(random.uniform(2.0, 4.0))  # Random delay between requests
                
                # Add page parameter
                page_url = f"{search_url}&page={page}"
                
                # Update referrer for subsequent pages
                if page > 1:
                    self.session.headers['Referer'] = f"{search_url}&page={page-1}"
                
                try:
                    response = self.session.get(page_url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Find thread links in search results
                    thread_links = soup.find_all('a', href=re.compile(r'/threads/.*\.\d+/'))
                    
                    for link in thread_links:
                        thread_url = urljoin(self.base_url, link['href'])
                        if thread_url not in thread_urls:
                            thread_urls.append(thread_url)
                    
                    # Rate limiting
                    time.sleep(random.uniform(1, 3))
                    
                except requests.RequestException as e:
                    logger.error(f"Error fetching search page {page}: {e}")
                    continue
                
                # Stop if no new threads found
                if not thread_links:
                    break
                    
        except Exception as e:
            logger.error(f"Error getting thread URLs for {search_keyword}: {e}")
        
        return thread_urls
    
    def scrape_thread_posts(self, thread_url: str, max_posts: int = 50) -> List[HardwareZonePost]:
        """
        Scrape posts from a HardwareZone thread
        
        Args:
            thread_url: URL of the thread to scrape
            max_posts: Maximum posts to scrape from thread
            
        Returns:
            List of HardwareZonePost objects
        """
        posts = []
        
        try:
            logger.info(f"Scraping thread: {thread_url}")
            
            self.rotate_user_agent()
            
            response = self.session.get(thread_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract thread title
            thread_title_elem = soup.find(['h1', 'h2'], class_=re.compile(r'title|heading'))
            thread_title = thread_title_elem.get_text(strip=True) if thread_title_elem else 'Unknown Thread'
            
            # Skip if thread title is not property-related
            if not self.is_property_related(thread_title):
                return posts
            
            # Find post containers
            post_containers = soup.find_all(['article', 'div'], class_=re.compile(r'message|post'))
            
            for i, container in enumerate(post_containers[:max_posts]):
                try:
                    # Extract post content
                    content_elem = container.find(['div'], class_=re.compile(r'content|message|bbWrapper'))
                    if not content_elem:
                        continue
                    
                    # Remove quotes and signatures
                    for quote in content_elem.find_all(['blockquote', 'div'], class_=re.compile(r'quote|signature')):
                        quote.decompose()
                    
                    post_content = content_elem.get_text(strip=True, separator=' ')
                    
                    # Skip short posts or non-property content
                    if len(post_content) < 50 or not self.is_property_related(post_content):
                        continue
                    
                    # Extract author
                    author_elem = container.find(['a', 'span'], class_=re.compile(r'username|author'))
                    author = author_elem.get_text(strip=True) if author_elem else 'Anonymous'
                    
                    # Extract post date
                    date_elem = container.find(['time', 'span'], class_=re.compile(r'date|time'))
                    date_str = date_elem.get('datetime') or date_elem.get_text(strip=True) if date_elem else ''
                    date_posted = self.parse_date(date_str) or datetime.now()
                    
                    # Check date range
                    if not (self.start_date <= date_posted <= self.end_date):
                        continue
                    
                    # Extract likes/reactions
                    likes_elem = container.find(['span'], class_=re.compile(r'like|reaction'))
                    likes = 0
                    if likes_elem:
                        likes_text = likes_elem.get_text(strip=True)
                        likes_match = re.search(r'(\d+)', likes_text)
                        if likes_match:
                            likes = int(likes_match.group(1))
                    
                    # Calculate sentiment
                    sentiment_score, sentiment_confidence = self.calculate_sentiment_score(post_content)
                    
                    # Create post object
                    post = HardwareZonePost(
                        id=f"hz_post_{hash(f'{thread_url}_{i}_{author}_{post_content[:50]}')}",
                        thread_title=thread_title,
                        post_content=post_content,
                        author=author,
                        post_number=i + 1,
                        date_posted=date_posted,
                        thread_url=thread_url,
                        post_url=f"{thread_url}#post-{i+1}",
                        likes=likes,
                        replies_count=0,  # Not easily extractable from individual posts
                        sentiment_score=sentiment_score,
                        sentiment_confidence=sentiment_confidence
                    )
                    
                    posts.append(post)
                    
                except Exception as e:
                    logger.warning(f"Error processing post {i}: {e}")
                    continue
            
            # Rate limiting
            time.sleep(random.uniform(2, 5))
            
        except requests.RequestException as e:
            logger.error(f"Error scraping thread {thread_url}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error scraping thread {thread_url}: {e}")
        
        return posts
    
    def save_posts_to_json(self, posts: List[HardwareZonePost], filename: str):
        """
        Save posts to JSON file with proper formatting
        
        Args:
            posts: List of HardwareZonePost objects
            filename: Output filename
        """
        posts_dict = []
        for post in posts:
            post_dict = {
                'id': post.id,
                'thread_title': post.thread_title,
                'post_content': post.post_content,
                'author': post.author,
                'post_number': post.post_number,
                'date_posted': post.date_posted.isoformat(),
                'thread_url': post.thread_url,
                'post_url': post.post_url,
                'likes': post.likes,
                'replies_count': post.replies_count,
                'sentiment_score': post.sentiment_score,
                'sentiment_confidence': post.sentiment_confidence,
                'source': 'hardwarezone',
                'scraped_at': datetime.now().isoformat(),
                'content_length': len(post.post_content),
                'word_count': len(post.post_content.split())
            }
            posts_dict.append(post_dict)
        
        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(posts_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(posts)} posts to {output_path}")
    
    def run_full_scrape(self) -> Dict[str, int]:
        """
        Run complete HardwareZone scraping process
        
        Returns:
            Dictionary with scraping statistics
        """
        logger.info("Starting HardwareZone EDMW scraping for PropInsight dataset")
        
        all_posts = []
        stats = {
            'total_posts': 0,
            'threads_scraped': 0,
            'total_words': 0,
            'sentiment_posts': 0,
            'no_sentiment_posts': 0,
            'keywords_processed': 0
        }
        
        # Process each search keyword
        for keyword in self.search_urls.keys():
            logger.info(f"Processing keyword: {keyword}")
            
            # Get thread URLs for this keyword (increased pages for more coverage)
            thread_urls = self.get_thread_urls(keyword, max_pages=10)
            logger.info(f"Found {len(thread_urls)} threads for keyword '{keyword}'")
            
            keyword_posts = []
            
            # Scrape posts from each thread (increased thread limit)
            for thread_url in thread_urls[:50]:  # Increased from 20 to 50 threads per keyword
                try:
                    posts = self.scrape_thread_posts(thread_url, max_posts=50)  # Increased from 30 to 50 posts per thread
                    keyword_posts.extend(posts)
                    
                    if posts:
                        stats['threads_scraped'] += 1
                    
                    # Rate limiting between threads
                    time.sleep(random.uniform(2, 4))  # Reduced delay for faster scraping
                    
                except Exception as e:
                    logger.error(f"Error processing thread {thread_url}: {e}")
                    continue
                
                # Stop if we have enough posts for this keyword (increased target)
                if len(keyword_posts) >= 1500:  # Increased from 1000 to 1500 per keyword
                    break
            
            all_posts.extend(keyword_posts)
            stats['keywords_processed'] += 1
            
            # Save keyword-specific file
            if keyword_posts:
                self.save_posts_to_json(
                    keyword_posts, 
                    f'hardwarezone_{keyword}_{datetime.now().strftime("%Y%m%d")}.json'
                )
            
            logger.info(f"Collected {len(keyword_posts)} posts for keyword '{keyword}'")
        
        # Remove duplicates based on post ID
        unique_posts = {}
        for post in all_posts:
            unique_posts[post.id] = post
        
        final_posts = list(unique_posts.values())
        
        # Calculate final statistics
        for post in final_posts:
            if post.sentiment_score is not None:
                stats['sentiment_posts'] += 1
            else:
                stats['no_sentiment_posts'] += 1
            
            stats['total_words'] += len(post.post_content.split())
        
        stats['total_posts'] = len(final_posts)
        
        # Save combined file
        if final_posts:
            self.save_posts_to_json(
                final_posts, 
                f'hardwarezone_combined_{datetime.now().strftime("%Y%m%d")}.json'
            )
        
        # Save statistics
        stats_path = self.output_dir / f'hardwarezone_stats_{datetime.now().strftime("%Y%m%d")}.json'
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"HardwareZone scraping completed. Total posts: {stats['total_posts']}")
        return stats

def main():
    """Main function to run HardwareZone scraper"""
    scraper = HardwareZoneScraper()
    stats = scraper.run_full_scrape()
    
    print("\n=== HardwareZone EDMW Scraping Results ===")
    print(f"Total posts collected: {stats['total_posts']}")
    print(f"Threads scraped: {stats['threads_scraped']}")
    print(f"Posts with sentiment: {stats['sentiment_posts']}")
    print(f"Posts without sentiment: {stats['no_sentiment_posts']}")
    print(f"Total words: {stats['total_words']:,}")
    print(f"Keywords processed: {stats['keywords_processed']}")

if __name__ == "__main__":
    main()