"""
PropInsight PropertyGuru Scraper

This module scrapes property reviews and discussions from PropertyGuru Singapore.
PropertyGuru is the largest property portal in Singapore with rich user-generated content.

Challenges addressed:
1. Anti-bot detection (rotating user agents, delays, session management)
2. Sparse ratings (multi-source strategy, text sentiment inference)
3. Dynamic content loading (handling JavaScript-rendered content)
4. Rate limiting (respectful scraping with delays)

Target: 4,000 samples from PropertyGuru reviews and discussions
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
class PropertyGuruReview:
    """Data structure for PropertyGuru reviews - handles both rated and unrated content"""
    id: str
    title: str
    content: str
    rating: Optional[float]  # May be None for unrated reviews
    rating_confidence: str  # 'explicit', 'inferred', 'none'
    author: str
    date_posted: datetime
    property_name: str
    property_type: str
    location: str
    url: str
    review_type: str  # 'property_review', 'forum_post', 'project_comment'
    
class PropertyGuruScraper:
    """
    PropertyGuru scraper with anti-bot measures and sparse rating handling
    
    Uses multiple strategies to overcome PropertyGuru's challenges:
    1. Rotating user agents and headers
    2. Session management with cookies
    3. Selenium for JavaScript-heavy pages
    4. Multi-source data collection (reviews, forums, comments)
    5. Intelligent rating inference from text
    """
    
    def __init__(self, output_dir: str = "d:\\Test\\New folder\\PropInsight\\data\\raw\\propertyguru"):
        """
        Initialize PropertyGuru scraper
        
        Args:
            output_dir: Directory to save scraped data
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Base URLs for different PropertyGuru sections
        self.base_urls = {
            'reviews': 'https://www.propertyguru.com.sg/property-reviews',
            'forum': 'https://www.propertyguru.com.sg/property-talk',
            'projects': 'https://www.propertyguru.com.sg/new-project-reviews',
            'search': 'https://www.propertyguru.com.sg/property-for-sale'
        }
        
        # User agents for rotation (anti-bot measure)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
        ]
        
        # Property-related keywords for Singapore context
        self.property_keywords = [
            # Property types
            'BTO', 'HDB', 'resale flat', 'condo', 'condominium', 'landed property',
            'EC', 'executive condo', 'private property', 'DBSS',
            
            # Sentiment indicators (for rating inference)
            'excellent', 'good', 'satisfied', 'happy', 'recommend', 'worth it',
            'terrible', 'bad', 'disappointed', 'regret', 'overpriced', 'avoid',
            'average', 'okay', 'decent', 'acceptable', 'not bad',
            
            # Location indicators
            'MRT', 'near station', 'convenient', 'accessible', 'central',
            'far from', 'inconvenient', 'isolated', 'noisy', 'quiet',
            
            # Facility indicators  
            'facilities', 'amenities', 'swimming pool', 'gym', 'playground',
            'security', 'maintenance', 'management', 'parking'
        ]
        
        # Sentiment keywords for rating inference
        self.sentiment_keywords = {
            'positive': ['excellent', 'amazing', 'fantastic', 'great', 'good', 'satisfied', 
                        'happy', 'recommend', 'worth it', 'love', 'perfect', 'wonderful'],
            'negative': ['terrible', 'awful', 'horrible', 'bad', 'disappointed', 'regret', 
                        'overpriced', 'avoid', 'hate', 'worst', 'useless', 'waste'],
            'neutral': ['average', 'okay', 'decent', 'acceptable', 'not bad', 'fine', 'alright']
        }
        
        # Date range
        self.start_date = datetime(2023, 9, 1)
        self.end_date = datetime(2025, 9, 18)
        
        # Setup session with rotating headers
        self.session = requests.Session()
        self.setup_session()
        
        # Selenium driver (initialized when needed)
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
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'Sec-CH-UA': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': '"Windows"'
        })
        
        # Set initial cookies to appear more legitimate
        self.session.cookies.update({
            'session_id': f'sess_{random.randint(100000, 999999)}',
            'visitor_id': f'vis_{random.randint(100000, 999999)}'
        })
    
    def rotate_user_agent(self):
        """Rotate user agent to avoid detection"""
        self.session.headers['User-Agent'] = random.choice(self.user_agents)
    
    def setup_selenium_driver(self) -> webdriver.Chrome:
        """
        Setup Selenium Chrome driver for JavaScript-heavy pages
        
        Returns:
            Chrome WebDriver instance
        """
        if self.driver:
            return self.driver
            
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Run in background
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument(f'--user-agent={random.choice(self.user_agents)}')
        
        # Anti-detection measures
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            return self.driver
        except Exception as e:
            logger.error(f"Failed to setup Selenium driver: {e}")
            return None
    
    def infer_rating_from_text(self, text: str) -> Tuple[Optional[float], str]:
        """
        Infer rating from review text using sentiment analysis
        
        Args:
            text: Review text content
            
        Returns:
            Tuple of (inferred_rating, confidence_level)
        """
        text_lower = text.lower()
        
        positive_count = sum(1 for word in self.sentiment_keywords['positive'] if word in text_lower)
        negative_count = sum(1 for word in self.sentiment_keywords['negative'] if word in text_lower)
        neutral_count = sum(1 for word in self.sentiment_keywords['neutral'] if word in text_lower)
        
        total_sentiment_words = positive_count + negative_count + neutral_count
        
        if total_sentiment_words == 0:
            return None, 'none'
        
        # Calculate sentiment score
        if positive_count > negative_count and positive_count > neutral_count:
            # Positive sentiment: 3.5-5.0 rating
            if positive_count >= 3:
                rating = 4.5 + (positive_count - 3) * 0.1
            else:
                rating = 3.5 + positive_count * 0.3
            rating = min(rating, 5.0)
            confidence = 'inferred'
        elif negative_count > positive_count and negative_count > neutral_count:
            # Negative sentiment: 1.0-2.5 rating
            if negative_count >= 3:
                rating = 1.0 + (3 - negative_count) * 0.1
            else:
                rating = 2.5 - negative_count * 0.3
            rating = max(rating, 1.0)
            confidence = 'inferred'
        else:
            # Neutral or mixed sentiment: 2.5-3.5 rating
            rating = 3.0
            confidence = 'inferred'
        
        return round(rating, 1), confidence
    
    def is_property_related(self, text: str) -> bool:
        """Check if content is property-related"""
        text_lower = text.lower()
        return any(keyword.lower() in text_lower for keyword in self.property_keywords)
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse various date formats from PropertyGuru
        
        Args:
            date_str: Date string from website
            
        Returns:
            Parsed datetime object or None
        """
        date_formats = [
            '%d %b %Y',  # 15 Sep 2024
            '%d/%m/%Y',  # 15/09/2024
            '%Y-%m-%d',  # 2024-09-15
            '%d %B %Y'   # 15 September 2024
        ]
        
        # Handle relative dates
        if 'ago' in date_str.lower():
            if 'day' in date_str:
                days = int(re.search(r'(\d+)', date_str).group(1))
                return datetime.now() - timedelta(days=days)
            elif 'week' in date_str:
                weeks = int(re.search(r'(\d+)', date_str).group(1))
                return datetime.now() - timedelta(weeks=weeks)
            elif 'month' in date_str:
                months = int(re.search(r'(\d+)', date_str).group(1))
                return datetime.now() - timedelta(days=months*30)
        
        # Try standard formats
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        return None
    
    def scrape_property_reviews(self, max_pages: int = 50) -> List[PropertyGuruReview]:
        """
        Scrape property reviews from PropertyGuru reviews section
        
        Args:
            max_pages: Maximum pages to scrape
            
        Returns:
            List of PropertyGuruReview objects
        """
        reviews = []
        
        try:
            for page in range(1, max_pages + 1):
                logger.info(f"Scraping property reviews page {page}")
                
                # Rotate user agent for each page
                self.rotate_user_agent()
                
                # Add referrer header to appear more legitimate
                if page > 1:
                    self.session.headers['Referer'] = f"{self.base_urls['reviews']}?page={page-1}"
                else:
                    self.session.headers['Referer'] = 'https://www.propertyguru.com.sg/'
                
                url = f"{self.base_urls['reviews']}?page={page}"
                
                # Add random delay between requests (1-3 seconds)
                time.sleep(random.uniform(1.0, 3.0))
                
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Find review containers (PropertyGuru-specific selectors)
                    review_containers = soup.find_all(['div', 'article'], class_=re.compile(r'review|listing|property'))
                    
                    if not review_containers:
                        logger.warning(f"No review containers found on page {page}")
                        break
                    
                    for container in review_containers:
                        try:
                            # Extract review data
                            title_elem = container.find(['h2', 'h3', 'h4'], class_=re.compile(r'title|heading'))
                            title = title_elem.get_text(strip=True) if title_elem else 'No Title'
                            
                            content_elem = container.find(['div', 'p'], class_=re.compile(r'content|description|review'))
                            content = content_elem.get_text(strip=True) if content_elem else ''
                            
                            # Skip if not property-related
                            if not self.is_property_related(f"{title} {content}"):
                                continue
                            
                            # Extract explicit rating
                            rating = None
                            rating_confidence = 'none'
                            
                            rating_elem = container.find(['span', 'div'], class_=re.compile(r'rating|star|score'))
                            if rating_elem:
                                rating_text = rating_elem.get_text(strip=True)
                                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                                if rating_match:
                                    rating = float(rating_match.group(1))
                                    rating_confidence = 'explicit'
                            
                            # If no explicit rating, infer from text
                            if rating is None and content:
                                rating, rating_confidence = self.infer_rating_from_text(content)
                            
                            # Extract other metadata
                            author_elem = container.find(['span', 'div'], class_=re.compile(r'author|user|name'))
                            author = author_elem.get_text(strip=True) if author_elem else 'Anonymous'
                            
                            date_elem = container.find(['span', 'div', 'time'], class_=re.compile(r'date|time'))
                            date_str = date_elem.get_text(strip=True) if date_elem else ''
                            date_posted = self.parse_date(date_str) or datetime.now()
                            
                            # Check date range
                            if not (self.start_date <= date_posted <= self.end_date):
                                continue
                            
                            property_elem = container.find(['a', 'span'], class_=re.compile(r'property|project'))
                            property_name = property_elem.get_text(strip=True) if property_elem else 'Unknown Property'
                            
                            # Create review object
                            review = PropertyGuruReview(
                                id=f"pg_review_{hash(f'{title}{content}{author}')}",
                                title=title,
                                content=content,
                                rating=rating,
                                rating_confidence=rating_confidence,
                                author=author,
                                date_posted=date_posted,
                                property_name=property_name,
                                property_type='Unknown',
                                location='Singapore',
                                url=url,
                                review_type='property_review'
                            )
                            
                            reviews.append(review)
                            
                        except Exception as e:
                            logger.warning(f"Error processing review container: {e}")
                            continue
                    
                    # Rate limiting
                    time.sleep(random.uniform(2, 5))
                    
                except requests.RequestException as e:
                    logger.error(f"Error fetching page {page}: {e}")
                    time.sleep(10)  # Longer delay on error
                    continue
                
                # Stop if we have enough reviews
                if len(reviews) >= 2000:
                    break
                    
        except Exception as e:
            logger.error(f"Error in scrape_property_reviews: {e}")
        
        return reviews
    
    def scrape_forum_discussions(self, max_pages: int = 30) -> List[PropertyGuruReview]:
        """
        Scrape property discussions from PropertyGuru forum
        
        Args:
            max_pages: Maximum pages to scrape
            
        Returns:
            List of PropertyGuruReview objects (forum posts)
        """
        discussions = []
        
        try:
            for page in range(1, max_pages + 1):
                logger.info(f"Scraping forum discussions page {page}")
                
                self.rotate_user_agent()
                
                url = f"{self.base_urls['forum']}?page={page}"
                
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Find discussion containers
                    discussion_containers = soup.find_all(['div', 'article'], class_=re.compile(r'topic|thread|post|discussion'))
                    
                    for container in discussion_containers:
                        try:
                            title_elem = container.find(['h2', 'h3', 'a'], class_=re.compile(r'title|subject'))
                            title = title_elem.get_text(strip=True) if title_elem else 'No Title'
                            
                            content_elem = container.find(['div', 'p'], class_=re.compile(r'content|message|post'))
                            content = content_elem.get_text(strip=True) if content_elem else ''
                            
                            # Skip if not property-related
                            if not self.is_property_related(f"{title} {content}"):
                                continue
                            
                            # Infer sentiment from forum post
                            rating, rating_confidence = self.infer_rating_from_text(f"{title} {content}")
                            
                            author_elem = container.find(['span', 'div'], class_=re.compile(r'author|user'))
                            author = author_elem.get_text(strip=True) if author_elem else 'Anonymous'
                            
                            date_elem = container.find(['span', 'time'], class_=re.compile(r'date|time'))
                            date_str = date_elem.get_text(strip=True) if date_elem else ''
                            date_posted = self.parse_date(date_str) or datetime.now()
                            
                            # Check date range
                            if not (self.start_date <= date_posted <= self.end_date):
                                continue
                            
                            discussion = PropertyGuruReview(
                                id=f"pg_forum_{hash(f'{title}{content}{author}')}",
                                title=title,
                                content=content,
                                rating=rating,
                                rating_confidence=rating_confidence,
                                author=author,
                                date_posted=date_posted,
                                property_name='General Discussion',
                                property_type='Forum',
                                location='Singapore',
                                url=url,
                                review_type='forum_post'
                            )
                            
                            discussions.append(discussion)
                            
                        except Exception as e:
                            logger.warning(f"Error processing forum post: {e}")
                            continue
                    
                    time.sleep(random.uniform(3, 6))
                    
                except requests.RequestException as e:
                    logger.error(f"Error fetching forum page {page}: {e}")
                    time.sleep(10)
                    continue
                
                if len(discussions) >= 1000:
                    break
                    
        except Exception as e:
            logger.error(f"Error in scrape_forum_discussions: {e}")
        
        return discussions
    
    def save_reviews_to_json(self, reviews: List[PropertyGuruReview], filename: str):
        """
        Save reviews to JSON file with proper formatting
        
        Args:
            reviews: List of PropertyGuruReview objects
            filename: Output filename
        """
        reviews_dict = []
        for review in reviews:
            review_dict = {
                'id': review.id,
                'title': review.title,
                'content': review.content,
                'rating': review.rating,
                'rating_confidence': review.rating_confidence,
                'author': review.author,
                'date_posted': review.date_posted.isoformat(),
                'property_name': review.property_name,
                'property_type': review.property_type,
                'location': review.location,
                'url': review.url,
                'review_type': review.review_type,
                'source': 'propertyguru',
                'scraped_at': datetime.now().isoformat(),
                'content_length': len(review.content),
                'word_count': len(review.content.split())
            }
            reviews_dict.append(review_dict)
        
        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(reviews_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(reviews)} reviews to {output_path}")
    
    def run_full_scrape(self) -> Dict[str, int]:
        """
        Run complete PropertyGuru scraping process
        
        Returns:
            Dictionary with scraping statistics
        """
        logger.info("Starting PropertyGuru scraping for PropInsight dataset")
        
        all_reviews = []
        stats = {
            'total_reviews': 0,
            'property_reviews': 0,
            'forum_posts': 0,
            'explicit_ratings': 0,
            'inferred_ratings': 0,
            'no_ratings': 0,
            'total_words': 0
        }
        
        # Scrape property reviews
        logger.info("Scraping property reviews...")
        property_reviews = self.scrape_property_reviews(max_pages=50)
        all_reviews.extend(property_reviews)
        stats['property_reviews'] = len(property_reviews)
        
        # Save property reviews
        if property_reviews:
            self.save_reviews_to_json(
                property_reviews, 
                f'propertyguru_reviews_{datetime.now().strftime("%Y%m%d")}.json'
            )
        
        # Scrape forum discussions
        logger.info("Scraping forum discussions...")
        forum_posts = self.scrape_forum_discussions(max_pages=30)
        all_reviews.extend(forum_posts)
        stats['forum_posts'] = len(forum_posts)
        
        # Save forum posts
        if forum_posts:
            self.save_reviews_to_json(
                forum_posts, 
                f'propertyguru_forum_{datetime.now().strftime("%Y%m%d")}.json'
            )
        
        # Calculate statistics
        for review in all_reviews:
            if review.rating_confidence == 'explicit':
                stats['explicit_ratings'] += 1
            elif review.rating_confidence == 'inferred':
                stats['inferred_ratings'] += 1
            else:
                stats['no_ratings'] += 1
            
            stats['total_words'] += len(review.content.split())
        
        stats['total_reviews'] = len(all_reviews)
        
        # Save combined file
        if all_reviews:
            self.save_reviews_to_json(
                all_reviews, 
                f'propertyguru_combined_{datetime.now().strftime("%Y%m%d")}.json'
            )
        
        # Save statistics
        stats_path = self.output_dir / f'propertyguru_stats_{datetime.now().strftime("%Y%m%d")}.json'
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        # Cleanup Selenium driver
        if self.driver:
            self.driver.quit()
        
        logger.info(f"PropertyGuru scraping completed. Total reviews: {stats['total_reviews']}")
        return stats

def main():
    """Main function to run PropertyGuru scraper"""
    scraper = PropertyGuruScraper()
    stats = scraper.run_full_scrape()
    
    print("\n=== PropertyGuru Scraping Results ===")
    print(f"Total reviews collected: {stats['total_reviews']}")
    print(f"Property reviews: {stats['property_reviews']}")
    print(f"Forum posts: {stats['forum_posts']}")
    print(f"Explicit ratings: {stats['explicit_ratings']}")
    print(f"Inferred ratings: {stats['inferred_ratings']}")
    print(f"No ratings: {stats['no_ratings']}")
    print(f"Total words: {stats['total_words']:,}")

if __name__ == "__main__":
    main()