"""
Renotalk Forum Scraper for PropInsight Dataset
Scrapes property discussions from Renotalk.com forum
Target: 2,000 property-related posts from renovation and property investment discussions
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from datetime import datetime
import os
import sys
from urllib.parse import urljoin, urlparse
import random
import re

# Add parent directories to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from error_handler import ErrorHandler
from data_validator import DataValidator
from proxy_manager import ProxyManager

class RenotalkScraper:
    """Scraper for Renotalk forum property discussions"""
    
    def __init__(self, output_dir="data/raw/renotalk", use_proxy=False):
        self.base_url = "https://www.renotalk.com"
        self.forum_url = "https://www.renotalk.com/forum/"
        self.output_dir = output_dir
        self.session = requests.Session()
        self.error_handler = ErrorHandler()
        self.data_validator = DataValidator()
        self.proxy_manager = ProxyManager() if use_proxy else None
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Headers to mimic real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Rate limiting
        self.rate_limit_delay = float(os.getenv('RATE_LIMIT_DELAY', 3))
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        
        # Property-related forum sections and keywords
        self.property_sections = [
            "/forum/topic/1-property-investment/",
            "/forum/topic/2-property-market-discussion/",
            "/forum/topic/3-hdb-resale/",
            "/forum/topic/4-private-property/",
            "/forum/topic/5-renovation-and-property/"
        ]
        
        # Property-related keywords for filtering
        self.property_keywords = [
            'property', 'hdb', 'bto', 'condo', 'resale', 'rental', 'investment',
            'mortgage', 'loan', 'valuation', 'renovation', 'interior design',
            'landed', 'ec', 'executive condo', 'private property', 'public housing',
            'property agent', 'viewing', 'offer', 'negotiation', 'market price',
            'psf', 'square feet', 'bedroom', 'bathroom', 'kitchen', 'living room'
        ]
        
    def setup_session(self):
        """Setup session with proxy if available"""
        if self.proxy_manager:
            proxy = self.proxy_manager.get_proxy()
            if proxy:
                self.session.proxies.update(proxy)
                self.logger.info(f"Using proxy: {proxy}")
        
        self.session.headers.update(self.headers)
        
    def get_page_content(self, url, max_retries=None):
        """Get page content with error handling and retries"""
        if max_retries is None:
            max_retries = self.max_retries
            
        for attempt in range(max_retries):
            try:
                # Add random delay to avoid rate limiting
                time.sleep(self.rate_limit_delay + random.uniform(0, 2))
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                return response.text
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = (2 ** attempt) + random.uniform(0, 2)
                    time.sleep(wait_time)
                    
                    # Try different proxy if available
                    if self.proxy_manager:
                        proxy = self.proxy_manager.get_proxy()
                        if proxy:
                            self.session.proxies.update(proxy)
                else:
                    self.error_handler.log_error(f"Failed to fetch {url} after {max_retries} attempts", str(e))
                    return None
        
        return None
    
    def is_property_related(self, text):
        """Check if text content is property-related"""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.property_keywords)
    
    def extract_forum_threads(self, html_content, source_url):
        """Extract forum thread URLs from forum page"""
        thread_urls = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for thread links (adjust selectors based on actual site structure)
            thread_links = soup.find_all('a', href=True)
            
            for link in thread_links:
                href = link.get('href')
                if href and ('/topic/' in href or '/thread/' in href):
                    # Convert relative URLs to absolute
                    full_url = urljoin(self.base_url, href)
                    
                    # Check if thread title suggests property content
                    link_text = link.get_text(strip=True)
                    if self.is_property_related(link_text):
                        thread_urls.append(full_url)
            
            # Remove duplicates
            thread_urls = list(set(thread_urls))
            self.logger.info(f"Found {len(thread_urls)} property-related threads from {source_url}")
            return thread_urls
            
        except Exception as e:
            self.error_handler.log_error(f"Error extracting threads from {source_url}", str(e))
            return []
    
    def extract_thread_posts(self, html_content, source_url):
        """Extract posts from a forum thread"""
        posts = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for post containers (adjust selectors based on actual site structure)
            post_containers = soup.find_all(['div', 'article'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['post', 'message', 'comment', 'reply']
            ))
            
            for container in post_containers:
                try:
                    # Extract post content
                    content_elem = container.find(['div', 'p'], class_=lambda x: x and any(
                        keyword in x.lower() for keyword in ['content', 'body', 'text', 'message']
                    ))
                    
                    if not content_elem:
                        # Fallback: get all text content from container
                        content_elem = container
                    
                    post_text = content_elem.get_text(strip=True) if content_elem else ""
                    
                    # Skip short posts or non-property related content
                    if len(post_text) < 50 or not self.is_property_related(post_text):
                        continue
                    
                    # Extract metadata
                    author_elem = container.find(['span', 'div'], class_=lambda x: x and 'author' in x.lower())
                    author = author_elem.get_text(strip=True) if author_elem else "Unknown"
                    
                    # Extract date
                    date_elem = container.find(['time', 'span'], class_=lambda x: x and 'date' in x.lower())
                    date_str = date_elem.get_text(strip=True) if date_elem else ""
                    
                    # Extract thread title
                    title_elem = soup.find(['h1', 'h2'], class_=lambda x: x and 'title' in x.lower())
                    thread_title = title_elem.get_text(strip=True) if title_elem else ""
                    
                    post_data = {
                        'text': post_text,
                        'author': author,
                        'thread_title': thread_title,
                        'date': date_str,
                        'source_url': source_url,
                        'scraped_at': datetime.now().isoformat(),
                        'platform': 'renotalk',
                        'data_type': 'forum_post'
                    }
                    
                    # Validate data
                    if self.data_validator.validate_text_content(post_text):
                        posts.append(post_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error extracting post: {str(e)}")
                    continue
            
            self.logger.info(f"Extracted {len(posts)} posts from {source_url}")
            return posts
            
        except Exception as e:
            self.error_handler.log_error(f"Error parsing thread {source_url}", str(e))
            return []
    
    def scrape_forum_section(self, section_path, max_pages=5):
        """Scrape a specific forum section"""
        all_posts = []
        
        for page in range(1, max_pages + 1):
            try:
                # Construct URL for pagination
                if page == 1:
                    url = urljoin(self.base_url, section_path)
                else:
                    url = urljoin(self.base_url, f"{section_path}?page={page}")
                
                self.logger.info(f"Scraping forum section page {page}: {url}")
                
                html_content = self.get_page_content(url)
                if not html_content:
                    continue
                
                # Extract thread URLs from this page
                thread_urls = self.extract_forum_threads(html_content, url)
                
                # Scrape posts from each thread
                for thread_url in thread_urls[:10]:  # Limit threads per page
                    thread_content = self.get_page_content(thread_url)
                    if thread_content:
                        posts = self.extract_thread_posts(thread_content, thread_url)
                        all_posts.extend(posts)
                        
                        # Stop if we have enough posts
                        if len(all_posts) >= 500:  # Limit per section
                            break
                
                # Stop if we have enough posts
                if len(all_posts) >= 500:
                    break
                
            except Exception as e:
                self.logger.error(f"Error scraping section page {page}: {str(e)}")
                continue
        
        return all_posts
    
    def scrape_general_forum(self, max_pages=10):
        """Scrape general forum areas for property discussions"""
        all_posts = []
        
        for page in range(1, max_pages + 1):
            try:
                url = f"{self.forum_url}?page={page}" if page > 1 else self.forum_url
                
                self.logger.info(f"Scraping general forum page {page}: {url}")
                
                html_content = self.get_page_content(url)
                if not html_content:
                    continue
                
                # Extract thread URLs
                thread_urls = self.extract_forum_threads(html_content, url)
                
                # Scrape posts from property-related threads
                for thread_url in thread_urls[:15]:  # Limit threads per page
                    thread_content = self.get_page_content(thread_url)
                    if thread_content:
                        posts = self.extract_thread_posts(thread_content, thread_url)
                        all_posts.extend(posts)
                        
                        # Stop if we have enough posts
                        if len(all_posts) >= 1000:
                            break
                
                # Stop if we have enough posts
                if len(all_posts) >= 1000:
                    break
                
            except Exception as e:
                self.logger.error(f"Error scraping general forum page {page}: {str(e)}")
                continue
        
        return all_posts
    
    def run_scraper(self, target_samples=2000):
        """Main scraper execution"""
        self.logger.info("Starting Renotalk scraping for PropInsight dataset")
        
        # Setup session
        self.setup_session()
        
        all_data = []
        
        try:
            # Test site accessibility first
            test_content = self.get_page_content(self.forum_url)
            
            if not test_content:
                self.logger.error("Cannot access Renotalk - site may be blocked or down")
                return []
            
            self.logger.info("Renotalk is accessible, proceeding with scraping")
            
            # Scrape specific property sections
            for section in self.property_sections:
                self.logger.info(f"Scraping property section: {section}")
                section_posts = self.scrape_forum_section(section, max_pages=3)
                all_data.extend(section_posts)
                
                # Stop if we have enough data
                if len(all_data) >= target_samples:
                    break
            
            # If we need more data, scrape general forum
            if len(all_data) < target_samples:
                self.logger.info("Scraping general forum for additional property discussions...")
                general_posts = self.scrape_general_forum(max_pages=5)
                all_data.extend(general_posts)
            
            # Limit to target samples
            all_data = all_data[:target_samples]
            
            # Save data
            if all_data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"renotalk_data_{timestamp}.json"
                filepath = os.path.join(self.output_dir, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(all_data, f, ensure_ascii=False, indent=2)
                
                self.logger.info(f"Saved {len(all_data)} samples to {filepath}")
            
            self.logger.info(f"Renotalk scraping completed. Total samples: {len(all_data)}")
            return all_data
            
        except Exception as e:
            self.error_handler.log_error("Error in Renotalk scraper execution", str(e))
            return []

def main():
    """Test the scraper"""
    scraper = RenotalkScraper(use_proxy=True)
    results = scraper.run_scraper(target_samples=100)  # Test with smaller sample
    print(f"Collected {len(results)} samples from Renotalk")

if __name__ == "__main__":
    main()