"""
99.co Property Scraper for PropInsight Dataset
Alternative to PropertyGuru for property reviews and discussions
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

# Add parent directories to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from error_handler import ErrorHandler
from data_validator import DataValidator
from proxy_manager import ProxyManager

class NinetyNineCoScraper:
    """Scraper for 99.co property reviews and discussions"""
    
    def __init__(self, output_dir="data/raw/ninety_nine_co", use_proxy=False):
        self.base_url = "https://www.99.co"
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
        self.rate_limit_delay = float(os.getenv('RATE_LIMIT_DELAY', 2))
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        
        # Target URLs for scraping
        self.target_urls = [
            "/singapore/buy",  # Property listings with reviews
            "/singapore/rent", # Rental listings with reviews
            "/insights",       # Market insights and discussions
            "/news",          # Property news and analysis
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
                time.sleep(self.rate_limit_delay + random.uniform(0, 1))
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                return response.text
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
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
    
    def extract_property_reviews(self, html_content, source_url):
        """Extract property reviews and discussions from page content"""
        reviews = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for review containers (adjust selectors based on actual site structure)
            review_containers = soup.find_all(['div', 'article'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['review', 'comment', 'feedback', 'discussion']
            ))
            
            for container in review_containers:
                try:
                    # Extract review text
                    text_elem = container.find(['p', 'div', 'span'], class_=lambda x: x and any(
                        keyword in x.lower() for keyword in ['text', 'content', 'body', 'description']
                    ))
                    
                    if not text_elem:
                        # Fallback: get all text content
                        text_elem = container
                    
                    review_text = text_elem.get_text(strip=True) if text_elem else ""
                    
                    if len(review_text) < 20:  # Skip very short content
                        continue
                    
                    # Extract metadata
                    title_elem = container.find(['h1', 'h2', 'h3', 'h4'], class_=lambda x: x and 'title' in x.lower())
                    title = title_elem.get_text(strip=True) if title_elem else ""
                    
                    # Extract date if available
                    date_elem = container.find(['time', 'span', 'div'], class_=lambda x: x and 'date' in x.lower())
                    date_str = date_elem.get_text(strip=True) if date_elem else ""
                    
                    # Extract rating if available
                    rating_elem = container.find(['span', 'div'], class_=lambda x: x and 'rating' in x.lower())
                    rating = rating_elem.get_text(strip=True) if rating_elem else ""
                    
                    review_data = {
                        'text': review_text,
                        'title': title,
                        'date': date_str,
                        'rating': rating,
                        'source_url': source_url,
                        'scraped_at': datetime.now().isoformat(),
                        'platform': '99.co',
                        'data_type': 'property_review'
                    }
                    
                    # Validate data
                    if self.data_validator.validate_text_content(review_text):
                        reviews.append(review_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error extracting review: {str(e)}")
                    continue
            
            self.logger.info(f"Extracted {len(reviews)} reviews from {source_url}")
            return reviews
            
        except Exception as e:
            self.error_handler.log_error(f"Error parsing content from {source_url}", str(e))
            return []
    
    def scrape_property_listings(self, base_path="/singapore/buy", max_pages=10):
        """Scrape property listings and reviews"""
        all_reviews = []
        
        for page in range(1, max_pages + 1):
            try:
                # Construct URL for pagination
                if page == 1:
                    url = urljoin(self.base_url, base_path)
                else:
                    url = urljoin(self.base_url, f"{base_path}?page={page}")
                
                self.logger.info(f"Scraping page {page}: {url}")
                
                html_content = self.get_page_content(url)
                if not html_content:
                    self.logger.warning(f"Failed to get content for page {page}")
                    continue
                
                # Extract reviews from this page
                page_reviews = self.extract_property_reviews(html_content, url)
                all_reviews.extend(page_reviews)
                
                # Check if we should continue (no more content)
                if len(page_reviews) == 0:
                    self.logger.info(f"No more reviews found, stopping at page {page}")
                    break
                
            except Exception as e:
                self.logger.error(f"Error scraping page {page}: {str(e)}")
                continue
        
        return all_reviews
    
    def scrape_insights_and_news(self):
        """Scrape property insights and news articles"""
        all_articles = []
        
        for path in ["/insights", "/news"]:
            try:
                url = urljoin(self.base_url, path)
                self.logger.info(f"Scraping insights/news: {url}")
                
                html_content = self.get_page_content(url)
                if not html_content:
                    continue
                
                articles = self.extract_property_reviews(html_content, url)
                all_articles.extend(articles)
                
            except Exception as e:
                self.logger.error(f"Error scraping {path}: {str(e)}")
                continue
        
        return all_articles
    
    def run_scraper(self, target_samples=3000):
        """Main scraper execution"""
        self.logger.info("Starting 99.co scraping for PropInsight dataset")
        
        # Setup session
        self.setup_session()
        
        all_data = []
        
        try:
            # Test site accessibility first
            test_url = urljoin(self.base_url, "/singapore")
            test_content = self.get_page_content(test_url)
            
            if not test_content:
                self.logger.error("Cannot access 99.co - site may be blocked or down")
                return []
            
            self.logger.info("99.co is accessible, proceeding with scraping")
            
            # Scrape property listings (buy)
            self.logger.info("Scraping property buy listings...")
            buy_reviews = self.scrape_property_listings("/singapore/buy", max_pages=15)
            all_data.extend(buy_reviews)
            
            # Scrape rental listings
            self.logger.info("Scraping property rental listings...")
            rent_reviews = self.scrape_property_listings("/singapore/rent", max_pages=10)
            all_data.extend(rent_reviews)
            
            # Scrape insights and news
            self.logger.info("Scraping property insights and news...")
            articles = self.scrape_insights_and_news()
            all_data.extend(articles)
            
            # Save data
            if all_data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"ninety_nine_co_data_{timestamp}.json"
                filepath = os.path.join(self.output_dir, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(all_data, f, ensure_ascii=False, indent=2)
                
                self.logger.info(f"Saved {len(all_data)} samples to {filepath}")
            
            self.logger.info(f"99.co scraping completed. Total samples: {len(all_data)}")
            return all_data
            
        except Exception as e:
            self.error_handler.log_error("Error in 99.co scraper execution", str(e))
            return []

def main():
    """Test the scraper"""
    scraper = NinetyNineCoScraper(use_proxy=True)
    results = scraper.run_scraper(target_samples=100)  # Test with smaller sample
    print(f"Collected {len(results)} samples from 99.co")

if __name__ == "__main__":
    main()