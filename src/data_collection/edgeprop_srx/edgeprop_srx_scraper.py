"""
EdgeProp and SRX Property Scraper for PropInsight Dataset
Tests accessibility and scrapes property market discussions, news, and analysis
Target: 2,000 samples combined from EdgeProp Singapore and SRX Property
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

class EdgePropSRXScraper:
    """Combined scraper for EdgeProp Singapore and SRX Property"""
    
    def __init__(self, output_dir="data/raw/edgeprop_srx", use_proxy=False):
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
        self.rate_limit_delay = float(os.getenv('RATE_LIMIT_DELAY', 4))
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        
        # Site configurations
        self.sites = {
            'edgeprop': {
                'name': 'EdgeProp Singapore',
                'base_url': 'https://www.edgeprop.sg',
                'news_urls': [
                    'https://www.edgeprop.sg/news',
                    'https://www.edgeprop.sg/property-news',
                    'https://www.edgeprop.sg/market-trends'
                ],
                'analysis_urls': [
                    'https://www.edgeprop.sg/property-analysis',
                    'https://www.edgeprop.sg/market-analysis'
                ]
            },
            'srx': {
                'name': 'SRX Property',
                'base_url': 'https://www.srx.com.sg',
                'news_urls': [
                    'https://www.srx.com.sg/property-news',
                    'https://www.srx.com.sg/singapore-property-news'
                ],
                'discussion_urls': [
                    'https://www.srx.com.sg/property-discussions',
                    'https://www.srx.com.sg/property-forum'
                ]
            }
        }
        
        # Property-related keywords for content filtering
        self.property_keywords = [
            'property', 'real estate', 'housing', 'hdb', 'bto', 'condo', 'condominium',
            'resale', 'rental', 'investment', 'mortgage', 'loan', 'valuation',
            'psf', 'price per square foot', 'market price', 'property agent',
            'viewing', 'offer', 'negotiation', 'landed', 'ec', 'executive condo',
            'private property', 'public housing', 'cooling measures', 'stamp duty',
            'property tax', 'ABSD', 'additional buyer stamp duty', 'LTV',
            'loan-to-value', 'TDSR', 'total debt servicing ratio'
        ]
        
    def setup_session(self):
        """Setup session with proxy if available"""
        if self.proxy_manager:
            proxy = self.proxy_manager.get_proxy()
            if proxy:
                self.session.proxies.update(proxy)
                self.logger.info(f"Using proxy: {proxy}")
        
        self.session.headers.update(self.headers)
        
    def test_site_accessibility(self, site_name, base_url):
        """Test if a site is accessible"""
        try:
            self.logger.info(f"Testing accessibility for {site_name}: {base_url}")
            response = self.session.get(base_url, timeout=30)
            
            if response.status_code == 200:
                self.logger.info(f"✅ {site_name} is accessible")
                return True
            elif response.status_code == 403:
                self.logger.warning(f"❌ {site_name} blocked (403 Forbidden)")
                return False
            elif response.status_code == 404:
                self.logger.warning(f"❌ {site_name} not found (404)")
                return False
            else:
                self.logger.warning(f"⚠️ {site_name} returned status {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ {site_name} connection failed: {str(e)}")
            return False
    
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
    
    def extract_articles_from_page(self, html_content, source_url, site_name):
        """Extract articles from a news/analysis page"""
        articles = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Common article selectors (adjust based on actual site structure)
            article_selectors = [
                'article',
                '.article',
                '.news-item',
                '.property-news',
                '.content-item',
                '.post',
                '.story'
            ]
            
            article_elements = []
            for selector in article_selectors:
                elements = soup.select(selector)
                if elements:
                    article_elements = elements
                    break
            
            # Fallback: look for divs with article-like content
            if not article_elements:
                article_elements = soup.find_all('div', class_=lambda x: x and any(
                    keyword in x.lower() for keyword in ['article', 'news', 'story', 'content']
                ))
            
            for element in article_elements[:20]:  # Limit per page
                try:
                    # Extract title
                    title_elem = element.find(['h1', 'h2', 'h3'], class_=lambda x: x and 'title' in x.lower())
                    if not title_elem:
                        title_elem = element.find(['h1', 'h2', 'h3'])
                    
                    title = title_elem.get_text(strip=True) if title_elem else ""
                    
                    # Extract content/summary
                    content_elem = element.find(['div', 'p'], class_=lambda x: x and any(
                        keyword in x.lower() for keyword in ['content', 'summary', 'excerpt', 'description']
                    ))
                    
                    if not content_elem:
                        # Fallback: get all text content
                        content_elem = element
                    
                    content = content_elem.get_text(strip=True) if content_elem else ""
                    
                    # Skip if not property-related or too short
                    full_text = f"{title} {content}"
                    if len(full_text) < 100 or not self.is_property_related(full_text):
                        continue
                    
                    # Extract article URL
                    link_elem = element.find('a', href=True)
                    article_url = ""
                    if link_elem:
                        href = link_elem.get('href')
                        if href:
                            article_url = urljoin(source_url, href)
                    
                    # Extract date
                    date_elem = element.find(['time', 'span'], class_=lambda x: x and 'date' in x.lower())
                    date_str = date_elem.get_text(strip=True) if date_elem else ""
                    
                    article_data = {
                        'title': title,
                        'content': content,
                        'url': article_url or source_url,
                        'date': date_str,
                        'source_page': source_url,
                        'scraped_at': datetime.now().isoformat(),
                        'platform': site_name.lower().replace(' ', '_'),
                        'data_type': 'property_news'
                    }
                    
                    # Validate data
                    if self.data_validator.validate_text_content(content):
                        articles.append(article_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error extracting article: {str(e)}")
                    continue
            
            self.logger.info(f"Extracted {len(articles)} articles from {source_url}")
            return articles
            
        except Exception as e:
            self.error_handler.log_error(f"Error parsing page {source_url}", str(e))
            return []
    
    def scrape_site(self, site_key, target_samples=1000):
        """Scrape a specific site (EdgeProp or SRX)"""
        site_config = self.sites[site_key]
        site_name = site_config['name']
        
        self.logger.info(f"Starting scraping for {site_name}")
        
        # Test accessibility first
        if not self.test_site_accessibility(site_name, site_config['base_url']):
            self.logger.error(f"Cannot access {site_name} - skipping")
            return []
        
        all_articles = []
        
        # Scrape news URLs
        if 'news_urls' in site_config:
            for news_url in site_config['news_urls']:
                try:
                    self.logger.info(f"Scraping news from: {news_url}")
                    html_content = self.get_page_content(news_url)
                    
                    if html_content:
                        articles = self.extract_articles_from_page(html_content, news_url, site_name)
                        all_articles.extend(articles)
                        
                        # Stop if we have enough articles
                        if len(all_articles) >= target_samples:
                            break
                    
                except Exception as e:
                    self.logger.error(f"Error scraping {news_url}: {str(e)}")
                    continue
        
        # Scrape analysis URLs if available
        if 'analysis_urls' in site_config and len(all_articles) < target_samples:
            for analysis_url in site_config['analysis_urls']:
                try:
                    self.logger.info(f"Scraping analysis from: {analysis_url}")
                    html_content = self.get_page_content(analysis_url)
                    
                    if html_content:
                        articles = self.extract_articles_from_page(html_content, analysis_url, site_name)
                        all_articles.extend(articles)
                        
                        # Stop if we have enough articles
                        if len(all_articles) >= target_samples:
                            break
                    
                except Exception as e:
                    self.logger.error(f"Error scraping {analysis_url}: {str(e)}")
                    continue
        
        # Scrape discussion URLs if available
        if 'discussion_urls' in site_config and len(all_articles) < target_samples:
            for discussion_url in site_config['discussion_urls']:
                try:
                    self.logger.info(f"Scraping discussions from: {discussion_url}")
                    html_content = self.get_page_content(discussion_url)
                    
                    if html_content:
                        articles = self.extract_articles_from_page(html_content, discussion_url, site_name)
                        all_articles.extend(articles)
                        
                        # Stop if we have enough articles
                        if len(all_articles) >= target_samples:
                            break
                    
                except Exception as e:
                    self.logger.error(f"Error scraping {discussion_url}: {str(e)}")
                    continue
        
        # Limit to target samples
        all_articles = all_articles[:target_samples]
        
        self.logger.info(f"Collected {len(all_articles)} articles from {site_name}")
        return all_articles
    
    def run_scraper(self, target_samples=2000):
        """Main scraper execution"""
        self.logger.info("Starting EdgeProp and SRX scraping for PropInsight dataset")
        
        # Setup session
        self.setup_session()
        
        all_data = []
        
        try:
            # Test both sites first
            accessibility_results = {}
            for site_key, site_config in self.sites.items():
                accessible = self.test_site_accessibility(site_config['name'], site_config['base_url'])
                accessibility_results[site_key] = accessible
            
            # Report accessibility results
            self.logger.info("=== Site Accessibility Results ===")
            for site_key, accessible in accessibility_results.items():
                status = "✅ ACCESSIBLE" if accessible else "❌ BLOCKED/UNAVAILABLE"
                self.logger.info(f"{self.sites[site_key]['name']}: {status}")
            
            # Scrape accessible sites
            samples_per_site = target_samples // 2  # Split evenly between sites
            
            for site_key, accessible in accessibility_results.items():
                if accessible:
                    site_articles = self.scrape_site(site_key, samples_per_site)
                    all_data.extend(site_articles)
                else:
                    self.logger.warning(f"Skipping {self.sites[site_key]['name']} - not accessible")
            
            # If one site is blocked, try to get more from the accessible one
            if len(all_data) < target_samples:
                accessible_sites = [k for k, v in accessibility_results.items() if v]
                if accessible_sites:
                    remaining_samples = target_samples - len(all_data)
                    for site_key in accessible_sites:
                        additional_articles = self.scrape_site(site_key, remaining_samples)
                        all_data.extend(additional_articles)
                        if len(all_data) >= target_samples:
                            break
            
            # Limit to target samples
            all_data = all_data[:target_samples]
            
            # Save data
            if all_data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"edgeprop_srx_data_{timestamp}.json"
                filepath = os.path.join(self.output_dir, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(all_data, f, ensure_ascii=False, indent=2)
                
                self.logger.info(f"Saved {len(all_data)} samples to {filepath}")
            
            self.logger.info(f"EdgeProp/SRX scraping completed. Total samples: {len(all_data)}")
            return all_data
            
        except Exception as e:
            self.error_handler.log_error("Error in EdgeProp/SRX scraper execution", str(e))
            return []

def main():
    """Test the scraper"""
    scraper = EdgePropSRXScraper(use_proxy=True)
    results = scraper.run_scraper(target_samples=50)  # Test with smaller sample
    print(f"Collected {len(results)} samples from EdgeProp/SRX")

if __name__ == "__main__":
    main()