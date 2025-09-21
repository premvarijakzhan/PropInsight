"""
SLA (Singapore Land Authority) Data Scraper for PropInsight

This scraper collects property-related data from SLA website based on the specifications. 
It focuses on land acquisition announcements, state property tenders, 
land betterment charge updates, and property statistics that provide upstream indicators
for property supply and development pipeline analysis.

Priority Sources:
1. Press Releases (50%) - Land acquisition, state property tenders, LBC updates
2. Statistics (30%) - State land sales data, property registration volumes  
3. Land Sales Management (15%) - SPIO property listings, tender results
4. Circulars (5%) - Regulatory process changes, technical clarifications

Data is saved to: data/raw/government/sla/ (relative path)
"""

import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
import time
import random
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any
import re
from urllib.parse import urljoin, urlparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class SLAArticle:
    """Data class for SLA articles following the JSON structure"""
    id: str
    source: str
    text: str
    timestamp: str
    url: str
    language: str
    metadata: Dict[str, Any]

class SLAScraper:
    """
    SLA Website Scraper for PropInsight
    
    Scrapes SLA content with priority-based approach:
    - Press Releases: 50% priority (land acquisition, tenders, policy updates)
    - Statistics: 30% priority (land sales data, property registration)
    - Land Sales Management: 15% priority (SPIO listings, tender results)
    - Circulars: 5% priority (regulatory updates, technical changes)
    """
    
    def __init__(self):
        # Get project root directory (3 levels up from current file)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        
        # Configure output directory using relative path from project root
        self.output_dir = os.path.join(project_root, "data", "raw", "government", "sla")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Base URLs for different SLA content types
        self.base_urls = {
            'press_releases': 'https://www.sla.gov.sg/news/press-release/',
            'statistics': 'https://www.sla.gov.sg/news/statistics',
            'land_sales': 'https://www.sla.gov.sg/properties/land-sales-and-lease-management/',
            'circulars': 'https://www.sla.gov.sg/news/circulars'
        }
        
        # Priority weights for content types (must sum to 100%)
        self.priority_weights = {
            'press_releases': 50,  # Highest priority - direct policy impact
            'statistics': 30,      # High priority - market indicators
            'land_sales': 15,      # Medium priority - development opportunities
            'circulars': 5         # Low priority - technical updates
        }
        
        # Date range for scraping (2023-2025)

        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2025, 12, 31)
        
        # Keywords for filtering relevant content
        self.relevant_keywords = [
            'land acquisition', 'BTO', 'public housing', 'development', 'tender',
            'state property', 'land betterment charge', 'LBC', 'property registration',
            'land sales', 'housing supply', 'infrastructure', 'compensation',
            'land swap', 'heritage site', 'adaptive reuse', 'development timeline'
        ]
        
        # Request headers to mimic browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Statistics for tracking scraping results
        self.stats = {
            'press_releases': 0,
            'statistics': 0,
            'land_sales': 0,
            'circulars': 0,
            'total_articles': 0,
            'errors': 0
        }

    def extract_sla_metadata(self, article_soup: BeautifulSoup, url: str, content_type: str) -> Dict[str, Any]:
        """Extract SLA-specific metadata fields"""
        
        metadata = {
            'agency': 'SLA',
            'content_length': len(article_soup.get_text()),
            'keywords': []
        }
        
        # Extract title
        title_elem = article_soup.find('h1') or article_soup.find('title')
        if title_elem:
            metadata['title'] = title_elem.get_text().strip()
        
        # Determine category and policy type based on content
        text_content = article_soup.get_text().lower()
        
        # Category classification
        if 'land acquisition' in text_content or 'compulsory acquisition' in text_content:
            metadata['category'] = 'land_acquisition'
            metadata['policy_type'] = 'public_development'
        elif 'tender' in text_content and 'state property' in text_content:
            metadata['category'] = 'state_property_development'
            metadata['policy_type'] = 'adaptive_reuse'
        elif 'land betterment charge' in text_content or 'lbc' in text_content:
            metadata['category'] = 'regulatory_update'
            metadata['policy_type'] = 'fee_revision'
        elif 'statistics' in text_content or 'data' in text_content:
            metadata['category'] = 'market_data'
            metadata['policy_type'] = 'statistical_release'
        else:
            metadata['category'] = 'general_announcement'
            metadata['policy_type'] = 'information_update'
        
        # Extract SLA-specific fields
        
        # Land size extraction
        land_size_match = re.search(r'(\d+(?:\.\d+)?)\s*hectares?', text_content)
        if land_size_match:
            metadata['land_size_hectares'] = float(land_size_match.group(1))
        
        # Projected units extraction
        units_match = re.search(r'(\d+(?:,\d+)?)\s*(?:new\s+)?(?:bto\s+)?units?', text_content)
        if units_match:
            units_str = units_match.group(1).replace(',', '')
            metadata['projected_units'] = int(units_str)
        
        # Timeline extraction
        timeline_match = re.search(r'(?:by\s+|completion\s+.*?)(\d{4})', text_content)
        if timeline_match:
            metadata['completion_timeline'] = timeline_match.group(1)
        
        # Location extraction
        location_patterns = [
            r'(?:at\s+|in\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*?)(?:\s+(?:Road|Street|Avenue|Drive|Lane|Park|Estate|Town|Area))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*?)\s+(?:development|project|site|area)'
        ]
        for pattern in location_patterns:
            location_match = re.search(pattern, article_soup.get_text())
            if location_match:
                metadata['location'] = location_match.group(1).strip()
                break
        
        # Development type extraction
        if 'bto' in text_content:
            metadata['development_type'] = 'BTO'
        elif 'private' in text_content and 'housing' in text_content:
            metadata['development_type'] = 'private_housing'
        elif 'commercial' in text_content:
            metadata['development_type'] = 'commercial'
        elif 'industrial' in text_content:
            metadata['development_type'] = 'industrial'
        
        # Compensation and affected residents
        if 'compensation' in text_content:
            metadata['compensation_provided'] = True
            
        affected_match = re.search(r'(\d+)\s*(?:residents?|households?|families)', text_content)
        if affected_match:
            metadata['affected_residents'] = int(affected_match.group(1))
        
        # Tender-specific fields
        if 'tender' in text_content:
            # Tenure extraction
            tenure_match = re.search(r'(\d+)(?:\+(\d+))?\s*years?', text_content)
            if tenure_match:
                base_tenure = tenure_match.group(1)
                extension = tenure_match.group(2) if tenure_match.group(2) else None
                metadata['tenure_years'] = f"{base_tenure}+{extension}" if extension else base_tenure
            
            # GFA extraction
            gfa_match = re.search(r'(\d+(?:,\d+)?)\s*(?:sq\s*ft|square\s*feet)', text_content)
            if gfa_match:
                gfa_str = gfa_match.group(1).replace(',', '')
                metadata['total_gfa_sqft'] = int(gfa_str)
        
        # Extract relevant keywords
        found_keywords = []
        for keyword in self.relevant_keywords:
            if keyword.lower() in text_content:
                found_keywords.append(keyword)
        metadata['keywords'] = found_keywords
        
        # Add press release ID if available
        if content_type == 'press_releases':
            # Generate press release ID based on date and title
            date_str = datetime.now().strftime('%Y%m%d')
            title_slug = re.sub(r'[^a-zA-Z0-9]', '_', metadata.get('title', 'unknown')).lower()[:30]
            metadata['press_release_id'] = f"SLA_{date_str}_{title_slug}"
        
        return metadata

    def scrape_press_releases(self) -> List[SLAArticle]:
        """Scrape SLA press releases (50% priority)"""
        logger.info("Scraping SLA press releases...")
        articles = []
        
        try:
            response = requests.get(self.base_urls['press_releases'], headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find press release links
            press_release_links = soup.find_all('a', href=True)
            
            for link in press_release_links[:20]:  # Limit to recent releases
                href = link.get('href')
                if not href or 'press-release' not in href:
                    continue
                
                full_url = urljoin(self.base_urls['press_releases'], href)
                
                try:
                    # Add delay to be respectful
                    time.sleep(random.uniform(1, 3))
                    
                    article_response = requests.get(full_url, headers=self.headers)
                    article_response.raise_for_status()
                    
                    article_soup = BeautifulSoup(article_response.content, 'html.parser')
                    
                    # Extract article content
                    content_div = article_soup.find('div', class_='content') or article_soup.find('main') or article_soup
                    article_text = content_div.get_text().strip() if content_div else ""
                    
                    if len(article_text) < 100:  # Skip very short articles
                        continue
                    
                    # Check if content is relevant
                    if not any(keyword.lower() in article_text.lower() for keyword in self.relevant_keywords):
                        continue
                    
                    # Extract metadata
                    metadata = self.extract_sla_metadata(article_soup, full_url, 'press_releases')
                    
                    # Create article object
                    article_id = f"sla_pr_{datetime.now().strftime('%Y%m%d')}_{len(articles)}"
                    
                    article = SLAArticle(
                        id=article_id,
                        source="government_sla",
                        text=article_text,
                        timestamp=datetime.now().isoformat(),
                        url=full_url,
                        language="en",
                        metadata=metadata
                    )
                    
                    articles.append(article)
                    self.stats['press_releases'] += 1
                    
                    logger.info(f"Scraped press release: {metadata.get('title', 'Unknown')}")
                    
                except Exception as e:
                    logger.error(f"Error scraping press release {full_url}: {str(e)}")
                    self.stats['errors'] += 1
                    continue
        
        except Exception as e:
            logger.error(f"Error accessing press releases page: {str(e)}")
            self.stats['errors'] += 1
        
        return articles

    def scrape_statistics(self) -> List[SLAArticle]:
        """Scrape SLA statistics and data (30% priority)"""
        logger.info("Scraping SLA statistics...")
        articles = []
        
        try:
            response = requests.get(self.base_urls['statistics'], headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for statistics content and data links
            stats_content = soup.get_text()
            
            if len(stats_content) > 200:  # If there's substantial content
                metadata = self.extract_sla_metadata(soup, self.base_urls['statistics'], 'statistics')
                metadata['category'] = 'market_statistics'
                metadata['policy_type'] = 'data_release'
                
                article_id = f"sla_stats_{datetime.now().strftime('%Y%m%d')}"
                
                article = SLAArticle(
                    id=article_id,
                    source="government_sla",
                    text=stats_content,
                    timestamp=datetime.now().isoformat(),
                    url=self.base_urls['statistics'],
                    language="en",
                    metadata=metadata
                )
                
                articles.append(article)
                self.stats['statistics'] += 1
                
                logger.info("Scraped SLA statistics page")
        
        except Exception as e:
            logger.error(f"Error scraping statistics: {str(e)}")
            self.stats['errors'] += 1
        
        return articles

    def scrape_land_sales(self) -> List[SLAArticle]:
        """Scrape land sales and lease management content (15% priority)"""
        logger.info("Scraping SLA land sales information...")
        articles = []
        
        try:
            response = requests.get(self.base_urls['land_sales'], headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract land sales content
            content = soup.get_text()
            
            if len(content) > 200 and any(keyword.lower() in content.lower() for keyword in ['land sales', 'tender', 'lease', 'property']):
                metadata = self.extract_sla_metadata(soup, self.base_urls['land_sales'], 'land_sales')
                metadata['category'] = 'land_sales_management'
                metadata['policy_type'] = 'property_availability'
                
                article_id = f"sla_land_sales_{datetime.now().strftime('%Y%m%d')}"
                
                article = SLAArticle(
                    id=article_id,
                    source="government_sla",
                    text=content,
                    timestamp=datetime.now().isoformat(),
                    url=self.base_urls['land_sales'],
                    language="en",
                    metadata=metadata
                )
                
                articles.append(article)
                self.stats['land_sales'] += 1
                
                logger.info("Scraped SLA land sales page")
        
        except Exception as e:
            logger.error(f"Error scraping land sales: {str(e)}")
            self.stats['errors'] += 1
        
        return articles

    def scrape_circulars(self) -> List[SLAArticle]:
        """Scrape SLA circulars and regulatory updates (5% priority)"""
        logger.info("Scraping SLA circulars...")
        articles = []
        
        try:
            response = requests.get(self.base_urls['circulars'], headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for circular links
            circular_links = soup.find_all('a', href=True)
            
            for link in circular_links[:5]:  # Limit to recent circulars
                href = link.get('href')
                if not href or not any(ext in href.lower() for ext in ['.pdf', 'circular']):
                    continue
                
                # For PDF links, we'll create a reference entry
                link_text = link.get_text().strip()
                if len(link_text) > 10:  # Skip very short link texts
                    
                    metadata = {
                        'agency': 'SLA',
                        'title': link_text,
                        'category': 'regulatory_circular',
                        'policy_type': 'technical_update',
                        'document_type': 'PDF' if '.pdf' in href.lower() else 'webpage',
                        'keywords': ['circular', 'regulatory', 'technical']
                    }
                    
                    article_id = f"sla_circular_{datetime.now().strftime('%Y%m%d')}_{len(articles)}"
                    
                    article = SLAArticle(
                        id=article_id,
                        source="government_sla",
                        text=f"SLA Circular: {link_text}",
                        timestamp=datetime.now().isoformat(),
                        url=urljoin(self.base_urls['circulars'], href),
                        language="en",
                        metadata=metadata
                    )
                    
                    articles.append(article)
                    self.stats['circulars'] += 1
                    
                    logger.info(f"Recorded SLA circular: {link_text}")
        
        except Exception as e:
            logger.error(f"Error scraping circulars: {str(e)}")
            self.stats['errors'] += 1
        
        return articles

    def save_articles(self, articles: List[SLAArticle], filename_prefix: str):
        """Save articles to JSONL file"""
        if not articles:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.jsonl"
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for article in articles:
                json.dump(asdict(article), f, ensure_ascii=False)
                f.write('\n')
        
        logger.info(f"Saved {len(articles)} articles to {filepath}")

    def save_statistics(self):
        """Save scraping statistics"""
        self.stats['total_articles'] = sum([
            self.stats['press_releases'],
            self.stats['statistics'], 
            self.stats['land_sales'],
            self.stats['circulars']
        ])
        
        stats_file = os.path.join(self.output_dir, "sla_scraping_stats.json")
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved scraping statistics to {stats_file}")

    def run_scraper(self):
        """Run the complete SLA scraping process"""
        logger.info("Starting SLA scraper...")
        logger.info(f"Output directory: {self.output_dir}")
        
        all_articles = []
        
        # Scrape based on priority weights
        logger.info("=== Scraping Press Releases (50% priority) ===")
        press_releases = self.scrape_press_releases()
        all_articles.extend(press_releases)
        
        logger.info("=== Scraping Statistics (30% priority) ===")
        statistics = self.scrape_statistics()
        all_articles.extend(statistics)
        
        logger.info("=== Scraping Land Sales (15% priority) ===")
        land_sales = self.scrape_land_sales()
        all_articles.extend(land_sales)
        
        logger.info("=== Scraping Circulars (5% priority) ===")
        circulars = self.scrape_circulars()
        all_articles.extend(circulars)
        
        # Save articles by category
        if press_releases:
            self.save_articles(press_releases, "sla_press_releases")
        if statistics:
            self.save_articles(statistics, "sla_statistics")
        if land_sales:
            self.save_articles(land_sales, "sla_land_sales")
        if circulars:
            self.save_articles(circulars, "sla_circulars")
        
        # Save all articles combined
        if all_articles:
            self.save_articles(all_articles, "sla_articles")
        
        # Save statistics
        self.save_statistics()
        
        # Print summary
        logger.info("=== SLA Scraping Complete ===")
        logger.info(f"Press Releases: {self.stats['press_releases']}")
        logger.info(f"Statistics: {self.stats['statistics']}")
        logger.info(f"Land Sales: {self.stats['land_sales']}")
        logger.info(f"Circulars: {self.stats['circulars']}")
        logger.info(f"Total Articles: {self.stats['total_articles']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Data saved to: {self.output_dir}")

if __name__ == "__main__":
    scraper = SLAScraper()
    scraper.run_scraper()