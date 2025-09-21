"""
HDB (Housing & Development Board) Web Scraper for PropInsight

This scraper focuses on collecting property-related content from the HDB website,
with emphasis on BTO launches, press releases, and housing policy announcements.

Priority Sources :
1. HIGH PRIORITY: Press Releases (70% of content) - BTO launches, policy changes, grants
2. HIGH PRIORITY: BTO Portal Content (20% of content) - Launch details, application rates
3. MEDIUM PRIORITY: Community Programs (7% of content) - Neighborhood improvements
4. LOW PRIORITY: Business/Commercial (3% of content) - Commercial development

Author: Prem Varijakzhan
Date: 2025-01-21
"""

import requests
import json
import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HDBArticle:
    """Data structure for HDB articles following PropInsight specification"""
    id: str
    source: str  # Always "government_hdb"
    text: str
    timestamp: str  # ISO format
    url: str
    language: str  # Always "en"
    metadata: Dict
    
    # Completeness indicators
    content_length: int = 0
    word_count: int = 0
    is_truncated: bool = False
    original_length: int = 0
    extraction_method: str = "web_scraping"

class HDBScraper:
    """
    HDB Website Scraper for PropInsight
    
    Scrapes property-related content from HDB website with focus on:
    - Press releases (highest priority) - BTO launches, policy changes
    - BTO portal content - Launch details, application information
    - Community programs - Neighborhood improvements, resident satisfaction
    - Commercial updates - Heartland business, commercial development
    """
    
    def __init__(self, output_dir: str = "data/raw/government/hdb"):
        self.base_url = "https://www.hdb.gov.sg"
        self.bto_portal_url = "https://homes.hdb.gov.sg"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Setup output directory - use relative path from project root
        if not os.path.isabs(output_dir):
            # Get the project root directory (3 levels up from current script location)
            project_root = Path(__file__).parent.parent.parent.parent
            self.output_dir = project_root / output_dir
        else:
            self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Date range for scraping (2023-2025 for comprehensive policy coverage)
        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2025, 12, 31)
        
        # Priority URLs 
        self.priority_urls = {
            'high': {
                'press_releases': '/cs/infoweb/about-us/news-and-publications/press-releases',
                'bto_portal': 'https://homes.hdb.gov.sg/home/landing',
                'bto_modes': '/cs/infoweb/residential/buying-a-flat/buying-procedure-for-new-flats/modes-of-sale',
                'resale_prices': 'https://services2.hdb.gov.sg/webapp/BB33RTIS/BB33PReslTrans.jsp'
            },
            'medium': {
                'community': '/cs/infoweb/community',
                'buying_flat': '/cs/infoweb/residential/buying-a-flat',
                'selling_flat': '/cs/infoweb/residential/selling-a-flat',
                'living_hdb': '/cs/infoweb/residential/living-in-an-hdb-flat'
            },
            'low': {
                'commercial': '/cs/infoweb/business/commercial',
                'building_professionals': '/cs/infoweb/business/building-professionals',
                'estate_agents': '/cs/infoweb/business/estate-agents-and-salespersons'
            }
        }
        
        # HDB-specific keywords for property relevance
        self.property_keywords = [
            'bto', 'build to order', 'ballot', 'application', 'launch',
            'housing', 'flat', 'apartment', 'resale', 'rental',
            'hdb', 'public housing', 'executive condominium', 'ec',
            'grant', 'subsidy', 'cpf', 'housing loan', 'mortgage',
            'fresh start', 'enhanced grant', 'proximity grant',
            'ang mo kio', 'bedok', 'bishan', 'bukit batok', 'bukit merah',
            'bukit panjang', 'bukit timah', 'central', 'choa chu kang',
            'clementi', 'geylang', 'hougang', 'jurong east', 'jurong west',
            'kallang', 'marine parade', 'pasir ris', 'punggol', 'queenstown',
            'sembawang', 'sengkang', 'serangoon', 'tampines', 'toa payoh',
            'woodlands', 'yishun', 'estate', 'neighborhood', 'precinct',
            'tender', 'development', 'construction', 'completion',
            'policy', 'scheme', 'eligibility', 'priority', 'quota'
        ]

    def is_property_related(self, text: str) -> bool:
        """Check if content is property-related using HDB-specific keywords"""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.property_keywords)

    def is_within_date_range(self, published_date: datetime) -> bool:
        """Check if the published date is within our target range"""
        return self.start_date <= published_date <= self.end_date

    def extract_date_from_text(self, text: str) -> Optional[datetime]:
        """Extract date from various text formats commonly used by HDB"""
        # Common date patterns in HDB content
        date_patterns = [
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(\d{4})-(\d{2})-(\d{2})',
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
            r'(\d{1,2})-(\d{1,2})-(\d{4})'
        ]
        
        month_map = {
            'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
            'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6,
            'jul': 7, 'july': 7, 'aug': 8, 'august': 8, 'sep': 9, 'september': 9,
            'oct': 10, 'october': 10, 'nov': 11, 'november': 11, 'dec': 12, 'december': 12
        }
        
        for pattern in date_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                match = matches[0]
                try:
                    if len(match) == 3:
                        if match[1].lower() in month_map:
                            # Format: "15 Jan 2024" or "15 January 2024"
                            day, month_str, year = match
                            month = month_map[month_str.lower()]
                            return datetime(int(year), month, int(day))
                        elif '-' in text or '/' in text:
                            # Format: "2024-01-15" or "15/01/2024" or "15-01-2024"
                            if len(match[0]) == 4:  # Year first
                                year, month, day = match
                            else:  # Day first
                                day, month, year = match
                            return datetime(int(year), int(month), int(day))
                except (ValueError, KeyError):
                    continue
        
        # Default to current date if no date found
        return datetime.now()

    def get_full_article_content(self, url: str) -> Tuple[str, Dict]:
        """Extract full article content and metadata from HDB article page"""
        content_selectors = [
            '.content-area .field-item',
            '.main-content .content',
            '.article-content',
            '.press-release-content',
            '.news-content',
            '.field-name-body .field-item',
            '.content .field-item',
            'main .content',
            '.page-content'
        ]
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove unwanted elements
            for element in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                element.decompose()
            
            content = ""
            metadata = {}
            
            # Try different content selectors
            for selector in content_selectors:
                elements = soup.select(selector)
                if elements:
                    content = ' '.join([elem.get_text(strip=True) for elem in elements])
                    if len(content) > 100:  # Ensure we got substantial content
                        break
            
            # Fallback to body text if no specific content found
            if not content or len(content) < 100:
                body = soup.find('body')
                if body:
                    content = body.get_text(strip=True)
            
            # Extract title
            title_elem = soup.find('title') or soup.find('h1')
            if title_elem:
                metadata['title'] = title_elem.get_text(strip=True)
            
            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                metadata['description'] = meta_desc.get('content', '')
            
            # Clean up content
            content = re.sub(r'\s+', ' ', content).strip()
            
            return content, metadata
            
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {str(e)}")
            return "", {}

    def _smart_truncate(self, text: str, max_length: int, content_info: dict) -> Tuple[str, dict]:
        """Intelligently truncate text while preserving important information"""
        if len(text) <= max_length:
            content_info.update({
                'is_truncated': False,
                'original_length': len(text),
                'content_length': len(text)
            })
            return text, content_info
        
        # Find good truncation point (end of sentence)
        truncate_at = max_length
        for i in range(max_length - 100, max_length):
            if i < len(text) and text[i] in '.!?':
                truncate_at = i + 1
                break
        
        truncated_text = text[:truncate_at].strip()
        content_info.update({
            'is_truncated': True,
            'original_length': len(text),
            'content_length': len(truncated_text)
        })
        
        return truncated_text, content_info

    def scrape_press_releases(self) -> List[Dict]:
        """Scrape HDB press releases - highest priority content"""
        articles = []
        press_releases_url = urljoin(self.base_url, self.priority_urls['high']['press_releases'])
        
        try:
            logger.info(f"Scraping HDB press releases from: {press_releases_url}")
            response = self.session.get(press_releases_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find press release links
            press_release_links = []
            
            # Common selectors for HDB press release listings
            link_selectors = [
                'a[href*="press-releases"]',
                '.press-release-item a',
                '.news-item a',
                '.content-list a',
                '.view-content a'
            ]
            
            for selector in link_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href and 'press-releases' in href:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in [item['url'] for item in press_release_links]:
                            press_release_links.append({
                                'url': full_url,
                                'title': link.get_text(strip=True)
                            })
            
            logger.info(f"Found {len(press_release_links)} press release links")
            
            for link_info in press_release_links[:20]:  # Limit to recent 20 articles
                try:
                    url = link_info['url']
                    title = link_info['title']
                    
                    # Get full article content
                    content, metadata = self.get_full_article_content(url)
                    
                    if not content:
                        logger.warning(f"No content extracted from {url}")
                        continue
                    
                    # Check if content is property-related
                    if not self.is_property_related(content):
                        logger.info(f"Skipping non-property related article: {title}")
                        continue
                    
                    # Extract and validate date
                    date_text = content[:500]  # Check first 500 chars for date
                    published_date = self.extract_date_from_text(date_text)
                    
                    if not self.is_within_date_range(published_date):
                        logger.info(f"Skipping article outside date range: {title}")
                        continue
                    
                    # Smart truncation for content length
                    content_info = {}
                    content, content_info = self._smart_truncate(content, 3000, content_info)
                    
                    # Generate unique article ID
                    article_id = f"hdb_pr_{published_date.strftime('%Y%m%d')}_{hashlib.md5(url.encode()).hexdigest()[:8]}"
                    
                    # Classify content type and extract HDB-specific metadata
                    policy_type = self._classify_hdb_policy_type(content)
                    bto_related = self._is_bto_related(content)
                    location = self._extract_location(content)
                    grant_info = self._extract_grant_info(content)
                    
                    article_data = {
                        'id': article_id,
                        'source': 'government_hdb',
                        'text': content,
                        'timestamp': published_date.isoformat(),
                        'url': url,
                        'language': 'en',
                        'metadata': {
                            'title': metadata.get('title', title),
                            'agency': 'HDB',
                            'press_release_id': f"HDB_{published_date.strftime('%Y_%m%d')}",
                            'category': 'press_release',
                            'policy_type': policy_type,
                            'bto_related': bto_related,
                            'location': location,
                            'content_length': content_info.get('content_length', len(content)),
                            'keywords': self._extract_hdb_keywords(content),
                            'target_group': self._identify_target_demographics(content),
                            'market_segment': self._identify_market_segments(content),
                            **grant_info,
                            **content_info
                        }
                    }
                    
                    articles.append(article_data)
                    logger.info(f"Successfully scraped press release: {title}")
                    time.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error processing press release {link_info.get('url', 'unknown')}: {str(e)}")
                    continue
        
        except Exception as e:
            logger.error(f"Error scraping press releases: {str(e)}")
        
        return articles

    def scrape_bto_content(self) -> List[Dict]:
        """Scrape BTO-related content from HDB portal and main site"""
        articles = []
        
        # BTO-related URLs to scrape
        bto_urls = [
            self.priority_urls['high']['bto_modes'],
            '/cs/infoweb/residential/buying-a-flat/buying-procedure-for-new-flats/application',
            '/cs/infoweb/residential/buying-a-flat/buying-procedure-for-new-flats/timeline'
        ]
        
        for bto_path in bto_urls:
            try:
                url = urljoin(self.base_url, bto_path)
                logger.info(f"Scraping BTO content from: {url}")
                
                content, metadata = self.get_full_article_content(url)
                
                if not content or len(content) < 200:
                    continue
                
                # Check if content is property-related (should be for BTO content)
                if not self.is_property_related(content):
                    continue
                
                # Use current date for BTO information pages
                published_date = datetime.now()
                
                # Smart truncation
                content_info = {}
                content, content_info = self._smart_truncate(content, 3000, content_info)
                
                # Generate unique article ID
                article_id = f"hdb_bto_{published_date.strftime('%Y%m%d')}_{hashlib.md5(url.encode()).hexdigest()[:8]}"
                
                article_data = {
                    'id': article_id,
                    'source': 'government_hdb',
                    'text': content,
                    'timestamp': published_date.isoformat(),
                    'url': url,
                    'language': 'en',
                    'metadata': {
                        'title': metadata.get('title', 'BTO Information'),
                        'agency': 'HDB',
                        'category': 'bto_information',
                        'policy_type': 'bto_process',
                        'bto_related': True,
                        'content_type': 'informational',
                        'content_length': content_info.get('content_length', len(content)),
                        'keywords': self._extract_hdb_keywords(content),
                        **content_info
                    }
                }
                
                articles.append(article_data)
                logger.info(f"Successfully scraped BTO content from: {url}")
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error scraping BTO content from {bto_path}: {str(e)}")
                continue
        
        return articles

    def scrape_community_content(self) -> List[Dict]:
        """Scrape community-related content from HDB"""
        articles = []
        community_url = urljoin(self.base_url, self.priority_urls['medium']['community'])
        
        try:
            logger.info(f"Scraping HDB community content from: {community_url}")
            response = self.session.get(community_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find community-related links
            community_links = []
            link_selectors = [
                'a[href*="community"]',
                '.community-item a',
                '.program-item a',
                '.content-list a'
            ]
            
            for selector in link_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href:
                        full_url = urljoin(self.base_url, href)
                        if full_url not in [item['url'] for item in community_links]:
                            community_links.append({
                                'url': full_url,
                                'title': link.get_text(strip=True)
                            })
            
            logger.info(f"Found {len(community_links)} community links")
            
            for link_info in community_links[:10]:  # Limit to 10 community articles
                try:
                    url = link_info['url']
                    title = link_info['title']
                    
                    content, metadata = self.get_full_article_content(url)
                    
                    if not content or len(content) < 200:
                        continue
                    
                    # Check if content is property-related
                    if not self.is_property_related(content):
                        continue
                    
                    published_date = datetime.now()  # Use current date for community content
                    
                    # Smart truncation
                    content_info = {}
                    content, content_info = self._smart_truncate(content, 2500, content_info)
                    
                    article_id = f"hdb_community_{published_date.strftime('%Y%m%d')}_{hashlib.md5(url.encode()).hexdigest()[:8]}"
                    
                    article_data = {
                        'id': article_id,
                        'source': 'government_hdb',
                        'text': content,
                        'timestamp': published_date.isoformat(),
                        'url': url,
                        'language': 'en',
                        'metadata': {
                            'title': metadata.get('title', title),
                            'agency': 'HDB',
                            'category': 'community_programs',
                            'policy_type': 'community_development',
                            'bto_related': False,
                            'content_length': content_info.get('content_length', len(content)),
                            'keywords': self._extract_hdb_keywords(content),
                            **content_info
                        }
                    }
                    
                    articles.append(article_data)
                    logger.info(f"Successfully scraped community content: {title}")
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing community content {link_info.get('url', 'unknown')}: {str(e)}")
                    continue
        
        except Exception as e:
            logger.error(f"Error scraping community content: {str(e)}")
        
        return articles

    def _classify_hdb_policy_type(self, content: str) -> str:
        """Classify HDB content into policy types"""
        content_lower = content.lower()
        
        if any(word in content_lower for word in ['bto', 'build to order', 'ballot', 'launch']):
            return 'bto_launch'
        elif any(word in content_lower for word in ['grant', 'subsidy', 'fresh start', 'enhanced']):
            return 'housing_grant'
        elif any(word in content_lower for word in ['resale', 'selling', 'transaction']):
            return 'resale_policy'
        elif any(word in content_lower for word in ['rental', 'rent', 'lease']):
            return 'rental_policy'
        elif any(word in content_lower for word in ['ec', 'executive condominium', 'tender']):
            return 'ec_development'
        elif any(word in content_lower for word in ['community', 'neighborhood', 'resident']):
            return 'community_development'
        elif any(word in content_lower for word in ['commercial', 'business', 'shop']):
            return 'commercial_development'
        else:
            return 'general_housing'

    def _is_bto_related(self, content: str) -> bool:
        """Check if content is BTO-related"""
        bto_keywords = ['bto', 'build to order', 'ballot', 'application', 'launch', 'exercise']
        content_lower = content.lower()
        return any(keyword in content_lower for keyword in bto_keywords)

    def _extract_location(self, content: str) -> Optional[str]:
        """Extract location/estate information from content"""
        locations = [
            'ang mo kio', 'bedok', 'bishan', 'bukit batok', 'bukit merah',
            'bukit panjang', 'bukit timah', 'central', 'choa chu kang',
            'clementi', 'geylang', 'hougang', 'jurong east', 'jurong west',
            'kallang', 'marine parade', 'pasir ris', 'punggol', 'queenstown',
            'sembawang', 'sengkang', 'serangoon', 'tampines', 'toa payoh',
            'woodlands', 'yishun'
        ]
        
        content_lower = content.lower()
        for location in locations:
            if location in content_lower:
                return location.title()
        return None

    def _extract_grant_info(self, content: str) -> Dict:
        """Extract grant-related information from content"""
        grant_info = {}
        
        # Look for grant amounts
        amount_pattern = r'\$(\d{1,3}(?:,\d{3})*)'
        amounts = re.findall(amount_pattern, content)
        if amounts:
            # Convert to integers and find the largest (likely the main grant amount)
            amounts_int = [int(amount.replace(',', '')) for amount in amounts]
            grant_info['grant_amount'] = max(amounts_int)
        
        # Look for grant types
        if 'fresh start' in content.lower():
            grant_info['grant_type'] = 'fresh_start'
        elif 'enhanced' in content.lower() and 'grant' in content.lower():
            grant_info['grant_type'] = 'enhanced_grant'
        elif 'proximity' in content.lower():
            grant_info['grant_type'] = 'proximity_grant'
        
        return grant_info

    def _extract_hdb_keywords(self, content: str) -> List[str]:
        """Extract relevant keywords from HDB content"""
        keywords = []
        content_lower = content.lower()
        
        for keyword in self.property_keywords:
            if keyword in content_lower:
                keywords.append(keyword)
        
        return keywords[:10]  # Limit to top 10 keywords

    def _identify_target_demographics(self, content: str) -> List[str]:
        """Identify target demographics from content"""
        demographics = []
        content_lower = content.lower()
        
        demographic_keywords = {
            'first_time_buyers': ['first-time', 'first time', 'new buyers'],
            'young_couples': ['young couples', 'newly married'],
            'families': ['families', 'family', 'children'],
            'elderly': ['elderly', 'seniors', 'senior citizens'],
            'singles': ['singles', 'single'],
            'public_rental_families': ['public rental', 'rental families']
        }
        
        for demo, keywords in demographic_keywords.items():
            if any(keyword in content_lower for keyword in keywords):
                demographics.append(demo)
        
        return demographics

    def _identify_market_segments(self, content: str) -> List[str]:
        """Identify market segments from content"""
        segments = []
        content_lower = content.lower()
        
        segment_keywords = {
            'bto_market': ['bto', 'build to order', 'new flats'],
            'resale_market': ['resale', 'resale flats', 'existing flats'],
            'rental_market': ['rental', 'rent', 'lease'],
            'ec_market': ['executive condominium', 'ec'],
            'commercial_market': ['commercial', 'business', 'retail']
        }
        
        for segment, keywords in segment_keywords.items():
            if any(keyword in content_lower for keyword in keywords):
                segments.append(segment)
        
        return segments

    def scrape_all_sources(self) -> List[HDBArticle]:
        """Scrape all HDB sources according to priority"""
        all_articles = []
        
        logger.info("Starting comprehensive HDB scraping...")
        
        # High priority: Press releases (70% of content focus)
        logger.info("Scraping high priority: Press releases")
        press_releases = self.scrape_press_releases()
        all_articles.extend(press_releases)
        
        # High priority: BTO content (20% of content focus)
        logger.info("Scraping high priority: BTO content")
        bto_articles = self.scrape_bto_content()
        all_articles.extend(bto_articles)
        
        # Medium priority: Community content (7% of content focus)
        logger.info("Scraping medium priority: Community content")
        community_articles = self.scrape_community_content()
        all_articles.extend(community_articles)
        
        # Convert to HDBArticle objects
        hdb_articles = []
        for article_data in all_articles:
            try:
                hdb_article = HDBArticle(
                    id=article_data['id'],
                    source=article_data['source'],
                    text=article_data['text'],
                    timestamp=article_data['timestamp'],
                    url=article_data['url'],
                    language=article_data['language'],
                    metadata=article_data['metadata'],
                    content_length=len(article_data['text']),
                    word_count=len(article_data['text'].split()),
                    is_truncated=article_data['metadata'].get('is_truncated', False),
                    original_length=article_data['metadata'].get('original_length', len(article_data['text'])),
                    extraction_method="web_scraping"
                )
                hdb_articles.append(hdb_article)
            except Exception as e:
                logger.error(f"Error creating HDBArticle object: {str(e)}")
                continue
        
        logger.info(f"Successfully scraped {len(hdb_articles)} HDB articles")
        return hdb_articles

    def save_articles_to_json(self, articles: List[HDBArticle], filename: str = "hdb_articles.json"):
        """Save articles to JSON file with comprehensive metadata"""
        if not articles:
            logger.warning("No articles to save")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"hdb_articles_{timestamp}.json"
        filepath = self.output_dir / filename
        
        # Convert articles to dictionaries
        articles_data = []
        for article in articles:
            article_dict = asdict(article)
            articles_data.append(article_dict)
        
        # Create comprehensive output with metadata
        output_data = {
            'scraping_metadata': {
                'scraper_version': '1.0',
                'scraping_date': datetime.now().isoformat(),
                'total_articles': len(articles),
                'date_range': {
                    'start': self.start_date.isoformat(),
                    'end': self.end_date.isoformat()
                },
                'sources_scraped': list(self.priority_urls.keys()),
                'source_breakdown': self._get_source_breakdown(articles),
                'policy_type_breakdown': self._get_policy_type_breakdown(articles)
            },
            'articles': articles_data
        }
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Successfully saved {len(articles)} articles to {filepath}")
            
            # Also save statistics
            stats_filename = f"hdb_scraping_stats_{timestamp}.json"
            stats_filepath = self.output_dir / stats_filename
            
            stats = {
                'total_articles': len(articles),
                'avg_content_length': sum(len(article.text) for article in articles) / len(articles),
                'date_range_coverage': {
                    'start': min(article.timestamp for article in articles),
                    'end': max(article.timestamp for article in articles)
                },
                'source_breakdown': self._get_source_breakdown(articles),
                'policy_type_breakdown': self._get_policy_type_breakdown(articles),
                'bto_related_count': sum(1 for article in articles if article.metadata.get('bto_related', False))
            }
            
            with open(stats_filepath, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Scraping statistics saved to {stats_filepath}")
            
        except Exception as e:
            logger.error(f"Error saving articles: {str(e)}")

    def _get_source_breakdown(self, articles: List[HDBArticle]) -> Dict[str, int]:
        """Get breakdown of articles by source category"""
        breakdown = {}
        for article in articles:
            category = article.metadata.get('category', 'unknown')
            breakdown[category] = breakdown.get(category, 0) + 1
        return breakdown

    def _get_policy_type_breakdown(self, articles: List[HDBArticle]) -> Dict[str, int]:
        """Get breakdown of articles by policy type"""
        breakdown = {}
        for article in articles:
            policy_type = article.metadata.get('policy_type', 'unknown')
            breakdown[policy_type] = breakdown.get(policy_type, 0) + 1
        return breakdown

def main():
    """Main execution function"""
    logger.info("Starting HDB scraper for PropInsight...")
    
    scraper = HDBScraper()
    articles = scraper.scrape_all_sources()
    
    if articles:
        scraper.save_articles_to_json(articles)
        logger.info(f"HDB scraping completed successfully. Total articles: {len(articles)}")
        
        # Print summary
        print(f"\n=== HDB Scraping Summary ===")
        print(f"Total articles scraped: {len(articles)}")
        print(f"Date range: {scraper.start_date.strftime('%Y-%m-%d')} to {scraper.end_date.strftime('%Y-%m-%d')}")
        
        # Source breakdown
        source_breakdown = scraper._get_source_breakdown(articles)
        print(f"\nSource breakdown:")
        for source, count in source_breakdown.items():
            print(f"  {source}: {count} articles")
        
        # Policy type breakdown
        policy_breakdown = scraper._get_policy_type_breakdown(articles)
        print(f"\nPolicy type breakdown:")
        for policy_type, count in policy_breakdown.items():
            print(f"  {policy_type}: {count} articles")
            
        # BTO-related count
        bto_count = sum(1 for article in articles if article.metadata.get('bto_related', False))
        print(f"\nBTO-related articles: {bto_count}")
        
    else:
        logger.warning("No articles were scraped")

if __name__ == "__main__":
    main()