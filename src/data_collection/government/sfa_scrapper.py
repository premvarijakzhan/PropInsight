"""
PropInsight SFA (Singapore Food Agency) Scraper

This module scrapes food safety and property-related announcements from Singapore Food Agency:
- Annual Reports
- Singapore Food Statistics (SGFS)
- Newsroom content
- Circulars and Notices
- Food Alerts & Recalls
- Food Hygiene Notices

Focus: Food safety regulations that may impact property development, restaurant licensing,
and food establishment requirements in residential and commercial properties.
"""

import requests
import json
import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import feedparser
import PyPDF2
import io

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SFAArticle:
    """Data structure for SFA articles - ensures consistent data format"""
    id: str
    title: str
    content: str
    summary: str
    published_date: datetime
    source_type: str  # annual_reports, sgfs, newsroom, circulars, alerts, hygiene_notices
    url: str
    category: str
    tags: List[str]
    # Content completeness indicators
    content_length: int = 0
    word_count: int = 0
    is_truncated: bool = False
    original_length: int = 0
    extraction_method: str = "unknown"
    
class SFAScraper:
    """
    SFA scraper for Singapore food safety and property-related announcements
    
    Scrapes from official SFA website sections that may impact property development
    and food establishment operations in residential and commercial properties
    """
    
    def __init__(self, output_dir: str = "data\\raw\\government\\sfa"):
        """
        Initialize SFA scraper
        
        Args:
            output_dir: Directory to save scraped data
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # SFA data sources
        self.sources = {
            'annual_reports': {
                'name': 'SFA Annual Reports',
                'url': 'https://www.sfa.gov.sg/news-publications/publications/annual-reports',
                'base_url': 'https://www.sfa.gov.sg',
                'description': 'Annual reports containing policy updates and statistics'
            },
            'sgfs': {
                'name': 'Singapore Food Statistics',
                'url': 'https://www.sfa.gov.sg/news-publications/publications/sgfs',
                'base_url': 'https://www.sfa.gov.sg',
                'description': 'Food sector trends and statistics that may impact property planning'
            },
            'newsroom': {
                'name': 'SFA Newsroom',
                'url': 'https://www.sfa.gov.sg/news-publications/newsroom',
                'base_url': 'https://www.sfa.gov.sg',
                'description': 'Latest news and press releases'
            },
            'circulars': {
                'name': 'SFA Circulars',
                'url': 'https://www.sfa.gov.sg/news-publications/circulars-and-notices#circulars',
                'base_url': 'https://www.sfa.gov.sg',
                'description': 'Official circulars and regulatory updates'
            },
            'food_alerts': {
                'name': 'Food Alerts & Recalls',
                'url': 'https://www.sfa.gov.sg/news-publications/circulars-and-notices#food-alerts-recalls',
                'base_url': 'https://www.sfa.gov.sg',
                'description': 'Food safety alerts and product recalls'
            },
            'hygiene_notices': {
                'name': 'Food Hygiene Notices',
                'url': 'https://www.sfa.gov.sg/news-publications/circulars-and-notices#food-hygiene-notices2c5a6d7f-beeb-4c29-9fcb-7ce833bfc958',
                'base_url': 'https://www.sfa.gov.sg',
                'description': 'Food establishment hygiene violations and suspensions'
            },
            'press_releases': {
                'name': 'SFA Press Releases',
                'url': 'https://www.sfa.gov.sg/news-publications/newsroom',
                'base_url': 'https://www.sfa.gov.sg',
                'description': 'Official press releases and announcements'
            }
        }
        
        # Property and development-related keywords for SFA content
        self.property_keywords = [
            # Food establishment licensing
            'food establishment', 'restaurant license', 'food court', 'hawker centre',
            'food retail', 'food manufacturing', 'food processing facility',
            
            # Property development related
            'commercial kitchen', 'food preparation area', 'restaurant space',
            'food service area', 'dining establishment', 'food outlet',
            
            # Regulatory compliance
            'licensing requirements', 'food safety standards', 'hygiene requirements',
            'establishment closure', 'suspension notice', 'compliance audit',
            
            # Location-specific
            'shopping mall', 'commercial building', 'residential area',
            'mixed development', 'food centre', 'market',
            
            # Business operations
            'food business', 'catering service', 'food delivery',
            'food retail outlet', 'supermarket', 'grocery store',
            
            # Development planning
            'food facility planning', 'kitchen design', 'food safety infrastructure',
            'waste management', 'ventilation requirements'
        ]
        
        # Date range for scraping
        self.start_date = datetime(2022, 1, 1)
        self.end_date = datetime(2025, 12, 31)
        
        # Setup session with proper headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
    
    def is_property_related(self, text: str) -> bool:
        """
        Check if content is related to property/real estate - REMOVED FILTERING
        Now returns True for all content to collect everything from SFA
        """
        # Remove property filtering - collect all SFA content
        return True
    
    def is_within_date_range(self, published_date: datetime) -> bool:
        """
        Check if article is within our target date range
        
        Args:
            published_date: Article publication date
            
        Returns:
            bool: True if within range
        """
        return self.start_date <= published_date <= self.end_date
    
    def extract_date_from_text(self, text: str) -> Optional[datetime]:
        """
        Extract date from various text formats commonly used by SFA
        
        Args:
            text: Text containing date information
            
        Returns:
            Parsed datetime object or None
        """
        if not text:
            return None
            
        # Common date patterns in SFA content
        date_patterns = [
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
            r'(\d{4})-(\d{1,2})-(\d{1,2})',
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})'
        ]
        
        month_map = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
            'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 3:
                        if groups[1] in month_map:  # Month name format
                            day, month, year = int(groups[0]), month_map[groups[1]], int(groups[2])
                        else:  # Numeric format
                            if '/' in text:  # DD/MM/YYYY or MM/DD/YYYY
                                day, month, year = int(groups[0]), int(groups[1]), int(groups[2])
                            else:  # YYYY-MM-DD
                                year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                        
                        return datetime(year, month, day)
                except (ValueError, KeyError):
                    continue
        
        return None
    
    def scrape_annual_reports(self) -> List[Dict]:
        """Scrape SFA annual reports"""
        articles = []
        source_info = self.sources['annual_reports']
        
        try:
            logger.info(f"Scraping {source_info['name']}: {source_info['url']}")
            
            response = self.session.get(source_info['url'], timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for actual report download links (PDFs, documents)
            report_links = soup.find_all('a', href=re.compile(r'\.(pdf|doc|docx|xls|xlsx)$', re.I))
            
            # Also look for report items in content areas
            content_areas = soup.find_all(['div', 'section'], class_=re.compile(r'content|main|publication|report', re.I))
            
            for area in content_areas:
                area_links = area.find_all('a', href=True)
                report_links.extend(area_links)
            
            # Remove duplicates
            seen_links = set()
            unique_links = []
            for link in report_links:
                href = link.get('href', '')
                if href and href not in seen_links:
                    seen_links.add(href)
                    unique_links.append(link)
            
            for item in unique_links[:15]:  # Limit to recent reports
                try:
                    title = item.get_text(strip=True)
                    link = item.get('href', '')
                    
                    if not title or not link or len(title) < 3:
                        continue
                    
                    # Skip navigation links
                    if any(nav_word in title.lower() for nav_word in ['home', 'back', 'menu', 'search', 'login']):
                        continue
                    
                    # Make absolute URL
                    if link.startswith('/'):
                        link = urljoin(source_info['base_url'], link)
                    
                    # Extract year from title or link for dating
                    year_match = re.search(r'20\d{2}', title + ' ' + link)
                    if year_match:
                        pub_date = datetime(int(year_match.group()), 12, 31)  # End of year
                    else:
                        pub_date = datetime.now()
                    
                    # Check date range
                    if not self.is_within_date_range(pub_date):
                        continue
                    
                    # Get additional context from parent elements
                    parent_text = ""
                    parent = item.parent
                    if parent:
                        parent_text = parent.get_text(strip=True)[:200]
                    
                    # Check if this is a PDF link
                    if link.lower().endswith('.pdf'):
                        # Download and extract PDF content
                        content = self.download_and_extract_pdf(link)
                        content_info = {'is_truncated': False, 'original_length': len(content), 'extraction_method': 'pdf'}
                        if not content:
                            # Fallback to getting content from the page
                            content, content_info = self.get_full_article_content(link)
                    else:
                        # Get full content from the article page
                        content, content_info = self.get_full_article_content(link)
                        if not content:
                            content = f"Annual report: {title}. {parent_text}"
                            content_info = {'is_truncated': False, 'original_length': len(content), 'extraction_method': 'fallback'}
                        
                        # Skip if we only got navigation content
                        if self._is_navigation_text(content):
                            continue
                        
                        article = {
                            'id': f"sfa_annual_{hash(link)}",
                            'title': title,
                            'content': content,
                            'summary': content[:200] + "..." if len(content) > 200 else content,
                            'published_date': pub_date,
                            'source_type': 'annual_reports',
                            'url': link,
                            'category': 'Annual Report',
                            'tags': ['annual report', 'statistics', 'policy'],
                            'content_length': len(content),
                            'word_count': len(content.split()),
                            'is_truncated': content_info.get('is_truncated', False),
                            'original_length': content_info.get('original_length', len(content)),
                            'extraction_method': content_info.get('extraction_method', 'unknown')
                        }
                    
                    articles.append(article)
                    
                except Exception as e:
                    logger.warning(f"Error processing annual report item: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping annual reports: {e}")
        
        return articles
    
    def scrape_sgfs(self) -> List[Dict]:
        """Scrape Singapore Food Statistics"""
        articles = []
        source_info = self.sources['sgfs']
        
        try:
            logger.info(f"Scraping {source_info['name']}: {source_info['url']}")
            
            response = self.session.get(source_info['url'], timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for SGFS publications
            stat_items = soup.find_all(['a', 'div'], class_=re.compile(r'statistic|publication|sgfs', re.I))
            
            for item in stat_items[:15]:  # Limit to recent statistics
                try:
                    # Extract title and link
                    if item.name == 'a':
                        title = item.get_text(strip=True)
                        link = item.get('href', '')
                    else:
                        link_elem = item.find('a')
                        if not link_elem:
                            continue
                        title = link_elem.get_text(strip=True) or item.get_text(strip=True)
                        link = link_elem.get('href', '')
                    
                    if not title or not link:
                        continue
                    
                    # Make absolute URL
                    if link.startswith('/'):
                        link = urljoin(source_info['base_url'], link)
                    
                    # Extract year from title for dating
                    year_match = re.search(r'20\d{2}', title)
                    if year_match:
                        pub_date = datetime(int(year_match.group()), 6, 30)  # Mid-year
                    else:
                        pub_date = datetime.now()
                    
                    # Check date range
                    if not self.is_within_date_range(pub_date):
                        continue
                    
                    # Check if this is a PDF link
                    if link.lower().endswith('.pdf'):
                        # Extract content from PDF
                        content = self.download_and_extract_pdf(link)
                        content_info = {'is_truncated': False, 'original_length': len(content), 'extraction_method': 'pdf'}
                        if not content:
                            content = f"Singapore Food Statistics: {title}"
                            content_info = {'is_truncated': False, 'original_length': len(content), 'extraction_method': 'fallback'}
                    else:
                        # Try to get full content from the page
                        content, content_info = self.get_full_article_content(link)
                        if not content:
                            content = f"Singapore Food Statistics: {title}"
                            content_info = {'is_truncated': False, 'original_length': len(content), 'extraction_method': 'fallback'}
                    
                    # Food statistics are generally property-relevant for planning
                    article = {
                        'id': f"sfa_sgfs_{hash(link)}",
                        'title': title,
                        'content': content,
                        'summary': content[:200] + "..." if len(content) > 200 else content,
                        'published_date': pub_date,
                        'source_type': 'sgfs',
                        'url': link,
                        'category': 'Food Statistics',
                        'tags': ['statistics', 'food security', 'planning'],
                        'content_length': len(content),
                        'word_count': len(content.split()),
                        'is_truncated': content_info.get('is_truncated', False),
                        'original_length': content_info.get('original_length', len(content)),
                        'extraction_method': content_info.get('extraction_method', 'unknown')
                    }
                    
                    articles.append(article)
                    
                except Exception as e:
                    logger.warning(f"Error processing SGFS item: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping SGFS: {e}")
        
        return articles
    
    def scrape_publications(self) -> List[Dict]:
        """Scrape SFA publications and guidelines"""
        articles = []
        
        # Publications are typically found in the newsroom or main pages
        # Since there's no dedicated publications source, we'll skip this method
        logger.info("Skipping publications scraping - no dedicated publications URL")
        return articles
    
    def scrape_press_releases(self) -> List[Dict]:
        """Scrape SFA press releases"""
        articles = []
        source_info = self.sources['press_releases']
        
        try:
            logger.info(f"Scraping {source_info['name']}: {source_info['url']}")
            
            response = self.session.get(source_info['url'], timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for press release links in various containers
            press_items = []
            
            # Try different selectors for press releases
            selectors = [
                'a[href*="press"]',
                'a[href*="news"]',
                'a[href*="release"]',
                '.press-release a',
                '.news-item a',
                '.publication-item a',
                'article a',
                '.content a'
            ]
            
            for selector in selectors:
                items = soup.select(selector)
                press_items.extend(items)
            
            # Also look in content areas
            content_areas = soup.find_all(['div', 'section', 'article'], 
                                        class_=re.compile(r'content|main|news|press|publication', re.I))
            
            for area in content_areas:
                area_links = area.find_all('a', href=True)
                press_items.extend(area_links)
            
            # Remove duplicates and filter
            seen_urls = set()
            unique_items = []
            
            for item in press_items:
                href = item.get('href', '')
                if href and href not in seen_urls:
                    seen_urls.add(href)
                    unique_items.append(item)
            
            for item in unique_items[:20]:  # Limit to recent releases
                try:
                    title = item.get_text(strip=True)
                    link = item.get('href', '')
                    
                    if not title or not link or len(title) < 5:
                        continue
                    
                    # Skip obvious navigation links
                    if any(nav_word in title.lower() for nav_word in 
                          ['home', 'back', 'menu', 'search', 'login', 'contact', 'about']):
                        continue
                    
                    # Make absolute URL
                    if link.startswith('/'):
                        link = urljoin(source_info['base_url'], link)
                    
                    # Try to extract date from various sources
                    pub_date = self.extract_date_from_content(item, title, link)
                    
                    # Check date range
                    if not self.is_within_date_range(pub_date):
                        continue
                    
                    # Get content from the actual press release page
                    content, content_info = self.get_full_article_content(link)
                    if not content:
                        content = f"Press release: {title}"
                        content_info = {'is_truncated': False, 'original_length': len(content), 'extraction_method': 'fallback'}
                    
                    # Skip if we only got navigation content
                    if self._is_navigation_text(content):
                        continue
                    
                    article = {
                        'id': f"sfa_press_{hash(link)}",
                        'title': title,
                        'content': content,
                        'summary': content[:200] + "..." if len(content) > 200 else content,
                        'published_date': pub_date,
                        'source_type': 'press_releases',
                        'url': link,
                        'category': 'Press Release',
                        'tags': ['press release', 'news', 'announcement'],
                        'content_length': len(content),
                        'word_count': len(content.split()),
                        'is_truncated': content_info.get('is_truncated', False),
                        'original_length': content_info.get('original_length', len(content)),
                        'extraction_method': content_info.get('extraction_method', 'unknown')
                    }
                    
                    articles.append(article)
                    
                except Exception as e:
                    logger.warning(f"Error processing press release item: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping press releases: {e}")
        
        return articles
    
    def scrape_newsroom(self) -> List[Dict]:
        """Scrape SFA newsroom content"""
        articles = []
        source_info = self.sources['newsroom']
        
        try:
            logger.info(f"Scraping {source_info['name']}: {source_info['url']}")
            
            response = self.session.get(source_info['url'], timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for news items
            news_items = soup.find_all(['article', 'div'], class_=re.compile(r'news|press|release|item', re.I))
            
            for item in news_items[:20]:  # Limit to recent news
                try:
                    # Extract title
                    title_elem = item.find(['h1', 'h2', 'h3', 'h4'], class_=re.compile(r'title|heading', re.I))
                    if not title_elem:
                        title_elem = item.find(['h1', 'h2', 'h3', 'h4'])
                    
                    if not title_elem:
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    
                    # Extract link
                    link_elem = item.find('a')
                    if not link_elem:
                        continue
                    
                    link = link_elem.get('href', '')
                    if link.startswith('/'):
                        link = urljoin(source_info['base_url'], link)
                    
                    # Extract date
                    date_elem = item.find(class_=re.compile(r'date|time|published', re.I))
                    if date_elem:
                        date_text = date_elem.get_text(strip=True)
                        pub_date = self.extract_date_from_text(date_text)
                    else:
                        pub_date = datetime.now()
                    
                    if not pub_date:
                        pub_date = datetime.now()
                    
                    # Check date range
                    if not self.is_within_date_range(pub_date):
                        continue
                    
                    # Extract summary/content
                    summary_elem = item.find(['p', 'div'], class_=re.compile(r'summary|excerpt|content', re.I))
                    summary = summary_elem.get_text(strip=True) if summary_elem else ""
                    
                    # Get full content from the article page
                    content, content_info = self.get_full_article_content(link)
                    if not content:
                        content = summary if summary else f"Newsroom article: {title}"
                        content_info = {'is_truncated': False, 'original_length': len(content), 'extraction_method': 'fallback'}
                    
                    # Skip if we only got navigation content
                    if self._is_navigation_text(content):
                        continue
                    
                    # Check if property-related
                    full_text = f"{title} {content}"
                    if not self.is_property_related(full_text):
                        continue
                    
                    article = {
                        'id': f"sfa_news_{hash(link)}",
                        'title': title,
                        'content': content,
                        'summary': content[:200] + "..." if len(content) > 200 else content,
                        'published_date': pub_date,
                        'source_type': 'newsroom',
                        'url': link,
                        'category': 'News',
                        'tags': ['news', 'announcement', 'update'],
                        'content_length': len(content),
                        'word_count': len(content.split()),
                        'is_truncated': content_info.get('is_truncated', False),
                        'original_length': content_info.get('original_length', len(content)),
                        'extraction_method': content_info.get('extraction_method', 'unknown')
                    }
                    
                    articles.append(article)
                    
                except Exception as e:
                    logger.warning(f"Error processing news item: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping newsroom: {e}")
        
        return articles
    
    def scrape_circulars_and_notices(self, source_type: str) -> List[Dict]:
        """
        Scrape circulars, food alerts, or hygiene notices
        
        Args:
            source_type: 'circulars', 'food_alerts', or 'hygiene_notices'
        """
        articles = []
        source_info = self.sources[source_type]
        
        try:
            logger.info(f"Scraping {source_info['name']}: {source_info['url']}")
            
            response = self.session.get(source_info['url'], timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for circular/notice items
            items = soup.find_all(['div', 'li', 'tr'], class_=re.compile(r'item|notice|circular|alert|suspension', re.I))
            
            for item in items[:25]:  # Limit to recent items
                try:
                    # Extract title
                    title_elem = item.find(['a', 'h3', 'h4', 'td'])
                    if not title_elem:
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    
                    # Extract link if available
                    link_elem = item.find('a')
                    link = ""
                    if link_elem:
                        link = link_elem.get('href', '')
                        if link.startswith('/'):
                            link = urljoin(source_info['base_url'], link)
                    
                    # Extract date
                    date_text = item.get_text()
                    pub_date = self.extract_date_from_text(date_text)
                    if not pub_date:
                        pub_date = datetime.now()
                    
                    # Check date range
                    if not self.is_within_date_range(pub_date):
                        continue
                    
                    # For hygiene notices, most are property-related (establishment suspensions)
                    # For circulars and alerts, check keywords
                    if source_type == 'hygiene_notices' or self.is_property_related(title):
                        
                        # Determine category based on source type
                        category_map = {
                            'circulars': 'Circular',
                            'food_alerts': 'Food Alert',
                            'hygiene_notices': 'Hygiene Notice'
                        }
                        
                        article = {
                            'id': f"sfa_{source_type}_{hash(title + str(pub_date))}",
                            'title': title,
                            'content': title,  # For notices, title often contains full info
                            'summary': title[:200] + "..." if len(title) > 200 else title,
                            'published_date': pub_date,
                            'source_type': source_type,
                            'url': link,
                            'category': category_map.get(source_type, 'Notice'),
                            'tags': [source_type.replace('_', ' '), 'regulatory', 'notice'],
                            'content_length': len(title),
                            'word_count': len(title.split()),
                            'is_truncated': False,
                            'original_length': len(title),
                            'extraction_method': 'title_only'
                        }
                        
                        articles.append(article)
                    
                except Exception as e:
                    logger.warning(f"Error processing {source_type} item: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping {source_type}: {e}")
        
        return articles
    
    def extract_date_from_content(self, item, title: str, link: str) -> datetime:
        """Extract date from item context, title, or link"""
        # Try to find date in parent elements
        parent = item.parent
        if parent:
            date_elem = parent.find(['time', 'span', 'div'], class_=re.compile(r'date|time|published', re.I))
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                parsed_date = self.extract_date_from_text(date_text)
                if parsed_date:
                    return parsed_date
        
        # Try to extract from title or link
        combined_text = f"{title} {link}"
        parsed_date = self.extract_date_from_text(combined_text)
        if parsed_date:
            return parsed_date
            
        # Default to current date
        return datetime.now()
    
    def get_full_article_content(self, url: str) -> tuple[str, dict]:
        """Extract full article content from URL with completeness indicators"""
        content_info = {
            'is_truncated': False,
            'original_length': 0,
            'extraction_method': 'none'
        }
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove navigation and header elements first
            for elem in soup.find_all(['nav', 'header', 'footer']):
                elem.decompose()
            
            # Remove common navigation classes
            for elem in soup.find_all(class_=re.compile(r'nav|menu|breadcrumb|sidebar|footer|header', re.I)):
                elem.decompose()
            
            # Look for main content areas with SFA-specific selectors
            content_selectors = [
                '.main-content',
                '.content-area',
                '.page-content',
                '.article-content',
                '.press-release-content',
                '.news-content',
                'main',
                'article',
                '.content',
                '#content',
                '.body-content',
                '.text-content'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Get text and filter out navigation-like content
                    text = content_elem.get_text(separator=' ', strip=True)
                    
                    # Skip if it's mostly navigation text
                    if self._is_navigation_text(text):
                        continue
                    
                    # Return meaningful content with smart truncation
                    if len(text) > 100:  # Ensure we have substantial content
                        content_info['original_length'] = len(text)
                        content_info['extraction_method'] = f'selector_{selector}'
                        return self._smart_truncate(text, 8000, content_info)
            
            # Try to find paragraphs with substantial content
            paragraphs = soup.find_all('p')
            content_parts = []
            
            for p in paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 50 and not self._is_navigation_text(text):
                    content_parts.append(text)
            
            if content_parts:
                full_text = ' '.join(content_parts)
                content_info['original_length'] = len(full_text)
                content_info['extraction_method'] = 'paragraphs'
                return self._smart_truncate(full_text, 8000, content_info)
            
            # Last resort: get body content but filter navigation
            body = soup.find('body')
            if body:
                text = body.get_text(separator=' ', strip=True)
                if not self._is_navigation_text(text):
                    content_info['original_length'] = len(text)
                    content_info['extraction_method'] = 'body'
                    return self._smart_truncate(text, 5000, content_info)  # Lower limit for body extraction
                
        except Exception as e:
            logger.debug(f"Could not fetch full content from {url}: {e}")
            
        return "", content_info
    
    def _smart_truncate(self, text: str, max_length: int, content_info: dict) -> tuple[str, dict]:
        """Truncate text at sentence boundaries when possible"""
        if len(text) <= max_length:
            return text, content_info
        
        content_info['is_truncated'] = True
        
        # Try to truncate at sentence boundary
        truncated = text[:max_length]
        
        # Find the last sentence ending within the limit
        sentence_endings = ['.', '!', '?', '。', '！', '？']  # Include Chinese punctuation
        last_sentence_end = -1
        
        for i in range(len(truncated) - 1, max(0, len(truncated) - 200), -1):
            if truncated[i] in sentence_endings and i < len(truncated) - 1:
                # Make sure it's not an abbreviation (basic check)
                if truncated[i] == '.' and i > 0 and truncated[i-1].isupper() and i < len(truncated) - 1 and truncated[i+1] == ' ':
                    continue
                last_sentence_end = i + 1
                break
        
        if last_sentence_end > max_length * 0.8:  # Only use sentence boundary if we keep at least 80% of content
            return truncated[:last_sentence_end].strip(), content_info
        else:
            # Fall back to word boundary
            words = truncated.split()
            if len(words) > 1:
                return ' '.join(words[:-1]), content_info
            else:
                return truncated, content_info
    
    def download_and_extract_pdf(self, pdf_url: str) -> str:
        """Download PDF and extract text content"""
        try:
            logger.info(f"Downloading PDF: {pdf_url}")
            response = self.session.get(pdf_url, timeout=60)
            response.raise_for_status()
            
            # Create PDF reader from response content
            pdf_file = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            # Extract text from all pages
            text_content = ""
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                text_content += page.extract_text() + "\n"
            
            # Clean up the text
            text_content = re.sub(r'\s+', ' ', text_content).strip()
            
            if len(text_content) > 100:  # Ensure we got meaningful content
                logger.info(f"Successfully extracted {len(text_content)} characters from PDF")
                return text_content
            else:
                logger.warning(f"PDF content too short: {len(text_content)} characters")
                return ""
                
        except Exception as e:
            logger.error(f"Error downloading/extracting PDF {pdf_url}: {str(e)}")
            return ""
    
    def scrape_rss_feeds(self) -> List[Dict]:
        """Scrape content from SFA RSS feeds"""
        articles = []
        
        rss_feeds = {
            'circulars': 'https://www.sfa.gov.sg/rss/annual-listing-circulars',
            'food_alerts': 'https://www.sfa.gov.sg/rss/annual-listing-food-alerts',
            'hygiene_notices': 'https://www.sfa.gov.sg/rss/food-hygiene-notices',
            'newsroom': 'https://www.sfa.gov.sg/rss/newsroom'
        }
        
        for feed_type, feed_url in rss_feeds.items():
            try:
                logger.info(f"Scraping RSS feed: {feed_type} - {feed_url}")
                
                # Parse RSS feed
                feed = feedparser.parse(feed_url)
                
                if not feed.entries:
                    logger.warning(f"No entries found in RSS feed: {feed_type}")
                    continue
                
                for entry in feed.entries[:20]:  # Limit to recent 20 entries
                    try:
                        title = entry.title if hasattr(entry, 'title') else 'No Title'
                        link = entry.link if hasattr(entry, 'link') else ''
                        
                        # Extract published date
                        pub_date = datetime.now()
                        if hasattr(entry, 'published_parsed') and entry.published_parsed:
                            pub_date = datetime(*entry.published_parsed[:6])
                        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                            pub_date = datetime(*entry.updated_parsed[:6])
                        
                        # Skip if outside date range
                        if not self.is_within_date_range(pub_date):
                            continue
                        
                        # Get content from RSS entry
                        content = ""
                        if hasattr(entry, 'summary'):
                            content = entry.summary
                        elif hasattr(entry, 'description'):
                            content = entry.description
                        
                        # Try to get full content from the link
                        if link:
                            full_content = self.get_full_article_content(link)
                            if full_content and len(full_content) > len(content):
                                content = full_content
                        
                        # Skip if no meaningful content
                        if not content or len(content.strip()) < 50:
                            continue
                        
                        # Clean content
                        content = BeautifulSoup(content, 'html.parser').get_text()
                        content = re.sub(r'\s+', ' ', content).strip()
                        
                        # Skip navigation text
                        if self._is_navigation_text(content):
                            continue
                        
                        # Check property relevance
                        full_text = f"{title} {content}"
                        if not self.is_property_related(full_text):
                            continue
                        
                        article = {
                            'id': f"sfa_rss_{feed_type}_{hash(link)}",
                            'title': title,
                            'content': content,
                            'summary': content[:200] + "..." if len(content) > 200 else content,
                            'published_date': pub_date.isoformat(),
                            'source_type': f'rss_{feed_type}',
                            'url': link,
                            'category': 'Food Safety',
                            'tags': ['SFA', 'RSS', feed_type.replace('_', ' ').title()]
                        }
                        
                        articles.append(article)
                        logger.info(f"Added RSS article: {title[:50]}...")
                        
                    except Exception as e:
                        logger.error(f"Error processing RSS entry from {feed_type}: {str(e)}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error scraping RSS feed {feed_type}: {str(e)}")
                continue
        
        logger.info(f"Collected {len(articles)} articles from RSS feeds")
        return articles

    def _is_navigation_text(self, text: str) -> bool:
        """Check if text is primarily navigation/menu content"""
        nav_indicators = [
            'About UsAbout Us',
            'Who We AreWhat We Do',
            'For IndustryFor Industry',
            'Food Import & ExportFood Import & Export',
            'Commercial ImportsCommercial Imports',
            'Commercial ExportsCommercial Exports',
            'Licence, Permit & Registration'
        ]
        
        # If text contains multiple navigation indicators, it's likely nav text
        nav_count = sum(1 for indicator in nav_indicators if indicator in text)
        
        # Also check for repetitive patterns common in navigation
        words = text.split()
        if len(words) > 10:
            # Check for high repetition of words (common in nav menus)
            word_freq = {}
            for word in words:
                word_freq[word] = word_freq.get(word, 0) + 1
            
            # If more than 30% of words are repeated, likely navigation
            repeated_words = sum(1 for count in word_freq.values() if count > 2)
            if repeated_words / len(word_freq) > 0.3:
                return True
        
        return nav_count >= 3
    
    def scrape_all_sources(self) -> List[SFAArticle]:
        """
        Scrape all SFA sources and return enhanced articles
        
        Returns:
            List of SFAArticle objects
        """
        all_articles = []
        
        # Scrape each source type
        scrapers = {
             'annual_reports': self.scrape_annual_reports,
             'publications': self.scrape_publications,
             'press_releases': self.scrape_press_releases,
             'sgfs': self.scrape_sgfs,
             'newsroom': self.scrape_newsroom,
             'circulars': lambda: self.scrape_circulars_and_notices('circulars'),
             'food_alerts': lambda: self.scrape_circulars_and_notices('food_alerts'),
             'hygiene_notices': lambda: self.scrape_circulars_and_notices('hygiene_notices'),
             'rss_feeds': self.scrape_rss_feeds
         }
        
        for source_name, scraper_func in scrapers.items():
            try:
                logger.info(f"Starting scrape for {source_name}")
                articles = scraper_func()
                
                # Convert to SFAArticle objects
                for article_dict in articles:
                    sfa_article = SFAArticle(
                        id=article_dict['id'],
                        title=article_dict['title'],
                        content=article_dict['content'],
                        summary=article_dict['summary'],
                        published_date=article_dict['published_date'],
                        source_type=article_dict['source_type'],
                        url=article_dict['url'],
                        category=article_dict['category'],
                        tags=article_dict['tags']
                    )
                    all_articles.append(sfa_article)
                
                logger.info(f"Collected {len(articles)} articles from {source_name}")
                
                # Rate limiting between sources
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error scraping {source_name}: {e}")
                continue
        
        logger.info(f"Total SFA articles collected: {len(all_articles)}")
        return all_articles
    
    def save_articles_to_json(self, articles: List[SFAArticle], filename: str = "sfa_articles.json"):
        """
        Save articles to JSON file with proper formatting
        
        Args:
            articles: List of SFAArticle objects
            filename: Output filename
        """
        # Convert dataclass objects to dictionaries
        articles_dict = []
        for article in articles:
            article_dict = {
                'id': article.id,
                'title': article.title,
                'content': article.content,
                'summary': article.summary,
                'published_date': article.published_date.isoformat() if isinstance(article.published_date, datetime) else article.published_date,
                'source_type': article.source_type,
                'url': article.url,
                'category': article.category,
                'tags': article.tags,
                'source': 'sfa',
                'scraped_at': datetime.now().isoformat(),
                'content_length': article.content_length,
                'word_count': article.word_count,
                'is_truncated': article.is_truncated,
                'original_length': article.original_length,
                'extraction_method': article.extraction_method
            }
            articles_dict.append(article_dict)
        
        # Save to JSON file
        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(articles_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(articles)} SFA articles to {output_path}")
        
        # Also save summary statistics
        stats = {
            'total_articles': len(articles),
            'source_breakdown': {},
            'category_breakdown': {},
            'date_range': {
                'earliest': min([a.published_date if isinstance(a.published_date, datetime) else datetime.fromisoformat(a.published_date.replace('Z', '+00:00')) for a in articles]).isoformat() if articles else None,
                'latest': max([a.published_date if isinstance(a.published_date, datetime) else datetime.fromisoformat(a.published_date.replace('Z', '+00:00')) for a in articles]).isoformat() if articles else None
            },
            'scraped_at': datetime.now().isoformat()
        }
        
        # Calculate breakdowns
        for article in articles:
            # Source type breakdown
            if article.source_type not in stats['source_breakdown']:
                stats['source_breakdown'][article.source_type] = 0
            stats['source_breakdown'][article.source_type] += 1
            
            # Category breakdown
            if article.category not in stats['category_breakdown']:
                stats['category_breakdown'][article.category] = 0
            stats['category_breakdown'][article.category] += 1
        
        stats_path = self.output_dir / f"sfa_scraping_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved scraping statistics to {stats_path}")

def main():
    """Main function to run SFA scraper"""
    try:
        # Initialize scraper
        scraper = SFAScraper()
        
        logger.info("Starting SFA scraping process...")
        logger.info("Target sources:")
        for source_name, source_info in scraper.sources.items():
            logger.info(f"  - {source_info['name']}: {source_info['url']}")
        
        # Scrape all sources
        articles = scraper.scrape_all_sources()
        
        if articles:
            # Save articles
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sfa_articles_{timestamp}.json"
            scraper.save_articles_to_json(articles, filename)
            
            # Print summary
            print(f"\n=== SFA Scraping Complete ===")
            print(f"Total articles collected: {len(articles)}")
            
            # Source breakdown
            source_counts = {}
            for article in articles:
                if article.source_type not in source_counts:
                    source_counts[article.source_type] = 0
                source_counts[article.source_type] += 1
            
            print("\nArticles by source:")
            for source, count in source_counts.items():
                print(f"  {source}: {count}")
            
            # Date range
            if articles:
                dates = [a.published_date if isinstance(a.published_date, datetime) else datetime.fromisoformat(a.published_date.replace('Z', '+00:00')) for a in articles]
                print(f"\nDate range: {min(dates).strftime('%Y-%m-%d')} to {max(dates).strftime('%Y-%m-%d')}")
            
        else:
            logger.warning("No articles were collected from SFA sources")
            
    except Exception as e:
        logger.error(f"Error in main SFA scraping process: {e}")
        raise

if __name__ == "__main__":
    main()