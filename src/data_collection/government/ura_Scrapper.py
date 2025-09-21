"""
URA (Urban Redevelopment Authority) Web Scraper for PropInsight

This scraper focuses on collecting property-related policy announcements, 
land sales information, market statistics, and planning updates from the URA website.

Priority Sources :
1. HIGH PRIORITY (60%): Media Room > Media Releases - GLS programmes, tender launches, market statistics
2. HIGH PRIORITY (25%): Property > Property Data - Price indices, supply data, market statistics  
3. MEDIUM PRIORITY (10%): Land Sales - Current sites, tender procedures, site details
4. LOW PRIORITY (5%): Planning - Master plan updates, long-term planning

Author: Prem Varijakzhan
Date: 2025-01-20
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
class URAArticle:
    """Data structure for URA articles following PropInsight specification"""
    id: str
    source: str  # Always "government_ura"
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

class URAScraper:
    """
    URA Website Scraper for PropInsight
    
    Scrapes property-related content from URA website with focus on:
    - Media releases (highest priority - 60%)
    - Property data and market statistics (high priority - 25%)
    - Land sales information (medium priority - 10%)
    - Planning updates (low priority - 5%)
    """
    
    def __init__(self, output_dir: str = "data/raw/government/ura"):
        self.base_url = "https://www.ura.gov.sg"
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
                # Media Room - 60% priority
                'media_releases': '/Corporate/Media-Room/Media-Releases',
                'media_releases_2025': '/Corporate/Media-Room/Media-Releases?filter=2025',
                'media_releases_2024': '/Corporate/Media-Room/Media-Releases?filter=2024',
                'media_releases_2023': '/Corporate/Media-Room/Media-Releases?filter=2023',
                # Property Data - 25% priority
                'property_data': '/Corporate/Property/Property-Data',
                'private_residential_data': '/Corporate/Property/Property-Data/Private-Residential-Properties',
                'commercial_data': '/Corporate/Property/Property-Data/Commercial-Properties',
                'quarterly_statistics': '/Corporate/Property/Property-Data'
            },
            'medium': {
                # Land Sales - 10% priority
                'current_gls_sites': '/Corporate/Land-Sales/Current-URA-GLS-Sites',
                'sites_for_tender': '/Corporate/Land-Sales/Sites-For-Tender',
                'land_sale_procedures': '/Corporate/Land-Sales/Land-Sale-Procedure',
                'past_sale_sites': '/Corporate/Land-Sales/Past-Sale-Sites'
            },
            'low': {
                # Planning - 5% priority
                'master_plan': '/Corporate/Planning/Master-Plan',
                'long_term_plan': '/Corporate/Planning/Long-Term-Plan-Review',
                'planning_process': '/Corporate/Planning/Our-Planning-Process'
            }
        }
        
        # URA-specific keywords for property relevance
        self.property_keywords = [
            # Land Sales & GLS
            'gls', 'government land sales', 'land sales programme', 'confirmed list', 'reserve list',
            'tender launch', 'tender award', 'site tender', 'land tender', 'development site',
            
            # Market Statistics
            'property price index', 'quarterly statistics', 'flash estimates', 'market data',
            'private residential', 'non-landed', 'landed properties', 'price change',
            'transaction volume', 'market performance', 'real estate statistics',
            
            # Property Types & Development
            'residential units', 'commercial gfa', 'hotel rooms', 'ec units', 'executive condominium',
            'serviced apartments', 'private housing', 'public housing', 'bto', 'build-to-order',
            
            # Locations & Planning
            'bedok', 'bukit timah', 'woodlands', 'dover', 'tanjong rhu', 'cross street',
            'master plan', 'urban planning', 'development control', 'zoning',
            
            # Policy & Regulation
            'cooling measures', 'property tax', 'absd', 'additional buyer stamp duty',
            'foreign ownership', 'property investment', 'rental market', 'short-term accommodation'
        ]

    def is_property_related(self, text: str) -> bool:
        """Check if content is property-related using URA-specific keywords"""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.property_keywords)

    def is_within_date_range(self, published_date: datetime) -> bool:
        """Check if the published date is within our target range (2023-2025)"""
        return self.start_date <= published_date <= self.end_date

    def extract_date_from_text(self, text: str) -> Optional[datetime]:
        """Extract date from various text formats found on URA website"""
        date_patterns = [
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(\d{4})-(\d{2})-(\d{2})',
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
            r'(\d{1,2})-(\d{1,2})-(\d{4})'
        ]
        
        month_map = {
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
            'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
            'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    if len(match.groups()) == 3:
                        if match.group(2).lower() in month_map:
                            # Format: DD Mon YYYY
                            day, month_str, year = match.groups()
                            month = month_map[month_str.lower()]
                            return datetime(int(year), month, int(day))
                        else:
                            # Format: YYYY-MM-DD or DD/MM/YYYY or DD-MM-YYYY
                            if len(match.group(1)) == 4:  # YYYY-MM-DD
                                year, month, day = match.groups()
                            else:  # DD/MM/YYYY or DD-MM-YYYY
                                day, month, year = match.groups()
                            return datetime(int(year), int(month), int(day))
                except (ValueError, KeyError):
                    continue
        return None

    def get_full_article_content(self, url: str) -> Tuple[str, Dict]:
        """Extract full article content and metadata from URA article page"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Initialize content info
            content_info = {
                'extraction_success': True,
                'content_sections': [],
                'has_tables': False,
                'has_images': False,
                'word_count': 0
            }
            
            # Extract main content - URA uses various content containers
            content_selectors = [
                '.content-body',
                '.press-release-content',
                '.article-content',
                '.main-content',
                '[class*="content"]',
                '.container .row .col'
            ]
            
            full_text = ""
            title = ""
            
            # Extract title
            title_selectors = ['h1', '.page-title', '.article-title', '.press-release-title']
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
            
            # Extract main content
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Remove navigation, sidebar, and footer elements
                    for unwanted in content_elem.select('nav, .sidebar, .footer, .breadcrumb, .social-share'):
                        unwanted.decompose()
                    
                    # Extract text content
                    paragraphs = content_elem.find_all(['p', 'div', 'li', 'td', 'th'])
                    for para in paragraphs:
                        text = para.get_text(strip=True)
                        if text and len(text) > 20:  # Filter out very short text
                            full_text += text + "\n\n"
                    
                    # Check for tables and images
                    content_info['has_tables'] = len(content_elem.find_all('table')) > 0
                    content_info['has_images'] = len(content_elem.find_all('img')) > 0
                    break
            
            # If no content found, try alternative extraction
            if not full_text:
                # Try extracting all paragraphs from the page
                all_paragraphs = soup.find_all('p')
                for para in all_paragraphs:
                    text = para.get_text(strip=True)
                    if text and len(text) > 30:
                        full_text += text + "\n\n"
            
            # Combine title and content
            if title:
                full_text = f"{title}\n\n{full_text}"
            
            # Update content info
            content_info['word_count'] = len(full_text.split())
            content_info['content_sections'] = ['title', 'main_content'] if title else ['main_content']
            
            return full_text.strip(), content_info
            
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {str(e)}")
            return "", {'extraction_success': False, 'error': str(e)}

    def _smart_truncate(self, text: str, max_length: int, content_info: dict) -> Tuple[str, dict]:
        """Intelligently truncate text while preserving important information"""
        if len(text) <= max_length:
            return text, content_info
        
        # Try to truncate at sentence boundaries
        sentences = text.split('. ')
        truncated = ""
        
        for sentence in sentences:
            if len(truncated + sentence + '. ') <= max_length:
                truncated += sentence + '. '
            else:
                break
        
        if not truncated:  # If no complete sentences fit, do hard truncation
            truncated = text[:max_length-3] + "..."
        
        content_info['is_truncated'] = True
        content_info['original_length'] = len(text)
        content_info['truncated_length'] = len(truncated)
        
        return truncated.strip(), content_info

    def scrape_media_releases(self) -> List[Dict]:
        """Scrape URA media releases (highest priority - 60%)"""
        articles = []
        
        # Scrape media releases for each year
        for year in ['2023', '2024', '2025']:
            try:
                url = f"{self.base_url}/Corporate/Media-Room/Media-Releases?filter={year}"
                logger.info(f"Scraping URA media releases for {year}: {url}")
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find media release links - URA uses various selectors
                release_selectors = [
                    '.media-release-item a',
                    '.press-release-item a',
                    '.news-item a',
                    'a[href*="Media-Releases"]',
                    '.list-item a'
                ]
                
                release_links = []
                for selector in release_selectors:
                    links = soup.select(selector)
                    if links:
                        release_links.extend(links)
                        break
                
                logger.info(f"Found {len(release_links)} media release links for {year}")
                
                for link in release_links[:20]:  # Limit to prevent overwhelming
                    try:
                        href = link.get('href')
                        if not href:
                            continue
                            
                        article_url = urljoin(self.base_url, href)
                        title = link.get_text(strip=True)
                        
                        # Skip if not property-related
                        if not self.is_property_related(title):
                            continue
                        
                        # Extract full content
                        full_content, content_info = self.get_full_article_content(article_url)
                        
                        if not full_content:
                            continue
                        
                        # Extract date from content or URL
                        published_date = self.extract_date_from_text(full_content)
                        if not published_date:
                            # Try to extract from URL or use current date
                            published_date = datetime.now()
                        
                        # Check date range
                        if not self.is_within_date_range(published_date):
                            continue
                        
                        # Truncate if necessary
                        if len(full_content) > 8000:
                            full_content, content_info = self._smart_truncate(full_content, 8000, content_info)
                        
                        # Generate unique ID
                        article_id = f"ura_mr_{published_date.strftime('%Y%m%d')}_{hashlib.md5(article_url.encode()).hexdigest()[:8]}"
                        
                        # Classify policy type and extract URA-specific metadata
                        policy_type = self._classify_policy_type(full_content)
                        ura_metadata = self._extract_ura_metadata(full_content, title)
                        
                        article_data = {
                            'id': article_id,
                            'source': 'government_ura',
                            'text': full_content,
                            'timestamp': published_date.isoformat(),
                            'url': article_url,
                            'language': 'en',
                            'metadata': {
                                'title': title,
                                'agency': 'URA',
                                'press_release_id': f"URA_{published_date.year}_{len(articles)+1:03d}",
                                'category': self._categorize_content(full_content),
                                'policy_type': policy_type,
                                'policy_subtype': self._get_policy_subtype(full_content, policy_type),
                                'content_length': len(full_content),
                                'keywords': self._extract_keywords(full_content),
                                'locations': self._extract_locations(full_content),
                                'sentiment_impact': self._assess_sentiment_impact(full_content),
                                **ura_metadata,
                                **content_info
                            }
                        }
                        
                        articles.append(article_data)
                        logger.info(f"Scraped media release: {title[:50]}...")
                        
                        # Rate limiting
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error scraping media release {link}: {str(e)}")
                        continue
                
            except Exception as e:
                logger.error(f"Error scraping media releases for {year}: {str(e)}")
                continue
        
        logger.info(f"Scraped {len(articles)} media releases")
        return articles

    def scrape_property_data(self) -> List[Dict]:
        """Scrape URA property data and market statistics (high priority - 25%)"""
        articles = []
        
        property_data_urls = [
            '/Corporate/Property/Property-Data',
            '/Corporate/Property/Property-Data/Private-Residential-Properties',
            '/Corporate/Property/Property-Data/Commercial-Properties'
        ]
        
        for data_url in property_data_urls:
            try:
                url = f"{self.base_url}{data_url}"
                logger.info(f"Scraping URA property data: {url}")
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract data reports and statistics
                data_links = soup.select('a[href*="statistics"], a[href*="data"], a[href*="report"]')
                
                for link in data_links[:10]:  # Limit to prevent overwhelming
                    try:
                        href = link.get('href')
                        if not href:
                            continue
                            
                        article_url = urljoin(self.base_url, href)
                        title = link.get_text(strip=True)
                        
                        # Skip if not property-related
                        if not self.is_property_related(title):
                            continue
                        
                        # Extract full content
                        full_content, content_info = self.get_full_article_content(article_url)
                        
                        if not full_content or len(full_content) < 100:
                            continue
                        
                        # Extract date
                        published_date = self.extract_date_from_text(full_content)
                        if not published_date:
                            published_date = datetime.now()
                        
                        # Check date range
                        if not self.is_within_date_range(published_date):
                            continue
                        
                        # Generate unique ID
                        article_id = f"ura_pd_{published_date.strftime('%Y%m%d')}_{hashlib.md5(article_url.encode()).hexdigest()[:8]}"
                        
                        # Extract URA-specific metadata
                        ura_metadata = self._extract_ura_metadata(full_content, title)
                        
                        article_data = {
                            'id': article_id,
                            'source': 'government_ura',
                            'text': full_content,
                            'timestamp': published_date.isoformat(),
                            'url': article_url,
                            'language': 'en',
                            'metadata': {
                                'title': title,
                                'agency': 'URA',
                                'category': 'market_data',
                                'policy_type': 'property_statistics',
                                'policy_subtype': self._get_data_subtype(title),
                                'content_length': len(full_content),
                                'keywords': self._extract_keywords(full_content),
                                'locations': self._extract_locations(full_content),
                                'sentiment_impact': 'market_wide',
                                **ura_metadata,
                                **content_info
                            }
                        }
                        
                        articles.append(article_data)
                        logger.info(f"Scraped property data: {title[:50]}...")
                        
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error scraping property data {link}: {str(e)}")
                        continue
                
            except Exception as e:
                logger.error(f"Error scraping property data from {data_url}: {str(e)}")
                continue
        
        logger.info(f"Scraped {len(articles)} property data articles")
        return articles

    def scrape_land_sales(self) -> List[Dict]:
        """Scrape URA land sales information (medium priority - 10%)"""
        articles = []
        
        land_sales_urls = [
            '/Corporate/Land-Sales/Current-URA-GLS-Sites',
            '/Corporate/Land-Sales/Sites-For-Tender',
            '/Corporate/Land-Sales/Past-Sale-Sites'
        ]
        
        for sales_url in land_sales_urls:
            try:
                url = f"{self.base_url}{sales_url}"
                logger.info(f"Scraping URA land sales: {url}")
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract land sales information
                site_links = soup.select('a[href*="site"], a[href*="tender"], a[href*="gls"]')
                
                for link in site_links[:15]:  # Limit to prevent overwhelming
                    try:
                        href = link.get('href')
                        if not href:
                            continue
                            
                        article_url = urljoin(self.base_url, href)
                        title = link.get_text(strip=True)
                        
                        # Extract content
                        full_content, content_info = self.get_full_article_content(article_url)
                        
                        if not full_content or len(full_content) < 100:
                            continue
                        
                        # Extract date
                        published_date = self.extract_date_from_text(full_content)
                        if not published_date:
                            published_date = datetime.now()
                        
                        # Check date range
                        if not self.is_within_date_range(published_date):
                            continue
                        
                        # Generate unique ID
                        article_id = f"ura_ls_{published_date.strftime('%Y%m%d')}_{hashlib.md5(article_url.encode()).hexdigest()[:8]}"
                        
                        # Extract URA-specific metadata
                        ura_metadata = self._extract_ura_metadata(full_content, title)
                        
                        article_data = {
                            'id': article_id,
                            'source': 'government_ura',
                            'text': full_content,
                            'timestamp': published_date.isoformat(),
                            'url': article_url,
                            'language': 'en',
                            'metadata': {
                                'title': title,
                                'agency': 'URA',
                                'category': 'land_sales',
                                'policy_type': 'gls_information',
                                'policy_subtype': self._get_land_sales_subtype(title),
                                'content_length': len(full_content),
                                'keywords': self._extract_keywords(full_content),
                                'locations': self._extract_locations(full_content),
                                'sentiment_impact': 'location_specific',
                                **ura_metadata,
                                **content_info
                            }
                        }
                        
                        articles.append(article_data)
                        logger.info(f"Scraped land sales: {title[:50]}...")
                        
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error scraping land sales {link}: {str(e)}")
                        continue
                
            except Exception as e:
                logger.error(f"Error scraping land sales from {sales_url}: {str(e)}")
                continue
        
        logger.info(f"Scraped {len(articles)} land sales articles")
        return articles

    def _classify_policy_type(self, content: str) -> str:
        """Classify URA content into policy types"""
        content_lower = content.lower()
        
        if any(keyword in content_lower for keyword in ['gls', 'government land sales', 'land sales programme']):
            return 'gls_programme'
        elif any(keyword in content_lower for keyword in ['tender launch', 'tender award', 'site tender']):
            return 'site_tender'
        elif any(keyword in content_lower for keyword in ['quarterly statistics', 'price index', 'market data']):
            return 'market_statistics'
        elif any(keyword in content_lower for keyword in ['flash estimates', 'price update']):
            return 'flash_estimates'
        elif any(keyword in content_lower for keyword in ['master plan', 'urban planning']):
            return 'urban_planning'
        else:
            return 'general_announcement'

    def _categorize_content(self, content: str) -> str:
        """Categorize URA content"""
        content_lower = content.lower()
        
        if any(keyword in content_lower for keyword in ['land sales', 'gls', 'tender']):
            return 'land_supply'
        elif any(keyword in content_lower for keyword in ['statistics', 'price index', 'market data']):
            return 'market_data'
        elif any(keyword in content_lower for keyword in ['planning', 'master plan']):
            return 'urban_planning'
        else:
            return 'policy_announcement'

    def _get_policy_subtype(self, content: str, policy_type: str) -> str:
        """Get specific policy subtype based on content"""
        content_lower = content.lower()
        
        if policy_type == 'gls_programme':
            if 'confirmed list' in content_lower:
                return 'confirmed_list'
            elif 'reserve list' in content_lower:
                return 'reserve_list'
            else:
                return 'programme_announcement'
        elif policy_type == 'site_tender':
            if 'tender launch' in content_lower:
                return 'tender_launch'
            elif 'tender award' in content_lower:
                return 'tender_award'
            else:
                return 'tender_update'
        elif policy_type == 'market_statistics':
            if 'quarterly' in content_lower:
                return 'quarterly_release'
            elif 'flash' in content_lower:
                return 'flash_estimate'
            else:
                return 'market_update'
        else:
            return 'general'

    def _get_data_subtype(self, title: str) -> str:
        """Get data subtype from title"""
        title_lower = title.lower()
        
        if 'quarterly' in title_lower:
            return 'quarterly_statistics'
        elif 'flash' in title_lower:
            return 'flash_estimates'
        elif 'private residential' in title_lower:
            return 'private_residential_data'
        elif 'commercial' in title_lower:
            return 'commercial_data'
        else:
            return 'general_data'

    def _get_land_sales_subtype(self, title: str) -> str:
        """Get land sales subtype from title"""
        title_lower = title.lower()
        
        if 'current' in title_lower or 'gls sites' in title_lower:
            return 'current_sites'
        elif 'tender' in title_lower:
            return 'sites_for_tender'
        elif 'past' in title_lower:
            return 'past_sites'
        else:
            return 'general_land_sales'

    def _extract_ura_metadata(self, content: str, title: str) -> Dict:
        """Extract URA-specific metadata from content"""
        metadata = {}
        content_lower = content.lower()
        
        # Extract GLS period
        gls_match = re.search(r'(\d+h\d{4})', content_lower)
        if gls_match:
            metadata['gls_period'] = gls_match.group(1).upper()
        
        # Extract residential units
        units_match = re.search(r'(\d+(?:,\d+)*)\s*(?:private\s*)?residential\s*units?', content_lower)
        if units_match:
            metadata['residential_units'] = int(units_match.group(1).replace(',', ''))
        
        # Extract commercial GFA
        gfa_match = re.search(r'(\d+(?:,\d+)*)\s*(?:sq\s*m|sqm|square\s*metres?)\s*(?:of\s*)?(?:commercial\s*)?gfa', content_lower)
        if gfa_match:
            metadata['commercial_gfa'] = int(gfa_match.group(1).replace(',', ''))
        
        # Extract hotel rooms
        hotel_match = re.search(r'(\d+(?:,\d+)*)\s*hotel\s*rooms?', content_lower)
        if hotel_match:
            metadata['hotel_rooms'] = int(hotel_match.group(1).replace(',', ''))
        
        # Extract EC units
        ec_match = re.search(r'(\d+(?:,\d+)*)\s*ec\s*units?', content_lower)
        if ec_match:
            metadata['ec_units'] = int(ec_match.group(1).replace(',', ''))
        
        # Extract price changes
        price_match = re.search(r'([+-]?\d+\.?\d*)\s*%', content)
        if price_match:
            metadata['price_change_percent'] = float(price_match.group(1))
        
        # Extract tender closing date
        closing_match = re.search(r'tender\s*(?:closes?|closing)\s*(?:on\s*)?(\d{1,2}\s+\w+\s+\d{4})', content_lower)
        if closing_match:
            metadata['tender_closing'] = closing_match.group(1)
        
        # Extract market segments
        if any(segment in content_lower for segment in ['non-landed', 'non landed']):
            metadata['market_segments'] = metadata.get('market_segments', []) + ['non_landed']
        if 'landed' in content_lower and 'non-landed' not in content_lower:
            metadata['market_segments'] = metadata.get('market_segments', []) + ['landed']
        if 'private residential' in content_lower:
            metadata['market_segments'] = metadata.get('market_segments', []) + ['private_residential']
        if 'serviced apartment' in content_lower:
            metadata['market_segments'] = metadata.get('market_segments', []) + ['serviced_apartments']
        
        return metadata

    def scrape_all_sources(self) -> List[URAArticle]:
        """Scrape all URA sources according to priority"""
        all_articles = []
        
        logger.info("Starting URA scraping process...")
        
        # High priority sources (85% of content)
        logger.info("Scraping high priority sources...")
        media_articles = self.scrape_media_releases()
        property_articles = self.scrape_property_data()
        all_articles.extend(media_articles)
        all_articles.extend(property_articles)
        
        # Medium priority sources (10% of content)
        logger.info("Scraping medium priority sources...")
        land_sales_articles = self.scrape_land_sales()
        all_articles.extend(land_sales_articles)
        
        # Convert to URAArticle objects
        ura_articles = []
        for article_data in all_articles:
            try:
                ura_article = URAArticle(
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
                ura_articles.append(ura_article)
            except Exception as e:
                logger.error(f"Error creating URAArticle object: {str(e)}")
                continue
        
        logger.info(f"Total articles scraped: {len(ura_articles)}")
        return ura_articles

    def _extract_keywords(self, content: str) -> List[str]:
        """Extract relevant keywords from content"""
        keywords = []
        content_lower = content.lower()
        
        for keyword in self.property_keywords:
            if keyword in content_lower:
                keywords.append(keyword)
        
        return list(set(keywords))  # Remove duplicates

    def _extract_locations(self, content: str) -> List[str]:
        """Extract Singapore locations mentioned in content"""
        locations = []
        content_lower = content.lower()
        
        singapore_locations = [
            'bedok', 'bukit timah', 'woodlands', 'dover', 'tanjong rhu', 'cross street',
            'orchard', 'marina bay', 'sentosa', 'jurong', 'tampines', 'punggol',
            'sengkang', 'hougang', 'ang mo kio', 'bishan', 'toa payoh', 'novena',
            'newton', 'dhoby ghaut', 'raffles place', 'shenton way', 'cbd',
            'changi', 'pasir ris', 'simei', 'tanah merah', 'expo', 'kallang',
            'geylang', 'katong', 'marine parade', 'east coast', 'west coast'
        ]
        
        for location in singapore_locations:
            if location in content_lower:
                locations.append(location.title())
        
        return list(set(locations))  # Remove duplicates

    def _assess_sentiment_impact(self, content: str) -> str:
        """Assess the potential sentiment impact of the content"""
        content_lower = content.lower()
        
        if any(keyword in content_lower for keyword in ['gls programme', 'land sales programme']):
            return 'market_wide'
        elif any(keyword in content_lower for keyword in ['quarterly statistics', 'price index']):
            return 'market_wide'
        elif any(keyword in content_lower for keyword in ['tender launch', 'tender award']):
            return 'location_specific'
        elif any(keyword in content_lower for keyword in ['cooling measures', 'policy change']):
            return 'policy_driven'
        else:
            return 'moderate'

    def save_articles_to_json(self, articles: List[URAArticle], filename: str = "ura_articles.json"):
        """Save articles to JSON file with comprehensive statistics"""
        if not articles:
            logger.warning("No articles to save")
            return
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"ura_articles_{timestamp}.json"
        json_path = self.output_dir / json_filename
        
        # Convert articles to dictionaries
        articles_data = [asdict(article) for article in articles]
        
        # Generate comprehensive statistics
        stats = {
            'scraping_summary': {
                'total_articles': len(articles),
                'date_range': f"{self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}",
                'scraping_timestamp': datetime.now().isoformat(),
                'source_breakdown': self._get_source_breakdown(articles),
                'policy_type_breakdown': self._get_policy_type_breakdown(articles),
                'category_breakdown': self._get_category_breakdown(articles),
                'location_breakdown': self._get_location_breakdown(articles),
                'content_statistics': {
                    'avg_content_length': sum(len(article.text) for article in articles) / len(articles),
                    'avg_word_count': sum(article.word_count for article in articles) / len(articles),
                    'truncated_articles': sum(1 for article in articles if article.is_truncated),
                    'total_content_length': sum(len(article.text) for article in articles)
                }
            }
        }
        
        # Save articles
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(articles_data, f, indent=2, ensure_ascii=False)
        
        # Save statistics
        stats_filename = f"ura_scraping_stats_{timestamp}.json"
        stats_path = self.output_dir / stats_filename
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(articles)} articles to {json_path}")
        logger.info(f"Saved statistics to {stats_path}")
        
        # Print summary
        print(f"\n=== URA Scraping Summary ===")
        print(f"Total articles scraped: {len(articles)}")
        print(f"Date range: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        print(f"Output directory: {self.output_dir}")
        print(f"Files saved: {json_filename}, {stats_filename}")
        
        print(f"\nPolicy Type Breakdown:")
        for policy_type, count in stats['scraping_summary']['policy_type_breakdown'].items():
            print(f"  {policy_type}: {count}")
        
        print(f"\nCategory Breakdown:")
        for category, count in stats['scraping_summary']['category_breakdown'].items():
            print(f"  {category}: {count}")

    def _get_source_breakdown(self, articles: List[URAArticle]) -> Dict[str, int]:
        """Get breakdown of articles by source type"""
        breakdown = {}
        for article in articles:
            source_type = article.metadata.get('category', 'unknown')
            breakdown[source_type] = breakdown.get(source_type, 0) + 1
        return breakdown

    def _get_policy_type_breakdown(self, articles: List[URAArticle]) -> Dict[str, int]:
        """Get breakdown of articles by policy type"""
        breakdown = {}
        for article in articles:
            policy_type = article.metadata.get('policy_type', 'unknown')
            breakdown[policy_type] = breakdown.get(policy_type, 0) + 1
        return breakdown

    def _get_category_breakdown(self, articles: List[URAArticle]) -> Dict[str, int]:
        """Get breakdown of articles by category"""
        breakdown = {}
        for article in articles:
            category = article.metadata.get('category', 'unknown')
            breakdown[category] = breakdown.get(category, 0) + 1
        return breakdown

    def _get_location_breakdown(self, articles: List[URAArticle]) -> Dict[str, int]:
        """Get breakdown of articles by locations mentioned"""
        breakdown = {}
        for article in articles:
            locations = article.metadata.get('locations', [])
            for location in locations:
                breakdown[location] = breakdown.get(location, 0) + 1
        return breakdown

def main():
    """Main function to run the URA scraper"""
    scraper = URAScraper()
    
    try:
        # Scrape all sources
        articles = scraper.scrape_all_sources()
        
        # Save to JSON
        scraper.save_articles_to_json(articles)
        
        print(f"\nURA scraping completed successfully!")
        print(f"Scraped {len(articles)} articles from URA website")
        
    except Exception as e:
        logger.error(f"Error in main scraping process: {str(e)}")
        raise

if __name__ == "__main__":
    main()