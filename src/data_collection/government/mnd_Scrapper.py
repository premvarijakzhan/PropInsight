
"""
MND (Ministry of National Development) Web Scraper for PropInsight

This scraper focuses on collecting property-related policy announcements, 
press releases, and housing information from the MND website.

Priority Sources (based on analysis):
1. HIGH PRIORITY: Newsroom (Press Releases, Speeches, Parliament Matters)
2. MEDIUM PRIORITY: Housing Policy Sections
3. LOW PRIORITY: Background/Historical Content

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
class MNDArticle:
    """Data structure for MND articles following PropInsight specification"""
    id: str
    source: str  # Always "government_mnd"
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

class MNDScraper:
    """
    MND Website Scraper for PropInsight
    
    Scrapes property-related content from MND website with focus on:
    - Press releases (highest priority)
    - Ministerial speeches 
    - Parliament matters (Q&As, speeches)
    - Housing policy information
    """
    
    def __init__(self, output_dir: str = "data/raw/government/mnd"):
        self.base_url = "https://www.mnd.gov.sg"
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
        
        # Priority URLs based on analysis
        self.priority_urls = {
            'high': {
                'press_releases': '/newsroom/press-releases',
                'speeches': '/newsroom/speeches',
                'parliament_speeches': '/newsroom/parliament-matters/speeches',
                'parliament_qas': '/newsroom/parliament-matters/q-as'
            },
            'medium': {
                'public_housing': '/our-work/housing-a-nation/public-housing',
                'private_housing': '/our-work/housing-a-nation/private-housing',
                'bto_classification': '/our-work/housing-a-nation/bto-classification',
                'estate_improvement': '/our-work/ensuring-high-quality-living-environment/rejuvenating-our-estates'
            },
            'low': {
                'bto_guide': '/highlights/bto-homebuying-guide',
                'master_plan': '/highlights/draft-master-plan-2025'
            }
        }
        
        # Property-related keywords for content filtering
        self.property_keywords = [
            'housing', 'property', 'bto', 'hdb', 'private housing', 'resale',
            'stamp duty', 'ssd', 'absd', 'cooling measures', 'gls', 'land sales',
            'home ownership', 'housing grant', 'affordability', 'public housing',
            'estate', 'upgrading', 'hip', 'ease', 'sers', 'en bloc'
        ]

    def is_property_related(self, text: str) -> bool:
        """Check if content is property-related based on keywords"""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.property_keywords)

    def is_within_date_range(self, published_date: datetime) -> bool:
        """Check if the published date is within our target range"""
        return self.start_date <= published_date <= self.end_date

    def extract_date_from_text(self, text: str) -> Optional[datetime]:
        """Extract date from various text formats found on MND website"""
        date_patterns = [
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(\d{4})-(\d{2})-(\d{2})',
            r'(\d{1,2})/(\d{1,2})/(\d{4})'
        ]
        
        month_map = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
            'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    if 'Jan' in pattern or 'January' in pattern:
                        day, month_str, year = match.groups()
                        month = month_map.get(month_str, 1)
                        return datetime(int(year), month, int(day))
                    elif '-' in pattern:
                        year, month, day = match.groups()
                        return datetime(int(year), int(month), int(day))
                    elif '/' in pattern:
                        day, month, year = match.groups()
                        return datetime(int(year), int(month), int(day))
                except ValueError:
                    continue
        
        return None

    def get_full_article_content(self, url: str) -> Tuple[str, Dict]:
        """
        Extract full article content from MND article page
        Returns content and metadata about extraction quality
        """
        content_info = {
            'is_truncated': False,
            'original_length': 0,
            'extraction_method': 'web_scraping'
        }
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove navigation and non-content elements
            for element in soup.find_all(['nav', 'header', 'footer', 'aside', 'script', 'style']):
                element.decompose()
            
            # Try different content selectors based on MND website structure
            content_selectors = [
                '.content-body',
                '.article-content',
                '.press-release-content',
                '.speech-content',
                '.main-content',
                'article',
                '.content'
            ]
            
            content_text = ""
            for selector in content_selectors:
                content_element = soup.select_one(selector)
                if content_element:
                    # Extract text while preserving paragraph structure
                    paragraphs = content_element.find_all(['p', 'div', 'li'])
                    if paragraphs:
                        content_text = '\n\n'.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
                    else:
                        content_text = content_element.get_text(strip=True)
                    break
            
            # Fallback to main content area
            if not content_text:
                main_content = soup.find('main') or soup.find('body')
                if main_content:
                    content_text = main_content.get_text(strip=True)
            
            # Clean up the content
            content_text = re.sub(r'\s+', ' ', content_text)
            content_text = re.sub(r'\n\s*\n', '\n\n', content_text)
            
            content_info['original_length'] = len(content_text)
            
            # Apply smart truncation if content is too long
            max_length = 8000  # Increased limit for government content
            if len(content_text) > max_length:
                content_text, content_info = self._smart_truncate(content_text, max_length, content_info)
            
            return content_text, content_info
            
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {str(e)}")
            content_info['extraction_method'] = 'error'
            return "", content_info

    def _smart_truncate(self, text: str, max_length: int, content_info: dict) -> Tuple[str, dict]:
        """
        Intelligently truncate text at sentence or paragraph boundaries
        """
        if len(text) <= max_length:
            return text, content_info
        
        content_info['is_truncated'] = True
        content_info['original_length'] = len(text)
        
        # Try to truncate at sentence boundary
        truncated = text[:max_length]
        last_sentence = truncated.rfind('. ')
        if last_sentence > max_length * 0.8:  # If we can keep 80% of content
            return truncated[:last_sentence + 1], content_info
        
        # Fallback to word boundary
        last_space = truncated.rfind(' ')
        if last_space > 0:
            return truncated[:last_space], content_info
        
        return truncated, content_info

    def scrape_press_releases(self) -> List[Dict]:
        """
        Scrape MND press releases - HIGH PRIORITY content
        These contain immediate policy announcements and market-moving information
        """
        logger.info("Scraping MND press releases...")
        articles = []
        
        # Known recent press release URLs from web search
        known_press_releases = [
            "2024-private-housing-supply-highest-since-2013-with-latest-2h2024-government-land-sales-(gls)-programme",
            "government-supply-of-private-housing-in-2023-is-highest-in-a-decade-with-release-of-2h2023-government-land-sales-programme",
            "changes-to-the-board-members-of-the-housing---development-board",
            "135-million-to-upgrade-private-estates-under-the--expanded-estate-upgrading-programme",
            "closer-families-stronger-ties-enhanced-proximity-housing-grant-to-help-more-families-live-closer-together",
            "providing-more-support-for-home-buyers-and-public-rental-families",
            "new-board-to-lead-professional-engineers",
            "board-composition-of-centre-for-liveable-cities-limited"
        ]
        
        for pr_slug in known_press_releases:
            try:
                article_url = f"{self.base_url}/newsroom/press-releases/view/{pr_slug}"
                
                # Get full article content
                response = self.session.get(article_url, timeout=30)
                if response.status_code != 200:
                    logger.warning(f"Could not access press release: {article_url}")
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract title
                title_element = soup.find('h1') or soup.find('title')
                if not title_element:
                    continue
                
                title = title_element.get_text(strip=True)
                
                # Skip if not property-related
                if not self.is_property_related(title):
                    logger.info(f"Skipping non-property related article: {title}")
                    continue
                
                # Extract date from content
                published_date = None
                date_patterns = soup.find_all(text=re.compile(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}\b'))
                if date_patterns:
                    published_date = self.extract_date_from_text(date_patterns[0])
                
                if not published_date:
                    # Try to extract from URL or use current date
                    if "2024" in pr_slug:
                        published_date = datetime(2024, 6, 1)  # Default to mid-2024
                    elif "2023" in pr_slug:
                        published_date = datetime(2023, 6, 1)  # Default to mid-2023
                    else:
                        published_date = datetime.now()
                
                if published_date and not self.is_within_date_range(published_date):
                    logger.info(f"Skipping article outside date range: {title}")
                    continue
                
                # Get full article content
                full_content, content_info = self.get_full_article_content(article_url)
                
                if not full_content:
                    logger.warning(f"Could not extract content from: {article_url}")
                    continue
                
                # Create article ID
                article_id = f"mnd_pr_{hashlib.md5(article_url.encode()).hexdigest()[:16]}"
                
                article = {
                    'id': article_id,
                    'title': title,
                    'content': full_content,
                    'url': article_url,
                    'published_date': published_date,
                    'source_type': 'press_releases',
                    'category': 'housing_policy',
                    'policy_type': self._classify_policy_type(title + ' ' + full_content),
                    'sentiment_impact': 'immediate',
                    'scraping_priority': 'high',
                    'content_length': content_info.get('original_length', len(full_content)),
                    'word_count': len(full_content.split()),
                    'is_truncated': content_info.get('is_truncated', False),
                    'extraction_method': content_info.get('extraction_method', 'web_scraping')
                }
                
                articles.append(article)
                logger.info(f"Scraped press release: {title}")
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing press release {pr_slug}: {str(e)}")
                continue
        
        logger.info(f"Scraped {len(articles)} press releases")
        return articles

    def scrape_speeches(self) -> List[Dict]:
        """
        Scrape ministerial speeches - HIGH PRIORITY content
        These provide policy direction and government confidence signals
        """
        logger.info("Scraping MND speeches...")
        articles = []
        
        try:
            url = urljoin(self.base_url, self.priority_urls['high']['speeches'])
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find speech listings
            speech_items = soup.find_all(['div', 'article'], class_=re.compile(r'speech|item|content'))
            
            for item in speech_items:
                try:
                    # Extract title and link
                    title_element = item.find(['h1', 'h2', 'h3', 'h4', 'a'])
                    if not title_element:
                        continue
                    
                    title = title_element.get_text(strip=True)
                    
                    # Skip if not property-related
                    if not self.is_property_related(title):
                        continue
                    
                    # Get article URL
                    link_element = title_element if title_element.name == 'a' else title_element.find('a')
                    if not link_element:
                        continue
                    
                    article_url = urljoin(self.base_url, link_element.get('href'))
                    
                    # Extract date
                    date_element = item.find(['time', 'span'], class_=re.compile(r'date|time'))
                    published_date = None
                    if date_element:
                        date_text = date_element.get_text(strip=True)
                        published_date = self.extract_date_from_text(date_text)
                    
                    if not published_date:
                        item_text = item.get_text()
                        published_date = self.extract_date_from_text(item_text)
                    
                    if published_date and not self.is_within_date_range(published_date):
                        continue
                    
                    # Get full article content
                    full_content, content_info = self.get_full_article_content(article_url)
                    
                    if not full_content:
                        continue
                    
                    # Create article ID
                    article_id = f"mnd_speech_{hashlib.md5(article_url.encode()).hexdigest()[:16]}"
                    
                    article = {
                        'id': article_id,
                        'title': title,
                        'content': full_content,
                        'url': article_url,
                        'published_date': published_date or datetime.now(),
                        'source_type': 'speeches',
                        'category': 'housing_policy',
                        'policy_type': self._classify_policy_type(title + ' ' + full_content),
                        'sentiment_impact': 'high',
                        'scraping_priority': 'high',
                        'content_length': content_info.get('original_length', len(full_content)),
                        'word_count': len(full_content.split()),
                        'is_truncated': content_info.get('is_truncated', False),
                        'extraction_method': content_info.get('extraction_method', 'web_scraping')
                    }
                    
                    articles.append(article)
                    logger.info(f"Scraped speech: {title}")
                    
                    # Rate limiting
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing speech item: {str(e)}")
                    continue
        
        except Exception as e:
            logger.error(f"Error scraping speeches: {str(e)}")
        
        logger.info(f"Scraped {len(articles)} speeches")
        return articles

    def scrape_parliament_matters(self) -> List[Dict]:
        """
        Scrape parliament Q&As and speeches - HIGH PRIORITY content
        These provide detailed policy explanations and responses to public concerns
        """
        logger.info("Scraping MND parliament matters...")
        articles = []
        
        # Scrape both parliament speeches and Q&As
        parliament_urls = [
            self.priority_urls['high']['parliament_speeches'],
            self.priority_urls['high']['parliament_qas']
        ]
        
        for parliament_url in parliament_urls:
            try:
                url = urljoin(self.base_url, parliament_url)
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find parliament items
                parliament_items = soup.find_all(['div', 'article'], class_=re.compile(r'parliament|qa|speech|item'))
                
                for item in parliament_items:
                    try:
                        # Extract title and link
                        title_element = item.find(['h1', 'h2', 'h3', 'h4', 'a'])
                        if not title_element:
                            continue
                        
                        title = title_element.get_text(strip=True)
                        
                        # Skip if not property-related
                        if not self.is_property_related(title):
                            continue
                        
                        # Get article URL
                        link_element = title_element if title_element.name == 'a' else title_element.find('a')
                        if not link_element:
                            continue
                        
                        article_url = urljoin(self.base_url, link_element.get('href'))
                        
                        # Extract date
                        date_element = item.find(['time', 'span'], class_=re.compile(r'date|time'))
                        published_date = None
                        if date_element:
                            date_text = date_element.get_text(strip=True)
                            published_date = self.extract_date_from_text(date_text)
                        
                        if not published_date:
                            item_text = item.get_text()
                            published_date = self.extract_date_from_text(item_text)
                        
                        if published_date and not self.is_within_date_range(published_date):
                            continue
                        
                        # Get full article content
                        full_content, content_info = self.get_full_article_content(article_url)
                        
                        if not full_content:
                            continue
                        
                        # Create article ID
                        source_type = 'parliament_qas' if 'q-as' in parliament_url else 'parliament_speeches'
                        article_id = f"mnd_{source_type}_{hashlib.md5(article_url.encode()).hexdigest()[:16]}"
                        
                        article = {
                            'id': article_id,
                            'title': title,
                            'content': full_content,
                            'url': article_url,
                            'published_date': published_date or datetime.now(),
                            'source_type': source_type,
                            'category': 'housing_policy',
                            'policy_type': self._classify_policy_type(title + ' ' + full_content),
                            'sentiment_impact': 'medium',
                            'scraping_priority': 'high',
                            'content_length': content_info.get('original_length', len(full_content)),
                            'word_count': len(full_content.split()),
                            'is_truncated': content_info.get('is_truncated', False),
                            'extraction_method': content_info.get('extraction_method', 'web_scraping')
                        }
                        
                        articles.append(article)
                        logger.info(f"Scraped parliament matter: {title}")
                        
                        # Rate limiting
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error processing parliament item: {str(e)}")
                        continue
            
            except Exception as e:
                logger.error(f"Error scraping parliament matters from {parliament_url}: {str(e)}")
        
        logger.info(f"Scraped {len(articles)} parliament matters")
        return articles

    def _classify_policy_type(self, content: str) -> str:
        """Classify the type of policy based on content"""
        content_lower = content.lower()
        
        if any(term in content_lower for term in ['stamp duty', 'ssd', 'absd', 'cooling']):
            return 'cooling_measures'
        elif any(term in content_lower for term in ['bto', 'ballot', 'launch']):
            return 'bto_policy'
        elif any(term in content_lower for term in ['grant', 'subsidy', 'assistance']):
            return 'housing_grants'
        elif any(term in content_lower for term in ['gls', 'land sales', 'supply']):
            return 'land_supply'
        elif any(term in content_lower for term in ['upgrading', 'hip', 'ease', 'improvement']):
            return 'estate_upgrading'
        else:
            return 'general_housing'

    def scrape_all_sources(self) -> List[MNDArticle]:
        """
        Scrape all priority sources and return list of MNDArticle objects
        """
        logger.info("Starting MND scraping for all priority sources...")
        
        all_articles = []
        
        # High priority sources
        all_articles.extend(self.scrape_press_releases())
        all_articles.extend(self.scrape_speeches())
        all_articles.extend(self.scrape_parliament_matters())
        
        # Convert to MNDArticle objects
        mnd_articles = []
        for article_data in all_articles:
            try:
                # Create metadata following PropInsight specification
                metadata = {
                    'title': article_data['title'],
                    'agency': 'MND',
                    'category': article_data['category'],
                    'policy_type': article_data['policy_type'],
                    'source_type': article_data['source_type'],
                    'sentiment_impact': article_data['sentiment_impact'],
                    'scraping_priority': article_data['scraping_priority'],
                    'content_length': article_data['content_length'],
                    'keywords': self._extract_keywords(article_data['content']),
                    'target_demographics': self._identify_target_demographics(article_data['content']),
                    'market_segments': self._identify_market_segments(article_data['content'])
                }
                
                mnd_article = MNDArticle(
                    id=article_data['id'],
                    source='government_mnd',
                    text=article_data['content'],
                    timestamp=article_data['published_date'].isoformat(),
                    url=article_data['url'],
                    language='en',
                    metadata=metadata,
                    content_length=article_data['content_length'],
                    word_count=article_data['word_count'],
                    is_truncated=article_data['is_truncated'],
                    original_length=article_data['content_length'],
                    extraction_method=article_data['extraction_method']
                )
                
                mnd_articles.append(mnd_article)
                
            except Exception as e:
                logger.error(f"Error creating MNDArticle object: {str(e)}")
                continue
        
        logger.info(f"Successfully scraped {len(mnd_articles)} MND articles")
        return mnd_articles

    def _extract_keywords(self, content: str) -> List[str]:
        """Extract relevant keywords from content"""
        keywords = []
        content_lower = content.lower()
        
        for keyword in self.property_keywords:
            if keyword in content_lower:
                keywords.append(keyword)
        
        return keywords[:10]  # Limit to top 10 keywords

    def _identify_target_demographics(self, content: str) -> List[str]:
        """Identify target demographics based on content"""
        demographics = []
        content_lower = content.lower()
        
        if any(term in content_lower for term in ['first-time', 'first time', 'young']):
            demographics.append('first_time_buyers')
        if any(term in content_lower for term in ['investor', 'investment']):
            demographics.append('investors')
        if any(term in content_lower for term in ['upgrade', 'upgrader']):
            demographics.append('upgraders')
        if any(term in content_lower for term in ['senior', 'elderly']):
            demographics.append('seniors')
        
        return demographics

    def _identify_market_segments(self, content: str) -> List[str]:
        """Identify affected market segments"""
        segments = []
        content_lower = content.lower()
        
        if any(term in content_lower for term in ['hdb', 'public housing', 'bto']):
            segments.append('public_housing')
        if any(term in content_lower for term in ['private', 'condo', 'landed']):
            segments.append('private_residential')
        if any(term in content_lower for term in ['resale', 'secondary']):
            segments.append('resale_market')
        if any(term in content_lower for term in ['rental', 'rent']):
            segments.append('rental_market')
        
        return segments

    def save_articles_to_json(self, articles: List[MNDArticle], filename: str = "mnd_articles.json"):
        """
        Save articles to JSON file with proper formatting
        """
        # Convert dataclass objects to dictionaries
        articles_dict = []
        for article in articles:
            article_dict = {
                'id': article.id,
                'source': article.source,
                'text': article.text,
                'timestamp': article.timestamp,
                'url': article.url,
                'language': article.language,
                'metadata': article.metadata,
                'content_length': article.content_length,
                'word_count': article.word_count,
                'is_truncated': article.is_truncated,
                'original_length': article.original_length,
                'extraction_method': article.extraction_method
            }
            articles_dict.append(article_dict)
        
        # Save to JSON file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"mnd_articles_{timestamp}.json"
        output_path = self.output_dir / output_filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(articles_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(articles)} articles to {output_path}")
        
        # Save scraping statistics
        stats = {
            'total_articles': len(articles),
            'scraping_date': datetime.now().isoformat(),
            'source_breakdown': self._get_source_breakdown(articles),
            'policy_type_breakdown': self._get_policy_type_breakdown(articles),
            'date_range': {
                'start': self.start_date.isoformat(),
                'end': self.end_date.isoformat()
            }
        }
        
        stats_filename = f"mnd_scraping_stats_{timestamp}.json"
        stats_path = self.output_dir / stats_filename
        
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved scraping statistics to {stats_path}")

    def _get_source_breakdown(self, articles: List[MNDArticle]) -> Dict[str, int]:
        """Get breakdown of articles by source type"""
        breakdown = {}
        for article in articles:
            source_type = article.metadata.get('source_type', 'unknown')
            breakdown[source_type] = breakdown.get(source_type, 0) + 1
        return breakdown

    def _get_policy_type_breakdown(self, articles: List[MNDArticle]) -> Dict[str, int]:
        """Get breakdown of articles by policy type"""
        breakdown = {}
        for article in articles:
            policy_type = article.metadata.get('policy_type', 'unknown')
            breakdown[policy_type] = breakdown.get(policy_type, 0) + 1
        return breakdown

def main():
    """Main function to run the MND scraper"""
    logger.info("Starting MND scraper for PropInsight...")
    
    try:
        scraper = MNDScraper()
        articles = scraper.scrape_all_sources()
        
        if articles:
            scraper.save_articles_to_json(articles)
            logger.info(f"MND scraping completed successfully. Scraped {len(articles)} articles.")
        else:
            logger.warning("No articles were scraped.")
    
    except Exception as e:
        logger.error(f"Error running MND scraper: {str(e)}")
        raise

if __name__ == "__main__":
    main()