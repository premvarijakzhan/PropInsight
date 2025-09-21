"""
PropInsight SFA (Singapore Food Agency) Scraper

Scrapes food infrastructure and property-related content from SFA:
- Newsroom (RSS feeds) - Primary source
- Food Retail Licensing Information - Secondary source  
- Wholesale Markets & Fishery Ports - Tertiary source
- Agricultural Land Allocation - Supplementary source

Focus: Food infrastructure, hawker centres, agricultural land use, and food facility developments
that impact property sentiment and regional development patterns.

Date Range: 2023-2025 
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

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SFAArticle:
    """Data structure for SFA articles following JSON specification"""
    id: str
    source: str
    text: str
    timestamp: str
    url: str
    language: str
    metadata: Dict
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

class SFAScraper:
    """
    SFA scraper implementing JSON specification
    
    Priority Data Sources:
    1. Newsroom (RSS) - 50% priority
    2. Food Retail Licensing - 30% priority  
    3. Wholesale Markets & Fishery Ports - 15% priority
    4. Agricultural Land Allocation - 5% priority
    """
    
    def __init__(self, output_dir: str = "../../../data/raw/government/sfa"):
        """
        Initialize SFA scraper with relative path configuration
        
        Args:
            output_dir: Relative directory path to save scraped data
        """
        # Use relative path from src/data_collection/government to project root
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Priority data sources
        self.priority_sources = {
            'newsroom': {
                'priority': 50,  # Primary source
                'rss_url': 'https://www.sfa.gov.sg/rss/newsroom',
                'web_url': 'https://www.sfa.gov.sg/news-publications/newsroom',
                'description': 'Latest news and press releases including food infrastructure developments'
            },
            'food_retail': {
                'priority': 30,  # Secondary source
                'url': 'https://www.sfa.gov.sg/food-retail',
                'description': 'Hawker centre, food court, coffeeshop licensing affecting residential areas'
            },
            'wholesale_markets': {
                'priority': 15,  # Tertiary source
                'urls': [
                    'https://www.sfa.gov.sg/wholesale-markets-fishery-ports/wholesale-markets/information-on-pasir-panjang-wholesale-centre',
                    'https://www.sfa.gov.sg/wholesale-markets-fishery-ports/fishery-ports/information-on-jurong-fishery-port'
                ],
                'description': 'Major food distribution infrastructure affecting regional development'
            },
            'agricultural_land': {
                'priority': 5,  # Supplementary source
                'url': 'https://www.sfa.gov.sg/news-publications/newsroom',  # Agricultural land tenders appear in newsroom
                'description': 'Agricultural land allocation affecting regional development patterns'
            }
        }
        
        # Date range: 2023-2025 
        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2025, 12, 31)
        
        # SFA-specific keywords for content filtering
        self.sfa_keywords = [
            # Food infrastructure (high priority)
            'hawker centre', 'food court', 'coffeeshop', 'food facility',
            'wholesale centre', 'fishery port', 'food distribution',
            
            # Land use and development
            'agricultural land', 'farming land', 'land parcel', 'land tender',
            'Lim Chu Kang', 'Sungei Tengah', 'Pasir Panjang', 'Jurong',
            
            # Lease and infrastructure
            'lease extension', 'infrastructure development', 'food security',
            'regional development', 'supply chain', 'long-term commitment',
            
            # Residential area amenities
            'HDB void deck', 'residential area', 'neighborhood', 'new town',
            'BTO', 'food establishment', 'community facilities'
        ]
        
        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive'
        })
        
        # Statistics tracking
        self.stats = {
            'newsroom': 0,
            'food_retail': 0, 
            'wholesale_markets': 0,
            'agricultural_land': 0,
            'total_articles': 0,
            'date_range': f"{self.start_date.year}-{self.end_date.year}"
        }
    
    def is_within_date_range(self, published_date: datetime) -> bool:
        """Check if article is within 2023-2025 date range"""
        return self.start_date <= published_date <= self.end_date
    
    def extract_date_from_text(self, text: str) -> Optional[datetime]:
        """Extract date from various text formats"""
        if not text:
            return None
            
        # Common date patterns
        date_patterns = [
            r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
            r'(\d{4})-(\d{1,2})-(\d{1,2})',
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
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
                            if '/' in text:
                                day, month, year = int(groups[0]), int(groups[1]), int(groups[2])
                            else:  # YYYY-MM-DD
                                year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                        
                        return datetime(year, month, day)
                except (ValueError, KeyError):
                    continue
        
        return None
    
    def create_sfa_metadata(self, title: str, content: str, source_type: str, url: str) -> Dict:
        """Create SFA-specific metadata """
        
        # Base metadata
        metadata = {
            "title": title,
            "agency": "SFA",
            "category": self.determine_category(content, source_type),
            "content_length": len(content),
            "keywords": self.extract_keywords(content)
        }
        
        # Add source-specific metadata
        if source_type == 'newsroom':
            metadata.update({
                "press_release_id": self.extract_press_release_id(content),
                "policy_type": self.determine_policy_type(content),
                "policy_subtype": self.determine_policy_subtype(content)
            })
        
        # SFA-specific fields based on content analysis
        if any(keyword in content.lower() for keyword in ['lease extension', 'wholesale centre', 'fishery port']):
            metadata.update({
                "facilities": self.extract_facilities(content),
                "lease_duration": self.extract_lease_duration(content),
                "infrastructure_type": "food_distribution",
                "regional_impact": self.extract_regional_impact(content),
                "supply_chain_security": True,
                "long_term_commitment": True
            })
        
        if any(keyword in content.lower() for keyword in ['agricultural land', 'farming', 'land parcel']):
            metadata.update({
                "locations": self.extract_locations(content),
                "land_use": "agricultural",
                "food_security": True,
                "rural_development": True,
                "regional_planning": True
            })
        
        if any(keyword in content.lower() for keyword in ['hawker centre', 'food court', 'hdb']):
            metadata.update({
                "amenity_type": "food_facility",
                "residential_impact": True,
                "community_facilities": True,
                "quality_of_life": True
            })
        
        return metadata
    
    def determine_category(self, content: str, source_type: str) -> str:
        """Determine article category based on content"""
        content_lower = content.lower()
        
        if 'hawker centre' in content_lower or 'food court' in content_lower:
            return 'food_infrastructure'
        elif 'agricultural land' in content_lower or 'farming' in content_lower:
            return 'agricultural_land'
        elif 'wholesale centre' in content_lower or 'fishery port' in content_lower:
            return 'wholesale_infrastructure'
        elif 'lease extension' in content_lower:
            return 'lease_extension'
        else:
            return 'general_announcement'
    
    def determine_policy_type(self, content: str) -> str:
        """Determine policy type from content"""
        content_lower = content.lower()
        
        if 'lease extension' in content_lower:
            return 'lease_extension'
        elif 'land allocation' in content_lower or 'tender' in content_lower:
            return 'land_allocation'
        elif 'licensing' in content_lower:
            return 'licensing'
        else:
            return 'announcement'
    
    def determine_policy_subtype(self, content: str) -> str:
        """Determine policy subtype from content"""
        content_lower = content.lower()
        
        if 'wholesale' in content_lower:
            return 'wholesale_facilities'
        elif 'farming' in content_lower:
            return 'farming_development'
        elif 'hawker' in content_lower:
            return 'hawker_development'
        else:
            return 'general'
    
    def extract_facilities(self, content: str) -> List[str]:
        """Extract facility names from content"""
        facilities = []
        facility_patterns = [
            r'Pasir Panjang Wholesale Centre',
            r'Jurong Fishery Port',
            r'([A-Z][a-z]+ (?:Hawker Centre|Food Court|Market))'
        ]
        
        for pattern in facility_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            facilities.extend(matches)
        
        return list(set(facilities))  # Remove duplicates
    
    def extract_lease_duration(self, content: str) -> Optional[str]:
        """Extract lease duration from content"""
        duration_match = re.search(r'(?:to|until)\s+(\d{4})', content, re.IGNORECASE)
        return duration_match.group(1) if duration_match else None
    
    def extract_regional_impact(self, content: str) -> List[str]:
        """Extract regional impact areas from content"""
        regions = []
        region_patterns = [
            r'(Pasir Panjang)', r'(Jurong)', r'(Lim Chu Kang)', r'(Sungei Tengah)',
            r'([A-Z][a-z]+ (?:Town|Area|Region))'
        ]
        
        for pattern in region_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            regions.extend(matches)
        
        return list(set(regions))
    
    def extract_locations(self, content: str) -> List[str]:
        """Extract location names from content"""
        locations = []
        location_patterns = [
            r'(Lim Chu Kang)', r'(Sungei Tengah)', r'(Pasir Panjang)', r'(Jurong)',
            r'([A-Z][a-z]+ [A-Z][a-z]+)'  # Two-word place names
        ]
        
        for pattern in location_patterns:
            matches = re.findall(pattern, content)
            locations.extend(matches)
        
        return list(set(locations))
    
    def extract_press_release_id(self, content: str) -> Optional[str]:
        """Extract press release ID if available"""
        id_match = re.search(r'SFA[_\s](\d{4})[_\s](\d+)', content)
        if id_match:
            return f"SFA_{id_match.group(1)}_{id_match.group(2).zfill(3)}"
        return None
    
    def extract_keywords(self, content: str) -> List[str]:
        """Extract relevant keywords from content"""
        keywords = []
        content_lower = content.lower()
        
        for keyword in self.sfa_keywords:
            if keyword.lower() in content_lower:
                keywords.append(keyword)
        
        return keywords[:10]  # Limit to top 10 keywords
    
    def scrape_newsroom_rss(self) -> List[SFAArticle]:
        """Scrape newsroom via RSS feed - Primary source (50% priority)"""
        articles = []
        source_info = self.priority_sources['newsroom']
        
        try:
            logger.info(f"Scraping Newsroom RSS: {source_info['rss_url']}")
            
            # Parse RSS feed
            feed = feedparser.parse(source_info['rss_url'])
            
            for entry in feed.entries[:20]:  # Limit to recent entries
                try:
                    # Extract basic information
                    title = entry.get('title', '')
                    link = entry.get('link', '')
                    summary = entry.get('summary', '')
                    
                    if not title or not link:
                        continue
                    
                    # Parse publication date
                    pub_date = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        pub_date = datetime(*entry.published_parsed[:6])
                    else:
                        pub_date = self.extract_date_from_text(title + ' ' + summary)
                    
                    if not pub_date:
                        pub_date = datetime.now()
                    
                    # Check date range
                    if not self.is_within_date_range(pub_date):
                        continue
                    
                    # Get full content
                    full_content = self.get_article_content(link)
                    if not full_content:
                        full_content = summary
                    
                    # Create article with SFA-specific metadata
                    article = SFAArticle(
                        id=f"sfa_newsroom_{hash(link)}",
                        source="government_sfa",
                        text=full_content,
                        timestamp=pub_date.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                        url=link,
                        language="en",
                        metadata=self.create_sfa_metadata(title, full_content, 'newsroom', link)
                    )
                    
                    articles.append(article)
                    self.stats['newsroom'] += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing RSS entry: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping newsroom RSS: {e}")
        
        return articles
    
    def scrape_food_retail_licensing(self) -> List[SFAArticle]:
        """Scrape food retail licensing information - Secondary source (30% priority)"""
        articles = []
        source_info = self.priority_sources['food_retail']
        
        try:
            logger.info(f"Scraping Food Retail Licensing: {source_info['url']}")
            
            response = self.session.get(source_info['url'], timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for licensing information sections
            content_sections = soup.find_all(['div', 'section'], class_=re.compile(r'content|main|licensing', re.I))
            
            for section in content_sections[:5]:  # Limit sections
                try:
                    title = "Food Retail Licensing Requirements"
                    content = section.get_text(strip=True)
                    
                    if len(content) < 100:  # Skip short content
                        continue
                    
                    # Create article
                    article = SFAArticle(
                        id=f"sfa_food_retail_{hash(content[:100])}",
                        source="government_sfa",
                        text=content,
                        timestamp=datetime.now().strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                        url=source_info['url'],
                        language="en",
                        metadata=self.create_sfa_metadata(title, content, 'food_retail', source_info['url'])
                    )
                    
                    articles.append(article)
                    self.stats['food_retail'] += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing food retail section: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping food retail licensing: {e}")
        
        return articles
    
    def scrape_wholesale_markets(self) -> List[SFAArticle]:
        """Scrape wholesale markets & fishery ports - Tertiary source (15% priority)"""
        articles = []
        source_info = self.priority_sources['wholesale_markets']
        
        for url in source_info['urls']:
            try:
                logger.info(f"Scraping Wholesale Markets: {url}")
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract main content
                content_div = soup.find(['div', 'main'], class_=re.compile(r'content|main', re.I))
                if not content_div:
                    content_div = soup.find('body')
                
                if content_div:
                    title = soup.find('title').get_text(strip=True) if soup.find('title') else "Wholesale Market Information"
                    content = content_div.get_text(strip=True)
                    
                    if len(content) < 100:
                        continue
                    
                    # Create article
                    article = SFAArticle(
                        id=f"sfa_wholesale_{hash(url)}",
                        source="government_sfa", 
                        text=content,
                        timestamp=datetime.now().strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                        url=url,
                        language="en",
                        metadata=self.create_sfa_metadata(title, content, 'wholesale_markets', url)
                    )
                    
                    articles.append(article)
                    self.stats['wholesale_markets'] += 1
                
            except Exception as e:
                logger.error(f"Error scraping wholesale market {url}: {e}")
                continue
        
        return articles
    
    def scrape_agricultural_land(self) -> List[SFAArticle]:
        """Scrape agricultural land allocation info - Supplementary source (5% priority)"""
        articles = []
        
        try:
            logger.info("Scraping Agricultural Land information from newsroom")
            
            # Agricultural land info typically appears in newsroom
            response = self.session.get(self.priority_sources['newsroom']['web_url'], timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for agricultural/farming related content
            agri_links = soup.find_all('a', string=re.compile(r'farming|agricultural|land.*tender', re.I))
            
            for link in agri_links[:3]:  # Limit to recent items
                try:
                    href = link.get('href', '')
                    if not href:
                        continue
                    
                    if href.startswith('/'):
                        href = urljoin('https://www.sfa.gov.sg', href)
                    
                    # Get full content
                    content = self.get_article_content(href)
                    if not content:
                        continue
                    
                    title = link.get_text(strip=True)
                    
                    # Create article
                    article = SFAArticle(
                        id=f"sfa_agricultural_{hash(href)}",
                        source="government_sfa",
                        text=content,
                        timestamp=datetime.now().strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                        url=href,
                        language="en",
                        metadata=self.create_sfa_metadata(title, content, 'agricultural_land', href)
                    )
                    
                    articles.append(article)
                    self.stats['agricultural_land'] += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing agricultural land item: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping agricultural land: {e}")
        
        return articles
    
    def get_article_content(self, url: str) -> str:
        """Get full article content from URL"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer']):
                element.decompose()
            
            # Find main content
            content_selectors = [
                'article', '.content', '.main-content', '#content',
                '.post-content', '.entry-content', 'main'
            ]
            
            content = ""
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    content = content_elem.get_text(strip=True)
                    break
            
            if not content:
                content = soup.get_text(strip=True)
            
            return content[:5000]  # Limit content length
            
        except Exception as e:
            logger.warning(f"Error getting article content from {url}: {e}")
            return ""
    
    def scrape_all_sources(self) -> List[SFAArticle]:
        """Scrape all priority data sources"""
        all_articles = []
        
        logger.info("Starting SFA scraping with priority sources...")
        
        # Scrape by priority
        logger.info("1. Scraping Newsroom (50% priority)")
        all_articles.extend(self.scrape_newsroom_rss())
        
        logger.info("2. Scraping Food Retail Licensing (30% priority)")
        all_articles.extend(self.scrape_food_retail_licensing())
        
        logger.info("3. Scraping Wholesale Markets (15% priority)")
        all_articles.extend(self.scrape_wholesale_markets())
        
        logger.info("4. Scraping Agricultural Land (5% priority)")
        all_articles.extend(self.scrape_agricultural_land())
        
        # Update total stats
        self.stats['total_articles'] = len(all_articles)
        
        logger.info(f"Scraping completed. Total articles: {len(all_articles)}")
        logger.info(f"Distribution - Newsroom: {self.stats['newsroom']}, Food Retail: {self.stats['food_retail']}, Wholesale: {self.stats['wholesale_markets']}, Agricultural: {self.stats['agricultural_land']}")
        
        return all_articles
    
    def save_articles(self, articles: List[SFAArticle]):
        """Save articles to JSONL files by category with relative paths"""
        if not articles:
            logger.warning("No articles to save")
            return
        
        # Group articles by source type
        grouped_articles = {
            'newsroom': [],
            'food_retail': [],
            'wholesale_markets': [],
            'agricultural_land': []
        }
        
        for article in articles:
            source_type = article.metadata.get('category', 'general')
            if 'food_infrastructure' in source_type or 'newsroom' in article.id:
                grouped_articles['newsroom'].append(article)
            elif 'food_retail' in article.id:
                grouped_articles['food_retail'].append(article)
            elif 'wholesale' in source_type or 'wholesale' in article.id:
                grouped_articles['wholesale_markets'].append(article)
            elif 'agricultural' in source_type or 'agricultural' in article.id:
                grouped_articles['agricultural_land'].append(article)
            else:
                grouped_articles['newsroom'].append(article)  # Default to newsroom
        
        # Save each category to separate JSONL files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for category, category_articles in grouped_articles.items():
            if category_articles:
                filename = f"sfa_{category}_{timestamp}.jsonl"
                filepath = self.output_dir / filename
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    for article in category_articles:
                        json.dump(article.to_dict(), f, ensure_ascii=False, default=str)
                        f.write('\n')
                
                logger.info(f"Saved {len(category_articles)} {category} articles to {filepath}")
        
        # Save all articles combined
        all_filename = f"sfa_articles_{timestamp}.jsonl"
        all_filepath = self.output_dir / all_filename
        
        with open(all_filepath, 'w', encoding='utf-8') as f:
            for article in articles:
                json.dump(article.to_dict(), f, ensure_ascii=False, default=str)
                f.write('\n')
        
        logger.info(f"Saved {len(articles)} total articles to {all_filepath}")
        
        # Save statistics
        stats_filename = "sfa_scraping_stats.json"
        stats_filepath = self.output_dir / stats_filename
        
        with open(stats_filepath, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved scraping statistics to {stats_filepath}")
        logger.info(f"Data saved to relative path: {self.output_dir}")

def main():
    """Main execution function"""
    logger.info("Starting SFA scraper")
    logger.info("Date range: 2023-2025")
    logger.info("Priority sources: Newsroom (50%), Food Retail (30%), Wholesale Markets (15%), Agricultural Land (5%)")
    
    # Initialize scraper with relative path
    scraper = SFAScraper(output_dir="data/raw/government/sfa")
    
    # Scrape all sources
    articles = scraper.scrape_all_sources()
    
    # Save articles
    scraper.save_articles(articles)
    
    logger.info("SFA scraping completed successfully!")
    logger.info(f"Articles saved to: {scraper.output_dir}")

if __name__ == "__main__":
    main()