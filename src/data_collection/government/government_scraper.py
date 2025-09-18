"""
PropInsight Government RSS Scraper

This module scrapes property-related announcements from Singapore government agencies:
- Ministry of National Development (MND)
- Housing & Development Board (HDB) 
- Urban Redevelopment Authority (URA)

We use RSS feeds because:
1. Official, structured data source
2. Real-time updates on policy changes
3. High-quality, authoritative content
4. Consistent format across agencies

Target: 2,000 samples from government sources
Time Range: September 2023 - September 2025 (24 months)
"""

import feedparser
import requests
import json
import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class GovernmentArticle:
    """Data structure for government articles - ensures consistent data format"""
    id: str
    title: str
    content: str
    summary: str
    published_date: datetime
    source_agency: str
    url: str
    category: str
    tags: List[str]
    
class GovernmentScraper:
    """
    Government RSS scraper for Singapore property announcements
    
    Scrapes from official government RSS feeds and websites
    Focuses on property policy, market updates, and housing announcements
    """
    
    def __init__(self, output_dir: str = "d:\\Test\\New folder\\PropInsight\\data\\raw\\government"):
        """
        Initialize Government scraper
        
        Args:
            output_dir: Directory to save scraped data
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Government RSS feeds and websites
        # These are the primary sources for Singapore property policy updates
        self.sources = {
            'MND': {
                'rss_feeds': [
                    'https://www.mnd.gov.sg/rss/press-releases',
                    'https://www.mnd.gov.sg/rss/news-and-publications'
                ],
                'base_url': 'https://www.mnd.gov.sg',
                'name': 'Ministry of National Development'
            },
            'HDB': {
                'rss_feeds': [
                    'https://www.hdb.gov.sg/cs/infoweb/rss/press-releases',
                    'https://www.hdb.gov.sg/cs/infoweb/rss/news'
                ],
                'base_url': 'https://www.hdb.gov.sg',
                'name': 'Housing & Development Board'
            },
            'URA': {
                'rss_feeds': [
                    'https://www.ura.gov.sg/Corporate/Media/Media-Room/rss/press-releases',
                    'https://www.ura.gov.sg/Corporate/Data/rss/property-market-updates'
                ],
                'base_url': 'https://www.ura.gov.sg',
                'name': 'Urban Redevelopment Authority'
            }
        }
        
        # Property-related keywords for filtering government content
        # These ensure we only collect property-relevant announcements
        self.property_keywords = [
            # Policy terms
            'cooling measures', 'property market', 'housing policy', 'ABSD', 
            'additional buyer stamp duty', 'loan-to-value', 'LTV', 'TDSR',
            'total debt servicing ratio', 'property tax', 'stamp duty',
            
            # Housing programs
            'BTO', 'build-to-order', 'SBF', 'sale of balance flats',
            'resale levy', 'housing grant', 'CPF housing scheme',
            'ethnic integration policy', 'EIP', 'SPR quota',
            
            # Development and planning
            'master plan', 'concept plan', 'land sale', 'GLS', 
            'government land sales', 'development charge', 'plot ratio',
            'urban planning', 'housing development', 'new towns',
            
            # Market data
            'property price index', 'private property', 'public housing',
            'rental market', 'property transaction', 'market outlook',
            'housing supply', 'property demand',
            
            # Specific locations
            'Punggol', 'Tampines', 'Jurong', 'Woodlands', 'Bishan',
            'Toa Payoh', 'Ang Mo Kio', 'Bedok', 'Clementi', 'Pasir Ris'
        ]
        
        # Date range: Last 24 months
        self.start_date = datetime(2023, 9, 1)
        self.end_date = datetime(2025, 9, 18)
        
        # Setup session with proper headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PropInsight Research Bot 1.0 (Academic Research)',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
    
    def is_property_related(self, text: str) -> bool:
        """
        Check if content contains property-related keywords
        
        Args:
            text: Text to check (title + content)
            
        Returns:
            bool: True if property-related
        """
        text_lower = text.lower()
        
        # Check for any property keywords
        for keyword in self.property_keywords:
            if keyword.lower() in text_lower:
                return True
                
        return False
    
    def is_within_date_range(self, published_date: datetime) -> bool:
        """
        Check if article is within our target date range
        
        Args:
            published_date: Article publication date
            
        Returns:
            bool: True if within range
        """
        return self.start_date <= published_date <= self.end_date
    
    def parse_rss_feed(self, rss_url: str, agency: str) -> List[Dict]:
        """
        Parse RSS feed and extract articles
        
        Args:
            rss_url: RSS feed URL
            agency: Government agency name (MND, HDB, URA)
            
        Returns:
            List of article dictionaries
        """
        articles = []
        
        try:
            logger.info(f"Parsing RSS feed: {rss_url}")
            
            # Fetch RSS feed with timeout and error handling
            response = self.session.get(rss_url, timeout=30)
            response.raise_for_status()
            
            # Parse RSS feed
            feed = feedparser.parse(response.content)
            
            if feed.bozo:
                logger.warning(f"RSS feed may have issues: {rss_url}")
            
            for entry in feed.entries:
                try:
                    # Extract publication date
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        pub_date = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        pub_date = datetime(*entry.updated_parsed[:6])
                    else:
                        # Skip entries without dates
                        continue
                    
                    # Check date range
                    if not self.is_within_date_range(pub_date):
                        continue
                    
                    # Extract content
                    title = entry.get('title', '')
                    summary = entry.get('summary', '')
                    content = entry.get('content', [{}])[0].get('value', '') if entry.get('content') else summary
                    
                    # Check if property-related
                    full_text = f"{title} {summary} {content}"
                    if not self.is_property_related(full_text):
                        continue
                    
                    # Create article dictionary
                    article = {
                        'id': entry.get('id', entry.get('link', '')),
                        'title': title,
                        'content': content,
                        'summary': summary,
                        'published_date': pub_date,
                        'source_agency': agency,
                        'url': entry.get('link', ''),
                        'category': entry.get('category', 'General'),
                        'tags': [tag.get('term', '') for tag in entry.get('tags', [])]
                    }
                    
                    articles.append(article)
                    
                except Exception as e:
                    logger.warning(f"Error processing RSS entry: {e}")
                    continue
            
            logger.info(f"Extracted {len(articles)} property-related articles from {rss_url}")
            
        except requests.RequestException as e:
            logger.error(f"Error fetching RSS feed {rss_url}: {e}")
        except Exception as e:
            logger.error(f"Error parsing RSS feed {rss_url}: {e}")
        
        return articles
    
    def scrape_full_article_content(self, article_url: str, agency: str) -> str:
        """
        Scrape full article content from government website
        
        Args:
            article_url: URL of the full article
            agency: Government agency (for agency-specific parsing)
            
        Returns:
            Full article content text
        """
        try:
            response = self.session.get(article_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Agency-specific content extraction
            # Each government website has different HTML structure
            content_selectors = {
                'MND': [
                    '.content-area .entry-content',
                    '.post-content',
                    '.article-content',
                    '.main-content'
                ],
                'HDB': [
                    '.content-wrapper .content',
                    '.article-content',
                    '.press-release-content',
                    '.main-content'
                ],
                'URA': [
                    '.content-area',
                    '.article-body',
                    '.press-release-body',
                    '.main-content'
                ]
            }
            
            # Try agency-specific selectors first
            selectors = content_selectors.get(agency, [])
            selectors.extend([
                # Generic fallback selectors
                'article', '.article', '.content', '.post',
                'main', '.main', '#content', '#main-content'
            ])
            
            content_text = ""
            
            for selector in selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Remove script and style elements
                    for script in content_elem(["script", "style", "nav", "footer", "header"]):
                        script.decompose()
                    
                    content_text = content_elem.get_text(strip=True, separator=' ')
                    break
            
            # If no content found, try getting all paragraph text
            if not content_text:
                paragraphs = soup.find_all('p')
                content_text = ' '.join([p.get_text(strip=True) for p in paragraphs])
            
            return content_text
            
        except Exception as e:
            logger.warning(f"Error scraping full content from {article_url}: {e}")
            return ""
    
    def enhance_articles_with_full_content(self, articles: List[Dict]) -> List[GovernmentArticle]:
        """
        Enhance RSS articles with full content from source pages
        
        Args:
            articles: List of article dictionaries from RSS
            
        Returns:
            List of enhanced GovernmentArticle objects
        """
        enhanced_articles = []
        
        for article in articles:
            try:
                # Get full content if URL is available
                full_content = article['content']
                if article['url'] and len(article['content']) < 500:
                    # Only scrape if current content is short (likely just summary)
                    scraped_content = self.scrape_full_article_content(
                        article['url'], 
                        article['source_agency']
                    )
                    if scraped_content and len(scraped_content) > len(full_content):
                        full_content = scraped_content
                
                # Create enhanced article object
                enhanced_article = GovernmentArticle(
                    id=article['id'],
                    title=article['title'],
                    content=full_content,
                    summary=article['summary'],
                    published_date=article['published_date'],
                    source_agency=article['source_agency'],
                    url=article['url'],
                    category=article['category'],
                    tags=article['tags']
                )
                
                enhanced_articles.append(enhanced_article)
                
                # Rate limiting between requests
                time.sleep(0.5)
                
            except Exception as e:
                logger.warning(f"Error enhancing article {article.get('id', 'unknown')}: {e}")
                continue
        
        return enhanced_articles
    
    def save_articles_to_json(self, articles: List[GovernmentArticle], filename: str):
        """
        Save articles to JSON file with proper formatting
        
        Args:
            articles: List of GovernmentArticle objects
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
                'published_date': article.published_date.isoformat(),
                'source_agency': article.source_agency,
                'url': article.url,
                'category': article.category,
                'tags': article.tags,
                'source': 'government',
                'scraped_at': datetime.now().isoformat(),
                'content_length': len(article.content),
                'word_count': len(article.content.split())
            }
            articles_dict.append(article_dict)
        
        # Save to JSON file
        output_path = self.output_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(articles_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(articles)} articles to {output_path}")
    
    def run_full_scrape(self) -> Dict[str, int]:
        """
        Run complete government RSS scraping process
        
        Returns:
            Dictionary with scraping statistics
        """
        logger.info("Starting Government RSS scraping for PropInsight dataset")
        
        all_articles = []
        stats = {
            'total_articles': 0,
            'mnd_articles': 0,
            'hdb_articles': 0,
            'ura_articles': 0,
            'total_words': 0,
            'failed_feeds': 0
        }
        
        for agency, config in self.sources.items():
            logger.info(f"Scraping {config['name']} ({agency})")
            
            agency_articles = []
            
            # Process each RSS feed for the agency
            for rss_url in config['rss_feeds']:
                try:
                    articles = self.parse_rss_feed(rss_url, agency)
                    agency_articles.extend(articles)
                    time.sleep(1)  # Rate limiting between feeds
                    
                except Exception as e:
                    logger.error(f"Failed to process RSS feed {rss_url}: {e}")
                    stats['failed_feeds'] += 1
            
            # Remove duplicates based on article ID/URL
            unique_articles = {}
            for article in agency_articles:
                key = article['id'] or article['url']
                if key not in unique_articles:
                    unique_articles[key] = article
            
            agency_articles = list(unique_articles.values())
            
            # Enhance articles with full content
            if agency_articles:
                enhanced_articles = self.enhance_articles_with_full_content(agency_articles)
                
                # Update statistics
                stats[f'{agency.lower()}_articles'] = len(enhanced_articles)
                stats['total_words'] += sum(len(article.content.split()) for article in enhanced_articles)
                
                all_articles.extend(enhanced_articles)
                
                # Save agency-specific file
                self.save_articles_to_json(
                    enhanced_articles, 
                    f'government_{agency.lower()}_{datetime.now().strftime("%Y%m%d")}.json'
                )
                
                logger.info(f"Collected {len(enhanced_articles)} articles from {agency}")
        
        # Remove duplicates across all agencies
        unique_all_articles = {}
        for article in all_articles:
            key = article.id or article.url
            if key not in unique_all_articles:
                unique_all_articles[key] = article
        
        final_articles = list(unique_all_articles.values())
        stats['total_articles'] = len(final_articles)
        
        # Save combined file
        if final_articles:
            self.save_articles_to_json(
                final_articles, 
                f'government_combined_{datetime.now().strftime("%Y%m%d")}.json'
            )
        
        # Save statistics
        stats_path = self.output_dir / f'government_stats_{datetime.now().strftime("%Y%m%d")}.json'
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Government scraping completed. Total articles: {stats['total_articles']}")
        return stats

def main():
    """Main function to run Government scraper"""
    scraper = GovernmentScraper()
    stats = scraper.run_full_scrape()
    
    print("\n=== Government RSS Scraping Results ===")
    print(f"Total articles collected: {stats['total_articles']}")
    print(f"Total words collected: {stats['total_words']:,}")
    print(f"MND articles: {stats['mnd_articles']}")
    print(f"HDB articles: {stats['hdb_articles']}")
    print(f"URA articles: {stats['ura_articles']}")
    print(f"Failed feeds: {stats['failed_feeds']}")

if __name__ == "__main__":
    main()