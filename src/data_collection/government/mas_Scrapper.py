"""
MAS (Monetary Authority of Singapore) Web Scraper for PropInsight
Author: Prem Varijakzhan
Date: 2025

This scraper extracts property-related financial regulatory content from MAS website:
- Media Releases (40% priority) - Policy announcements, ABSD/TDSR changes
- Banking Regulations (35% priority) - Property loan requirements
- Macroprudential Policies (15% priority) - Policy framework and history
- Parliamentary/Speeches (10% priority) - Context and explanations

Data is saved to: data/raw/government/mas/
"""

import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Set
import time
import re
from urllib.parse import urljoin, urlparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class MASArticle:
    """Data class for MAS articles with MAS-specific metadata"""
    id: str
    source: str
    text: str
    timestamp: str
    url: str
    language: str
    metadata: Dict

class MASScraper:
    def __init__(self):
        """Initialize MAS scraper with priority URLs and configuration"""
        # Use relative path from project root as requested
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        self.output_dir = os.path.join(project_root, "data", "raw", "government", "mas")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Date range: 2023-2025 
        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2025, 12, 31)
        
        # Priority URLs 
        self.priority_urls = {
            # HIGH PRIORITY - Media Releases (40% of content)
            "media_releases": [
                "https://www.mas.gov.sg/news/media-releases",
                "https://www.mas.gov.sg/news?q=property",
                "https://www.mas.gov.sg/news?q=ABSD",
                "https://www.mas.gov.sg/news?q=TDSR",
                "https://www.mas.gov.sg/news?q=lending"
            ],
            # HIGH PRIORITY - Banking Regulations (35% of content)
            "banking_regulations": [
                "https://www.mas.gov.sg/regulation/banking/notices",
                "https://www.mas.gov.sg/regulation/banking/regulations-and-guidance",
                "https://www.mas.gov.sg/regulation/notices/notice-645",  # TDSR computation
                "https://www.mas.gov.sg/regulation/notices/notice-831",  # Property loans
                "https://www.mas.gov.sg/regulation/notices/notice-1115"  # Housing loans
            ],
            # MEDIUM PRIORITY - Macroprudential Policies (15% of content)
            "macroprudential": [
                "https://www.mas.gov.sg/publications/macroprudential-policies-in-singapore"
            ],
            # LOW PRIORITY - Parliamentary/Speeches (10% of content)
            "parliamentary_speeches": [
                "https://www.mas.gov.sg/news/parliamentary-replies",
                "https://www.mas.gov.sg/news/speeches"
            ]
        }
        
        # Property-related keywords for filtering
        self.property_keywords = {
            'high_priority': ['property', 'ABSD', 'TDSR', 'LTV', 'MSR', 'stamp duty', 'lending measures', 'macroprudential'],
            'medium_priority': ['housing', 'residential', 'mortgage', 'loan', 'borrower', 'debt servicing'],
            'low_priority': ['real estate', 'investment', 'cooling measures', 'market stability']
        }
        
        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Statistics tracking
        self.stats = {
            'media_releases': 0,
            'banking_regulations': 0,
            'macroprudential': 0,
            'parliamentary_speeches': 0,
            'total_articles': 0,
            'policy_types': {},
            'affected_measures': {}
        }

    def is_property_related(self, text: str, title: str = "") -> tuple[bool, str]:
        """Check if content is property-related and return priority level"""
        combined_text = f"{title} {text}".lower()
        
        # Check high priority keywords first
        for keyword in self.property_keywords['high_priority']:
            if keyword.lower() in combined_text:
                return True, 'high'
        
        # Check medium priority keywords
        for keyword in self.property_keywords['medium_priority']:
            if keyword.lower() in combined_text:
                return True, 'medium'
        
        # Check low priority keywords
        for keyword in self.property_keywords['low_priority']:
            if keyword.lower() in combined_text:
                return True, 'low'
        
        return False, 'none'

    def extract_mas_metadata(self, soup: BeautifulSoup, url: str, category: str) -> Dict:
        """Extract MAS-specific metadata from article"""
        metadata = {
            'agency': 'MAS',
            'category': category,
            'content_length': len(soup.get_text()),
            'keywords': []
        }
        
        # Extract title
        title_elem = soup.find('h1') or soup.find('title')
        if title_elem:
            metadata['title'] = title_elem.get_text().strip()
        
        # Determine policy type based on content and URL
        text_content = soup.get_text().lower()
        
        if 'media-release' in url or 'news' in url:
            metadata['policy_type'] = 'policy_announcement'
            if 'absd' in text_content:
                metadata['policy_subtype'] = 'absd_adjustment'
            elif 'tdsr' in text_content:
                metadata['policy_subtype'] = 'tdsr_framework'
            elif 'cooling' in text_content:
                metadata['policy_subtype'] = 'cooling_measures'
            elif 'lending' in text_content:
                metadata['policy_subtype'] = 'lending_measures'
        elif 'regulation' in url or 'notice' in url:
            metadata['policy_type'] = 'regulatory_requirement'
            if 'tdsr' in text_content:
                metadata['policy_subtype'] = 'tdsr_computation'
            elif 'property loan' in text_content:
                metadata['policy_subtype'] = 'property_lending'
            elif 'housing loan' in text_content:
                metadata['policy_subtype'] = 'housing_lending'
        elif 'macroprudential' in url:
            metadata['policy_type'] = 'macroprudential_policy'
            metadata['policy_subtype'] = 'policy_framework'
        elif 'parliamentary' in url or 'speech' in url:
            metadata['policy_type'] = 'policy_explanation'
            metadata['policy_subtype'] = 'parliamentary_reply' if 'parliamentary' in url else 'speech'
        
        # Extract affected measures
        affected_measures = []
        if 'absd' in text_content:
            affected_measures.append('ABSD')
        if 'tdsr' in text_content:
            affected_measures.append('TDSR')
        if 'ltv' in text_content:
            affected_measures.append('LTV')
        if 'msr' in text_content:
            affected_measures.append('MSR')
        if affected_measures:
            metadata['affected_measures'] = affected_measures
        
        # Extract borrower segments
        borrower_segments = []
        if 'private property' in text_content:
            borrower_segments.append('private_property')
        if 'hdb' in text_content:
            borrower_segments.append('hdb_upgraders')
        if 'first-time' in text_content:
            borrower_segments.append('first_time_buyers')
        if 'investor' in text_content:
            borrower_segments.append('investors')
        if borrower_segments:
            metadata['borrower_segments'] = borrower_segments
        
        # Extract financial institutions affected
        if 'bank' in text_content:
            metadata['financial_institutions_affected'] = ['banks']
            if 'finance companies' in text_content:
                metadata['financial_institutions_affected'].append('finance_companies')
        
        # Extract effective date if mentioned
        date_pattern = r'effective\s+(?:from\s+)?(\d{1,2}\s+\w+\s+\d{4})'
        date_match = re.search(date_pattern, text_content, re.IGNORECASE)
        if date_match:
            try:
                effective_date = datetime.strptime(date_match.group(1), '%d %B %Y')
                metadata['effective_date'] = effective_date.strftime('%Y-%m-%dT00:00:00+08:00')
            except:
                pass
        
        # Extract keywords
        keywords = []
        for keyword_list in self.property_keywords.values():
            for keyword in keyword_list:
                if keyword.lower() in text_content:
                    keywords.append(keyword)
        metadata['keywords'] = list(set(keywords))
        
        # Risk management indicator
        if any(term in text_content for term in ['prudent', 'risk', 'stability', 'sustainable']):
            metadata['risk_management'] = True
        
        return metadata

    def scrape_media_releases(self) -> List[MASArticle]:
        """Scrape MAS media releases (40% priority)"""
        articles = []
        logger.info("Scraping MAS media releases...")
        
        for url in self.priority_urls['media_releases']:
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find article links
                article_links = soup.find_all('a', href=True)
                
                for link in article_links:
                    href = link.get('href')
                    if not href:
                        continue
                    
                    # Convert relative URLs to absolute
                    full_url = urljoin(url, href)
                    
                    # Skip if not a media release or news article
                    if not any(pattern in full_url for pattern in ['/news/', '/media-release']):
                        continue
                    
                    # Get article title for initial filtering
                    title = link.get_text().strip()
                    if not title:
                        continue
                    
                    # Quick property relevance check
                    is_relevant, priority = self.is_property_related("", title)
                    if not is_relevant:
                        continue
                    
                    # Scrape individual article
                    article = self.scrape_individual_article(full_url, 'media_release')
                    if article:
                        articles.append(article)
                        self.stats['media_releases'] += 1
                        time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error scraping media releases from {url}: {e}")
                continue
        
        return articles

    def scrape_banking_regulations(self) -> List[MASArticle]:
        """Scrape MAS banking regulations (35% priority)"""
        articles = []
        logger.info("Scraping MAS banking regulations...")
        
        for url in self.priority_urls['banking_regulations']:
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # For regulation pages, look for notice links and documents
                regulation_links = soup.find_all('a', href=True)
                
                for link in regulation_links:
                    href = link.get('href')
                    if not href:
                        continue
                    
                    full_url = urljoin(url, href)
                    title = link.get_text().strip()
                    
                    # Focus on property-related regulations
                    if not any(term in title.lower() for term in ['property', 'housing', 'tdsr', 'loan']):
                        continue
                    
                    # Skip PDF files for now
                    if full_url.endswith('.pdf'):
                        continue
                    
                    article = self.scrape_individual_article(full_url, 'banking_regulation')
                    if article:
                        articles.append(article)
                        self.stats['banking_regulations'] += 1
                        time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error scraping banking regulations from {url}: {e}")
                continue
        
        return articles

    def scrape_macroprudential_policies(self) -> List[MASArticle]:
        """Scrape MAS macroprudential policies (15% priority)"""
        articles = []
        logger.info("Scraping MAS macroprudential policies...")
        
        for url in self.priority_urls['macroprudential']:
            try:
                article = self.scrape_individual_article(url, 'macroprudential_policy')
                if article:
                    articles.append(article)
                    self.stats['macroprudential'] += 1
                
            except Exception as e:
                logger.error(f"Error scraping macroprudential policies from {url}: {e}")
                continue
        
        return articles

    def scrape_parliamentary_speeches(self) -> List[MASArticle]:
        """Scrape MAS parliamentary replies and speeches (10% priority)"""
        articles = []
        logger.info("Scraping MAS parliamentary replies and speeches...")
        
        for url in self.priority_urls['parliamentary_speeches']:
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find speech/reply links
                content_links = soup.find_all('a', href=True)
                
                for link in content_links:
                    href = link.get('href')
                    if not href:
                        continue
                    
                    full_url = urljoin(url, href)
                    title = link.get_text().strip()
                    
                    # Check if property-related
                    is_relevant, priority = self.is_property_related("", title)
                    if not is_relevant:
                        continue
                    
                    article = self.scrape_individual_article(full_url, 'parliamentary_speech')
                    if article:
                        articles.append(article)
                        self.stats['parliamentary_speeches'] += 1
                        time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error scraping parliamentary content from {url}: {e}")
                continue
        
        return articles

    def scrape_individual_article(self, url: str, category: str) -> Optional[MASArticle]:
        """Scrape individual article and return MASArticle object"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract main content
            content_selectors = [
                '.content-body',
                '.article-content',
                '.news-content',
                'main',
                '.main-content',
                'article'
            ]
            
            content_elem = None
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    break
            
            if not content_elem:
                content_elem = soup.find('body')
            
            if not content_elem:
                return None
            
            # Clean and extract text
            for script in content_elem(["script", "style", "nav", "header", "footer"]):
                script.decompose()
            
            text_content = content_elem.get_text()
            text_content = ' '.join(text_content.split())
            
            if len(text_content) < 100:  # Skip very short content
                return None
            
            # Check if property-related
            title = soup.find('h1')
            title_text = title.get_text().strip() if title else ""
            is_relevant, priority = self.is_property_related(text_content, title_text)
            
            if not is_relevant:
                return None
            
            # Extract metadata
            metadata = self.extract_mas_metadata(soup, url, category)
            
            # Update statistics
            policy_type = metadata.get('policy_type', 'unknown')
            self.stats['policy_types'][policy_type] = self.stats['policy_types'].get(policy_type, 0) + 1
            
            if 'affected_measures' in metadata:
                for measure in metadata['affected_measures']:
                    self.stats['affected_measures'][measure] = self.stats['affected_measures'].get(measure, 0) + 1
            
            # Generate article ID
            url_hash = abs(hash(url)) % (10**8)
            article_id = f"mas_{category}_{url_hash}"
            
            # Create timestamp (use current time as we don't have exact publication dates)
            timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+08:00')
            
            return MASArticle(
                id=article_id,
                source="government_mas",
                text=text_content,
                timestamp=timestamp,
                url=url,
                language="en",
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error scraping article {url}: {e}")
            return None

    def save_articles(self, articles: List[MASArticle], filename: str):
        """Save articles to JSONL file"""
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for article in articles:
                json.dump(asdict(article), f, ensure_ascii=False)
                f.write('\n')
        
        logger.info(f"Saved {len(articles)} articles to {filepath}")

    def save_statistics(self):
        """Save scraping statistics"""
        stats_file = os.path.join(self.output_dir, "mas_scraping_stats.json")
        
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Statistics saved to {stats_file}")

    def run_scraper(self):
        """Main scraper execution following priority order"""
        logger.info("Starting MAS scraper...")
        logger.info(f"Date range: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        logger.info(f"Output directory: {self.output_dir}")
        
        all_articles = []
        
        # Scrape by priority (40% Media Releases, 35% Banking Regulations, 15% Macroprudential, 10% Parliamentary)
        
        # 1. Media Releases (Highest Priority)
        media_articles = self.scrape_media_releases()
        all_articles.extend(media_articles)
        
        # 2. Banking Regulations (High Priority)
        banking_articles = self.scrape_banking_regulations()
        all_articles.extend(banking_articles)
        
        # 3. Macroprudential Policies (Medium Priority)
        macro_articles = self.scrape_macroprudential_policies()
        all_articles.extend(macro_articles)
        
        # 4. Parliamentary/Speeches (Low Priority)
        parliamentary_articles = self.scrape_parliamentary_speeches()
        all_articles.extend(parliamentary_articles)
        
        # Update total statistics
        self.stats['total_articles'] = len(all_articles)
        
        # Save all articles
        if all_articles:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.save_articles(all_articles, f"mas_articles_{timestamp}.jsonl")
            
            # Save by category
            categories = {
                'media_release': [a for a in all_articles if 'media_release' in a.metadata.get('category', '')],
                'banking_regulation': [a for a in all_articles if 'banking_regulation' in a.metadata.get('category', '')],
                'macroprudential_policy': [a for a in all_articles if 'macroprudential' in a.metadata.get('category', '')],
                'parliamentary_speech': [a for a in all_articles if 'parliamentary' in a.metadata.get('category', '')]
            }
            
            for category, articles in categories.items():
                if articles:
                    self.save_articles(articles, f"mas_{category}_{timestamp}.jsonl")
        
        # Save statistics
        self.save_statistics()
        
        # Print summary
        logger.info("=== MAS Scraping Summary ===")
        logger.info(f"Total articles scraped: {self.stats['total_articles']}")
        logger.info(f"Media releases: {self.stats['media_releases']}")
        logger.info(f"Banking regulations: {self.stats['banking_regulations']}")
        logger.info(f"Macroprudential policies: {self.stats['macroprudential']}")
        logger.info(f"Parliamentary/speeches: {self.stats['parliamentary_speeches']}")
        
        if self.stats['policy_types']:
            logger.info("Policy types found:")
            for policy_type, count in self.stats['policy_types'].items():
                logger.info(f"  {policy_type}: {count}")
        
        if self.stats['affected_measures']:
            logger.info("Affected measures found:")
            for measure, count in self.stats['affected_measures'].items():
                logger.info(f"  {measure}: {count}")
        
        logger.info(f"Data saved to: {self.output_dir}")
        logger.info("MAS scraping completed!")

if __name__ == "__main__":
    scraper = MASScraper()
    scraper.run_scraper()