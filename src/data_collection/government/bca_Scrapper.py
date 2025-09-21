"""
BCA (Building and Construction Authority) Web Scraper for PropInsight

This scraper collects construction industry data from BCA's website following the priority sources:
1. Media Releases (40%) - Construction industry announcements, awards, regulations
2. Construction InfoNet Data (35%) - Construction Price Index, Tender Price Index, Material costs
3. Productivity Reports (15%) - Industry efficiency metrics, construction timeline factors
4. Regulatory/Sustainability Updates (10%) - Building standards, green requirements

Data is saved to: data/raw/government/bca/
Date Range: 2023-2025
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import random
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any
import re
import os
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class BCAArticle:
    """Data structure for BCA articles following PropInsight JSON schema"""
    id: str
    source: str
    text: str
    timestamp: str
    url: str
    language: str
    metadata: Dict[str, Any]

class BCAScraper:
    """BCA Website Scraper with priority-based data collection"""
    
    def __init__(self):
        # Get project root directory (3 levels up from current file)
        current_file = Path(__file__).resolve()
        self.project_root = current_file.parent.parent.parent.parent
        
        # Configure output directory using relative path
        self.output_dir = self.project_root / "data" / "raw" / "government" / "bca"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Date range for scraping (2023-2025)
        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2025, 12, 31)
        
        # Priority URLs 
        self.priority_urls = {
            # HIGH PRIORITY - Media Releases (40%)
            "media_releases": {
                "url": "https://www1.bca.gov.sg/about-us/news-and-publications/media-releases",
                "weight": 0.40,
                "category": "media_releases"
            },
            # HIGH PRIORITY - Construction InfoNet (35%) 
            "construction_data": {
                "url": "https://www1.bca.gov.sg/about-us/news-and-publications/publications-reports/industry-information",
                "weight": 0.35,
                "category": "construction_data"
            },
            # MEDIUM PRIORITY - Productivity Reports (15%)
            "productivity": {
                "url": "https://www1.bca.gov.sg/buildsg/productivity/site-productivity/measuring-project-productivity",
                "weight": 0.15,
                "category": "productivity_reports"
            },
            # LOW PRIORITY - Regulatory Updates (10%)
            "regulatory": {
                "url": "https://www1.bca.gov.sg/buildsg/sustainability",
                "weight": 0.10,
                "category": "regulatory_updates"
            }
        }
        
        # BCA-specific keywords for content filtering
        self.bca_keywords = [
            "construction cost", "price index", "tender price", "material cost",
            "productivity", "building standards", "green mark", "sustainability",
            "construction industry", "building regulation", "safety standards",
            "construction demand", "industry transformation", "digital construction",
            "prefabrication", "automation", "construction technology"
        ]
        
        # Request headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Statistics tracking
        self.stats = {
            "total_articles": 0,
            "by_category": {},
            "by_date": {},
            "scraping_errors": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None
        }

    def extract_bca_metadata(self, soup: BeautifulSoup, url: str, category: str) -> Dict[str, Any]:
        """Extract BCA-specific metadata from article content"""
        
        # Base metadata structure
        metadata = {
            "agency": "BCA",
            "category": category,
            "content_length": 0,
            "keywords": []
        }
        
        # Extract title
        title_elem = soup.find('h1') or soup.find('title') or soup.find('h2')
        if title_elem:
            metadata["title"] = title_elem.get_text().strip()
        
        # Extract content for analysis
        content_text = ""
        content_elem = soup.find('div', class_='content') or soup.find('article') or soup.find('main')
        if content_elem:
            content_text = content_elem.get_text()
        else:
            content_text = soup.get_text()
        
        metadata["content_length"] = len(content_text)
        
        # Category-specific metadata extraction
        if category == "media_releases":
            metadata.update(self._extract_media_release_metadata(content_text, soup))
        elif category == "construction_data":
            metadata.update(self._extract_construction_data_metadata(content_text, soup))
        elif category == "productivity_reports":
            metadata.update(self._extract_productivity_metadata(content_text, soup))
        elif category == "regulatory_updates":
            metadata.update(self._extract_regulatory_metadata(content_text, soup))
        
        # Extract keywords
        found_keywords = []
        content_lower = content_text.lower()
        for keyword in self.bca_keywords:
            if keyword.lower() in content_lower:
                found_keywords.append(keyword)
        metadata["keywords"] = found_keywords[:10]  # Limit to top 10 keywords
        
        return metadata

    def _extract_media_release_metadata(self, content: str, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract metadata specific to BCA media releases"""
        metadata = {
            "policy_type": "industry_announcement",
            "policy_subtype": "media_release"
        }
        
        # Look for press release ID
        pr_id_match = re.search(r'BCA[_\s]*(\d{4})[_\s]*(\d+)', content, re.IGNORECASE)
        if pr_id_match:
            metadata["press_release_id"] = f"BCA_{pr_id_match.group(1)}_{pr_id_match.group(2)}"
        
        # Extract cost/price information
        price_match = re.search(r'(\d+\.?\d*)%?\s*(?:increase|decrease|change)', content, re.IGNORECASE)
        if price_match:
            metadata["price_change_percent"] = float(price_match.group(1))
        
        # Identify key materials mentioned
        materials = []
        material_keywords = ["steel", "concrete", "cement", "sand", "timber", "labor", "labour"]
        for material in material_keywords:
            if material in content.lower():
                materials.append(material)
        if materials:
            metadata["key_materials"] = materials
        
        # Identify cost drivers
        cost_drivers = []
        driver_keywords = ["supply chain", "material shortage", "labor costs", "inflation", "demand"]
        for driver in driver_keywords:
            if driver in content.lower():
                cost_drivers.append(driver.replace(" ", "_"))
        if cost_drivers:
            metadata["cost_drivers"] = cost_drivers
        
        # Industry impact assessment
        impact_keywords = ["developers", "contractors", "buyers", "construction industry"]
        industry_impact = []
        for impact in impact_keywords:
            if impact in content.lower():
                industry_impact.append(impact.replace(" ", "_"))
        if industry_impact:
            metadata["industry_impact"] = industry_impact
        
        return metadata

    def _extract_construction_data_metadata(self, content: str, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract metadata specific to construction data and price indices"""
        metadata = {
            "policy_type": "market_data",
            "policy_subtype": "price_index"
        }
        
        # Extract reporting period
        period_match = re.search(r'Q(\d)\s*(\d{4})', content)
        if period_match:
            metadata["reporting_period"] = f"Q{period_match.group(1)}_{period_match.group(2)}"
        
        # Extract index values
        index_match = re.search(r'index.*?(\d+\.?\d*)', content, re.IGNORECASE)
        if index_match:
            metadata["index_value"] = float(index_match.group(1))
        
        # Market outlook indicators
        if "increase" in content.lower() or "rising" in content.lower():
            metadata["market_outlook"] = "inflationary_pressure"
        elif "decrease" in content.lower() or "falling" in content.lower():
            metadata["market_outlook"] = "deflationary_pressure"
        else:
            metadata["market_outlook"] = "stable"
        
        return metadata

    def _extract_productivity_metadata(self, content: str, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract metadata specific to productivity reports"""
        metadata = {
            "policy_type": "industry_metrics",
            "policy_subtype": "productivity_index"
        }
        
        # Extract productivity values
        productivity_match = re.search(r'(\d+\.?\d*)\s*m²?\s*per\s*manday', content, re.IGNORECASE)
        if productivity_match:
            metadata["overall_productivity"] = float(productivity_match.group(1))
        
        # Extract HDB productivity
        hdb_match = re.search(r'hdb.*?(\d+\.?\d*)\s*m²?\s*per\s*manday', content, re.IGNORECASE)
        if hdb_match:
            metadata["hdb_productivity"] = float(hdb_match.group(1))
        
        # Extract private residential productivity
        private_match = re.search(r'private.*?residential.*?(\d+\.?\d*)\s*m²?\s*per\s*manday', content, re.IGNORECASE)
        if private_match:
            metadata["private_residential_productivity"] = float(private_match.group(1))
        
        # Productivity trend
        if "improving" in content.lower() or "increase" in content.lower():
            metadata["productivity_trend"] = "improving"
        elif "declining" in content.lower() or "decrease" in content.lower():
            metadata["productivity_trend"] = "declining"
        else:
            metadata["productivity_trend"] = "stable"
        
        # Efficiency factors
        efficiency_factors = []
        factor_keywords = ["automation", "prefabrication", "digitalization", "planning", "technology"]
        for factor in factor_keywords:
            if factor in content.lower():
                efficiency_factors.append(factor)
        if efficiency_factors:
            metadata["efficiency_factors"] = efficiency_factors
        
        return metadata

    def _extract_regulatory_metadata(self, content: str, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract metadata specific to regulatory and sustainability updates"""
        metadata = {
            "policy_type": "regulatory_update",
            "policy_subtype": "building_standards"
        }
        
        # Green building indicators
        if "green mark" in content.lower():
            metadata["green_building_program"] = "green_mark"
        if "zero energy" in content.lower():
            metadata["sustainability_program"] = "zero_energy_building"
        
        # Regulatory compliance
        compliance_keywords = ["mandatory", "requirement", "standard", "regulation"]
        for keyword in compliance_keywords:
            if keyword in content.lower():
                metadata["compliance_type"] = "mandatory"
                break
        else:
            metadata["compliance_type"] = "voluntary"
        
        return metadata

    def scrape_priority_source(self, source_name: str, source_config: Dict) -> List[BCAArticle]:
        """Scrape articles from a priority source"""
        articles = []
        
        try:
            logger.info(f"Scraping {source_name} from {source_config['url']}")
            
            response = requests.get(source_config['url'], headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find article links (adapt selectors based on BCA website structure)
            article_links = []
            
            # Common selectors for BCA articles
            link_selectors = [
                'a[href*="/media-releases/"]',
                'a[href*="/news/"]', 
                'a[href*="/publications/"]',
                'a[href*="/reports/"]',
                '.news-item a',
                '.publication-item a',
                '.media-release a'
            ]
            
            for selector in link_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href:
                        if href.startswith('/'):
                            href = 'https://www1.bca.gov.sg' + href
                        article_links.append(href)
            
            # Remove duplicates
            article_links = list(set(article_links))
            
            # Limit articles based on priority weight
            max_articles = int(20 * source_config['weight'])  # Scale based on priority
            article_links = article_links[:max_articles]
            
            logger.info(f"Found {len(article_links)} article links for {source_name}")
            
            # Scrape individual articles
            for i, article_url in enumerate(article_links):
                try:
                    article = self.scrape_article(article_url, source_config['category'])
                    if article and self.is_date_in_range(article.timestamp):
                        articles.append(article)
                        logger.info(f"Scraped article {i+1}/{len(article_links)}: {article.metadata.get('title', 'No title')}")
                    
                    # Rate limiting
                    time.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    logger.error(f"Error scraping article {article_url}: {str(e)}")
                    self.stats["scraping_errors"] += 1
                    continue
        
        except Exception as e:
            logger.error(f"Error scraping {source_name}: {str(e)}")
            self.stats["scraping_errors"] += 1
        
        return articles

    def scrape_article(self, url: str, category: str) -> Optional[BCAArticle]:
        """Scrape individual article from BCA website"""
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract article content
            content_selectors = [
                '.content-body',
                '.article-content', 
                '.news-content',
                '.publication-content',
                'main',
                'article'
            ]
            
            content_text = ""
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    content_text = content_elem.get_text(strip=True)
                    break
            
            if not content_text:
                content_text = soup.get_text(strip=True)
            
            # Filter by keywords
            if not any(keyword.lower() in content_text.lower() for keyword in self.bca_keywords):
                return None
            
            # Extract timestamp
            timestamp = self.extract_timestamp(soup, content_text)
            
            # Generate article ID
            article_id = f"bca_{category}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(url) % 10000}"
            
            # Extract metadata
            metadata = self.extract_bca_metadata(soup, url, category)
            
            # Create article object
            article = BCAArticle(
                id=article_id,
                source="government_bca",
                text=content_text,
                timestamp=timestamp,
                url=url,
                language="en",
                metadata=metadata
            )
            
            return article
            
        except Exception as e:
            logger.error(f"Error scraping article {url}: {str(e)}")
            return None

    def extract_timestamp(self, soup: BeautifulSoup, content: str) -> str:
        """Extract timestamp from article"""
        # Try various date selectors
        date_selectors = [
            '.date',
            '.publish-date',
            '.article-date',
            '.news-date',
            'time',
            '[datetime]'
        ]
        
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                date_text = date_elem.get('datetime') or date_elem.get_text()
                try:
                    # Parse various date formats
                    for fmt in ['%Y-%m-%d', '%d %B %Y', '%B %d, %Y', '%d/%m/%Y']:
                        try:
                            parsed_date = datetime.strptime(date_text.strip(), fmt)
                            return parsed_date.strftime('%Y-%m-%dT%H:%M:%S+08:00')
                        except ValueError:
                            continue
                except:
                    pass
        
        # Fallback to current timestamp
        return datetime.now().strftime('%Y-%m-%dT%H:%M:%S+08:00')

    def is_date_in_range(self, timestamp: str) -> bool:
        """Check if article date is within target range (2023-2025)"""
        try:
            article_date = datetime.fromisoformat(timestamp.replace('+08:00', ''))
            return self.start_date <= article_date <= self.end_date
        except:
            return True  # Include if date parsing fails

    def save_articles(self, articles: List[BCAArticle], category: str):
        """Save articles to JSONL files"""
        if not articles:
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"bca_{category}_{timestamp}.jsonl"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for article in articles:
                json.dump(asdict(article), f, ensure_ascii=False)
                f.write('\n')
        
        logger.info(f"Saved {len(articles)} articles to {filepath}")
        
        # Update statistics
        self.stats["by_category"][category] = len(articles)

    def save_all_articles(self, all_articles: List[BCAArticle]):
        """Save all articles to a combined file"""
        if not all_articles:
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"bca_articles_{timestamp}.jsonl"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for article in all_articles:
                json.dump(asdict(article), f, ensure_ascii=False)
                f.write('\n')
        
        logger.info(f"Saved {len(all_articles)} total articles to {filepath}")

    def save_statistics(self):
        """Save scraping statistics"""
        self.stats["end_time"] = datetime.now().isoformat()
        self.stats["total_articles"] = sum(self.stats["by_category"].values())
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        stats_file = self.output_dir / "bca_scraping_stats.json"
        
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved scraping statistics to {stats_file}")

    def run(self):
        """Main scraping execution"""
        logger.info("Starting BCA scraper...")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Date range: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        
        all_articles = []
        
        # Scrape each priority source
        for source_name, source_config in self.priority_urls.items():
            logger.info(f"\n--- Scraping {source_name} (Priority: {source_config['weight']*100}%) ---")
            
            articles = self.scrape_priority_source(source_name, source_config)
            
            if articles:
                # Save category-specific file
                self.save_articles(articles, source_config['category'])
                all_articles.extend(articles)
                
                logger.info(f"Collected {len(articles)} articles from {source_name}")
            else:
                logger.warning(f"No articles collected from {source_name}")
        
        # Save combined file
        if all_articles:
            self.save_all_articles(all_articles)
        
        # Save statistics
        self.save_statistics()
        
        # Print summary
        logger.info(f"\n=== BCA Scraping Complete ===")
        logger.info(f"Total articles collected: {len(all_articles)}")
        for category, count in self.stats["by_category"].items():
            logger.info(f"  {category}: {count} articles")
        logger.info(f"Scraping errors: {self.stats['scraping_errors']}")
        logger.info(f"Files saved to: {self.output_dir}")

if __name__ == "__main__":
    scraper = BCAScraper()
    scraper.run()