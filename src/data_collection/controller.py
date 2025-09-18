"""
PropInsight Data Collection Controller

This is the master controller that orchestrates parallel execution of all scrapers:
- Reddit scraper (PRAW API)
- Government RSS scraper (MND, HDB, URA)
- PropertyGuru scraper (web scraping with anti-bot measures)
- HardwareZone EDMW scraper (forum discussions)

Key features:
1. Parallel execution using multiprocessing for efficiency
2. Centralized configuration and logging
3. Progress monitoring and statistics collection
4. Error handling and recovery mechanisms
5. Data validation and quality checks
6. Automatic retry logic for failed scraping attempts

Target: 15,000 total samples across all sources
Timeline: 5-7 days for complete data collection
"""

import multiprocessing as mp
import logging
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import queue
import signal
import sys

# Import our scrapers
from reddit.reddit_scraper import RedditScraper
from government.government_scraper import GovernmentScraper
from propertyguru.propertyguru_scraper import PropertyGuruScraper
from hardwarezone.hardwarezone_scraper import HardwareZoneScraper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('propinsight_scraping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ScrapingConfig:
    """Configuration for scraping operations"""
    # Target sample counts per source
    reddit_target: int = 4000
    government_target: int = 1000
    propertyguru_target: int = 5000
    hardwarezone_target: int = 5000
    
    # Parallel execution settings
    max_workers: int = 4
    use_multiprocessing: bool = True
    
    # Retry settings
    max_retries: int = 3
    retry_delay: int = 300  # 5 minutes
    
    # Rate limiting
    requests_per_minute: int = 60
    
    # Output directories
    base_output_dir: str = "data/raw"
    
    # Date range
    start_date: str = "2023-09-01"
    end_date: str = "2025-09-18"

@dataclass
class ScrapingResult:
    """Result from a scraping operation"""
    source: str
    success: bool
    samples_collected: int
    execution_time: float
    error_message: Optional[str] = None
    output_files: List[str] = None
    statistics: Dict[str, Any] = None

class ProgressMonitor:
    """Monitor scraping progress across all sources"""
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.progress = {
            'reddit': {'collected': 0, 'target': config.reddit_target, 'status': 'pending'},
            'government': {'collected': 0, 'target': config.government_target, 'status': 'pending'},
            'propertyguru': {'collected': 0, 'target': config.propertyguru_target, 'status': 'pending'},
            'hardwarezone': {'collected': 0, 'target': config.hardwarezone_target, 'status': 'pending'}
        }
        self.start_time = datetime.now()
        self.lock = threading.Lock()
    
    def update_progress(self, source: str, collected: int, status: str = 'running'):
        """Update progress for a specific source"""
        with self.lock:
            self.progress[source]['collected'] = collected
            self.progress[source]['status'] = status
    
    def get_overall_progress(self) -> Dict[str, Any]:
        """Get overall progress statistics"""
        with self.lock:
            total_collected = sum(p['collected'] for p in self.progress.values())
            total_target = sum(p['target'] for p in self.progress.values())
            
            elapsed_time = datetime.now() - self.start_time
            
            return {
                'total_collected': total_collected,
                'total_target': total_target,
                'completion_percentage': (total_collected / total_target) * 100 if total_target > 0 else 0,
                'elapsed_time': str(elapsed_time),
                'sources': dict(self.progress),
                'estimated_completion': self._estimate_completion_time()
            }
    
    def _estimate_completion_time(self) -> str:
        """Estimate completion time based on current progress"""
        total_collected = sum(p['collected'] for p in self.progress.values())
        total_target = sum(p['target'] for p in self.progress.values())
        
        if total_collected == 0:
            return "Unknown"
        
        elapsed_time = datetime.now() - self.start_time
        rate = total_collected / elapsed_time.total_seconds()
        remaining_samples = total_target - total_collected
        
        if remaining_samples <= 0:
            return "Completed"
        
        estimated_seconds = remaining_samples / rate
        estimated_completion = datetime.now() + timedelta(seconds=estimated_seconds)
        
        return estimated_completion.strftime("%Y-%m-%d %H:%M:%S")

def run_reddit_scraper(config: ScrapingConfig, progress_monitor: ProgressMonitor) -> ScrapingResult:
    """
    Run Reddit scraper in separate process
    
    Reasoning: Reddit API has rate limits, so we run this separately to avoid
    blocking other scrapers. PRAW handles authentication and rate limiting internally.
    """
    start_time = time.time()
    
    try:
        logger.info("Starting Reddit scraper...")
        progress_monitor.update_progress('reddit', 0, 'running')
        
        # Initialize Reddit scraper
        output_dir = os.path.join(config.base_output_dir, 'reddit')
        scraper = RedditScraper(output_dir=output_dir)
        
        # Run scraping
        stats = scraper.run_full_scrape()
        
        execution_time = time.time() - start_time
        samples_collected = stats.get('total_posts', 0)
        
        progress_monitor.update_progress('reddit', samples_collected, 'completed')
        
        return ScrapingResult(
            source='reddit',
            success=True,
            samples_collected=samples_collected,
            execution_time=execution_time,
            statistics=stats,
            output_files=[f"{output_dir}/reddit_combined_{datetime.now().strftime('%Y%m%d')}.json"]
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        progress_monitor.update_progress('reddit', 0, 'failed')
        
        logger.error(f"Reddit scraper failed: {e}")
        return ScrapingResult(
            source='reddit',
            success=False,
            samples_collected=0,
            execution_time=execution_time,
            error_message=str(e)
        )

def run_government_scraper(config: ScrapingConfig, progress_monitor: ProgressMonitor) -> ScrapingResult:
    """
    Run Government RSS scraper
    
    Reasoning: Government RSS feeds are stable and fast to process.
    This scraper typically completes quickly (30-60 minutes) so we run it early.
    """
    start_time = time.time()
    
    try:
        logger.info("Starting Government RSS scraper...")
        progress_monitor.update_progress('government', 0, 'running')
        
        # Initialize Government scraper
        output_dir = os.path.join(config.base_output_dir, 'government')
        scraper = GovernmentScraper(output_dir=output_dir)
        
        # Run scraping
        stats = scraper.run_full_scrape()
        
        execution_time = time.time() - start_time
        samples_collected = stats.get('total_articles', 0)
        
        progress_monitor.update_progress('government', samples_collected, 'completed')
        
        return ScrapingResult(
            source='government',
            success=True,
            samples_collected=samples_collected,
            execution_time=execution_time,
            statistics=stats,
            output_files=[f"{output_dir}/government_combined_{datetime.now().strftime('%Y%m%d')}.json"]
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        progress_monitor.update_progress('government', 0, 'failed')
        
        logger.error(f"Government scraper failed: {e}")
        return ScrapingResult(
            source='government',
            success=False,
            samples_collected=0,
            execution_time=execution_time,
            error_message=str(e)
        )

def run_propertyguru_scraper(config: ScrapingConfig, progress_monitor: ProgressMonitor) -> ScrapingResult:
    """
    Run PropertyGuru scraper
    
    Reasoning: PropertyGuru has anti-bot measures and requires careful rate limiting.
    This is the most complex scraper and may take 2-3 days to complete.
    """
    start_time = time.time()
    
    try:
        logger.info("Starting PropertyGuru scraper...")
        progress_monitor.update_progress('propertyguru', 0, 'running')
        
        # Initialize PropertyGuru scraper
        output_dir = os.path.join(config.base_output_dir, 'propertyguru')
        scraper = PropertyGuruScraper(output_dir=output_dir)
        
        # Run scraping
        stats = scraper.run_full_scrape()
        
        execution_time = time.time() - start_time
        samples_collected = stats.get('total_reviews', 0)
        
        progress_monitor.update_progress('propertyguru', samples_collected, 'completed')
        
        return ScrapingResult(
            source='propertyguru',
            success=True,
            samples_collected=samples_collected,
            execution_time=execution_time,
            statistics=stats,
            output_files=[f"{output_dir}/propertyguru_combined_{datetime.now().strftime('%Y%m%d')}.json"]
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        progress_monitor.update_progress('propertyguru', 0, 'failed')
        
        logger.error(f"PropertyGuru scraper failed: {e}")
        return ScrapingResult(
            source='propertyguru',
            success=False,
            samples_collected=0,
            execution_time=execution_time,
            error_message=str(e)
        )

def run_hardwarezone_scraper(config: ScrapingConfig, progress_monitor: ProgressMonitor) -> ScrapingResult:
    """
    Run HardwareZone EDMW scraper
    
    Reasoning: HardwareZone forum has good content but requires session management.
    Forum scraping can be intensive, so we use careful rate limiting.
    """
    start_time = time.time()
    
    try:
        logger.info("Starting HardwareZone EDMW scraper...")
        progress_monitor.update_progress('hardwarezone', 0, 'running')
        
        # Initialize HardwareZone scraper
        output_dir = os.path.join(config.base_output_dir, 'hardwarezone')
        scraper = HardwareZoneScraper(output_dir=output_dir)
        
        # Run scraping
        stats = scraper.run_full_scrape()
        
        execution_time = time.time() - start_time
        samples_collected = stats.get('total_posts', 0)
        
        progress_monitor.update_progress('hardwarezone', samples_collected, 'completed')
        
        return ScrapingResult(
            source='hardwarezone',
            success=True,
            samples_collected=samples_collected,
            execution_time=execution_time,
            statistics=stats,
            output_files=[f"{output_dir}/hardwarezone_combined_{datetime.now().strftime('%Y%m%d')}.json"]
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        progress_monitor.update_progress('hardwarezone', 0, 'failed')
        
        logger.error(f"HardwareZone scraper failed: {e}")
        return ScrapingResult(
            source='hardwarezone',
            success=False,
            samples_collected=0,
            execution_time=execution_time,
            error_message=str(e)
        )

class PropInsightController:
    """
    Master controller for PropInsight data collection
    
    Orchestrates parallel execution of all scrapers with proper error handling,
    progress monitoring, and data validation.
    """
    
    def __init__(self, config: ScrapingConfig = None):
        """Initialize controller with configuration"""
        self.config = config or ScrapingConfig()
        self.progress_monitor = ProgressMonitor(self.config)
        self.results = {}
        
        # Setup output directories
        self.setup_output_directories()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.shutdown_requested = False
    
    def setup_output_directories(self):
        """Create output directories for all scrapers"""
        base_dir = Path(self.config.base_output_dir)
        
        for source in ['reddit', 'government', 'propertyguru', 'hardwarezone']:
            source_dir = base_dir / source
            source_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created output directory: {source_dir}")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.shutdown_requested = True
    
    def run_scraper_with_retry(self, scraper_func, max_retries: int = 3) -> ScrapingResult:
        """
        Run scraper with retry logic
        
        Args:
            scraper_func: Function to run scraper
            max_retries: Maximum retry attempts
            
        Returns:
            ScrapingResult object
        """
        for attempt in range(max_retries + 1):
            if self.shutdown_requested:
                break
                
            try:
                logger.info(f"Running {scraper_func.__name__} (attempt {attempt + 1}/{max_retries + 1})")
                result = scraper_func(self.config, self.progress_monitor)
                
                if result.success:
                    return result
                else:
                    logger.warning(f"Scraper failed: {result.error_message}")
                    
            except Exception as e:
                logger.error(f"Unexpected error in {scraper_func.__name__}: {e}")
                
            # Wait before retry (except on last attempt)
            if attempt < max_retries:
                logger.info(f"Waiting {self.config.retry_delay} seconds before retry...")
                time.sleep(self.config.retry_delay)
        
        # All attempts failed
        return ScrapingResult(
            source=scraper_func.__name__.replace('run_', '').replace('_scraper', ''),
            success=False,
            samples_collected=0,
            execution_time=0,
            error_message="All retry attempts failed"
        )
    
    def run_parallel_scraping(self) -> Dict[str, ScrapingResult]:
        """
        Run all scrapers in parallel using threading
        
        Returns:
            Dictionary of scraping results by source
        """
        logger.info("Starting parallel scraping with PropInsight Controller")
        
        # Define scraper functions
        scraper_functions = [
            run_reddit_scraper,
            run_government_scraper,
            run_propertyguru_scraper,
            run_hardwarezone_scraper
        ]
        
        results = {}
        
        if self.config.use_multiprocessing:
            # Use ProcessPoolExecutor for true parallelism
            #with ProcessPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Use ThreadPoolExecutor instead of ProcessPoolExecutor to avoid pickle issues
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                # Submit all scraping tasks
                future_to_scraper = {
                    executor.submit(self.run_scraper_with_retry, func): func.__name__
                    for func in scraper_functions
                }
                
                # Monitor progress and collect results
                for future in as_completed(future_to_scraper):
                    if self.shutdown_requested:
                        break
                        
                    scraper_name = future_to_scraper[future]
                    
                    try:
                        result = future.result()
                        results[result.source] = result
                        
                        logger.info(f"Completed {scraper_name}: {result.samples_collected} samples in {result.execution_time:.2f}s")
                        
                    except Exception as e:
                        logger.error(f"Error in {scraper_name}: {e}")
                        results[scraper_name.replace('run_', '').replace('_scraper', '')] = ScrapingResult(
                            source=scraper_name.replace('run_', '').replace('_scraper', ''),
                            success=False,
                            samples_collected=0,
                            execution_time=0,
                            error_message=str(e)
                        )
        else:
            # Sequential execution for debugging
            for func in scraper_functions:
                if self.shutdown_requested:
                    break
                    
                result = self.run_scraper_with_retry(func)
                results[result.source] = result
        
        return results
    
    def validate_data_quality(self, results: Dict[str, ScrapingResult]) -> Dict[str, Any]:
        """
        Validate data quality across all sources
        
        Args:
            results: Dictionary of scraping results
            
        Returns:
            Data quality report
        """
        quality_report = {
            'total_samples': 0,
            'successful_sources': 0,
            'failed_sources': 0,
            'quality_metrics': {},
            'recommendations': []
        }
        
        for source, result in results.items():
            if result.success:
                quality_report['successful_sources'] += 1
                quality_report['total_samples'] += result.samples_collected
                
                # Calculate quality metrics
                target = getattr(self.config, f"{source}_target", 1000)
                completion_rate = (result.samples_collected / target) * 100
                
                quality_report['quality_metrics'][source] = {
                    'samples_collected': result.samples_collected,
                    'target_samples': target,
                    'completion_rate': completion_rate,
                    'execution_time': result.execution_time,
                    'samples_per_hour': result.samples_collected / (result.execution_time / 3600) if result.execution_time > 0 else 0
                }
                
                # Add recommendations based on completion rate
                if completion_rate < 50:
                    quality_report['recommendations'].append(
                        f"{source}: Low completion rate ({completion_rate:.1f}%). Consider adjusting scraping parameters."
                    )
                elif completion_rate > 120:
                    quality_report['recommendations'].append(
                        f"{source}: Exceeded target by {completion_rate - 100:.1f}%. Excellent performance!"
                    )
                    
            else:
                quality_report['failed_sources'] += 1
                quality_report['recommendations'].append(
                    f"{source}: Scraping failed. Error: {result.error_message}"
                )
        
        return quality_report
    
    def save_final_report(self, results: Dict[str, ScrapingResult], quality_report: Dict[str, Any]):
        """
        Save comprehensive final report
        
        Args:
            results: Scraping results
            quality_report: Data quality report
        """
        report = {
            'scraping_session': {
                'start_time': self.progress_monitor.start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'total_duration': str(datetime.now() - self.progress_monitor.start_time),
                'configuration': asdict(self.config)
            },
            'results_summary': {
                source: {
                    'success': result.success,
                    'samples_collected': result.samples_collected,
                    'execution_time': result.execution_time,
                    'error_message': result.error_message,
                    'output_files': result.output_files or []
                }
                for source, result in results.items()
            },
            'quality_report': quality_report,
            'final_progress': self.progress_monitor.get_overall_progress()
        }
        
        # Save report
        report_path = Path(self.config.base_output_dir) / f"propinsight_scraping_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Final report saved to: {report_path}")
        
        # Print summary to console
        print("\n" + "="*60)
        print("PROPINSIGHT DATA COLLECTION SUMMARY")
        print("="*60)
        print(f"Total samples collected: {quality_report['total_samples']:,}")
        print(f"Successful sources: {quality_report['successful_sources']}/4")
        print(f"Total duration: {datetime.now() - self.progress_monitor.start_time}")
        print("\nPer-source results:")
        
        for source, metrics in quality_report['quality_metrics'].items():
            print(f"  {source.capitalize()}: {metrics['samples_collected']:,} samples ({metrics['completion_rate']:.1f}% of target)")
        
        if quality_report['recommendations']:
            print("\nRecommendations:")
            for rec in quality_report['recommendations']:
                print(f"  • {rec}")
        
        print("="*60)
    
    def run_full_collection(self) -> Dict[str, ScrapingResult]:
        """
        Run complete data collection process
        
        Returns:
            Dictionary of scraping results
        """
        logger.info("Starting PropInsight data collection process")
        
        try:
            # Run parallel scraping
            results = self.run_parallel_scraping()
            
            # Validate data quality
            quality_report = self.validate_data_quality(results)
            
            # Save final report
            self.save_final_report(results, quality_report)
            
            return results
            
        except KeyboardInterrupt:
            logger.info("Data collection interrupted by user")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error in data collection: {e}")
            return {}

def main():
    """Main function to run PropInsight data collection"""
    # Create configuration
    config = ScrapingConfig(
        reddit_target=4000,
        government_target=1000,
        propertyguru_target=5000,
        hardwarezone_target=5000,
        max_workers=4,
        use_multiprocessing=True,
        max_retries=3,
        retry_delay=300
    )
    
    # Initialize and run controller
    controller = PropInsightController(config)
    results = controller.run_full_collection()
    
    return results

if __name__ == "__main__":
    main()