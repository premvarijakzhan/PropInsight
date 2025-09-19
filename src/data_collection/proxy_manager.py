"""
PropInsight Proxy Manager

Advanced proxy rotation and request headers randomization to avoid IP blocking
and enhance scraping success rates. This module provides intelligent proxy
management with health checking, rotation strategies, and fallback mechanisms.

Key features:
1. Proxy pool management with health checking
2. Intelligent rotation strategies (round-robin, random, performance-based)
3. Request headers randomization and fingerprint obfuscation
4. Proxy performance monitoring and blacklisting
5. Geographic proxy selection for region-specific content
6. Session persistence with proxy affinity

Reasoning: Many websites implement IP-based rate limiting and blocking.
Using proxy rotation with randomized headers helps distribute requests
across multiple IP addresses and makes scraping patterns less detectable.
"""

import random
import time
import requests
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import threading
import logging
from fake_useragent import UserAgent
import json
from pathlib import Path

logger = logging.getLogger(__name__)

class ProxyType(Enum):
    """Proxy types supported"""
    HTTP = "http"
    HTTPS = "https"
    SOCKS4 = "socks4"
    SOCKS5 = "socks5"

class RotationStrategy(Enum):
    """Proxy rotation strategies"""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    PERFORMANCE_BASED = "performance_based"
    GEOGRAPHIC = "geographic"

@dataclass
class ProxyInfo:
    """Proxy information and statistics"""
    host: str
    port: int
    proxy_type: ProxyType
    username: Optional[str] = None
    password: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    success_count: int = 0
    failure_count: int = 0
    avg_response_time: float = 0.0
    last_used: Optional[datetime] = None
    last_checked: Optional[datetime] = None
    is_working: bool = True
    blacklisted_until: Optional[datetime] = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        total = self.success_count + self.failure_count
        return self.success_count / total if total > 0 else 0.0

    @property
    def proxy_url(self) -> str:
        """Get formatted proxy URL"""
        if self.username and self.password:
            return f"{self.proxy_type.value}://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"{self.proxy_type.value}://{self.host}:{self.port}"

    @property
    def is_available(self) -> bool:
        """Check if proxy is available for use"""
        if not self.is_working:
            return False
        if self.blacklisted_until and datetime.now() < self.blacklisted_until:
            return False
        return True

class HeadersGenerator:
    """Generate randomized HTTP headers to avoid detection"""
    
    def __init__(self):
        self.ua = UserAgent()
        
        # Common browser headers patterns
        self.browsers = {
            'chrome': {
                'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1'
            },
            'firefox': {
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1'
            },
            'safari': {
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'upgrade-insecure-requests': '1'
            }
        }
        
        self.languages = [
            'en-US,en;q=0.9',
            'en-GB,en;q=0.9',
            'en-US,en;q=0.8,zh-CN;q=0.6',
            'en-US,en;q=0.9,zh;q=0.8',
            'en-US,en;q=0.5'
        ]
        
        self.encodings = [
            'gzip, deflate, br',
            'gzip, deflate',
            'gzip, deflate, br, zstd'
        ]

    def generate_headers(self, referer: Optional[str] = None, browser: Optional[str] = None) -> Dict[str, str]:
        """
        Generate randomized headers
        
        Args:
            referer: Optional referer URL
            browser: Specific browser to mimic
            
        Returns:
            Dictionary of HTTP headers
        """
        if not browser:
            browser = random.choice(['chrome', 'firefox', 'safari'])
        
        headers = {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': random.choice(self.languages),
            'Accept-Encoding': random.choice(self.encodings),
            'Connection': 'keep-alive',
            'DNT': '1',
            'Cache-Control': 'max-age=0'
        }
        
        # Add browser-specific headers
        if browser in self.browsers:
            headers.update(self.browsers[browser])
        
        # Add referer if provided
        if referer:
            headers['Referer'] = referer
        
        # Randomly add some optional headers
        if random.random() < 0.3:
            headers['X-Requested-With'] = 'XMLHttpRequest'
        
        if random.random() < 0.5:
            headers['Pragma'] = 'no-cache'
        
        return headers

class ProxyManager:
    """
    Advanced proxy management with rotation and health checking
    """
    
    def __init__(self, 
                 rotation_strategy: RotationStrategy = RotationStrategy.ROUND_ROBIN,
                 health_check_interval: int = 300,
                 max_failures: int = 3,
                 blacklist_duration: int = 1800):
        """
        Initialize proxy manager
        
        Args:
            rotation_strategy: Strategy for proxy rotation
            health_check_interval: Seconds between health checks
            max_failures: Max failures before blacklisting
            blacklist_duration: Seconds to blacklist failed proxies
        """
        self.proxies: List[ProxyInfo] = []
        self.rotation_strategy = rotation_strategy
        self.health_check_interval = health_check_interval
        self.max_failures = max_failures
        self.blacklist_duration = blacklist_duration
        
        self.current_index = 0
        self.lock = threading.Lock()
        self.headers_generator = HeadersGenerator()
        
        # Performance tracking
        self.total_requests = 0
        self.successful_requests = 0
        
        logger.info(f"ProxyManager initialized with {rotation_strategy.value} strategy")

    def add_proxy(self, host: str, port: int, proxy_type: ProxyType = ProxyType.HTTP,
                  username: Optional[str] = None, password: Optional[str] = None,
                  country: Optional[str] = None, city: Optional[str] = None):
        """
        Add a proxy to the pool
        
        Args:
            host: Proxy host
            port: Proxy port
            proxy_type: Type of proxy
            username: Optional username
            password: Optional password
            country: Optional country code
            city: Optional city name
        """
        proxy = ProxyInfo(
            host=host,
            port=port,
            proxy_type=proxy_type,
            username=username,
            password=password,
            country=country,
            city=city
        )
        
        with self.lock:
            self.proxies.append(proxy)
        
        logger.info(f"Added proxy: {host}:{port} ({proxy_type.value})")

    def load_proxies_from_file(self, file_path: str):
        """
        Load proxies from JSON file
        
        Args:
            file_path: Path to JSON file with proxy list
        """
        try:
            with open(file_path, 'r') as f:
                proxy_data = json.load(f)
            
            for proxy_info in proxy_data:
                self.add_proxy(
                    host=proxy_info['host'],
                    port=proxy_info['port'],
                    proxy_type=ProxyType(proxy_info.get('type', 'http')),
                    username=proxy_info.get('username'),
                    password=proxy_info.get('password'),
                    country=proxy_info.get('country'),
                    city=proxy_info.get('city')
                )
            
            logger.info(f"Loaded {len(proxy_data)} proxies from {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to load proxies from {file_path}: {e}")

    def get_next_proxy(self, country: Optional[str] = None) -> Optional[ProxyInfo]:
        """
        Get next proxy based on rotation strategy
        
        Args:
            country: Optional country filter
            
        Returns:
            Next proxy to use or None if no proxies available
        """
        with self.lock:
            available_proxies = [p for p in self.proxies if p.is_available]
            
            # Filter by country if specified
            if country:
                available_proxies = [p for p in available_proxies if p.country == country]
            
            if not available_proxies:
                logger.warning("No available proxies found")
                return None
            
            if self.rotation_strategy == RotationStrategy.ROUND_ROBIN:
                proxy = available_proxies[self.current_index % len(available_proxies)]
                self.current_index += 1
            
            elif self.rotation_strategy == RotationStrategy.RANDOM:
                proxy = random.choice(available_proxies)
            
            elif self.rotation_strategy == RotationStrategy.PERFORMANCE_BASED:
                # Sort by success rate and response time
                available_proxies.sort(key=lambda p: (p.success_rate, -p.avg_response_time), reverse=True)
                proxy = available_proxies[0]
            
            elif self.rotation_strategy == RotationStrategy.GEOGRAPHIC:
                # Prefer proxies from different countries
                if country:
                    proxy = random.choice(available_proxies)
                else:
                    # Group by country and rotate
                    countries = list(set(p.country for p in available_proxies if p.country))
                    if countries:
                        selected_country = random.choice(countries)
                        country_proxies = [p for p in available_proxies if p.country == selected_country]
                        proxy = random.choice(country_proxies)
                    else:
                        proxy = random.choice(available_proxies)
            
            proxy.last_used = datetime.now()
            return proxy

    def record_success(self, proxy: ProxyInfo, response_time: float):
        """
        Record successful request
        
        Args:
            proxy: Proxy that was used
            response_time: Response time in seconds
        """
        with self.lock:
            proxy.success_count += 1
            # Update average response time
            total_time = proxy.avg_response_time * (proxy.success_count - 1) + response_time
            proxy.avg_response_time = total_time / proxy.success_count
            
            self.successful_requests += 1
            self.total_requests += 1

    def record_failure(self, proxy: ProxyInfo):
        """
        Record failed request
        
        Args:
            proxy: Proxy that failed
        """
        with self.lock:
            proxy.failure_count += 1
            self.total_requests += 1
            
            # Blacklist proxy if too many failures
            if proxy.failure_count >= self.max_failures:
                proxy.blacklisted_until = datetime.now() + timedelta(seconds=self.blacklist_duration)
                logger.warning(f"Blacklisted proxy {proxy.host}:{proxy.port} for {self.blacklist_duration}s")

    def check_proxy_health(self, proxy: ProxyInfo, test_url: str = "http://httpbin.org/ip") -> bool:
        """
        Check if proxy is working
        
        Args:
            proxy: Proxy to test
            test_url: URL to test against
            
        Returns:
            True if proxy is working
        """
        try:
            proxies = {
                'http': proxy.proxy_url,
                'https': proxy.proxy_url
            }
            
            start_time = time.time()
            response = requests.get(test_url, proxies=proxies, timeout=10)
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                proxy.is_working = True
                proxy.last_checked = datetime.now()
                self.record_success(proxy, response_time)
                return True
            else:
                proxy.is_working = False
                self.record_failure(proxy)
                return False
                
        except Exception as e:
            logger.debug(f"Proxy health check failed for {proxy.host}:{proxy.port}: {e}")
            proxy.is_working = False
            self.record_failure(proxy)
            return False

    def get_session_with_proxy(self, country: Optional[str] = None, referer: Optional[str] = None) -> Tuple[requests.Session, Optional[ProxyInfo]]:
        """
        Get a requests session configured with proxy and randomized headers
        
        Args:
            country: Optional country filter for proxy
            referer: Optional referer for headers
            
        Returns:
            Tuple of (session, proxy_info)
        """
        session = requests.Session()
        proxy = self.get_next_proxy(country)
        
        if proxy:
            session.proxies = {
                'http': proxy.proxy_url,
                'https': proxy.proxy_url
            }
        
        # Set randomized headers
        headers = self.headers_generator.generate_headers(referer=referer)
        session.headers.update(headers)
        
        return session, proxy

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get proxy pool statistics
        
        Returns:
            Dictionary with statistics
        """
        with self.lock:
            working_proxies = len([p for p in self.proxies if p.is_working])
            available_proxies = len([p for p in self.proxies if p.is_available])
            blacklisted_proxies = len([p for p in self.proxies if p.blacklisted_until and datetime.now() < p.blacklisted_until])
            
            success_rate = self.successful_requests / self.total_requests if self.total_requests > 0 else 0
            
            return {
                'total_proxies': len(self.proxies),
                'working_proxies': working_proxies,
                'available_proxies': available_proxies,
                'blacklisted_proxies': blacklisted_proxies,
                'total_requests': self.total_requests,
                'successful_requests': self.successful_requests,
                'success_rate': success_rate,
                'rotation_strategy': self.rotation_strategy.value
            }

# Global proxy manager instance
default_proxy_manager = ProxyManager()

def main():
    """Test proxy manager functionality"""
    pm = ProxyManager(rotation_strategy=RotationStrategy.RANDOM)
    
    # Add some test proxies (these are example proxies, replace with real ones)
    test_proxies = [
        {'host': '8.8.8.8', 'port': 8080, 'type': 'http'},
        {'host': '1.1.1.1', 'port': 8080, 'type': 'http'},
    ]
    
    for proxy_info in test_proxies:
        pm.add_proxy(
            host=proxy_info['host'],
            port=proxy_info['port'],
            proxy_type=ProxyType(proxy_info['type'])
        )
    
    # Test proxy rotation
    for i in range(5):
        proxy = pm.get_next_proxy()
        if proxy:
            print(f"Selected proxy: {proxy.host}:{proxy.port}")
        else:
            print("No proxy available")
    
    # Print statistics
    stats = pm.get_statistics()
    print(f"Proxy statistics: {stats}")

if __name__ == "__main__":
    main()