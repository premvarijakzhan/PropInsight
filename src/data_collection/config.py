"""
PropInsight Configuration Management

Centralized configuration system for all scrapers with environment-specific settings,
validation, and secure credential management.

Key features:
1. Environment-based configuration (dev, staging, prod)
2. Secure credential management
3. Configuration validation
4. Dynamic configuration updates
5. Logging configuration
6. Rate limiting settings
7. Output path management

Reasoning: Centralized configuration ensures consistency across all scrapers,
makes deployment easier, and provides secure credential management.
Having environment-specific configs allows for different settings during
development vs production.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import yaml
from cryptography.fernet import Fernet
import base64

@dataclass
class ScraperConfig:
    """Configuration for individual scrapers"""
    name: str
    enabled: bool = True
    max_workers: int = 1
    rate_limit_delay: float = 1.0
    max_retries: int = 3
    timeout: int = 30
    user_agents: List[str] = field(default_factory=list)
    headers: Dict[str, str] = field(default_factory=dict)
    proxy_enabled: bool = False
    proxy_list: List[str] = field(default_factory=list)
    output_format: str = "jsonl"
    max_items: int = 5000
    date_range_days: int = 730  # 2 years default
    custom_settings: Dict[str, Any] = field(default_factory=dict)

@dataclass
class DatabaseConfig:
    """Database configuration"""
    type: str = "sqlite"  # sqlite, postgresql, mysql
    host: str = "localhost"
    port: int = 5432
    database: str = "propinsight"
    username: str = ""
    password: str = ""
    connection_pool_size: int = 5
    ssl_enabled: bool = False

@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_enabled: bool = True
    file_path: str = "logs/propinsight.log"
    console_enabled: bool = True
    max_file_size: int = 10485760  # 10MB
    backup_count: int = 5
    structured_logging: bool = True

@dataclass
class NotificationConfig:
    """Notification configuration"""
    email_enabled: bool = False
    email_smtp_server: str = ""
    email_smtp_port: int = 587
    email_username: str = ""
    email_password: str = ""
    email_recipients: List[str] = field(default_factory=list)
    slack_enabled: bool = False
    slack_webhook_url: str = ""
    discord_enabled: bool = False
    discord_webhook_url: str = ""

class ConfigManager:
    """
    Centralized configuration management system
    
    Handles loading, validation, and management of all configuration settings
    """
    
    def __init__(self, config_dir: str = "config", environment: str = "development"):
        """
        Initialize configuration manager
        
        Args:
            config_dir: Directory containing configuration files
            environment: Current environment (development, staging, production)
        """
        self.config_dir = Path(config_dir)
        self.environment = environment
        self.config_file = self.config_dir / f"{environment}.yaml"
        self.secrets_file = self.config_dir / "secrets.encrypted"
        
        # Create config directory if it doesn't exist
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize encryption key for secrets
        self.encryption_key = self._get_or_create_encryption_key()
        
        # Load configuration
        self.config = self._load_configuration()
        
        # Validate configuration
        self._validate_configuration()
        
        # Setup logging
        self._setup_logging()
    
    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for secrets"""
        key_file = self.config_dir / "encryption.key"
        
        if key_file.exists():
            with open(key_file, "rb") as f:
                return f.read()
        else:
            key = Fernet.generate_key()
            with open(key_file, "wb") as f:
                f.write(key)
            # Set restrictive permissions
            os.chmod(key_file, 0o600)
            return key
    
    def _load_configuration(self) -> Dict[str, Any]:
        """Load configuration from files"""
        # Default configuration
        default_config = self._get_default_configuration()
        
        # Load environment-specific configuration
        if self.config_file.exists():
            with open(self.config_file, "r") as f:
                env_config = yaml.safe_load(f) or {}
        else:
            env_config = {}
            # Create default config file
            self._save_configuration(default_config)
        
        # Merge configurations (env_config overrides default_config)
        config = self._deep_merge(default_config, env_config)
        
        # Load encrypted secrets
        secrets = self._load_secrets()
        if secrets:
            config = self._deep_merge(config, secrets)
        
        return config
    
    def _get_default_configuration(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            "environment": self.environment,
            "project_name": "PropInsight",
            "version": "1.0.0",
            "debug": self.environment == "development",
            
            # Paths
            "paths": {
                "data_dir": "data",
                "raw_data_dir": "data/raw",
                "processed_data_dir": "data/processed",
                "logs_dir": "logs",
                "temp_dir": "temp"
            },
            
            # Database configuration
            "database": {
                "type": "sqlite",
                "database": "data/propinsight.db",
                "connection_pool_size": 5
            },
            
            # Logging configuration
            "logging": {
                "level": "INFO" if self.environment == "production" else "DEBUG",
                "file_enabled": True,
                "console_enabled": True,
                "structured_logging": True,
                "max_file_size": 10485760,
                "backup_count": 5
            },
            
            # Notification configuration
            "notifications": {
                "email_enabled": False,
                "slack_enabled": False,
                "discord_enabled": False
            },
            
            # Scraper configurations
            "scrapers": {
                "reddit": {
                    "name": "reddit",
                    "enabled": True,
                    "max_workers": 2,
                    "rate_limit_delay": 2.0,
                    "max_retries": 3,
                    "timeout": 30,
                    "max_items": 5000,
                    "date_range_days": 730,
                    "custom_settings": {
                        "subreddits": ["singapore", "singaporefi", "asksingapore"],
                        "keywords": [
                            "property", "housing", "HDB", "condo", "landed",
                            "real estate", "mortgage", "rental", "buy", "sell"
                        ]
                    }
                },
                
                "government": {
                    "name": "government",
                    "enabled": True,
                    "max_workers": 1,
                    "rate_limit_delay": 1.0,
                    "max_retries": 3,
                    "timeout": 30,
                    "max_items": 1000,
                    "date_range_days": 730,
                    "custom_settings": {
                        "rss_feeds": [
                            "https://www.mnd.gov.sg/rss",
                            "https://www.hdb.gov.sg/rss",
                            "https://www.ura.gov.sg/rss"
                        ],
                        "keywords": [
                            "property", "housing", "development", "policy",
                            "regulation", "market", "price", "supply"
                        ]
                    }
                },
                
                "propertyguru": {
                    "name": "propertyguru",
                    "enabled": True,
                    "max_workers": 1,
                    "rate_limit_delay": 3.0,
                    "max_retries": 5,
                    "timeout": 45,
                    "max_items": 5000,
                    "date_range_days": 365,
                    "proxy_enabled": True,
                    "custom_settings": {
                        "use_selenium": True,
                        "headless": True,
                        "page_load_timeout": 30,
                        "implicit_wait": 10,
                        "property_types": ["hdb", "condo", "landed"],
                        "regions": ["central", "east", "west", "north", "northeast"]
                    }
                },
                
                "hardwarezone": {
                    "name": "hardwarezone",
                    "enabled": True,
                    "max_workers": 1,
                    "rate_limit_delay": 2.0,
                    "max_retries": 3,
                    "timeout": 30,
                    "max_items": 5000,
                    "date_range_days": 730,
                    "custom_settings": {
                        "forum_sections": ["edmw"],
                        "keywords": [
                            "property", "housing", "HDB", "condo", "landed",
                            "buy", "sell", "rent", "investment", "market"
                        ],
                        "sentiment_analysis": True
                    }
                }
            },
            
            # Global scraping settings
            "scraping": {
                "concurrent_scrapers": 2,
                "global_rate_limit": 0.5,
                "user_agents": [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                ],
                "default_headers": {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1"
                }
            },
            
            # Error handling
            "error_handling": {
                "max_consecutive_failures": 10,
                "circuit_breaker_threshold": 5,
                "circuit_breaker_timeout": 300,
                "retry_exponential_base": 2.0,
                "retry_max_delay": 300.0,
                "alert_on_critical_errors": True
            },
            
            # Performance monitoring
            "monitoring": {
                "enabled": True,
                "metrics_retention_days": 30,
                "performance_alerts": True,
                "memory_threshold_mb": 1024,
                "cpu_threshold_percent": 80
            }
        }
    
    def _load_secrets(self) -> Optional[Dict[str, Any]]:
        """Load encrypted secrets"""
        if not self.secrets_file.exists():
            return None
        
        try:
            fernet = Fernet(self.encryption_key)
            with open(self.secrets_file, "rb") as f:
                encrypted_data = f.read()
            
            decrypted_data = fernet.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode())
        
        except Exception as e:
            logging.warning(f"Failed to load secrets: {e}")
            return None
    
    def _save_secrets(self, secrets: Dict[str, Any]):
        """Save encrypted secrets"""
        try:
            fernet = Fernet(self.encryption_key)
            secrets_json = json.dumps(secrets).encode()
            encrypted_data = fernet.encrypt(secrets_json)
            
            with open(self.secrets_file, "wb") as f:
                f.write(encrypted_data)
            
            # Set restrictive permissions
            os.chmod(self.secrets_file, 0o600)
        
        except Exception as e:
            logging.error(f"Failed to save secrets: {e}")
    
    def _save_configuration(self, config: Dict[str, Any]):
        """Save configuration to file"""
        with open(self.config_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False, indent=2)
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries"""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _validate_configuration(self):
        """Validate configuration settings"""
        required_sections = ["paths", "database", "logging", "scrapers"]
        
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate scraper configurations
        for scraper_name, scraper_config in self.config["scrapers"].items():
            if not isinstance(scraper_config.get("enabled"), bool):
                raise ValueError(f"Invalid 'enabled' setting for scraper {scraper_name}")
            
            if not isinstance(scraper_config.get("max_workers", 1), int):
                raise ValueError(f"Invalid 'max_workers' setting for scraper {scraper_name}")
        
        # Validate paths
        for path_name, path_value in self.config["paths"].items():
            if not isinstance(path_value, str):
                raise ValueError(f"Invalid path configuration: {path_name}")
    
    def _setup_logging(self):
        """Setup logging based on configuration"""
        log_config = self.config["logging"]
        
        # Create logs directory
        logs_dir = Path(self.config["paths"]["logs_dir"])
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, log_config["level"]),
            format=log_config["format"],
            handlers=[]
        )
        
        logger = logging.getLogger()
        
        # File handler
        if log_config["file_enabled"]:
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                logs_dir / "propinsight.log",
                maxBytes=log_config["max_file_size"],
                backupCount=log_config["backup_count"]
            )
            file_handler.setFormatter(logging.Formatter(log_config["format"]))
            logger.addHandler(file_handler)
        
        # Console handler
        if log_config["console_enabled"]:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(log_config["format"]))
            logger.addHandler(console_handler)
    
    def get_scraper_config(self, scraper_name: str) -> ScraperConfig:
        """
        Get configuration for a specific scraper
        
        Args:
            scraper_name: Name of the scraper
            
        Returns:
            ScraperConfig instance
        """
        if scraper_name not in self.config["scrapers"]:
            raise ValueError(f"No configuration found for scraper: {scraper_name}")
        
        scraper_data = self.config["scrapers"][scraper_name]
        
        return ScraperConfig(
            name=scraper_data["name"],
            enabled=scraper_data.get("enabled", True),
            max_workers=scraper_data.get("max_workers", 1),
            rate_limit_delay=scraper_data.get("rate_limit_delay", 1.0),
            max_retries=scraper_data.get("max_retries", 3),
            timeout=scraper_data.get("timeout", 30),
            user_agents=scraper_data.get("user_agents", self.config["scraping"]["user_agents"]),
            headers=scraper_data.get("headers", self.config["scraping"]["default_headers"]),
            proxy_enabled=scraper_data.get("proxy_enabled", False),
            proxy_list=scraper_data.get("proxy_list", []),
            output_format=scraper_data.get("output_format", "jsonl"),
            max_items=scraper_data.get("max_items", 5000),
            date_range_days=scraper_data.get("date_range_days", 730),
            custom_settings=scraper_data.get("custom_settings", {})
        )
    
    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration"""
        db_data = self.config["database"]
        
        return DatabaseConfig(
            type=db_data.get("type", "sqlite"),
            host=db_data.get("host", "localhost"),
            port=db_data.get("port", 5432),
            database=db_data.get("database", "propinsight"),
            username=db_data.get("username", ""),
            password=db_data.get("password", ""),
            connection_pool_size=db_data.get("connection_pool_size", 5),
            ssl_enabled=db_data.get("ssl_enabled", False)
        )
    
    def get_logging_config(self) -> LoggingConfig:
        """Get logging configuration"""
        log_data = self.config["logging"]
        
        return LoggingConfig(
            level=log_data.get("level", "INFO"),
            format=log_data.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            file_enabled=log_data.get("file_enabled", True),
            file_path=log_data.get("file_path", "logs/propinsight.log"),
            console_enabled=log_data.get("console_enabled", True),
            max_file_size=log_data.get("max_file_size", 10485760),
            backup_count=log_data.get("backup_count", 5),
            structured_logging=log_data.get("structured_logging", True)
        )
    
    def get_notification_config(self) -> NotificationConfig:
        """Get notification configuration"""
        notif_data = self.config["notifications"]
        
        return NotificationConfig(
            email_enabled=notif_data.get("email_enabled", False),
            email_smtp_server=notif_data.get("email_smtp_server", ""),
            email_smtp_port=notif_data.get("email_smtp_port", 587),
            email_username=notif_data.get("email_username", ""),
            email_password=notif_data.get("email_password", ""),
            email_recipients=notif_data.get("email_recipients", []),
            slack_enabled=notif_data.get("slack_enabled", False),
            slack_webhook_url=notif_data.get("slack_webhook_url", ""),
            discord_enabled=notif_data.get("discord_enabled", False),
            discord_webhook_url=notif_data.get("discord_webhook_url", "")
        )
    
    def get_path(self, path_name: str) -> Path:
        """
        Get configured path
        
        Args:
            path_name: Name of the path
            
        Returns:
            Path object
        """
        if path_name not in self.config["paths"]:
            raise ValueError(f"No path configuration found for: {path_name}")
        
        path = Path(self.config["paths"][path_name])
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    def update_scraper_setting(self, scraper_name: str, setting_name: str, value: Any):
        """
        Update a scraper setting
        
        Args:
            scraper_name: Name of the scraper
            setting_name: Name of the setting
            value: New value
        """
        if scraper_name not in self.config["scrapers"]:
            raise ValueError(f"No configuration found for scraper: {scraper_name}")
        
        self.config["scrapers"][scraper_name][setting_name] = value
        self._save_configuration(self.config)
    
    def add_secret(self, key: str, value: str):
        """
        Add a secret to encrypted storage
        
        Args:
            key: Secret key
            value: Secret value
        """
        secrets = self._load_secrets() or {}
        secrets[key] = value
        self._save_secrets(secrets)
    
    def get_secret(self, key: str) -> Optional[str]:
        """
        Get a secret from encrypted storage
        
        Args:
            key: Secret key
            
        Returns:
            Secret value or None if not found
        """
        secrets = self._load_secrets()
        return secrets.get(key) if secrets else None
    
    def get_all_settings(self) -> Dict[str, Any]:
        """Get all configuration settings"""
        return self.config.copy()

# Global configuration manager instance
config_manager = None

def get_config_manager(environment: str = None) -> ConfigManager:
    """
    Get global configuration manager instance
    
    Args:
        environment: Environment name (uses env var if None)
        
    Returns:
        ConfigManager instance
    """
    global config_manager
    
    if config_manager is None:
        if environment is None:
            environment = os.getenv("PROPINSIGHT_ENV", "development")
        config_manager = ConfigManager(environment=environment)
    
    return config_manager

def main():
    """Example usage of configuration management"""
    
    # Initialize configuration manager
    config = get_config_manager("development")
    
    # Get scraper configuration
    reddit_config = config.get_scraper_config("reddit")
    print(f"Reddit scraper config: {reddit_config}")
    
    # Get database configuration
    db_config = config.get_database_config()
    print(f"Database config: {db_config}")
    
    # Get paths
    data_dir = config.get_path("data_dir")
    print(f"Data directory: {data_dir}")
    
    # Add a secret
    config.add_secret("reddit_client_secret", "your_secret_here")
    
    # Get a secret
    secret = config.get_secret("reddit_client_secret")
    print(f"Retrieved secret: {secret}")

if __name__ == "__main__":
    main()