"""
PropInsight Error Handler

Comprehensive error handling, retry mechanisms, and logging utilities for all scrapers.
This module provides centralized error management to ensure robust data collection
even when facing network issues, rate limits, or unexpected website changes.

Key features:
1. Exponential backoff retry mechanisms
2. Circuit breaker pattern for failing services
3. Structured logging with context
4. Error categorization and recovery strategies
5. Performance monitoring and alerting
6. Graceful degradation for partial failures

Reasoning: Robust error handling is critical for long-running scraping operations.
Web scraping faces many challenges (rate limits, network issues, site changes),
so we need sophisticated error handling to maximize data collection success.
"""

import logging
import time
import traceback
import functools
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Type
from dataclasses import dataclass, field
from enum import Enum
import json
import threading
from pathlib import Path
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class ErrorCategory(Enum):
    """Error categories for different handling strategies"""
    NETWORK = "network"
    RATE_LIMIT = "rate_limit"
    AUTHENTICATION = "authentication"
    PARSING = "parsing"
    VALIDATION = "validation"
    RESOURCE = "resource"
    UNKNOWN = "unknown"

@dataclass
class ErrorContext:
    """Context information for error tracking"""
    scraper_name: str
    function_name: str
    url: Optional[str] = None
    attempt_number: int = 1
    timestamp: datetime = field(default_factory=datetime.now)
    additional_info: Dict[str, Any] = field(default_factory=dict)

@dataclass
class RetryConfig:
    """Configuration for retry mechanisms"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 300.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on_exceptions: tuple = (Exception,)
    stop_on_exceptions: tuple = ()

class CircuitBreaker:
    """
    Circuit breaker pattern implementation
    
    Prevents cascading failures by temporarily stopping calls to failing services
    """
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        """
        Initialize circuit breaker
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
        self.lock = threading.Lock()
    
    def call(self, func: Callable, *args, **kwargs):
        """
        Execute function with circuit breaker protection
        
        Args:
            func: Function to execute
            *args, **kwargs: Function arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerOpenError: When circuit is open
        """
        with self.lock:
            if self.state == "open":
                if self._should_attempt_reset():
                    self.state = "half-open"
                else:
                    raise CircuitBreakerOpenError("Circuit breaker is open")
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except Exception as e:
                self._on_failure()
                raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self.last_failure_time is None:
            return True
        return (datetime.now() - self.last_failure_time).seconds >= self.recovery_timeout
    
    def _on_success(self):
        """Handle successful call"""
        self.failure_count = 0
        self.state = "closed"
    
    def _on_failure(self):
        """Handle failed call"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "open"

class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open"""
    pass

class ErrorHandler:
    """
    Centralized error handling and logging system
    
    Provides structured error handling, retry mechanisms, and monitoring
    """
    
    def __init__(self, log_file: str = "propinsight_errors.log"):
        """
        Initialize error handler
        
        Args:
            log_file: Path to error log file
        """
        self.log_file = log_file
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.error_stats: Dict[str, Dict[str, int]] = {}
        self.lock = threading.Lock()
        
        # Setup logging
        self.logger = self._setup_logger()
        
        # Error categorization patterns
        self.error_patterns = {
            ErrorCategory.NETWORK: [
                "connection", "timeout", "network", "dns", "socket",
                "unreachable", "refused", "reset"
            ],
            ErrorCategory.RATE_LIMIT: [
                "rate limit", "too many requests", "429", "throttle",
                "quota exceeded", "limit exceeded"
            ],
            ErrorCategory.AUTHENTICATION: [
                "unauthorized", "forbidden", "401", "403", "authentication",
                "invalid credentials", "access denied"
            ],
            ErrorCategory.PARSING: [
                "parse", "json", "xml", "html", "decode", "encoding",
                "malformed", "invalid format"
            ],
            ErrorCategory.VALIDATION: [
                "validation", "invalid", "missing", "required",
                "constraint", "format error"
            ],
            ErrorCategory.RESOURCE: [
                "memory", "disk", "cpu", "resource", "limit",
                "out of", "insufficient"
            ]
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging"""
        logger = logging.getLogger("propinsight_errors")
        logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def categorize_error(self, error: Exception) -> ErrorCategory:
        """
        Categorize error based on error message and type
        
        Args:
            error: Exception to categorize
            
        Returns:
            ErrorCategory enum value
        """
        error_msg = str(error).lower()
        error_type = type(error).__name__.lower()
        
        for category, patterns in self.error_patterns.items():
            for pattern in patterns:
                if pattern in error_msg or pattern in error_type:
                    return category
        
        return ErrorCategory.UNKNOWN
    
    def determine_severity(self, error: Exception, context: ErrorContext) -> ErrorSeverity:
        """
        Determine error severity based on error type and context
        
        Args:
            error: Exception that occurred
            context: Error context information
            
        Returns:
            ErrorSeverity enum value
        """
        category = self.categorize_error(error)
        
        # Critical errors that stop scraping
        if category == ErrorCategory.AUTHENTICATION:
            return ErrorSeverity.CRITICAL
        
        # High severity for resource issues
        if category == ErrorCategory.RESOURCE:
            return ErrorSeverity.HIGH
        
        # Medium severity for network and rate limit issues
        if category in [ErrorCategory.NETWORK, ErrorCategory.RATE_LIMIT]:
            return ErrorSeverity.MEDIUM
        
        # Low severity for parsing and validation issues
        if category in [ErrorCategory.PARSING, ErrorCategory.VALIDATION]:
            return ErrorSeverity.LOW
        
        # Default to medium for unknown errors
        return ErrorSeverity.MEDIUM
    
    def log_error(self, error: Exception, context: ErrorContext, severity: ErrorSeverity = None):
        """
        Log error with structured information
        
        Args:
            error: Exception that occurred
            context: Error context information
            severity: Error severity (auto-determined if None)
        """
        if severity is None:
            severity = self.determine_severity(error, context)
        
        category = self.categorize_error(error)
        
        # Update error statistics
        with self.lock:
            scraper_stats = self.error_stats.setdefault(context.scraper_name, {})
            scraper_stats[category.value] = scraper_stats.get(category.value, 0) + 1
        
        # Create structured log entry
        log_entry = {
            "timestamp": context.timestamp.isoformat(),
            "scraper": context.scraper_name,
            "function": context.function_name,
            "url": context.url,
            "attempt": context.attempt_number,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "category": category.value,
            "severity": severity.value,
            "traceback": traceback.format_exc(),
            "additional_info": context.additional_info
        }
        
        # Log based on severity
        if severity == ErrorSeverity.CRITICAL:
            self.logger.critical(json.dumps(log_entry, indent=2))
        elif severity == ErrorSeverity.HIGH:
            self.logger.error(json.dumps(log_entry, indent=2))
        elif severity == ErrorSeverity.MEDIUM:
            self.logger.warning(json.dumps(log_entry, indent=2))
        else:
            self.logger.info(json.dumps(log_entry, indent=2))
    
    def get_circuit_breaker(self, service_name: str) -> CircuitBreaker:
        """
        Get or create circuit breaker for a service
        
        Args:
            service_name: Name of the service
            
        Returns:
            CircuitBreaker instance
        """
        if service_name not in self.circuit_breakers:
            self.circuit_breakers[service_name] = CircuitBreaker()
        return self.circuit_breakers[service_name]
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive error statistics
        
        Returns:
            Dictionary with error statistics
        """
        with self.lock:
            total_errors = sum(
                sum(categories.values()) for categories in self.error_stats.values()
            )
            
            return {
                "total_errors": total_errors,
                "by_scraper": dict(self.error_stats),
                "circuit_breaker_states": {
                    name: cb.state for name, cb in self.circuit_breakers.items()
                },
                "generated_at": datetime.now().isoformat()
            }

def retry_with_backoff(
    config: RetryConfig = None,
    error_handler: ErrorHandler = None
):
    """
    Decorator for implementing retry with exponential backoff
    
    Args:
        config: Retry configuration
        error_handler: Error handler instance
        
    Returns:
        Decorated function with retry logic
    """
    if config is None:
        config = RetryConfig()
    
    if error_handler is None:
        error_handler = ErrorHandler()
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                
                except config.stop_on_exceptions as e:
                    # Don't retry on these exceptions
                    context = ErrorContext(
                        scraper_name=getattr(func, '__module__', 'unknown'),
                        function_name=func.__name__,
                        attempt_number=attempt
                    )
                    error_handler.log_error(e, context, ErrorSeverity.CRITICAL)
                    raise e
                
                except config.retry_on_exceptions as e:
                    last_exception = e
                    
                    # Log the error
                    context = ErrorContext(
                        scraper_name=getattr(func, '__module__', 'unknown'),
                        function_name=func.__name__,
                        attempt_number=attempt
                    )
                    error_handler.log_error(e, context)
                    
                    # Don't sleep on the last attempt
                    if attempt < config.max_attempts:
                        delay = min(
                            config.base_delay * (config.exponential_base ** (attempt - 1)),
                            config.max_delay
                        )
                        
                        # Add jitter to prevent thundering herd
                        if config.jitter:
                            import random
                            delay *= (0.5 + random.random() * 0.5)
                        
                        time.sleep(delay)
            
            # All attempts failed
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator

def handle_scraper_errors(
    scraper_name: str,
    error_handler: ErrorHandler = None,
    circuit_breaker: bool = True
):
    """
    Decorator for comprehensive scraper error handling
    
    Args:
        scraper_name: Name of the scraper
        error_handler: Error handler instance
        circuit_breaker: Whether to use circuit breaker
        
    Returns:
        Decorated function with error handling
    """
    if error_handler is None:
        error_handler = ErrorHandler()
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            context = ErrorContext(
                scraper_name=scraper_name,
                function_name=func.__name__
            )
            
            try:
                if circuit_breaker:
                    cb = error_handler.get_circuit_breaker(scraper_name)
                    return cb.call(func, *args, **kwargs)
                else:
                    return func(*args, **kwargs)
                    
            except Exception as e:
                error_handler.log_error(e, context)
                
                # Re-raise for upstream handling
                raise e
        
        return wrapper
    return decorator

class PerformanceMonitor:
    """
    Monitor scraper performance and detect anomalies
    """
    
    def __init__(self):
        """Initialize performance monitor"""
        self.metrics: Dict[str, List[Dict[str, Any]]] = {}
        self.lock = threading.Lock()
    
    def record_operation(self, scraper_name: str, operation: str, 
                        duration: float, success: bool, **kwargs):
        """
        Record operation metrics
        
        Args:
            scraper_name: Name of the scraper
            operation: Operation name
            duration: Operation duration in seconds
            success: Whether operation succeeded
            **kwargs: Additional metrics
        """
        with self.lock:
            if scraper_name not in self.metrics:
                self.metrics[scraper_name] = []
            
            self.metrics[scraper_name].append({
                "timestamp": datetime.now().isoformat(),
                "operation": operation,
                "duration": duration,
                "success": success,
                **kwargs
            })
    
    def get_performance_summary(self, scraper_name: str = None) -> Dict[str, Any]:
        """
        Get performance summary
        
        Args:
            scraper_name: Specific scraper name (all if None)
            
        Returns:
            Performance summary
        """
        with self.lock:
            if scraper_name:
                scrapers = {scraper_name: self.metrics.get(scraper_name, [])}
            else:
                scrapers = self.metrics
            
            summary = {}
            
            for name, operations in scrapers.items():
                if not operations:
                    continue
                
                durations = [op["duration"] for op in operations]
                successes = [op["success"] for op in operations]
                
                summary[name] = {
                    "total_operations": len(operations),
                    "success_rate": sum(successes) / len(successes) if successes else 0,
                    "avg_duration": sum(durations) / len(durations) if durations else 0,
                    "min_duration": min(durations) if durations else 0,
                    "max_duration": max(durations) if durations else 0,
                    "recent_operations": operations[-10:]  # Last 10 operations
                }
            
            return summary

# Global instances for easy access
default_error_handler = ErrorHandler()
default_performance_monitor = PerformanceMonitor()

def main():
    """Example usage of error handling utilities"""
    
    # Example 1: Using retry decorator
    @retry_with_backoff(RetryConfig(max_attempts=3, base_delay=1.0))
    def flaky_function():
        import random
        if random.random() < 0.7:  # 70% chance of failure
            raise Exception("Random failure")
        return "Success!"
    
    # Example 2: Using scraper error handler
    @handle_scraper_errors("example_scraper")
    def scraper_function():
        # Simulate scraping operation
        raise Exception("Scraping failed")
    
    try:
        result = flaky_function()
        print(f"Result: {result}")
    except Exception as e:
        print(f"Final failure: {e}")
    
    try:
        scraper_function()
    except Exception as e:
        print(f"Scraper error handled: {e}")
    
    # Print error statistics
    stats = default_error_handler.get_error_statistics()
    print(f"Error statistics: {json.dumps(stats, indent=2)}")

if __name__ == "__main__":
    main()