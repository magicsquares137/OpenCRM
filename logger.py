"""
Arkitekt OpenCRM - Centralized Logging Module

This module provides centralized logging configuration for the entire application.
It sets up file and console handlers with proper formatting, log rotation, and
module-specific loggers.

Features:
- File and console logging with rotation
- Configurable log levels from environment
- Structured log format with timestamps
- Module-specific loggers
- Log rotation (size-based)
- Thread-safe logging
- Color-coded console output (optional)
- JSON logging support (optional)

Usage:
    from logger import get_logger
    
    # Get logger for your module
    logger = get_logger(__name__)
    
    # Log messages
    logger.info("Application started")
    logger.error("An error occurred", exc_info=True)

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional
from datetime import datetime

from config import config


# Track if logging has been initialized
_logging_initialized = False


def setup_logging() -> None:
    """
    Set up application-wide logging configuration.
    
    This function configures the root logger and creates file and console
    handlers with appropriate formatters and log levels.
    
    Should be called once at application startup.
    """
    global _logging_initialized
    
    if _logging_initialized:
        return
    
    # Get configuration
    log_level = getattr(logging, config.log_level)
    log_file = config.log_file_path
    console_enabled = config.log_console_enabled
    rotation_enabled = config.log_rotation_enabled
    max_bytes = config.log_rotation_max_mb * 1024 * 1024  # Convert MB to bytes
    backup_count = config.log_rotation_backup_count
    
    # Ensure log directory exists
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()
    
    # Create formatters
    if config.log_format == 'json':
        # JSON formatter for log aggregation systems
        formatter = JsonFormatter()
    else:
        # Standard text formatter
        detailed_format = (
            '%(asctime)s - %(name)s - %(levelname)s - '
            '[%(filename)s:%(lineno)d] - %(message)s'
        )
        formatter = logging.Formatter(
            detailed_format,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # File handler with rotation
    try:
        if rotation_enabled:
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
        else:
            file_handler = logging.FileHandler(
                log_file,
                encoding='utf-8'
            )
        
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
    except Exception as e:
        print(f"[LOGGER] Warning: Failed to create file handler: {e}", file=sys.stderr)
    
    # Console handler
    if console_enabled:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        
        # Use color formatter for console if available
        if config.log_format != 'json':
            try:
                import colorlog
                color_formatter = colorlog.ColoredFormatter(
                    '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    log_colors={
                        'DEBUG': 'cyan',
                        'INFO': 'green',
                        'WARNING': 'yellow',
                        'ERROR': 'red',
                        'CRITICAL': 'red,bg_white',
                    }
                )
                console_handler.setFormatter(color_formatter)
            except ImportError:
                # Fallback to standard formatter if colorlog not available
                console_handler.setFormatter(formatter)
        else:
            console_handler.setFormatter(formatter)
        
        root_logger.addHandler(console_handler)
    
    # Set log levels for noisy third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('msal').setLevel(logging.WARNING)
    
    _logging_initialized = True
    
    # Log initialization message
    root_logger.info("="*80)
    root_logger.info("Arkitekt OpenCRM - Logging Initialized")
    root_logger.info(f"Log Level: {config.log_level}")
    root_logger.info(f"Log File: {log_file}")
    root_logger.info(f"Console Logging: {console_enabled}")
    root_logger.info(f"Rotation: {rotation_enabled} (Max: {config.log_rotation_max_mb}MB, Backups: {backup_count})")
    root_logger.info("="*80)


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Logger name (typically __name__ of the calling module)
        level: Optional specific log level for this logger
        
    Returns:
        Configured logger instance
        
    Example:
        logger = get_logger(__name__)
        logger.info("Module initialized")
    """
    # Ensure logging is initialized
    if not _logging_initialized:
        setup_logging()
    
    logger = logging.getLogger(name)
    
    # Set specific level if provided
    if level is not None:
        logger.setLevel(level)
    
    return logger


class JsonFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.
    
    Outputs log records as JSON for easy parsing by log aggregation systems.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON formatted log string
        """
        import json
        
        log_data = {
            'timestamp': datetime.utcfromtimestamp(record.created).isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'thread': record.thread,
            'thread_name': record.threadName,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)
        
        return json.dumps(log_data)


class LoggerAdapter(logging.LoggerAdapter):
    """
    Custom logger adapter for adding contextual information to logs.
    
    Allows adding extra fields to all log messages from this adapter.
    
    Example:
        adapter = LoggerAdapter(logger, {'component': 'meta_client'})
        adapter.info("Fetching leads")
    """
    
    def process(self, msg, kwargs):
        """
        Process log message and kwargs.
        
        Args:
            msg: Log message
            kwargs: Keyword arguments
            
        Returns:
            Tuple of (message, kwargs)
        """
        # Add extra fields from adapter context
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra'].update(self.extra)
        
        return msg, kwargs


# Pre-configured loggers for different components
def get_meta_logger() -> logging.Logger:
    """Get logger for Meta API client."""
    return get_logger('meta_client')


def get_email_logger() -> logging.Logger:
    """Get logger for email client."""
    return get_logger('email_client')


def get_database_logger() -> logging.Logger:
    """Get logger for database operations."""
    return get_logger('database')


def get_main_logger() -> logging.Logger:
    """Get logger for main application logic."""
    return get_logger('main')


def get_pipeline_logger() -> logging.Logger:
    """Get logger for pipeline operations."""
    return get_logger('pipeline')


def log_exception(logger: logging.Logger, message: str, exc: Exception) -> None:
    """
    Log an exception with full traceback.
    
    Args:
        logger: Logger instance
        message: Descriptive message
        exc: Exception instance
    """
    logger.error(f"{message}: {exc}", exc_info=True)


def log_function_call(logger: logging.Logger):
    """
    Decorator to log function calls with arguments.
    
    Args:
        logger: Logger instance to use
        
    Returns:
        Decorator function
        
    Example:
        @log_function_call(logger)
        def my_function(arg1, arg2):
            pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.debug(
                f"Calling {func.__name__} with args={args}, kwargs={kwargs}"
            )
            try:
                result = func(*args, **kwargs)
                logger.debug(f"{func.__name__} completed successfully")
                return result
            except Exception as e:
                logger.error(
                    f"{func.__name__} failed with error: {e}",
                    exc_info=True
                )
                raise
        return wrapper
    return decorator


def set_log_level(level: str) -> None:
    """
    Dynamically change log level for all loggers.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    level = level.upper()
    
    if level not in valid_levels:
        print(f"[LOGGER] Invalid log level: {level}. Must be one of {valid_levels}")
        return
    
    log_level = getattr(logging, level)
    
    # Update root logger
    logging.getLogger().setLevel(log_level)
    
    # Update all handlers
    for handler in logging.getLogger().handlers:
        handler.setLevel(log_level)
    
    logging.info(f"Log level changed to: {level}")


def log_system_info(logger: logging.Logger) -> None:
    """
    Log system information for debugging purposes.
    
    Args:
        logger: Logger instance
    """
    import platform
    import sys
    
    logger.info("="*80)
    logger.info("System Information:")
    logger.info(f"  Python Version: {sys.version}")
    logger.info(f"  Platform: {platform.platform()}")
    logger.info(f"  Architecture: {platform.machine()}")
    logger.info(f"  Processor: {platform.processor()}")
    logger.info(f"  Hostname: {platform.node()}")
    logger.info("="*80)


def log_config_summary(logger: logging.Logger) -> None:
    """
    Log configuration summary (without sensitive data).
    
    Args:
        logger: Logger instance
    """
    logger.info("="*80)
    logger.info("Configuration Summary:")
    logger.info(f"  Environment: {config.environment}")
    logger.info(f"  Debug Mode: {config.debug}")
    logger.info(f"  Dry Run: {config.dry_run}")
    logger.info(f"  Poll Interval: {config.poll_interval_seconds}s")
    logger.info(f"  Database: {config.db_path}")
    logger.info(f"  Log Level: {config.log_level}")
    logger.info(f"  Sender Email: {config.ms_sender_email}")
    logger.info(f"  Booking URL: {config.booking_url}")
    logger.info("="*80)


if __name__ == '__main__':
    """
    Test logging configuration when run as script.
    
    Usage:
        python logger.py
    """
    print("\n" + "="*80)
    print("Logger Module - Test")
    print("="*80 + "\n")
    
    # Initialize logging
    setup_logging()
    
    # Get test logger
    test_logger = get_logger('test')
    
    # Test different log levels
    print("[TEST] Testing log levels...")
    test_logger.debug("This is a DEBUG message")
    test_logger.info("This is an INFO message")
    test_logger.warning("This is a WARNING message")
    test_logger.error("This is an ERROR message")
    test_logger.critical("This is a CRITICAL message")
    
    # Test exception logging
    print("\n[TEST] Testing exception logging...")
    try:
        raise ValueError("Test exception for logging")
    except Exception as e:
        log_exception(test_logger, "Test exception", e)
    
    # Test module-specific loggers
    print("\n[TEST] Testing module-specific loggers...")
    meta_logger = get_meta_logger()
    meta_logger.info("Meta client logger test")
    
    email_logger = get_email_logger()
    email_logger.info("Email client logger test")
    
    db_logger = get_database_logger()
    db_logger.info("Database logger test")
    
    main_logger = get_main_logger()
    main_logger.info("Main application logger test")
    
    # Log system info
    print("\n[TEST] Logging system information...")
    log_system_info(test_logger)
    
    # Log config summary
    print("\n[TEST] Logging configuration summary...")
    log_config_summary(test_logger)
    
    print("\n" + "="*80)
    print("[SUCCESS] All logging tests completed!")
    print(f"[INFO] Check log file: {config.log_file_path}")
    print("="*80 + "\n")
