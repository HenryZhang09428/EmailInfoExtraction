"""
Unified logging module for the EmailsInfoExtraction project.

Usage:
    from core.logger import get_logger
    
    logger = get_logger(__name__)
    logger.info("Processing file: %s", filename)
    logger.debug("Detailed debug info")
    logger.warning("Something unexpected happened")
    logger.error("An error occurred: %s", error)
"""

import logging
import sys
from typing import Optional

# Default log format with timestamp, level, and module name
DEFAULT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LEVEL = logging.INFO

# Global flag to track if root logger has been configured
_root_configured = False


def _configure_root_logger() -> None:
    """Configure the root logger with console output handler."""
    global _root_configured
    if _root_configured:
        return
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(DEFAULT_LEVEL)
    
    # Create formatter
    formatter = logging.Formatter(DEFAULT_FORMAT, datefmt=DEFAULT_DATE_FORMAT)
    console_handler.setFormatter(formatter)
    
    # Configure root logger for this project
    root_logger = logging.getLogger("core")
    root_logger.setLevel(DEFAULT_LEVEL)
    root_logger.addHandler(console_handler)
    root_logger.propagate = False
    
    _root_configured = True


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: The name of the logger, typically __name__ of the calling module.
        level: Optional logging level. If not specified, uses DEFAULT_LEVEL (INFO).
    
    Returns:
        A configured logging.Logger instance.
    
    Example:
        logger = get_logger(__name__)
        logger.info("Starting extraction for file: %s", filepath)
    """
    # Ensure root logger is configured
    _configure_root_logger()
    
    # Get or create logger
    logger = logging.getLogger(name)
    
    # Set level if specified
    if level is not None:
        logger.setLevel(level)
    
    return logger


def set_level(level: int, logger_name: Optional[str] = None) -> None:
    """
    Set the logging level for a specific logger or the root project logger.
    
    Args:
        level: The logging level (e.g., logging.DEBUG, logging.INFO).
        logger_name: Optional logger name. If None, sets level for root project logger.
    
    Example:
        set_level(logging.DEBUG)  # Enable debug logging for all core modules
        set_level(logging.DEBUG, "core.pipeline")  # Enable debug only for pipeline
    """
    if logger_name:
        logger = logging.getLogger(logger_name)
    else:
        logger = logging.getLogger("core")
    logger.setLevel(level)
