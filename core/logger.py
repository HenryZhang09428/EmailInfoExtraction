"""
统一日志模块 (Unified Logging Module)
====================================

为 EmailsInfoExtraction 项目提供统一的日志配置和获取接口。

使用示例:
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
    """
    配置根日志器，添加控制台输出处理器。

    仅执行一次，通过全局标志 _root_configured 避免重复配置。
    """
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
    获取指定名称的日志器实例。

    参数:
        name: 日志器名称，通常传入调用模块的 __name__
        level: 可选的日志级别；未指定时使用 DEFAULT_LEVEL (INFO)

    返回:
        已配置的 logging.Logger 实例

    示例:
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
    设置指定日志器或项目根日志器的日志级别。

    参数:
        level: 日志级别（如 logging.DEBUG、logging.INFO）
        logger_name: 可选的日志器名称；为 None 时设置项目根日志器

    示例:
        set_level(logging.DEBUG)  # 为所有 core 模块启用 debug
        set_level(logging.DEBUG, "core.pipeline")  # 仅对 pipeline 启用 debug
    """
    if logger_name:
        logger = logging.getLogger(logger_name)
    else:
        logger = logging.getLogger("core")
    logger.setLevel(level)
