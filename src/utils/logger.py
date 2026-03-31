"""Logging configuration module.

This module sets up structured logging using structlog.
Provides both console and file logging with JSON format support.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional

import structlog


def setup_logging(
    log_level: str = "INFO",
    log_to_file: bool = True,
    log_file_path: Optional[Path] = None,
    log_format: str = "json",
    project_root: Optional[Path] = None
) -> None:
    """Set up structured logging with structlog.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to file in addition to console
        log_file_path: Path to log file. Defaults to PROJECT_ROOT/logs/app.log
        log_format: Log format ("json" or "text")
        project_root: Project root directory

    Examples:
        >>> setup_logging(log_level="DEBUG", log_to_file=True)
        >>> logger = structlog.get_logger()
        >>> logger.info("application_started", version="1.0.0")
    """
    # Determine project root if not provided
    if project_root is None:
        project_root = Path(__file__).parent.parent.parent

    # Create logs directory if it doesn't exist
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)

    # Set log file path
    if log_file_path is None:
        log_file_path = logs_dir / "app.log"
    else:
        log_file_path = Path(log_file_path)

    # Configure log level
    log_level_value = getattr(logging, log_level.upper(), logging.INFO)

    # Clear existing handlers
    logging.root.handlers.clear()

    # Create handlers
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level_value)
    handlers.append(console_handler)

    # File handler with rotation
    if log_to_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level_value)
        handlers.append(file_handler)

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        level=log_level_value,
        handlers=handlers,
        force=True
    )

    # Configure structlog processors
    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # Add appropriate renderer based on format
    if log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    # Configure structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: Optional[str] = None) -> structlog.BoundLogger:
    """Get a configured logger instance.

    Args:
        name: Logger name. If None, uses the calling module's name.

    Returns:
        Configured structlog logger

    Examples:
        >>> logger = get_logger(__name__)
        >>> logger.info("processing_started", document_id="123", status="pending")
        >>> logger.error("parsing_failed", document_id="123", error="Invalid PDF")
    """
    if name is None:
        import inspect
        frame = inspect.currentframe()
        if frame and frame.f_back:
            name = frame.f_back.f_globals.get('__name__', 'root')
        else:
            name = 'root'

    return structlog.get_logger(name)


class LoggerMixin:
    """Mixin class to add logging capability to any class.

    Usage:
        class MyClass(LoggerMixin):
            def process(self):
                self.logger.info("processing", item_id=123)
    """

    @property
    def logger(self) -> structlog.BoundLogger:
        """Get logger instance for this class."""
        return get_logger(self.__class__.__module__ + "." + self.__class__.__name__)
