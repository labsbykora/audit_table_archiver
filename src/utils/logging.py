"""Structured logging configuration using structlog."""

import logging
import sys
from typing import Any, Optional

import structlog


def configure_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    correlation_id: Optional[str] = None,
) -> structlog.BoundLogger:
    """Configure structured logging.

    Args:
        log_level: Log level (DEBUG, INFO, WARN, ERROR, CRITICAL)
        log_format: Log format ('json' or 'console')
        correlation_id: Optional correlation ID for this run

    Returns:
        Configured logger instance
    """
    # Convert string level to logging constant
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    # Configure structlog processors
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,  # Add context variables
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Get logger and add correlation ID if provided
    logger = structlog.get_logger()
    if correlation_id:
        logger = logger.bind(correlation_id=correlation_id)

    return logger


def get_logger(name: Optional[str] = None) -> structlog.BoundLogger:
    """Get a logger instance.

    Args:
        name: Optional logger name

    Returns:
        Logger instance
    """
    if name:
        return structlog.get_logger(name)
    return structlog.get_logger()
