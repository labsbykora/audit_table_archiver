"""Unit tests for logging module."""

import logging
from unittest.mock import patch

import pytest

from utils.logging import configure_logging, get_logger


def test_configure_logging_json_format() -> None:
    """Test logging configuration with JSON format."""
    logger = configure_logging(log_level="INFO", log_format="json")
    assert logger is not None
    logger.info("Test message")


def test_configure_logging_console_format() -> None:
    """Test logging configuration with console format."""
    logger = configure_logging(log_level="DEBUG", log_format="console")
    assert logger is not None
    logger.debug("Test message")


def test_configure_logging_with_correlation_id() -> None:
    """Test logging configuration with correlation ID."""
    logger = configure_logging(log_level="INFO", correlation_id="test-123")
    assert logger is not None
    logger.info("Test message")


def test_get_logger() -> None:
    """Test getting logger instance."""
    logger = get_logger()
    assert logger is not None


def test_get_logger_with_name() -> None:
    """Test getting named logger instance."""
    logger = get_logger("test_module")
    assert logger is not None

