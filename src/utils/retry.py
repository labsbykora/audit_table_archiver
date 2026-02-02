"""Enhanced retry utilities with exponential backoff and jitter."""

import asyncio
import random
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

import structlog

from utils.logging import get_logger

T = TypeVar("T")


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
    ) -> None:
        """Initialize retry configuration.

        Args:
            max_attempts: Maximum number of retry attempts (default: 3)
            initial_delay: Initial delay in seconds (default: 1.0)
            max_delay: Maximum delay in seconds (default: 60.0)
            exponential_base: Base for exponential backoff (default: 2.0)
            jitter: Whether to add random jitter to delays (default: True)
            retryable_exceptions: Tuple of exception types to retry on
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions


def calculate_backoff_delay(
    attempt: int,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
) -> float:
    """Calculate delay for exponential backoff with optional jitter.

    Args:
        attempt: Current attempt number (0-indexed)
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff
        jitter: Whether to add random jitter

    Returns:
        Delay in seconds
    """
    # Exponential backoff: delay = initial * (base ^ attempt)
    delay = initial_delay * (exponential_base ** attempt)

    # Cap at max_delay
    delay = min(delay, max_delay)

    # Add jitter: random value between 0 and delay * 0.1
    if jitter:
        jitter_amount = delay * 0.1 * random.random()
        delay = delay + jitter_amount

    return delay


async def retry_async(
    func: Callable[..., Any],
    *args: Any,
    config: Optional[RetryConfig] = None,
    logger: Optional[structlog.BoundLogger] = None,
    **kwargs: Any,
) -> Any:
    """Retry an async function with exponential backoff and jitter.

    Args:
        func: Async function to retry
        *args: Positional arguments for function
        config: Retry configuration (uses defaults if None)
        logger: Optional logger instance
        **kwargs: Keyword arguments for function

    Returns:
        Function result

    Raises:
        Last exception if all retries exhausted
    """
    if config is None:
        config = RetryConfig()

    logger = logger or get_logger("retry")

    last_exception: Optional[Exception] = None

    for attempt in range(config.max_attempts):
        try:
            return await func(*args, **kwargs)
        except config.retryable_exceptions as e:
            last_exception = e

            if attempt < config.max_attempts - 1:
                delay = calculate_backoff_delay(
                    attempt=attempt,
                    initial_delay=config.initial_delay,
                    max_delay=config.max_delay,
                    exponential_base=config.exponential_base,
                    jitter=config.jitter,
                )

                logger.warning(
                    "Retry attempt failed, retrying",
                    attempt=attempt + 1,
                    max_attempts=config.max_attempts,
                    delay=delay,
                    error=str(e),
                )

                await asyncio.sleep(delay)
            else:
                logger.error(
                    "All retry attempts exhausted",
                    max_attempts=config.max_attempts,
                    error=str(e),
                )
                raise

    # Should never reach here, but satisfy type checker
    if last_exception:
        raise last_exception
    raise RuntimeError("Retry logic error")


def retry_sync(
    func: Callable[..., Any],
    *args: Any,
    config: Optional[RetryConfig] = None,
    logger: Optional[structlog.BoundLogger] = None,
    **kwargs: Any,
) -> Any:
    """Retry a sync function with exponential backoff and jitter.

    Args:
        func: Sync function to retry
        *args: Positional arguments for function
        config: Retry configuration (uses defaults if None)
        logger: Optional logger instance
        **kwargs: Keyword arguments for function

    Returns:
        Function result

    Raises:
        Last exception if all retries exhausted
    """
    import time

    if config is None:
        config = RetryConfig()

    logger = logger or get_logger("retry")

    last_exception: Optional[Exception] = None

    for attempt in range(config.max_attempts):
        try:
            return func(*args, **kwargs)
        except config.retryable_exceptions as e:
            last_exception = e

            if attempt < config.max_attempts - 1:
                delay = calculate_backoff_delay(
                    attempt=attempt,
                    initial_delay=config.initial_delay,
                    max_delay=config.max_delay,
                    exponential_base=config.exponential_base,
                    jitter=config.jitter,
                )

                logger.warning(
                    "Retry attempt failed, retrying",
                    attempt=attempt + 1,
                    max_attempts=config.max_attempts,
                    delay=delay,
                    error=str(e),
                )

                time.sleep(delay)
            else:
                logger.error(
                    "All retry attempts exhausted",
                    max_attempts=config.max_attempts,
                    error=str(e),
                )
                raise

    # Should never reach here, but satisfy type checker
    if last_exception:
        raise last_exception
    raise RuntimeError("Retry logic error")

