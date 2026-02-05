"""S3 rate limiter using token bucket algorithm."""

import time
from typing import Any, Optional

import structlog

from utils.logging import get_logger


class TokenBucket:
    """Token bucket for rate limiting."""

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        initial_tokens: Optional[float] = None,
    ) -> None:
        """Initialize token bucket.

        Args:
            capacity: Maximum number of tokens (burst capacity)
            refill_rate: Tokens added per second
            initial_tokens: Initial number of tokens (defaults to capacity)
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = initial_tokens if initial_tokens is not None else capacity
        self.last_refill = time.time()

    def consume(self, tokens: float = 1.0) -> bool:
        """Try to consume tokens.

        Args:
            tokens: Number of tokens to consume

        Returns:
            True if tokens were consumed, False if insufficient tokens
        """
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def wait_time(self, tokens: float = 1.0) -> float:
        """Calculate time to wait before tokens are available.

        Args:
            tokens: Number of tokens needed

        Returns:
            Time in seconds to wait (0 if tokens are available)
        """
        self._refill()
        if self.tokens >= tokens:
            return 0.0
        needed = tokens - self.tokens
        return needed / self.refill_rate

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now


class S3RateLimiter:
    """Rate limiter for S3 API calls."""

    def __init__(
        self,
        requests_per_second: float = 10.0,
        burst_capacity: Optional[float] = None,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize S3 rate limiter.

        Args:
            requests_per_second: Target requests per second
            burst_capacity: Maximum burst capacity (defaults to 2 * requests_per_second)
            logger: Optional logger instance
        """
        self.requests_per_second = requests_per_second
        self.burst_capacity = burst_capacity or (2.0 * requests_per_second)
        self.logger = logger or get_logger("s3_rate_limiter")

        # Token bucket for API calls
        self.token_bucket = TokenBucket(
            capacity=self.burst_capacity,
            refill_rate=requests_per_second,
        )

        # Statistics
        self.total_requests = 0
        self.throttled_requests = 0
        self.total_wait_time = 0.0

    def acquire(self, tokens: float = 1.0, wait: bool = True) -> bool:
        """Acquire tokens for an API call.

        Args:
            tokens: Number of tokens to consume (default 1.0)
            wait: If True, wait for tokens to become available; if False, return immediately

        Returns:
            True if tokens were acquired, False if not available and wait=False
        """
        if self.token_bucket.consume(tokens):
            self.total_requests += 1
            return True

        if not wait:
            self.throttled_requests += 1
            return False

        # Wait for tokens
        wait_time = self.token_bucket.wait_time(tokens)
        if wait_time > 0:
            self.logger.debug(
                "Rate limiting: waiting for tokens",
                wait_time=wait_time,
                tokens_needed=tokens,
            )
            time.sleep(wait_time)
            self.total_wait_time += wait_time
            self.throttled_requests += 1

        # Try again after waiting
        if self.token_bucket.consume(tokens):
            self.total_requests += 1
            return True

        # Should not happen, but handle gracefully
        self.logger.warning(
            "Failed to acquire tokens after waiting",
            tokens_needed=tokens,
        )
        return False

    def handle_slowdown(self, retry_after: Optional[float] = None) -> None:
        """Handle 503 SlowDown response from S3.

        Args:
            retry_after: Optional retry-after header value in seconds
        """
        # Reduce rate by 50% temporarily
        current_rate = self.token_bucket.refill_rate
        new_rate = max(1.0, current_rate * 0.5)
        self.token_bucket.refill_rate = new_rate

        # If retry_after is provided, wait that long
        if retry_after:
            time.sleep(retry_after)
            self.total_wait_time += retry_after

        self.logger.warning(
            "S3 SlowDown detected, reducing rate",
            old_rate=current_rate,
            new_rate=new_rate,
            retry_after=retry_after,
        )

        # Gradually increase rate back (will be done on next refill)
        # For now, we'll reset after a delay
        # In a production system, you might want a more sophisticated backoff

    def reset_rate(self) -> None:
        """Reset rate to original value."""
        self.token_bucket.refill_rate = self.requests_per_second
        self.logger.info(
            "Rate limiter reset to original rate",
            rate=self.requests_per_second,
        )

    def get_stats(self) -> dict[str, Any]:
        """Get rate limiter statistics.

        Returns:
            Dictionary with statistics
        """
        return {
            "total_requests": self.total_requests,
            "throttled_requests": self.throttled_requests,
            "total_wait_time": self.total_wait_time,
            "current_rate": self.token_bucket.refill_rate,
            "available_tokens": self.token_bucket.tokens,
        }
