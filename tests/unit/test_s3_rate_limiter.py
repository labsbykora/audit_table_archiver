"""Unit tests for S3 rate limiter."""

import time

from archiver.s3_rate_limiter import S3RateLimiter, TokenBucket


class TestTokenBucket:
    """Tests for TokenBucket."""

    def test_consume_available_tokens(self):
        """Test consuming tokens when available."""
        bucket = TokenBucket(capacity=10.0, refill_rate=1.0, initial_tokens=10.0)
        assert bucket.consume(5.0) is True
        assert bucket.tokens == 5.0

    def test_consume_insufficient_tokens(self):
        """Test consuming tokens when insufficient."""
        bucket = TokenBucket(capacity=10.0, refill_rate=1.0, initial_tokens=5.0)
        assert bucket.consume(10.0) is False
        assert abs(bucket.tokens - 5.0) < 0.1  # Allow small floating point differences

    def test_refill(self):
        """Test token refill over time."""
        bucket = TokenBucket(capacity=10.0, refill_rate=1.0, initial_tokens=0.0)
        time.sleep(0.1)  # Wait 100ms
        bucket._refill()
        assert bucket.tokens > 0.0
        assert bucket.tokens <= 10.0

    def test_wait_time(self):
        """Test calculating wait time."""
        bucket = TokenBucket(capacity=10.0, refill_rate=1.0, initial_tokens=5.0)
        wait_time = bucket.wait_time(10.0)
        assert abs(wait_time - 5.0) < 0.1  # Allow small floating point differences


class TestS3RateLimiter:
    """Tests for S3RateLimiter."""

    def test_init(self):
        """Test rate limiter initialization."""
        limiter = S3RateLimiter(requests_per_second=10.0)
        assert limiter.requests_per_second == 10.0
        assert limiter.burst_capacity == 20.0

    def test_acquire_success(self):
        """Test successful token acquisition."""
        limiter = S3RateLimiter(requests_per_second=10.0)
        assert limiter.acquire(wait=False) is True
        assert limiter.total_requests == 1

    def test_acquire_throttled(self):
        """Test throttled token acquisition."""
        limiter = S3RateLimiter(requests_per_second=0.1)  # Very slow rate
        # Consume all tokens
        for _ in range(20):
            limiter.acquire(wait=False)

        # Next request should be throttled
        assert limiter.acquire(wait=False) is False
        assert limiter.throttled_requests > 0

    def test_handle_slowdown(self):
        """Test handling 503 SlowDown response."""
        limiter = S3RateLimiter(requests_per_second=10.0)
        original_rate = limiter.token_bucket.refill_rate

        limiter.handle_slowdown(retry_after=1.0)

        # Rate should be reduced
        assert limiter.token_bucket.refill_rate < original_rate
        assert limiter.token_bucket.refill_rate >= 1.0

    def test_reset_rate(self):
        """Test resetting rate to original."""
        limiter = S3RateLimiter(requests_per_second=10.0)
        limiter.handle_slowdown()
        limiter.reset_rate()
        assert limiter.token_bucket.refill_rate == 10.0

    def test_get_stats(self):
        """Test getting statistics."""
        limiter = S3RateLimiter(requests_per_second=10.0)
        limiter.acquire()

        stats = limiter.get_stats()
        assert stats["total_requests"] == 1
        assert stats["current_rate"] == 10.0
        assert "available_tokens" in stats
