"""Circuit breaker pattern for handling repeated failures."""

import time
from enum import Enum
from typing import Any, Callable, Optional

import structlog

from utils.logging import get_logger


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """Circuit breaker to prevent cascading failures."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type[Exception] = Exception,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit (default: 5)
            recovery_timeout: Seconds to wait before attempting recovery (default: 60.0)
            expected_exception: Exception type that triggers failures (default: Exception)
            logger: Optional logger instance
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.logger = logger or get_logger("circuit_breaker")

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.success_count = 0  # For half-open state

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Call function through circuit breaker.

        Args:
            func: Function to call
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            CircuitBreakerOpenError: If circuit is open
            Original exception: If function call fails
        """
        # Check if we should attempt recovery
        if self.state == CircuitState.OPEN:
            if self.last_failure_time:
                elapsed = time.time() - self.last_failure_time
                if elapsed >= self.recovery_timeout:
                    self.logger.debug(
                        "Attempting circuit recovery",
                        elapsed=elapsed,
                        recovery_timeout=self.recovery_timeout,
                    )
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                else:
                    from archiver.exceptions import ArchiverError

                    raise ArchiverError(
                        f"Circuit breaker is OPEN. Retry after {self.recovery_timeout - elapsed:.1f}s",
                        context={
                            "state": self.state.value,
                            "failure_count": self.failure_count,
                            "last_failure_time": self.last_failure_time,
                        },
                    )

        # Call function
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception:
            self._on_failure()
            raise

    async def call_async(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Call async function through circuit breaker.

        Args:
            func: Async function to call
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            CircuitBreakerOpenError: If circuit is open
            Original exception: If function call fails
        """
        # Check if we should attempt recovery
        if self.state == CircuitState.OPEN:
            if self.last_failure_time:
                elapsed = time.time() - self.last_failure_time
                if elapsed >= self.recovery_timeout:
                    self.logger.debug(
                        "Attempting circuit recovery",
                        elapsed=elapsed,
                        recovery_timeout=self.recovery_timeout,
                    )
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                else:
                    from archiver.exceptions import ArchiverError

                    raise ArchiverError(
                        f"Circuit breaker is OPEN. Retry after {self.recovery_timeout - elapsed:.1f}s",
                        context={
                            "state": self.state.value,
                            "failure_count": self.failure_count,
                            "last_failure_time": self.last_failure_time,
                        },
                    )

        # Call function
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        """Handle successful call."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            # If we get a few successes, close the circuit
            if self.success_count >= 2:
                self.logger.debug(
                    "Circuit breaker closed after successful recovery",
                    success_count=self.success_count,
                )
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == CircuitState.CLOSED:
            # Reset failure count on success
            self.failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == CircuitState.HALF_OPEN:
            # Failed during recovery, open again
            self.logger.warning(
                "Circuit breaker reopened after recovery failure",
                failure_count=self.failure_count,
            )
            self.state = CircuitState.OPEN
            self.success_count = 0
        elif (
            self.state == CircuitState.CLOSED
            and self.failure_count >= self.failure_threshold
        ):
            # Too many failures, open circuit
            self.logger.error(
                "Circuit breaker opened due to repeated failures",
                failure_count=self.failure_count,
                threshold=self.failure_threshold,
            )
            self.state = CircuitState.OPEN

    def reset(self) -> None:
        """Manually reset circuit breaker to closed state."""
        self.logger.debug("Circuit breaker manually reset")
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.success_count = 0

    def get_state(self) -> CircuitState:
        """Get current circuit breaker state.

        Returns:
            Current state
        """
        return self.state

