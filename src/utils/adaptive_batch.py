"""Adaptive batch sizing based on performance metrics."""

from typing import Optional

import structlog

from utils.logging import get_logger


class AdaptiveBatchSizer:
    """Adjusts batch size based on query performance."""

    def __init__(
        self,
        initial_batch_size: int = 10000,
        min_batch_size: int = 1000,
        max_batch_size: int = 50000,
        target_query_time: float = 2.0,  # Target query time in seconds
        adjustment_factor: float = 0.2,  # Adjust by 20% at a time
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize adaptive batch sizer.

        Args:
            initial_batch_size: Starting batch size (default: 10000)
            min_batch_size: Minimum batch size (default: 1000)
            max_batch_size: Maximum batch size (default: 50000)
            target_query_time: Target query execution time in seconds (default: 2.0)
            adjustment_factor: How much to adjust batch size (default: 0.2 = 20%)
            logger: Optional logger instance
        """
        self.current_batch_size = initial_batch_size
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.target_query_time = target_query_time
        self.adjustment_factor = adjustment_factor
        self.logger = logger or get_logger("adaptive_batch")

        self.query_times: list[float] = []  # Track recent query times
        self.max_history = 10  # Keep last 10 query times

    def record_query_time(self, query_time: float, records_fetched: int) -> None:
        """Record query execution time and adjust batch size if needed.

        Args:
            query_time: Query execution time in seconds
            records_fetched: Number of records fetched
        """
        self.query_times.append(query_time)
        if len(self.query_times) > self.max_history:
            self.query_times.pop(0)

        # Calculate average query time
        avg_query_time = sum(self.query_times) / len(self.query_times)

        # Adjust batch size based on performance
        if avg_query_time < self.target_query_time * 0.7:
            # Query is fast, increase batch size
            new_size = int(self.current_batch_size * (1 + self.adjustment_factor))
            new_size = min(new_size, self.max_batch_size)
            if new_size != self.current_batch_size:
                self.logger.debug(
                    "Increasing batch size (query is fast)",
                    old_size=self.current_batch_size,
                    new_size=new_size,
                    avg_query_time=avg_query_time,
                    target_time=self.target_query_time,
                )
                self.current_batch_size = new_size
        elif avg_query_time > self.target_query_time * 1.5:
            # Query is slow, decrease batch size
            new_size = int(self.current_batch_size * (1 - self.adjustment_factor))
            new_size = max(new_size, self.min_batch_size)
            if new_size != self.current_batch_size:
                self.logger.debug(
                    "Decreasing batch size (query is slow)",
                    old_size=self.current_batch_size,
                    new_size=new_size,
                    avg_query_time=avg_query_time,
                    target_time=self.target_query_time,
                )
                self.current_batch_size = new_size

    def get_batch_size(self) -> int:
        """Get current batch size.

        Returns:
            Current batch size
        """
        return self.current_batch_size

    def reset(self) -> None:
        """Reset to initial batch size."""
        self.current_batch_size = self.min_batch_size
        self.query_times.clear()
        self.logger.debug("Adaptive batch sizer reset")
