"""Replica selection strategies for load balancing."""

from abc import ABC, abstractmethod
from typing import Optional

from archiver.replica_pool import ReplicaPool


class ReplicaSelector(ABC):
    """Base class for replica selection strategies."""

    @abstractmethod
    def select_replica(
        self, replicas: list[ReplicaPool], fallback_to_primary: bool = True
    ) -> Optional[ReplicaPool]:
        """Select a replica from the list.

        Args:
            replicas: List of available replica pools
            fallback_to_primary: Whether to return None if no replica is available

        Returns:
            Selected replica pool, or None if no replica is available
        """
        pass


class RoundRobinSelector(ReplicaSelector):
    """Round-robin replica selection (distributes evenly)."""

    def __init__(self) -> None:
        """Initialize round-robin selector."""
        self._current_index = 0

    def select_replica(
        self, replicas: list[ReplicaPool], fallback_to_primary: bool = True
    ) -> Optional[ReplicaPool]:
        """Select replica using round-robin strategy.

        Args:
            replicas: List of available replica pools
            fallback_to_primary: Whether to return None if no replica is available

        Returns:
            Selected replica pool, or None if no replica is available
        """
        available = [r for r in replicas if r.is_available]
        if not available:
            return None

        if self._current_index >= len(available):
            self._current_index = 0

        selected = available[self._current_index]
        self._current_index = (self._current_index + 1) % len(available)
        return selected


class LeastConnectionsSelector(ReplicaSelector):
    """Select replica with fewest active connections."""

    def select_replica(
        self, replicas: list[ReplicaPool], fallback_to_primary: bool = True
    ) -> Optional[ReplicaPool]:
        """Select replica with least connections.

        Args:
            replicas: List of available replica pools
            fallback_to_primary: Whether to return None if no replica is available

        Returns:
            Selected replica pool, or None if no replica is available
        """
        available = [r for r in replicas if r.is_available]
        if not available:
            return None

        # Select replica with fewest active connections
        selected = min(available, key=lambda r: r.active_connections)
        return selected


class LagAwareSelector(ReplicaSelector):
    """Select replica with lowest replication lag."""

    def select_replica(
        self, replicas: list[ReplicaPool], fallback_to_primary: bool = True
    ) -> Optional[ReplicaPool]:
        """Select replica with lowest lag.

        Args:
            replicas: List of available replica pools
            fallback_to_primary: Whether to return None if no replica is available

        Returns:
            Selected replica pool, or None if no replica is available
        """
        available = [r for r in replicas if r.is_available]
        if not available:
            return None

        # Select replica with lowest lag
        selected = min(available, key=lambda r: r.health.lag_seconds)
        return selected


def create_selector(strategy: str) -> ReplicaSelector:
    """Create replica selector based on strategy name.

    Args:
        strategy: Selection strategy name

    Returns:
        ReplicaSelector instance

    Raises:
        ValueError: If strategy is unknown
    """
    strategies = {
        "round_robin": RoundRobinSelector,
        "least_connections": LeastConnectionsSelector,
        "lag_aware": LagAwareSelector,
    }

    strategy_class = strategies.get(strategy.lower())
    if not strategy_class:
        raise ValueError(
            f"Unknown replica selection strategy: {strategy}. "
            f"Valid strategies: {list(strategies.keys())}"
        )

    return strategy_class()

