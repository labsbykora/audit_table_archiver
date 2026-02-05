"""Custom exception hierarchy for the archiver."""

from typing import Any, Optional


class ArchiverError(Exception):
    """Base exception for all archiver errors."""

    def __init__(
        self,
        message: str,
        *,
        correlation_id: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize archiver error.

        Args:
            message: Error message
            correlation_id: Optional correlation ID for tracing
            context: Optional context dictionary with additional details
        """
        super().__init__(message)
        self.message = message
        self.correlation_id = correlation_id
        self.context = context or {}

    def __str__(self) -> str:
        """Return formatted error message."""
        parts = [self.message]
        if self.correlation_id:
            parts.append(f"[correlation_id={self.correlation_id}]")
        if self.context:
            parts.append(f"[context={self.context}]")
        return " ".join(parts)


class ConfigurationError(ArchiverError):
    """Configuration-related errors."""

    pass


class DatabaseError(ArchiverError):
    """Database-related errors."""

    pass


class S3Error(ArchiverError):
    """S3-related errors."""

    pass


class VerificationError(ArchiverError):
    """Data verification errors."""

    pass


class LockError(ArchiverError):
    """Distributed locking errors."""

    pass


class TransactionError(ArchiverError):
    """Transaction-related errors."""

    pass
