"""Unit tests for exception classes."""

from archiver.exceptions import (
    ArchiverError,
    ConfigurationError,
    DatabaseError,
    S3Error,
    VerificationError,
)


def test_archiver_error_basic() -> None:
    """Test basic ArchiverError."""
    error = ArchiverError("Test error")
    assert str(error) == "Test error"
    assert error.message == "Test error"
    assert error.correlation_id is None
    assert error.context == {}


def test_archiver_error_with_correlation_id() -> None:
    """Test ArchiverError with correlation ID."""
    error = ArchiverError("Test error", correlation_id="abc123")
    assert "abc123" in str(error)
    assert error.correlation_id == "abc123"


def test_archiver_error_with_context() -> None:
    """Test ArchiverError with context."""
    error = ArchiverError("Test error", context={"database": "test_db", "table": "test_table"})
    assert error.context == {"database": "test_db", "table": "test_table"}


def test_error_hierarchy() -> None:
    """Test exception hierarchy."""
    assert issubclass(ConfigurationError, ArchiverError)
    assert issubclass(DatabaseError, ArchiverError)
    assert issubclass(S3Error, ArchiverError)
    assert issubclass(VerificationError, ArchiverError)
