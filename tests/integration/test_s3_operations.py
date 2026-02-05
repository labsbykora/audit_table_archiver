"""Integration tests for S3 operations."""

import tempfile
from pathlib import Path

import pytest

# Fixtures are auto-discovered from conftest.py by pytest


@pytest.mark.integration
def test_s3_bucket_validation(s3_client) -> None:
    """Test S3 bucket validation."""
    # Should not raise
    s3_client.validate_bucket()


@pytest.mark.integration
def test_s3_upload_and_verify(s3_client) -> None:
    """Test S3 upload and verification."""
    # Create test file
    with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
        test_content = b"test content for S3 upload"
        f.write(test_content)
        file_path = Path(f.name)

    try:
        # Upload
        result = s3_client.upload_file(file_path, "test/upload_test.txt")

        assert result["bucket"] == "test-archives"
        assert "test/upload_test.txt" in result["key"]
        assert result["size"] == len(test_content)

        # Verify object exists (use the key returned from upload, which includes prefix)
        # The upload returns the full key with prefix, so use that
        assert s3_client.object_exists(result["key"]) is True

        # Verify non-existent object (use a key that doesn't exist)
        assert s3_client.object_exists("nonexistent.txt") is False

    finally:
        file_path.unlink()


@pytest.mark.integration
def test_s3_upload_retry(s3_client) -> None:
    """Test S3 upload retry logic."""
    # Create test file
    with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
        test_content = b"test content"
        f.write(test_content)
        file_path = Path(f.name)

    try:
        # Upload should succeed (even if it needs retries)
        result = s3_client.upload_file(file_path, "test/retry_test.txt", max_retries=3)

        assert result["size"] == len(test_content)
        # Verify object exists (use the key returned from upload)
        assert s3_client.object_exists(result["key"]) is True

    finally:
        file_path.unlink()
