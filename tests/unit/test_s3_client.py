"""Unit tests for S3 client module."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from archiver.config import S3Config
from archiver.exceptions import S3Error
from archiver.s3_client import S3Client


@pytest.fixture
def s3_config() -> S3Config:
    """Create test S3 configuration."""
    return S3Config(
        bucket="test-bucket",
        region="us-east-1",
        prefix="archives/",
    )


def test_s3_client_init(s3_config: S3Config) -> None:
    """Test S3 client initialization."""
    client = S3Client(s3_config)
    assert client.config == s3_config
    assert client._client is None


@patch("boto3.Session")
def test_s3_client_creation(mock_session: MagicMock, s3_config: S3Config) -> None:
    """Test S3 client creation."""
    mock_s3_client = MagicMock()
    mock_session.return_value.client.return_value = mock_s3_client

    client = S3Client(s3_config)
    s3_client = client.client

    assert s3_client == mock_s3_client
    mock_session.return_value.client.assert_called_once()


@patch("boto3.Session")
def test_s3_client_with_custom_endpoint(mock_session: MagicMock) -> None:
    """Test S3 client with custom endpoint."""
    s3_config = S3Config(
        bucket="test-bucket",
        endpoint="https://s3.example.com",
        region="us-east-1",
    )

    mock_s3_client = MagicMock()
    mock_session.return_value.client.return_value = mock_s3_client

    client = S3Client(s3_config)
    _ = client.client

    # Verify endpoint_url was passed
    call_kwargs = mock_session.return_value.client.call_args[1]
    assert call_kwargs["endpoint_url"] == "https://s3.example.com"


@patch("boto3.Session")
def test_validate_bucket_success(mock_session: MagicMock, s3_config: S3Config) -> None:
    """Test bucket validation succeeds."""
    mock_s3_client = MagicMock()
    mock_session.return_value.client.return_value = mock_s3_client

    client = S3Client(s3_config)

    # Mock successful validation
    mock_s3_client.head_bucket.return_value = {}
    mock_s3_client.put_object.return_value = {}
    mock_s3_client.delete_object.return_value = {}

    # Run validation (synchronous)
    client.validate_bucket()

    mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")


@patch("boto3.Session")
def test_validate_bucket_not_found(mock_session: MagicMock, s3_config: S3Config) -> None:
    """Test bucket validation fails when bucket not found."""
    mock_s3_client = MagicMock()
    mock_session.return_value.client.return_value = mock_s3_client

    client = S3Client(s3_config)

    # Mock 404 error
    error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
    mock_s3_client.head_bucket.side_effect = ClientError(error_response, "HeadBucket")

    with pytest.raises(S3Error, match="Bucket not found"):
        client.validate_bucket()


@patch("boto3.Session")
def test_upload_file_success(mock_session: MagicMock, s3_config: S3Config) -> None:
    """Test successful file upload."""
    mock_s3_client = MagicMock()
    mock_session.return_value.client.return_value = mock_s3_client

    client = S3Client(s3_config)

    # Create temporary file
    with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
        f.write(b"test content")
        file_path = Path(f.name)

    try:
        # Mock successful upload
        mock_s3_client.put_object.return_value = {"ETag": '"test-etag"'}
        mock_s3_client.head_object.return_value = {"ContentLength": len(b"test content")}

        result = client.upload_file(file_path, "test-key.jsonl.gz")

        assert result["bucket"] == "test-bucket"
        assert "archives/test-key.jsonl.gz" in result["key"]
        assert result["size"] == len(b"test content")
        mock_s3_client.put_object.assert_called_once()
    finally:
        file_path.unlink()


@patch("boto3.Session")
def test_upload_file_retry(mock_session: MagicMock, s3_config: S3Config) -> None:
    """Test file upload with retry on failure."""
    mock_s3_client = MagicMock()
    mock_session.return_value.client.return_value = mock_s3_client

    client = S3Client(s3_config)

    # Create temporary file
    with tempfile.NamedTemporaryFile(delete=False, mode="wb") as f:
        f.write(b"test content")
        file_path = Path(f.name)

    try:
        # Mock first attempt failure, second success
        error_response = {"Error": {"Code": "500", "Message": "Internal Error"}}
        mock_s3_client.put_object.side_effect = [
            ClientError(error_response, "PutObject"),
            {"ETag": '"test-etag"'},
        ]
        mock_s3_client.head_object.return_value = {"ContentLength": len(b"test content")}

        result = client.upload_file(file_path, "test-key.jsonl.gz", max_retries=3)

        assert result["size"] == len(b"test content")
        assert mock_s3_client.put_object.call_count == 2
    finally:
        file_path.unlink()


@patch("boto3.Session")
def test_object_exists_true(mock_session: MagicMock, s3_config: S3Config) -> None:
    """Test object_exists returns True when object exists."""
    mock_s3_client = MagicMock()
    mock_session.return_value.client.return_value = mock_s3_client

    client = S3Client(s3_config)
    mock_s3_client.head_object.return_value = {}

    result = client.object_exists("test-key.jsonl.gz")
    assert result is True


@patch("boto3.Session")
def test_object_exists_false(mock_session: MagicMock, s3_config: S3Config) -> None:
    """Test object_exists returns False when object doesn't exist."""
    mock_s3_client = MagicMock()
    mock_session.return_value.client.return_value = mock_s3_client

    client = S3Client(s3_config)

    error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
    mock_s3_client.head_object.side_effect = ClientError(error_response, "HeadObject")

    result = client.object_exists("nonexistent-key.jsonl.gz")
    assert result is False

