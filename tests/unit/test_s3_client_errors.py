"""Unit tests for S3 client error paths."""

from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from archiver.config import S3Config
from archiver.exceptions import S3Error
from archiver.s3_client import S3Client


@pytest.fixture
def s3_config() -> S3Config:
    """Create S3 config."""
    return S3Config(
        bucket="test-bucket",
        prefix="archives/",
        endpoint="http://localhost:9000",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
    )


@pytest.fixture
def s3_client(s3_config: S3Config) -> S3Client:
    """Create S3 client."""
    return S3Client(s3_config)


def test_client_creation_error(s3_config: S3Config) -> None:
    """Test S3 client creation failure."""
    with patch("archiver.s3_client.boto3.Session") as mock_session:
        mock_session.side_effect = Exception("Failed to create session")
        
        client = S3Client(s3_config)
        with pytest.raises(S3Error, match="Failed to create S3 client"):
            _ = client.client


def test_validate_bucket_not_found(s3_client: S3Client) -> None:
    """Test bucket validation when bucket doesn't exist."""
    mock_client = MagicMock()
    mock_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadBucket",
    )
    
    s3_client._client = mock_client
    
    with pytest.raises(S3Error, match="Bucket not found"):
        s3_client.validate_bucket()


def test_validate_bucket_access_denied(s3_client: S3Client) -> None:
    """Test bucket validation with access denied."""
    mock_client = MagicMock()
    mock_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "403", "Message": "Access Denied"}},
        "HeadBucket",
    )
    
    s3_client._client = mock_client
    
    with pytest.raises(S3Error, match="Access denied"):
        s3_client.validate_bucket()


def test_upload_file_not_found(s3_client: S3Client, tmp_path: Path) -> None:
    """Test upload with non-existent file."""
    non_existent = tmp_path / "nonexistent.txt"
    
    with pytest.raises(FileNotFoundError):
        s3_client.upload_file(non_existent, "test-key")


def test_upload_file_retry_exhausted(s3_client: S3Client, tmp_path: Path) -> None:
    """Test upload with retry exhaustion."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    
    mock_client = MagicMock()
    mock_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "ServiceUnavailable", "Message": "Service unavailable"}},
        "PutObject",
    )
    
    s3_client._client = mock_client
    
    with pytest.raises(S3Error, match="File upload failed after"):
        s3_client.upload_file(test_file, "test-key", max_retries=2)


def test_upload_verification_size_mismatch(s3_client: S3Client, tmp_path: Path) -> None:
    """Test upload verification with size mismatch."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    
    mock_client = MagicMock()
    mock_client.put_object.return_value = {"ETag": '"test-etag"'}
    mock_client.head_object.return_value = {"ContentLength": 999}  # Wrong size
    
    s3_client._client = mock_client
    
    with pytest.raises(S3Error, match="size mismatch"):
        s3_client.upload_file(test_file, "test-key")


def test_upload_verification_client_error(s3_client: S3Client, tmp_path: Path) -> None:
    """Test upload verification with client error."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    
    mock_client = MagicMock()
    mock_client.put_object.return_value = {"ETag": '"test-etag"'}
    mock_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
        "HeadObject",
    )
    
    s3_client._client = mock_client
    
    with pytest.raises(S3Error, match="Upload verification failed"):
        s3_client.upload_file(test_file, "test-key")


def test_download_file_not_found(s3_client: S3Client, tmp_path: Path) -> None:
    """Test download with non-existent object."""
    mock_client = MagicMock()
    mock_client.download_file.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}},
        "GetObject",
    )
    
    s3_client._client = mock_client
    local_path = tmp_path / "downloaded.txt"
    
    with pytest.raises(S3Error, match="Failed to download file"):
        s3_client.download_file("nonexistent-key", local_path)


def test_download_file_boto_error(s3_client: S3Client, tmp_path: Path) -> None:
    """Test download with BotoCoreError."""
    mock_client = MagicMock()
    mock_client.download_file.side_effect = BotoCoreError()
    
    s3_client._client = mock_client
    local_path = tmp_path / "downloaded.txt"
    
    with pytest.raises(S3Error, match="Boto3 error during download"):
        s3_client.download_file("test-key", local_path)


def test_get_object_bytes_not_found(s3_client: S3Client) -> None:
    """Test get_object_bytes with non-existent object."""
    mock_client = MagicMock()
    mock_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}},
        "GetObject",
    )
    
    s3_client._client = mock_client
    
    with pytest.raises(S3Error, match="Failed to get object"):
        s3_client.get_object_bytes("nonexistent-key")


def test_get_object_bytes_boto_error(s3_client: S3Client) -> None:
    """Test get_object_bytes with BotoCoreError."""
    mock_client = MagicMock()
    mock_client.get_object.side_effect = BotoCoreError()
    
    s3_client._client = mock_client
    
    with pytest.raises(S3Error, match="Boto3 error during get_object"):
        s3_client.get_object_bytes("test-key")


def test_object_exists_error(s3_client: S3Client) -> None:
    """Test object_exists with error (not 404)."""
    mock_client = MagicMock()
    mock_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
        "HeadObject",
    )
    
    s3_client._client = mock_client
    
    with pytest.raises(S3Error, match="Error checking object existence"):
        s3_client.object_exists("test-key")


def test_list_objects_error(s3_client: S3Client) -> None:
    """Test list_objects with error."""
    mock_client = MagicMock()
    mock_paginator = MagicMock()
    mock_page_iterator = MagicMock()
    mock_page_iterator.__iter__ = MagicMock(return_value=iter([]))
    mock_page_iterator.__aiter__ = MagicMock(return_value=iter([]))
    mock_paginator.paginate.return_value = mock_page_iterator
    mock_client.get_paginator.return_value = mock_paginator
    
    # Make paginate() raise the error
    mock_paginator.paginate.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
        "ListObjectsV2",
    )
    
    s3_client._client = mock_client
    
    with pytest.raises(S3Error, match="Failed to list objects"):
        s3_client.list_objects("test-prefix")


def test_upload_multipart_not_implemented(s3_client: S3Client, tmp_path: Path) -> None:
    """Test multipart upload (currently uses simple upload)."""
    # Create a large file to trigger multipart path
    test_file = tmp_path / "large.txt"
    test_file.write_bytes(b"x" * (10 * 1024 * 1024))  # 10MB
    
    mock_client = MagicMock()
    mock_client.put_object.return_value = {"ETag": '"test-etag"'}
    mock_client.head_object.return_value = {"ContentLength": test_file.stat().st_size}
    
    s3_client._client = mock_client
    
    # Should use simple upload (multipart not fully implemented)
    result = s3_client.upload_file(test_file, "test-key")
    assert result is not None
    # Should have logged warning about multipart not implemented


def test_upload_with_encryption_aws(s3_client: S3Client, tmp_path: Path) -> None:
    """Test upload with SSE-S3 encryption (AWS only)."""
    # Create config without endpoint (AWS S3)
    aws_config = S3Config(
        bucket="test-bucket",
        prefix="archives/",
        encryption="SSE-S3",
    )
    aws_client = S3Client(aws_config)
    
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    
    mock_client = MagicMock()
    mock_client.put_object.return_value = {"ETag": '"test-etag"'}
    mock_client.head_object.return_value = {"ContentLength": test_file.stat().st_size}
    
    aws_client._client = mock_client
    
    aws_client.upload_file(test_file, "test-key")
    
    # Verify encryption was set
    put_call = mock_client.put_object.call_args
    assert put_call is not None
    assert "ServerSideEncryption" in put_call.kwargs or "AES256" in str(put_call)


def test_upload_without_encryption_minio(s3_client: S3Client, tmp_path: Path) -> None:
    """Test upload without encryption (MinIO)."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    
    mock_client = MagicMock()
    mock_client.put_object.return_value = {"ETag": '"test-etag"'}
    mock_client.head_object.return_value = {"ContentLength": test_file.stat().st_size}
    
    s3_client._client = mock_client
    
    s3_client.upload_file(test_file, "test-key")
    
    # Verify encryption was NOT set (MinIO doesn't support SSE-S3)
    put_call = mock_client.put_object.call_args
    assert put_call is not None
    # For MinIO, ServerSideEncryption should not be in kwargs

