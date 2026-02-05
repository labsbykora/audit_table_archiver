"""Unit tests for multipart cleanup module."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from archiver.exceptions import S3Error
from archiver.multipart_cleanup import MultipartCleanup


@pytest.fixture
def mock_s3_client() -> MagicMock:
    """Create a mock S3 client."""
    client = MagicMock()
    s3_config = MagicMock()
    s3_config.bucket = "test-bucket"
    s3_config.prefix = "archives/"

    s3_client = MagicMock()
    s3_client.config = s3_config
    s3_client.client = client

    return s3_client


@pytest.fixture
def cleanup(mock_s3_client: MagicMock) -> MultipartCleanup:
    """Create a MultipartCleanup instance."""
    return MultipartCleanup(mock_s3_client, stale_threshold_hours=24)


@pytest.mark.asyncio
async def test_list_orphaned_uploads_success(
    cleanup: MultipartCleanup, mock_s3_client: MagicMock
) -> None:
    """Test successful listing of orphaned uploads."""
    # Create mock paginator
    mock_paginator = MagicMock()
    mock_page_iterator = MagicMock()

    stale_time = datetime.now(timezone.utc) - timedelta(hours=25)
    fresh_time = datetime.now(timezone.utc) - timedelta(hours=1)

    mock_page_iterator.paginate.return_value = [
        {
            "Uploads": [
                {
                    "Key": "archives/db1/table1/file1.jsonl.gz",
                    "UploadId": "upload-1",
                    "Initiated": stale_time.isoformat(),
                    "Initiator": {"ID": "user1"},
                },
                {
                    "Key": "archives/db1/table1/file2.jsonl.gz",
                    "UploadId": "upload-2",
                    "Initiated": fresh_time.isoformat(),
                    "Initiator": {"ID": "user1"},
                },
            ]
        }
    ]

    mock_s3_client.client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = mock_page_iterator.paginate.return_value

    result = await cleanup.list_orphaned_uploads()

    assert len(result) == 1
    assert result[0]["key"] == "archives/db1/table1/file1.jsonl.gz"
    assert result[0]["upload_id"] == "upload-1"


@pytest.mark.asyncio
async def test_list_orphaned_uploads_with_prefix(
    cleanup: MultipartCleanup, mock_s3_client: MagicMock
) -> None:
    """Test listing with prefix filter."""
    mock_paginator = MagicMock()
    stale_time = datetime.now(timezone.utc) - timedelta(hours=25)

    mock_page_iterator = MagicMock()
    mock_page_iterator.paginate.return_value = [
        {
            "Uploads": [
                {
                    "Key": "archives/db1/table1/file1.jsonl.gz",
                    "UploadId": "upload-1",
                    "Initiated": stale_time.isoformat(),
                    "Initiator": {"ID": "user1"},
                },
                {
                    "Key": "other/db2/table2/file2.jsonl.gz",
                    "UploadId": "upload-2",
                    "Initiated": stale_time.isoformat(),
                    "Initiator": {"ID": "user1"},
                },
            ]
        }
    ]

    mock_s3_client.client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = mock_page_iterator.paginate.return_value

    result = await cleanup.list_orphaned_uploads(prefix="archives/db1/")

    assert len(result) == 1
    assert result[0]["key"] == "archives/db1/table1/file1.jsonl.gz"


@pytest.mark.asyncio
async def test_list_orphaned_uploads_client_error(
    cleanup: MultipartCleanup, mock_s3_client: MagicMock
) -> None:
    """Test listing with S3 client error."""
    mock_s3_client.client.get_paginator.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
        "ListMultipartUploads",
    )

    with pytest.raises(S3Error, match="Failed to list multipart uploads"):
        await cleanup.list_orphaned_uploads()


@pytest.mark.asyncio
async def test_list_orphaned_uploads_invalid_timestamp(
    cleanup: MultipartCleanup, mock_s3_client: MagicMock
) -> None:
    """Test listing with invalid timestamp."""
    mock_paginator = MagicMock()
    mock_page_iterator = MagicMock()
    mock_page_iterator.paginate.return_value = [
        {
            "Uploads": [
                {
                    "Key": "archives/db1/table1/file1.jsonl.gz",
                    "UploadId": "upload-1",
                    "Initiated": "invalid-timestamp",
                    "Initiator": {"ID": "user1"},
                },
            ]
        }
    ]

    mock_s3_client.client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = mock_page_iterator.paginate.return_value

    # Should not raise, but log warning
    result = await cleanup.list_orphaned_uploads()
    assert len(result) == 0


@pytest.mark.asyncio
async def test_abort_upload_success(cleanup: MultipartCleanup, mock_s3_client: MagicMock) -> None:
    """Test successful abort of upload."""
    mock_s3_client.client.abort_multipart_upload.return_value = {}

    await cleanup.abort_upload("test-key", "upload-id")

    mock_s3_client.client.abort_multipart_upload.assert_called_once_with(
        Bucket="test-bucket", Key="test-key", UploadId="upload-id"
    )


@pytest.mark.asyncio
async def test_abort_upload_not_found(cleanup: MultipartCleanup, mock_s3_client: MagicMock) -> None:
    """Test abort of non-existent upload."""
    mock_s3_client.client.abort_multipart_upload.side_effect = ClientError(
        {"Error": {"Code": "NoSuchUpload", "Message": "Upload not found"}},
        "AbortMultipartUpload",
    )

    # Should not raise, just log
    await cleanup.abort_upload("test-key", "upload-id")


@pytest.mark.asyncio
async def test_abort_upload_error(cleanup: MultipartCleanup, mock_s3_client: MagicMock) -> None:
    """Test abort with error."""
    mock_s3_client.client.abort_multipart_upload.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
        "AbortMultipartUpload",
    )

    with pytest.raises(S3Error, match="Failed to abort multipart upload"):
        await cleanup.abort_upload("test-key", "upload-id")


@pytest.mark.asyncio
async def test_cleanup_orphaned_uploads_dry_run(
    cleanup: MultipartCleanup, mock_s3_client: MagicMock
) -> None:
    """Test cleanup in dry-run mode."""
    stale_time = datetime.now(timezone.utc) - timedelta(hours=25)

    mock_paginator = MagicMock()
    mock_page_iterator = MagicMock()
    mock_page_iterator.paginate.return_value = [
        {
            "Uploads": [
                {
                    "Key": "archives/db1/table1/file1.jsonl.gz",
                    "UploadId": "upload-1",
                    "Initiated": stale_time.isoformat(),
                    "Initiator": {"ID": "user1"},
                },
            ]
        }
    ]

    mock_s3_client.client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = mock_page_iterator.paginate.return_value

    result = await cleanup.cleanup_orphaned_uploads(dry_run=True)

    assert result["total_found"] == 1
    assert result["aborted"] == 0
    mock_s3_client.client.abort_multipart_upload.assert_not_called()


@pytest.mark.asyncio
async def test_cleanup_orphaned_uploads_with_failures(
    cleanup: MultipartCleanup, mock_s3_client: MagicMock
) -> None:
    """Test cleanup with some failures."""
    stale_time = datetime.now(timezone.utc) - timedelta(hours=25)

    mock_paginator = MagicMock()
    mock_page_iterator = MagicMock()
    mock_page_iterator.paginate.return_value = [
        {
            "Uploads": [
                {
                    "Key": "archives/db1/table1/file1.jsonl.gz",
                    "UploadId": "upload-1",
                    "Initiated": stale_time.isoformat(),
                    "Initiator": {"ID": "user1"},
                },
                {
                    "Key": "archives/db1/table1/file2.jsonl.gz",
                    "UploadId": "upload-2",
                    "Initiated": stale_time.isoformat(),
                    "Initiator": {"ID": "user1"},
                },
            ]
        }
    ]

    mock_s3_client.client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = mock_page_iterator.paginate.return_value

    # First abort succeeds, second fails
    def abort_side_effect(*args, **kwargs):
        if kwargs.get("UploadId") == "upload-2":
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
                "AbortMultipartUpload",
            )

    mock_s3_client.client.abort_multipart_upload.side_effect = abort_side_effect

    result = await cleanup.cleanup_orphaned_uploads(dry_run=False)

    assert result["total_found"] == 2
    assert result["aborted"] == 1
    assert result["failed"] == 1
    assert len(result["errors"]) == 1


@pytest.mark.asyncio
async def test_cleanup_for_database_table(
    cleanup: MultipartCleanup, mock_s3_client: MagicMock
) -> None:
    """Test cleanup for specific database/table."""
    with patch.object(cleanup, "cleanup_orphaned_uploads", new_callable=AsyncMock) as mock_cleanup:
        mock_cleanup.return_value = {"total_found": 0, "aborted": 0, "failed": 0, "errors": []}

        result = await cleanup.cleanup_for_database_table("db1", "table1", dry_run=True)

        mock_cleanup.assert_called_once_with(prefix="db1/table1/", dry_run=True)
        assert result["total_found"] == 0


@pytest.mark.asyncio
async def test_list_orphaned_uploads_with_max_age(
    cleanup: MultipartCleanup, mock_s3_client: MagicMock
) -> None:
    """Test listing with custom max_age_hours."""
    mock_paginator = MagicMock()
    old_time = datetime.now(timezone.utc) - timedelta(hours=50)
    recent_time = datetime.now(timezone.utc) - timedelta(hours=10)

    mock_page_iterator = MagicMock()
    mock_page_iterator.paginate.return_value = [
        {
            "Uploads": [
                {
                    "Key": "archives/db1/table1/file1.jsonl.gz",
                    "UploadId": "upload-1",
                    "Initiated": old_time.isoformat(),
                    "Initiator": {"ID": "user1"},
                },
                {
                    "Key": "archives/db1/table1/file2.jsonl.gz",
                    "UploadId": "upload-2",
                    "Initiated": recent_time.isoformat(),
                    "Initiator": {"ID": "user1"},
                },
            ]
        }
    ]

    mock_s3_client.client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = mock_page_iterator.paginate.return_value

    # Use max_age_hours=30, so only the 50-hour-old upload should be orphaned
    result = await cleanup.list_orphaned_uploads(max_age_hours=30)

    assert len(result) == 1
    assert result[0]["key"] == "archives/db1/table1/file1.jsonl.gz"
