"""Unit tests for S3 archive reader."""

import gzip
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from archiver.config import S3Config
from archiver.exceptions import S3Error
from restore.s3_reader import ArchiveFile, S3ArchiveReader


@pytest.fixture
def s3_config() -> S3Config:
    """Create test S3 configuration."""
    return S3Config(
        bucket="test-bucket",
        prefix="archives/",
        endpoint="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )


@pytest.fixture
def sample_metadata() -> dict:
    """Create sample metadata."""
    return {
        "version": "1.0",
        "batch_info": {
            "database": "test_db",
            "schema": "public",
            "table": "audit_logs",
            "batch_number": 1,
            "batch_id": "test_db_audit_logs_001",
            "archived_at": "2026-01-06T12:00:00Z",
        },
        "data_info": {
            "record_count": 100,
            "uncompressed_size_bytes": 10000,
            "compressed_size_bytes": 5000,
            "compression_ratio": 50.0,
        },
        "checksums": {
            "jsonl_sha256": "abc123",
            "compressed_sha256": "def456",
        },
    }


@pytest.fixture
def sample_jsonl_data() -> bytes:
    """Create sample JSONL data."""
    records = [
        {"id": 1, "name": "test1", "created_at": "2026-01-01T00:00:00Z"},
        {"id": 2, "name": "test2", "created_at": "2026-01-02T00:00:00Z"},
    ]
    return "\n".join(json.dumps(r) for r in records).encode("utf-8")


class TestArchiveFile:
    """Tests for ArchiveFile class."""

    def test_init(self, sample_metadata: dict, sample_jsonl_data: bytes) -> None:
        """Test ArchiveFile initialization."""
        compressed_data = gzip.compress(sample_jsonl_data)
        archive = ArchiveFile(
            s3_key="test.jsonl.gz",
            metadata=sample_metadata,
            data=compressed_data,
            jsonl_data=sample_jsonl_data,
        )

        assert archive.s3_key == "test.jsonl.gz"
        assert archive.record_count == 100
        assert archive.batch_id == "test_db_audit_logs_001"
        assert archive.database_name == "test_db"
        assert archive.table_name == "audit_logs"
        assert archive.schema_name == "public"

    def test_parse_records(self, sample_metadata: dict, sample_jsonl_data: bytes) -> None:
        """Test parsing records from JSONL."""
        compressed_data = gzip.compress(sample_jsonl_data)
        archive = ArchiveFile(
            s3_key="test.jsonl.gz",
            metadata=sample_metadata,
            data=compressed_data,
            jsonl_data=sample_jsonl_data,
        )

        records = archive.parse_records()
        assert len(records) == 2
        assert records[0]["id"] == 1
        assert records[1]["id"] == 2


class TestS3ArchiveReader:
    """Tests for S3ArchiveReader class."""

    @pytest.mark.asyncio
    async def test_read_archive_success(
        self, s3_config: S3Config, sample_metadata: dict, sample_jsonl_data: bytes
    ) -> None:
        """Test successful archive read."""
        compressed_data = gzip.compress(sample_jsonl_data)
        metadata_bytes = json.dumps(sample_metadata).encode("utf-8")

        with patch("restore.s3_reader.S3Client") as mock_s3_client_class:
            mock_s3_client = MagicMock()
            # get_object_bytes is synchronous, returns bytes directly
            mock_s3_client.get_object_bytes = MagicMock(
                side_effect=[metadata_bytes, compressed_data]
            )
            mock_s3_client_class.return_value = mock_s3_client

            reader = S3ArchiveReader(s3_config)
            # Replace the s3_client instance with our mock
            reader.s3_client = mock_s3_client

            archive = await reader.read_archive("test.jsonl.gz", validate_checksum=False)

            assert archive.s3_key == "test.jsonl.gz"
            assert archive.record_count == 100
            assert len(archive.jsonl_data) == len(sample_jsonl_data)

    @pytest.mark.asyncio
    async def test_read_archive_with_checksum_validation(
        self, s3_config: S3Config, sample_metadata: dict, sample_jsonl_data: bytes
    ) -> None:
        """Test archive read with checksum validation."""
        compressed_data = gzip.compress(sample_jsonl_data)

        # Calculate actual checksums (ensure we pass bytes)
        from utils.checksum import ChecksumCalculator

        checksum_calc = ChecksumCalculator()
        # sample_jsonl_data is already bytes
        actual_jsonl_checksum = checksum_calc.calculate_sha256(sample_jsonl_data)
        actual_compressed_checksum = checksum_calc.calculate_sha256(compressed_data)

        # Update metadata with correct checksums
        sample_metadata["checksums"]["jsonl_sha256"] = actual_jsonl_checksum
        sample_metadata["checksums"]["compressed_sha256"] = actual_compressed_checksum
        metadata_bytes = json.dumps(sample_metadata).encode("utf-8")

        with patch("restore.s3_reader.S3Client") as mock_s3_client_class:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object_bytes = MagicMock(
                side_effect=[metadata_bytes, compressed_data]
            )
            mock_s3_client_class.return_value = mock_s3_client

            reader = S3ArchiveReader(s3_config)
            reader.s3_client = mock_s3_client
            archive = await reader.read_archive("test.jsonl.gz", validate_checksum=True)

            assert archive.record_count == 100

    @pytest.mark.asyncio
    async def test_read_archive_checksum_mismatch(
        self, s3_config: S3Config, sample_metadata: dict, sample_jsonl_data: bytes
    ) -> None:
        """Test archive read with checksum mismatch."""
        compressed_data = gzip.compress(sample_jsonl_data)
        # Use wrong checksum in metadata
        sample_metadata["checksums"]["jsonl_sha256"] = "wrong_checksum"
        metadata_bytes = json.dumps(sample_metadata).encode("utf-8")

        with patch("restore.s3_reader.S3Client") as mock_s3_client_class:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object_bytes = MagicMock(
                side_effect=[metadata_bytes, compressed_data]
            )
            mock_s3_client_class.return_value = mock_s3_client

            reader = S3ArchiveReader(s3_config)
            reader.s3_client = mock_s3_client
            with pytest.raises(S3Error, match="checksum mismatch"):
                await reader.read_archive("test.jsonl.gz", validate_checksum=True)

    @pytest.mark.asyncio
    async def test_read_archive_metadata_not_found(self, s3_config: S3Config) -> None:
        """Test archive read when metadata file is missing."""
        with patch("restore.s3_reader.S3Client") as mock_s3_client_class:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object_bytes = MagicMock(side_effect=Exception("Not found"))
            mock_s3_client_class.return_value = mock_s3_client

            reader = S3ArchiveReader(s3_config)
            reader.s3_client = mock_s3_client
            with pytest.raises(S3Error, match="Failed to read metadata"):
                await reader.read_archive("test.jsonl.gz")

    @pytest.mark.asyncio
    async def test_read_archive_decompression_error(
        self, s3_config: S3Config, sample_metadata: dict
    ) -> None:
        """Test archive read with decompression error."""
        metadata_bytes = json.dumps(sample_metadata).encode("utf-8")
        invalid_compressed = b"invalid gzip data"

        with patch("restore.s3_reader.S3Client") as mock_s3_client_class:
            mock_s3_client = MagicMock()
            mock_s3_client.get_object_bytes = MagicMock(
                side_effect=[metadata_bytes, invalid_compressed]
            )
            mock_s3_client_class.return_value = mock_s3_client

            reader = S3ArchiveReader(s3_config)
            reader.s3_client = mock_s3_client
            with pytest.raises(S3Error, match="Failed to decompress"):
                await reader.read_archive("test.jsonl.gz")

    def test_get_metadata_key(self, s3_config: S3Config) -> None:
        """Test metadata key generation."""
        reader = S3ArchiveReader(s3_config)
        assert reader._get_metadata_key("test.jsonl.gz") == "test.metadata.json"
        assert reader._get_metadata_key("path/to/file.jsonl.gz") == "path/to/file.metadata.json"

    @pytest.mark.asyncio
    async def test_list_archives(self, s3_config: S3Config) -> None:
        """Test listing archives."""
        mock_objects = [
            {"Key": "archives/db/table/year=2026/month=01/day=04/file1.jsonl.gz"},
            {"Key": "archives/db/table/year=2026/month=01/day=04/file2.jsonl.gz"},
            {
                "Key": "archives/db/table/year=2026/month=01/day=04/file1.metadata.json"
            },  # Should be filtered
        ]

        with patch("restore.s3_reader.S3Client") as mock_s3_client_class:
            mock_s3_client = MagicMock()
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = [{"Contents": mock_objects}]
            mock_s3_client.client.get_paginator.return_value = mock_paginator
            mock_s3_client_class.return_value = mock_s3_client

            reader = S3ArchiveReader(s3_config)
            archives = await reader.list_archives(database_name="db", table_name="table")

            assert len(archives) == 2
            assert all(key.endswith(".jsonl.gz") for key in archives)

    @pytest.mark.asyncio
    async def test_list_archives_with_date_filter(self, s3_config: S3Config) -> None:
        """Test listing archives with date filter."""

        mock_objects = [
            {"Key": "archives/db/table/year=2026/month=01/day=04/file1.jsonl.gz"},
            {"Key": "archives/db/table/year=2026/month=01/day=05/file2.jsonl.gz"},
        ]

        with patch("restore.s3_reader.S3Client") as mock_s3_client_class:
            mock_s3_client = MagicMock()
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = [{"Contents": mock_objects}]
            mock_s3_client.client.get_paginator.return_value = mock_paginator
            mock_s3_client_class.return_value = mock_s3_client

            reader = S3ArchiveReader(s3_config)
            start_date = datetime(2026, 1, 4, tzinfo=timezone.utc)
            end_date = datetime(2026, 1, 4, tzinfo=timezone.utc)
            archives = await reader.list_archives(
                database_name="db",
                table_name="table",
                start_date=start_date,
                end_date=end_date,
            )

            assert len(archives) == 1
            assert "year=2026/month=01/day=04/file1.jsonl.gz" in archives[0]
