"""Unit tests for archive validation module."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from archiver.config import S3Config
from archiver.exceptions import S3Error, VerificationError
from restore.s3_reader import ArchiveFile
from validate.archive_validator import ArchiveValidator, ValidationResult


class TestValidationResult:
    """Tests for ValidationResult class."""

    def test_init(self):
        """Test ValidationResult initialization."""
        result = ValidationResult(
            total_archives=10,
            valid_archives=8,
            invalid_archives=2,
            orphaned_files=["file1.jsonl.gz"],
            missing_metadata=["file2.jsonl.gz"],
            checksum_failures=["file3.jsonl.gz"],
            record_count_mismatches=["file4.jsonl.gz"],
            errors=[{"file": "file5.jsonl.gz", "error": "test error"}],
        )

        assert result.total_archives == 10
        assert result.valid_archives == 8
        assert result.invalid_archives == 2
        assert len(result.orphaned_files) == 1
        assert result.is_valid is False

    def test_is_valid_true(self):
        """Test is_valid when all archives are valid."""
        result = ValidationResult(
            total_archives=10,
            valid_archives=10,
            invalid_archives=0,
            orphaned_files=[],
            missing_metadata=[],
            checksum_failures=[],
            record_count_mismatches=[],
            errors=[],
        )

        assert result.is_valid is True

    def test_to_dict(self):
        """Test converting ValidationResult to dictionary."""
        result = ValidationResult(
            total_archives=5,
            valid_archives=5,
            invalid_archives=0,
            orphaned_files=[],
            missing_metadata=[],
            checksum_failures=[],
            record_count_mismatches=[],
            errors=[],
        )

        result_dict = result.to_dict()

        assert result_dict["total_archives"] == 5
        assert result_dict["valid_archives"] == 5
        assert result_dict["is_valid"] is True

    def test_to_string(self):
        """Test converting ValidationResult to string."""
        result = ValidationResult(
            total_archives=10,
            valid_archives=8,
            invalid_archives=2,
            orphaned_files=["file1.jsonl.gz"],
            missing_metadata=[],
            checksum_failures=[],
            record_count_mismatches=[],
            errors=[],
        )

        result_str = result.to_string()

        assert "Archive Validation Report" in result_str
        assert "10 archive(s)" in result_str
        assert "Valid: 8" in result_str
        assert "Invalid: 2" in result_str


class TestArchiveValidator:
    """Tests for ArchiveValidator class."""

    @pytest.fixture
    def s3_config(self):
        """Create S3Config fixture."""
        return S3Config(
            bucket="test-bucket",
            region="us-east-1",
            prefix="archives/",
        )

    @pytest.fixture
    def validator(self, s3_config):
        """Create ArchiveValidator fixture."""
        return ArchiveValidator(s3_config)

    @pytest.mark.asyncio
    async def test_validate_archive_success(self, validator):
        """Test successful archive validation."""
        mock_archive = MagicMock(spec=ArchiveFile)
        mock_archive.metadata = {
            "batch_info": {"batch_id": "test"},
            "data_info": {"record_count": 100},
            "checksums": {"jsonl_sha256": "abc123"},
        }
        mock_archive.record_count = 100
        mock_archive.parse_records.return_value = [{"id": i} for i in range(100)]

        with patch.object(validator.s3_reader, "read_archive", new_callable=AsyncMock) as mock_read:
            mock_read.return_value = mock_archive

            result = await validator.validate_archive("test-key.jsonl.gz")

            assert result["valid"] is True
            assert len(result["errors"]) == 0

    @pytest.mark.asyncio
    async def test_validate_archive_record_count_mismatch(self, validator):
        """Test archive validation with record count mismatch."""
        mock_archive = MagicMock(spec=ArchiveFile)
        mock_archive.metadata = {
            "batch_info": {"batch_id": "test"},
            "data_info": {"record_count": 100},
            "checksums": {"jsonl_sha256": "abc123"},
        }
        mock_archive.record_count = 100
        mock_archive.parse_records.return_value = [{"id": i} for i in range(99)]  # Only 99 records

        with patch.object(validator.s3_reader, "read_archive", new_callable=AsyncMock) as mock_read:
            mock_read.return_value = mock_archive

            result = await validator.validate_archive("test-key.jsonl.gz", validate_record_count=True)

            assert result["valid"] is False
            assert any("record count" in error.lower() for error in result["errors"])

    @pytest.mark.asyncio
    async def test_validate_archive_missing_metadata(self, validator):
        """Test archive validation with missing metadata."""
        mock_archive = MagicMock(spec=ArchiveFile)
        mock_archive.metadata = None

        with patch.object(validator.s3_reader, "read_archive", new_callable=AsyncMock) as mock_read:
            mock_read.return_value = mock_archive

            result = await validator.validate_archive("test-key.jsonl.gz")

            assert result["valid"] is False
            assert any("missing metadata" in error.lower() for error in result["errors"])

    @pytest.mark.asyncio
    async def test_validate_archive_s3_error(self, validator):
        """Test archive validation with S3 error."""
        with patch.object(validator.s3_reader, "read_archive", new_callable=AsyncMock) as mock_read:
            mock_read.side_effect = S3Error("S3 error", context={})

            result = await validator.validate_archive("test-key.jsonl.gz")

            assert result["valid"] is False
            assert any("s3 error" in error.lower() for error in result["errors"])

    @pytest.mark.asyncio
    async def test_validate_archive_verification_error(self, validator):
        """Test archive validation with verification error."""
        with patch.object(validator.s3_reader, "read_archive", new_callable=AsyncMock) as mock_read:
            mock_read.side_effect = VerificationError("Checksum mismatch", context={})

            result = await validator.validate_archive("test-key.jsonl.gz")

            assert result["valid"] is False
            assert any("verification" in error.lower() for error in result["errors"])

    @pytest.mark.asyncio
    async def test_validate_archives_empty(self, validator):
        """Test validating archives when none exist."""
        with patch.object(validator.s3_reader, "list_archives", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = []

            result = await validator.validate_archives()

            assert result.total_archives == 0
            assert result.valid_archives == 0
            assert result.is_valid is True

    @pytest.mark.asyncio
    async def test_validate_archives_with_filters(self, validator):
        """Test validating archives with filters."""
        mock_archive = MagicMock(spec=ArchiveFile)
        mock_archive.metadata = {
            "batch_info": {"batch_id": "test"},
            "data_info": {"record_count": 100},
            "checksums": {"jsonl_sha256": "abc123"},
        }
        mock_archive.record_count = 100
        mock_archive.parse_records.return_value = [{"id": i} for i in range(100)]

        with patch.object(validator.s3_reader, "list_archives", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = ["archive1.jsonl.gz", "archive2.jsonl.gz"]

            with patch.object(validator, "validate_archive", new_callable=AsyncMock) as mock_validate:
                mock_validate.return_value = {"valid": True, "errors": []}

                result = await validator.validate_archives(
                    database_name="test_db",
                    table_name="test_table",
                    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    end_date=datetime(2026, 1, 31, tzinfo=timezone.utc),
                )

                assert result.total_archives == 2
                assert result.valid_archives == 2
                assert mock_list.call_count == 1

    @pytest.mark.asyncio
    async def test_validate_archives_with_errors(self, validator):
        """Test validating archives with some errors."""
        with patch.object(validator.s3_reader, "list_archives", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = ["archive1.jsonl.gz", "archive2.jsonl.gz"]

            with patch.object(validator, "validate_archive", new_callable=AsyncMock) as mock_validate:
                mock_validate.side_effect = [
                    {"valid": True, "errors": []},
                    {"valid": False, "errors": ["checksum mismatch"]},
                ]

                result = await validator.validate_archives()

                assert result.total_archives == 2
                assert result.valid_archives == 1
                assert result.invalid_archives == 1
                assert len(result.checksum_failures) == 1

    @pytest.mark.asyncio
    async def test_find_orphaned_files(self, validator):
        """Test finding orphaned files."""
        with patch.object(validator.s3_reader, "list_archives", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = ["archive1.jsonl.gz"]

            with patch.object(validator.s3_reader, "read_archive", new_callable=AsyncMock) as mock_read:
                mock_read.side_effect = S3Error("metadata not found", context={})

                orphaned = await validator._find_orphaned_files(["archive1.jsonl.gz"])

                assert len(orphaned) == 1
                assert "archive1.jsonl.gz" in orphaned[0]

