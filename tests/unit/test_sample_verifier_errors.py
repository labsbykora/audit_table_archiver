"""Unit tests for sample verifier error paths."""

import gzip
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from archiver.database import DatabaseManager
from archiver.exceptions import VerificationError
from archiver.sample_verifier import SampleVerifier


@pytest.fixture
def verifier() -> SampleVerifier:
    """Create a SampleVerifier instance."""
    return SampleVerifier(sample_percentage=0.01, min_samples=10, max_samples=1000)


@pytest.fixture
def mock_db_manager() -> MagicMock:
    """Create a mock database manager."""
    return MagicMock(spec=DatabaseManager)


def test_init_invalid_percentage() -> None:
    """Test initialization with invalid sample percentage."""
    with pytest.raises(ValueError, match="Sample percentage must be between 0 and 1"):
        SampleVerifier(sample_percentage=1.5)


def test_init_invalid_min_samples() -> None:
    """Test initialization with invalid min samples."""
    with pytest.raises(ValueError, match="Min samples must be at least 1"):
        SampleVerifier(min_samples=0)


def test_init_invalid_max_samples() -> None:
    """Test initialization with max_samples < min_samples."""
    with pytest.raises(ValueError, match="Max samples.*must be >= min samples"):
        SampleVerifier(min_samples=100, max_samples=50)


def test_select_samples_empty_records(verifier: SampleVerifier) -> None:
    """Test sample selection with empty records."""
    result = verifier.select_samples([], "id")
    assert result == []


def test_select_samples_fewer_than_min(verifier: SampleVerifier) -> None:
    """Test sample selection when records < min_samples."""
    records = [{"id": i} for i in range(5)]  # Less than min_samples (10)
    result = verifier.select_samples(records, "id")
    assert len(result) == 5  # Should sample all


def test_select_samples_more_than_max(verifier: SampleVerifier) -> None:
    """Test sample selection when calculated > max_samples."""
    records = [{"id": i} for i in range(100000)]  # Would calculate 1000 samples
    result = verifier.select_samples(records, "id")
    assert len(result) == 1000  # Should cap at max_samples


def test_extract_samples_from_s3_decompression_error(verifier: SampleVerifier) -> None:
    """Test extract_samples_from_s3 with decompression error."""
    invalid_data = b"not gzip data"

    with pytest.raises(VerificationError, match="Failed to decompress"):
        verifier.extract_samples_from_s3(invalid_data, "id", [1, 2, 3])


def test_extract_samples_from_s3_invalid_jsonl(verifier: SampleVerifier) -> None:
    """Test extract_samples_from_s3 with invalid JSONL."""
    # Create valid gzip but invalid JSONL
    invalid_jsonl = b"not json\nalso not json"
    compressed = gzip.compress(invalid_jsonl)

    # Should not raise, but log warnings
    result = verifier.extract_samples_from_s3(compressed, "id", [1, 2, 3])
    assert result == []  # No valid records found


def test_extract_samples_from_s3_missing_keys(verifier: SampleVerifier) -> None:
    """Test extract_samples_from_s3 when some keys are missing."""
    records = [
        {"id": 1, "name": "test1"},
        {"id": 2, "name": "test2"},
        {"id": 3, "name": "test3"},
    ]
    jsonl = "\n".join(json.dumps(r) for r in records)
    compressed = gzip.compress(jsonl.encode("utf-8"))

    # Request keys that don't all exist
    result = verifier.extract_samples_from_s3(compressed, "id", [1, 5, 10])

    # Should find key 1, but log warning about missing 5 and 10
    assert len(result) == 1
    assert result[0]["id"] == 1


@pytest.mark.asyncio
async def test_verify_samples_not_in_database_found(
    verifier: SampleVerifier, mock_db_manager: MagicMock
) -> None:
    """Test verify_samples_not_in_database when samples are found."""
    # Simulate finding some samples in database
    mock_db_manager.fetch = AsyncMock(return_value=[{"id": 1}, {"id": 2}])  # Found 2 of 3 samples

    with pytest.raises(VerificationError, match="Sample verification failed"):
        await verifier.verify_samples_not_in_database(
            mock_db_manager, "public", "test_table", "id", [1, 2, 3]
        )


@pytest.mark.asyncio
async def test_verify_samples_not_in_database_no_samples(
    verifier: SampleVerifier, mock_db_manager: MagicMock
) -> None:
    """Test verify_samples_not_in_database with no samples."""
    # Should not raise, just log warning
    await verifier.verify_samples_not_in_database(mock_db_manager, "public", "test_table", "id", [])


@pytest.mark.asyncio
async def test_verify_samples_not_in_database_error(
    verifier: SampleVerifier, mock_db_manager: MagicMock
) -> None:
    """Test verify_samples_not_in_database with database error."""
    mock_db_manager.fetch = AsyncMock(side_effect=Exception("Database error"))

    with pytest.raises(VerificationError, match="Failed to verify samples"):
        await verifier.verify_samples_not_in_database(
            mock_db_manager, "public", "test_table", "id", [1, 2, 3]
        )


@pytest.mark.asyncio
async def test_verify_samples_not_in_database_success(
    verifier: SampleVerifier, mock_db_manager: MagicMock
) -> None:
    """Test verify_samples_not_in_database when all samples are deleted."""
    mock_db_manager.fetch = AsyncMock(return_value=[])  # No samples found

    # Should not raise
    await verifier.verify_samples_not_in_database(
        mock_db_manager, "public", "test_table", "id", [1, 2, 3]
    )
