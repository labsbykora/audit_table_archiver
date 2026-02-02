"""Unit tests for sample verifier."""

import json
import gzip
import pytest
from unittest.mock import AsyncMock, MagicMock

from archiver.sample_verifier import SampleVerifier


def test_select_samples():
    """Test random sample selection."""
    verifier = SampleVerifier(
        sample_percentage=0.1,
        min_samples=5,
        max_samples=50,
    )

    # Create 100 records
    records = [{"id": i, "data": f"record_{i}"} for i in range(100)]

    samples = verifier.select_samples(records, "id")

    # Should get 10 samples (10% of 100)
    assert len(samples) == 10
    assert all(isinstance(pk, int) for pk in samples)
    assert all(0 <= pk < 100 for pk in samples)


def test_select_samples_min_samples():
    """Test that minimum samples are selected even if percentage is lower."""
    verifier = SampleVerifier(
        sample_percentage=0.01,
        min_samples=10,
        max_samples=1000,
    )

    # Create 50 records (1% would be 0.5, but min is 10)
    records = [{"id": i, "data": f"record_{i}"} for i in range(50)]

    samples = verifier.select_samples(records, "id")

    # Should get 10 samples (min_samples)
    assert len(samples) == 10


def test_select_samples_max_samples():
    """Test that maximum samples are not exceeded."""
    verifier = SampleVerifier(
        sample_percentage=0.1,
        min_samples=10,
        max_samples=50,
    )

    # Create 1000 records (10% would be 100, but max is 50)
    records = [{"id": i, "data": f"record_{i}"} for i in range(1000)]

    samples = verifier.select_samples(records, "id")

    # Should get 50 samples (max_samples)
    assert len(samples) == 50


def test_select_samples_all_records():
    """Test that all records are selected if count is less than min_samples."""
    verifier = SampleVerifier(
        sample_percentage=0.1,
        min_samples=10,
        max_samples=1000,
    )

    # Create 5 records (less than min_samples)
    records = [{"id": i, "data": f"record_{i}"} for i in range(5)]

    samples = verifier.select_samples(records, "id")

    # Should get all 5 records
    assert len(samples) == 5
    assert set(samples) == {0, 1, 2, 3, 4}


def test_select_samples_empty():
    """Test sample selection with empty records."""
    verifier = SampleVerifier()

    samples = verifier.select_samples([], "id")

    assert samples == []


def test_extract_samples_from_s3():
    """Test extracting samples from S3 JSONL data."""
    verifier = SampleVerifier()

    # Create sample records
    records = [
        {"id": 1, "data": "record_1"},
        {"id": 2, "data": "record_2"},
        {"id": 3, "data": "record_3"},
        {"id": 4, "data": "record_4"},
        {"id": 5, "data": "record_5"},
    ]

    # Serialize to JSONL and compress
    jsonl_data = "\n".join(json.dumps(record) for record in records)
    compressed_data = gzip.compress(jsonl_data.encode("utf-8"))

    # Extract samples
    sample_pks = [2, 4]  # Select records 2 and 4
    samples = verifier.extract_samples_from_s3(
        s3_data=compressed_data,
        primary_key_column="id",
        sample_pks=sample_pks,
    )

    assert len(samples) == 2
    assert {s["id"] for s in samples} == {2, 4}


def test_extract_samples_from_s3_missing():
    """Test extracting samples when some are missing."""
    verifier = SampleVerifier()

    records = [{"id": 1, "data": "record_1"}, {"id": 2, "data": "record_2"}]

    jsonl_data = "\n".join(json.dumps(record) for record in records)
    compressed_data = gzip.compress(jsonl_data.encode("utf-8"))

    # Request samples that don't exist
    sample_pks = [1, 2, 99]  # 99 doesn't exist
    samples = verifier.extract_samples_from_s3(
        s3_data=compressed_data,
        primary_key_column="id",
        sample_pks=sample_pks,
    )

    # Should find 1 and 2, but not 99
    assert len(samples) == 2
    assert {s["id"] for s in samples} == {1, 2}


@pytest.mark.asyncio
async def test_verify_samples_not_in_database():
    """Test verifying samples are not in database."""
    from unittest.mock import AsyncMock, MagicMock
    from archiver.database import DatabaseManager

    verifier = SampleVerifier()

    # Mock database manager
    mock_db_manager = MagicMock(spec=DatabaseManager)
    mock_db_manager.fetch = AsyncMock(return_value=[])  # No records found (good)

    sample_pks = [1, 2, 3]

    # Should not raise error (samples not in database)
    await verifier.verify_samples_not_in_database(
        db_manager=mock_db_manager,
        table_schema="public",
        table_name="test_table",
        primary_key_column="id",
        sample_pks=sample_pks,
    )

    # Verify query was called
    assert mock_db_manager.fetch.called


@pytest.mark.asyncio
async def test_verify_samples_found_in_database():
    """Test that verification fails when samples are found in database."""
    from unittest.mock import AsyncMock, MagicMock
    from archiver.database import DatabaseManager
    from archiver.exceptions import VerificationError

    verifier = SampleVerifier()

    # Mock database manager to return found records
    mock_db_manager = MagicMock(spec=DatabaseManager)
    mock_db_manager.fetch = AsyncMock(return_value=[
        {"id": 1},
        {"id": 2},
    ])

    sample_pks = [1, 2, 3]

    # Should raise VerificationError
    with pytest.raises(VerificationError):
        await verifier.verify_samples_not_in_database(
            db_manager=mock_db_manager,
            table_schema="public",
            table_name="test_table",
            primary_key_column="id",
            sample_pks=sample_pks,
        )


def test_sample_verifier_init_invalid_percentage():
    """Test that invalid sample percentage raises error."""
    with pytest.raises(ValueError, match="Sample percentage must be between 0 and 1"):
        SampleVerifier(sample_percentage=1.5)

    with pytest.raises(ValueError, match="Sample percentage must be between 0 and 1"):
        SampleVerifier(sample_percentage=0)


def test_sample_verifier_init_invalid_min_samples():
    """Test that invalid min_samples raises error."""
    with pytest.raises(ValueError, match="Min samples must be at least 1"):
        SampleVerifier(min_samples=0)


def test_sample_verifier_init_invalid_max_samples():
    """Test that invalid max_samples raises error."""
    with pytest.raises(ValueError, match="Max samples.*must be >= min samples"):
        SampleVerifier(min_samples=10, max_samples=5)

