"""Unit tests for metadata generation."""

import json
from datetime import datetime, timezone, timedelta

import pytest

from archiver.metadata import MetadataGenerator


def test_generate_batch_metadata():
    """Test batch metadata generation."""
    generator = MetadataGenerator()

    metadata = generator.generate_batch_metadata(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=1,
        batch_id="batch_123",
        record_count=1000,
        jsonl_checksum="abc123",
        compressed_checksum="def456",
        uncompressed_size=100000,
        compressed_size=50000,
        primary_keys=[1, 2, 3, 4, 5],
    )

    assert metadata["version"] == "1.0"
    assert metadata["batch_info"]["database"] == "test_db"
    assert metadata["batch_info"]["table"] == "test_table"
    assert metadata["batch_info"]["schema"] == "public"
    assert metadata["batch_info"]["batch_number"] == 1
    assert metadata["batch_info"]["batch_id"] == "batch_123"
    assert metadata["data_info"]["record_count"] == 1000
    assert metadata["data_info"]["compressed_size_bytes"] == 50000
    assert metadata["data_info"]["uncompressed_size_bytes"] == 100000
    assert metadata["checksums"]["jsonl_sha256"] == "abc123"
    assert metadata["checksums"]["compressed_sha256"] == "def456"
    assert metadata["primary_keys"]["count"] == 5
    assert len(metadata["primary_keys"]["sample"]) == 5


def test_generate_batch_metadata_with_timestamp_range():
    """Test metadata generation with timestamp range."""
    generator = MetadataGenerator()

    min_timestamp = datetime.now(timezone.utc) - timedelta(days=2)
    max_timestamp = datetime.now(timezone.utc) - timedelta(days=1)

    metadata = generator.generate_batch_metadata(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=1,
        batch_id="batch_123",
        record_count=1000,
        jsonl_checksum="abc123",
        compressed_checksum="def456",
        uncompressed_size=100000,
        compressed_size=50000,
        primary_keys=[1, 2, 3],
        timestamp_range={"min": min_timestamp, "max": max_timestamp},
    )

    assert "timestamp_range" in metadata
    assert metadata["timestamp_range"]["min"] == min_timestamp.isoformat()
    assert metadata["timestamp_range"]["max"] == max_timestamp.isoformat()


def test_generate_batch_metadata_with_schema():
    """Test metadata generation with table schema."""
    generator = MetadataGenerator()

    table_schema = {
        "columns": [
            {"name": "id", "data_type": "integer"},
            {"name": "created_at", "data_type": "timestamp"},
        ],
        "primary_key": {"columns": ["id"]},
    }

    metadata = generator.generate_batch_metadata(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=1,
        batch_id="batch_123",
        record_count=1000,
        jsonl_checksum="abc123",
        compressed_checksum="def456",
        uncompressed_size=100000,
        compressed_size=50000,
        primary_keys=[1, 2, 3],
        table_schema=table_schema,
    )

    assert "table_schema" in metadata
    assert metadata["table_schema"] == table_schema


def test_metadata_to_json():
    """Test metadata JSON serialization."""
    generator = MetadataGenerator()

    metadata = generator.generate_batch_metadata(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=1,
        batch_id="batch_123",
        record_count=1000,
        jsonl_checksum="abc123",
        compressed_checksum="def456",
        uncompressed_size=100000,
        compressed_size=50000,
        primary_keys=[1, 2, 3],
    )

    json_str = generator.metadata_to_json(metadata)

    # Should be valid JSON
    parsed = json.loads(json_str)
    assert parsed["batch_info"]["database"] == "test_db"
    assert parsed["batch_info"]["table"] == "test_table"
    assert parsed["data_info"]["record_count"] == 1000
