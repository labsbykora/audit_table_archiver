"""Unit tests for serializer module."""

import base64
import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from archiver.serializer import PostgreSQLSerializer


def test_serialize_row_basic() -> None:
    """Test basic row serialization."""
    serializer = PostgreSQLSerializer()
    row = {"id": 1, "name": "test", "active": True}
    archived_at = datetime.now(timezone.utc)

    result = serializer.serialize_row(
        row=row,
        batch_id="test-batch-123",
        database_name="test_db",
        table_name="test_table",
        archived_at=archived_at,
    )

    assert result["id"] == 1
    assert result["name"] == "test"
    assert result["active"] is True
    assert result["_batch_id"] == "test-batch-123"
    assert result["_source_database"] == "test_db"
    assert result["_source_table"] == "test_table"
    assert "_archived_at" in result


def test_serialize_row_with_null() -> None:
    """Test serialization with NULL values."""
    serializer = PostgreSQLSerializer()
    row = {"id": 1, "name": None, "description": "test"}
    archived_at = datetime.now(timezone.utc)

    result = serializer.serialize_row(
        row=row,
        batch_id="test-batch",
        database_name="test_db",
        table_name="test_table",
        archived_at=archived_at,
    )

    assert result["id"] == 1
    assert result["name"] is None
    assert result["description"] == "test"


def test_serialize_datetime() -> None:
    """Test datetime serialization."""
    serializer = PostgreSQLSerializer()
    dt = datetime(2025, 1, 1, 12, 30, 45, tzinfo=timezone.utc)
    result = serializer._serialize_datetime(dt)
    assert "2025-01-01T12:30:45" in result


def test_serialize_date() -> None:
    """Test date serialization."""
    serializer = PostgreSQLSerializer()
    d = date(2025, 1, 1)
    result = serializer._serialize_datetime(d)
    assert result == "2025-01-01"


def test_serialize_decimal() -> None:
    """Test decimal serialization."""
    serializer = PostgreSQLSerializer()
    value = Decimal("123.45678901234567890")
    result = serializer._serialize_value(value)
    assert result == "123.45678901234567890"
    assert isinstance(result, str)


def test_serialize_uuid() -> None:
    """Test UUID serialization."""
    serializer = PostgreSQLSerializer()
    uuid_val = UUID("123e4567-e89b-12d3-a456-426614174000")
    result = serializer._serialize_value(uuid_val)
    assert result == "123e4567-e89b-12d3-a456-426614174000"
    assert isinstance(result, str)


def test_serialize_bytes() -> None:
    """Test BYTEA serialization."""
    serializer = PostgreSQLSerializer()
    byte_val = b"test binary data"
    result = serializer._serialize_value(byte_val)
    assert isinstance(result, str)
    # Should be base64 encoded
    decoded = base64.b64decode(result)
    assert decoded == byte_val


def test_serialize_array() -> None:
    """Test array serialization."""
    serializer = PostgreSQLSerializer()
    array_val = [1, 2, 3, "test"]
    result = serializer._serialize_value(array_val)
    assert result == [1, 2, 3, "test"]


def test_serialize_jsonb() -> None:
    """Test JSONB serialization."""
    serializer = PostgreSQLSerializer()
    json_val = {"key": "value", "nested": {"inner": 123}}
    result = serializer._serialize_value(json_val)
    assert result == {"key": "value", "nested": {"inner": 123}}


def test_to_jsonl() -> None:
    """Test JSONL conversion."""
    serializer = PostgreSQLSerializer()
    rows = [
        {"id": 1, "name": "test1"},
        {"id": 2, "name": "test2"},
    ]

    jsonl_data = serializer.to_jsonl(rows)
    assert isinstance(jsonl_data, bytes)

    lines = jsonl_data.decode("utf-8").split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": 1, "name": "test1"}
    assert json.loads(lines[1]) == {"id": 2, "name": "test2"}


def test_count_jsonl_lines() -> None:
    """Test JSONL line counting."""
    serializer = PostgreSQLSerializer()
    jsonl_data = b'{"id":1}\n{"id":2}\n{"id":3}'
    count = serializer.count_jsonl_lines(jsonl_data)
    assert count == 3


def test_count_jsonl_lines_empty() -> None:
    """Test JSONL line counting with empty data."""
    serializer = PostgreSQLSerializer()
    count = serializer.count_jsonl_lines(b"")
    assert count == 0  # Empty string has 0 lines


def test_count_jsonl_lines_single_line() -> None:
    """Test JSONL line counting with single line."""
    serializer = PostgreSQLSerializer()
    jsonl_data = b'{"id":1}'
    count = serializer.count_jsonl_lines(jsonl_data)
    assert count == 1

