"""Unit tests for schema detection."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from archiver.database import DatabaseManager
from archiver.schema_detector import SchemaDetector


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager."""
    db_manager = MagicMock(spec=DatabaseManager)
    return db_manager


@pytest.mark.asyncio
async def test_detect_table_schema(mock_db_manager):
    """Test table schema detection."""
    # Mock column data
    columns_data = [
        {
            "column_name": "id",
            "data_type": "integer",
            "udt_name": "int4",
            "is_nullable": "NO",
            "column_default": None,
            "character_maximum_length": None,
            "numeric_precision": 32,
            "numeric_scale": 0,
            "ordinal_position": 1,
        },
        {
            "column_name": "created_at",
            "data_type": "timestamp with time zone",
            "udt_name": "timestamptz",
            "is_nullable": "NO",
            "column_default": None,
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
            "ordinal_position": 2,
        },
    ]

    # Mock foreign keys (empty)
    fk_data = []

    # Mock indexes (empty)
    indexes_data = []

    # Mock check constraints (empty)
    check_data = []

    # Mock unique constraints (empty)
    unique_data = []

    # Primary key uses fetchone(), not fetch()
    mock_db_manager.fetchone = AsyncMock(
        return_value={
            "constraint_name": "test_table_pkey",
            "columns": ["id"],
        }
    )
    mock_db_manager.fetch = AsyncMock(
        side_effect=[
            columns_data,  # First call for columns
            fk_data,  # Second call for foreign keys
            indexes_data,  # Third call for indexes
            check_data,  # Fourth call for check constraints
            unique_data,  # Fifth call for unique constraints
        ]
    )

    detector = SchemaDetector()

    schema = await detector.detect_table_schema(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
    )

    assert schema["table_name"] == "test_table"
    assert schema["schema_name"] == "public"
    assert len(schema["columns"]) == 2
    assert schema["columns"][0]["name"] == "id"
    assert schema["columns"][1]["name"] == "created_at"
    assert schema["primary_key"] is not None
    assert schema["primary_key"]["columns"] == ["id"]
    assert len(schema["foreign_keys"]) == 0
    assert len(schema["indexes"]) == 0


@pytest.mark.asyncio
async def test_detect_table_schema_no_primary_key(mock_db_manager):
    """Test schema detection for table without primary key."""
    columns_data = [
        {
            "column_name": "id",
            "data_type": "integer",
            "udt_name": "int4",
            "is_nullable": "NO",
            "column_default": None,
            "character_maximum_length": None,
            "numeric_precision": 32,
            "numeric_scale": 0,
            "ordinal_position": 1,
        }
    ]

    # Primary key uses fetchone(), not fetch()
    mock_db_manager.fetchone = AsyncMock(return_value=None)  # No primary key
    mock_db_manager.fetch = AsyncMock(
        side_effect=[
            columns_data,  # Columns
            [],  # Foreign keys
            [],  # Indexes
            [],  # Check constraints
            [],  # Unique constraints
        ]
    )

    detector = SchemaDetector()

    schema = await detector.detect_table_schema(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
    )

    assert schema["primary_key"] is None


@pytest.mark.asyncio
async def test_detect_table_schema_with_foreign_keys(mock_db_manager):
    """Test schema detection with foreign keys."""
    columns_data = [
        {
            "column_name": "id",
            "data_type": "integer",
            "udt_name": "int4",
            "is_nullable": "NO",
            "column_default": None,
            "character_maximum_length": None,
            "numeric_precision": 32,
            "numeric_scale": 0,
            "ordinal_position": 1,
        },
        {
            "column_name": "user_id",
            "data_type": "integer",
            "udt_name": "int4",
            "is_nullable": "YES",
            "column_default": None,
            "character_maximum_length": None,
            "numeric_precision": 32,
            "numeric_scale": 0,
            "ordinal_position": 2,
        },
    ]

    # Primary key uses fetchone(), not fetch()
    mock_db_manager.fetchone = AsyncMock(
        return_value={
            "constraint_name": "test_table_pkey",
            "columns": ["id"],
        }
    )

    # Foreign key query returns rows with these fields
    fk_data = [
        {
            "constraint_name": "test_table_user_id_fkey",
            "column_name": "user_id",
            "foreign_table_schema": "public",
            "foreign_table_name": "users",
            "foreign_column_name": "id",
        }
    ]

    mock_db_manager.fetch = AsyncMock(
        side_effect=[
            columns_data,  # Columns
            fk_data,  # Foreign keys
            [],  # Indexes
            [],  # Check constraints
            [],  # Unique constraints
        ]
    )

    detector = SchemaDetector()

    schema = await detector.detect_table_schema(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
    )

    assert len(schema["foreign_keys"]) == 1
    assert schema["foreign_keys"][0]["constraint_name"] == "test_table_user_id_fkey"
    assert schema["foreign_keys"][0]["referenced_table"] == "users"
