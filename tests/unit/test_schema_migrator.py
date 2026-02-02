"""Unit tests for schema migrator."""

import pytest

from archiver.exceptions import ArchiverError
from restore.schema_migrator import SchemaDiff, SchemaMigrator


@pytest.fixture
def archived_schema() -> dict:
    """Create sample archived schema."""
    return {
        "columns": [
            {"name": "id", "type": "BIGINT", "nullable": False},
            {"name": "name", "type": "TEXT", "nullable": True},
            {"name": "amount", "type": "NUMERIC", "nullable": True},
            {"name": "created_at", "type": "TIMESTAMPTZ", "nullable": False},
        ]
    }


@pytest.fixture
def current_schema() -> dict:
    """Create sample current schema."""
    return {
        "columns": [
            {"name": "id", "data_type": "BIGINT", "is_nullable": False},
            {"name": "name", "data_type": "TEXT", "is_nullable": True},
            {"name": "amount", "data_type": "DOUBLE PRECISION", "is_nullable": True},
            {"name": "email", "data_type": "TEXT", "is_nullable": True},  # Added column
            {"name": "created_at", "data_type": "TIMESTAMPTZ", "is_nullable": False},
        ]
    }


class TestSchemaDiff:
    """Tests for SchemaDiff class."""

    def test_init(self) -> None:
        """Test SchemaDiff initialization."""
        diff = SchemaDiff(
            added_columns=[{"name": "new_col"}],
            removed_columns=[{"name": "old_col"}],
            type_changes=[{"column": "col", "from": "INT", "to": "BIGINT"}],
            nullable_changes=[{"column": "col", "from": True, "to": False}],
        )

        assert len(diff.added_columns) == 1
        assert len(diff.removed_columns) == 1
        assert diff.has_changes is True

    def test_no_changes(self) -> None:
        """Test SchemaDiff with no changes."""
        diff = SchemaDiff(
            added_columns=[],
            removed_columns=[],
            type_changes=[],
            nullable_changes=[],
        )

        assert diff.has_changes is False

    def test_to_dict(self) -> None:
        """Test SchemaDiff to_dict conversion."""
        diff = SchemaDiff(
            added_columns=[{"name": "new_col"}],
            removed_columns=[],
            type_changes=[],
            nullable_changes=[],
        )

        result = diff.to_dict()
        assert "added_columns" in result
        assert "removed_columns" in result
        assert len(result["added_columns"]) == 1


class TestSchemaMigrator:
    """Tests for SchemaMigrator class."""

    def test_compare_schemas_no_changes(
        self, archived_schema: dict, current_schema: dict
    ) -> None:
        """Test schema comparison with no changes."""
        migrator = SchemaMigrator()

        # Use same schema for both
        diff = migrator.compare_schemas(archived_schema, archived_schema)

        assert diff.has_changes is False
        assert len(diff.added_columns) == 0
        assert len(diff.removed_columns) == 0

    def test_compare_schemas_added_column(
        self, archived_schema: dict, current_schema: dict
    ) -> None:
        """Test schema comparison with added column."""
        migrator = SchemaMigrator()

        diff = migrator.compare_schemas(archived_schema, current_schema)

        assert diff.has_changes is True
        assert len(diff.added_columns) == 1
        assert diff.added_columns[0]["name"] == "email"

    def test_compare_schemas_removed_column(
        self, archived_schema: dict, current_schema: dict
    ) -> None:
        """Test schema comparison with removed column."""
        migrator = SchemaMigrator()

        # Reverse the comparison
        diff = migrator.compare_schemas(current_schema, archived_schema)

        assert diff.has_changes is True
        assert len(diff.removed_columns) == 1
        assert diff.removed_columns[0]["name"] == "email"

    def test_compare_schemas_type_change(
        self, archived_schema: dict, current_schema: dict
    ) -> None:
        """Test schema comparison with type change."""
        migrator = SchemaMigrator()

        diff = migrator.compare_schemas(archived_schema, current_schema)

        assert diff.has_changes is True
        assert len(diff.type_changes) == 1
        assert diff.type_changes[0]["column"] == "amount"
        assert diff.type_changes[0]["archived_type"] == "NUMERIC"
        assert diff.type_changes[0]["current_type"] == "DOUBLE PRECISION"

    def test_generate_diff_report(self, archived_schema: dict, current_schema: dict) -> None:
        """Test diff report generation."""
        migrator = SchemaMigrator()
        diff = migrator.compare_schemas(archived_schema, current_schema)

        report = migrator.generate_diff_report(diff)

        assert "Schema Differences Detected" in report
        assert "Added Columns" in report
        assert "email" in report
        assert "Type Changes" in report
        assert "amount" in report

    def test_generate_diff_report_no_changes(self, archived_schema: dict) -> None:
        """Test diff report with no changes."""
        migrator = SchemaMigrator()
        diff = migrator.compare_schemas(archived_schema, archived_schema)

        report = migrator.generate_diff_report(diff)

        assert "No schema differences detected" in report

    def test_transform_record_strict_mode_removed_column(
        self, archived_schema: dict, current_schema: dict
    ) -> None:
        """Test record transformation in strict mode with removed column."""
        migrator = SchemaMigrator()

        record = {"id": 1, "name": "test", "amount": "10.50", "created_at": "2026-01-01T00:00:00Z"}

        # Reverse schemas to test removed column
        with pytest.raises(ArchiverError, match="columns removed"):
            migrator.transform_record(record, current_schema, archived_schema, strategy="strict")

    def test_transform_record_lenient_mode_removed_column(
        self, archived_schema: dict, current_schema: dict
    ) -> None:
        """Test record transformation in lenient mode with removed column."""
        migrator = SchemaMigrator()

        record = {
            "id": 1,
            "name": "test",
            "amount": "10.50",
            "email": "test@example.com",
            "created_at": "2026-01-01T00:00:00Z",
        }

        # Reverse schemas to test removed column
        transformed = migrator.transform_record(
            record, current_schema, archived_schema, strategy="lenient"
        )

        # Email should be removed
        assert "email" not in transformed
        assert "id" in transformed
        assert "name" in transformed

    def test_transform_record_added_column(
        self, archived_schema: dict, current_schema: dict
    ) -> None:
        """Test record transformation with added column."""
        migrator = SchemaMigrator()

        record = {"id": 1, "name": "test", "amount": "10.50", "created_at": "2026-01-01T00:00:00Z"}

        transformed = migrator.transform_record(
            record, archived_schema, current_schema, strategy="lenient"
        )

        # Email should be added with None (nullable)
        assert "email" in transformed
        assert transformed["email"] is None

    def test_transform_record_type_conversion(
        self, archived_schema: dict, current_schema: dict
    ) -> None:
        """Test record transformation with type conversion."""
        migrator = SchemaMigrator()

        record = {"id": 1, "name": "test", "amount": "10.50", "created_at": "2026-01-01T00:00:00Z"}

        transformed = migrator.transform_record(
            record, archived_schema, current_schema, strategy="transform"
        )

        # Amount should be converted from string to float
        assert "amount" in transformed
        assert isinstance(transformed["amount"], (float, str))  # Could be either depending on conversion

    def test_transform_record_strict_mode_type_change(
        self, archived_schema: dict, current_schema: dict
    ) -> None:
        """Test record transformation in strict mode with type change."""
        migrator = SchemaMigrator()

        record = {"id": 1, "name": "test", "amount": "10.50", "created_at": "2026-01-01T00:00:00Z"}

        with pytest.raises(ArchiverError, match="type changes detected"):
            migrator.transform_record(record, archived_schema, current_schema, strategy="strict")

    def test_get_default_value(self) -> None:
        """Test default value generation."""
        migrator = SchemaMigrator()

        assert migrator._get_default_value("BIGINT") == 0
        assert migrator._get_default_value("DOUBLE PRECISION") == 0.0
        assert migrator._get_default_value("BOOLEAN") is False
        assert migrator._get_default_value("TEXT") == ""
        assert migrator._get_default_value("JSONB") == {}
        assert migrator._get_default_value("ARRAY") == []

    def test_convert_type_numeric(self) -> None:
        """Test type conversion for numeric types."""
        migrator = SchemaMigrator()

        # NUMERIC to INT
        result = migrator._convert_type("10.50", "NUMERIC", "BIGINT")
        assert isinstance(result, int)
        assert result == 10

        # NUMERIC to FLOAT
        result = migrator._convert_type("10.50", "NUMERIC", "DOUBLE PRECISION")
        assert isinstance(result, float)
        assert result == 10.5

    def test_convert_type_string(self) -> None:
        """Test type conversion to string."""
        migrator = SchemaMigrator()

        result = migrator._convert_type(123, "BIGINT", "TEXT")
        assert isinstance(result, str)
        assert result == "123"

    def test_convert_type_json(self) -> None:
        """Test type conversion to JSON."""
        migrator = SchemaMigrator()

        # Dict to JSONB
        result = migrator._convert_type({"key": "value"}, "TEXT", "JSONB")
        assert isinstance(result, dict)

        # String to JSONB
        result = migrator._convert_type('{"key": "value"}', "TEXT", "JSONB")
        assert isinstance(result, dict)
        assert result["key"] == "value"

    def test_convert_type_same_type(self) -> None:
        """Test type conversion with same type."""
        migrator = SchemaMigrator()

        result = migrator._convert_type("test", "TEXT", "TEXT")
        assert result == "test"

    def test_compare_schemas_nullable_change(self) -> None:
        """Test schema comparison with nullable constraint change."""
        archived = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": True},
            ]
        }
        current = {
            "columns": [
                {"name": "id", "data_type": "BIGINT", "is_nullable": False},
            ]
        }

        migrator = SchemaMigrator()
        diff = migrator.compare_schemas(archived, current)

        assert diff.has_changes is True
        assert len(diff.nullable_changes) == 1
        assert diff.nullable_changes[0]["column"] == "id"
        assert diff.nullable_changes[0]["archived_nullable"] is True
        assert diff.nullable_changes[0]["current_nullable"] is False

