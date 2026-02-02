"""Unit tests for schema drift detection."""

import copy

import pytest

from archiver.exceptions import VerificationError
from archiver.schema_drift import SchemaDriftDetector


@pytest.fixture
def detector() -> SchemaDriftDetector:
    """Create a SchemaDriftDetector instance."""
    return SchemaDriftDetector(fail_on_drift=False)


@pytest.fixture
def strict_detector() -> SchemaDriftDetector:
    """Create a strict SchemaDriftDetector instance."""
    return SchemaDriftDetector(fail_on_drift=True)


@pytest.fixture
def base_schema() -> dict:
    """Create a base schema for testing."""
    return {
        "columns": [
            {"name": "id", "data_type": "bigint", "is_nullable": False},
            {"name": "name", "data_type": "text", "is_nullable": True},
            {"name": "created_at", "data_type": "timestamp", "is_nullable": False},
        ],
        "primary_key": {"constraint_name": "pk_test", "columns": ["id"]},
        "foreign_keys": [],
        "indexes": [],
        "check_constraints": [],
        "unique_constraints": [],
    }


def test_compare_schemas_no_previous(detector: SchemaDriftDetector) -> None:
    """Test comparison when no previous schema exists."""
    current = {
        "columns": [{"name": "id", "data_type": "bigint"}],
        "primary_key": None,
        "foreign_keys": [],
        "indexes": [],
        "check_constraints": [],
        "unique_constraints": [],
    }
    
    result = detector.compare_schemas(current, None, "test_db", "test_table")
    
    assert result["has_drift"] is False
    assert len(result["changes"]) == 0


def test_compare_schemas_no_drift(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test comparison with identical schemas."""
    result = detector.compare_schemas(base_schema, base_schema, "test_db", "test_table")
    
    assert result["has_drift"] is False
    assert len(result["changes"]) == 0


def test_compare_schemas_column_added(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of added column."""
    current = copy.deepcopy(base_schema)
    current["columns"].append({"name": "email", "data_type": "text", "is_nullable": True})
    
    result = detector.compare_schemas(current, base_schema, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert len(result["column_additions"]) == 1
    assert "email" in result["column_additions"]
    assert any("Column added: email" in change for change in result["changes"])


def test_compare_schemas_column_removed(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of removed column."""
    current = base_schema.copy()
    current["columns"] = [col for col in current["columns"] if col["name"] != "name"]
    
    result = detector.compare_schemas(current, base_schema, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert len(result["column_removals"]) == 1
    assert "name" in result["column_removals"]
    assert any("Column removed: name" in change for change in result["changes"])


def test_compare_schemas_column_type_changed(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of column type change."""
    current = copy.deepcopy(base_schema)
    current["columns"][1]["data_type"] = "varchar"
    
    result = detector.compare_schemas(current, base_schema, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert len(result["column_type_changes"]) == 1
    assert result["column_type_changes"][0]["column"] == "name"
    assert any("Column type changed: name" in change for change in result["changes"])


def test_compare_schemas_nullable_changed(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of nullable change."""
    current = copy.deepcopy(base_schema)
    current["columns"][1]["is_nullable"] = False
    
    result = detector.compare_schemas(current, base_schema, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert any("Column nullable changed: name" in change for change in result["changes"])


def test_compare_schemas_primary_key_changed(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of primary key change."""
    current = base_schema.copy()
    current["primary_key"] = {"constraint_name": "pk_test2", "columns": ["id", "name"]}
    
    result = detector.compare_schemas(current, base_schema, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert len(result["constraint_changes"]) == 1
    assert any("Primary key changed" in change for change in result["changes"])


def test_compare_schemas_foreign_key_added(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of added foreign key."""
    current = base_schema.copy()
    current["foreign_keys"] = [
        {
            "constraint_name": "fk_test",
            "columns": ["user_id"],
            "referenced_schema": "public",
            "referenced_table": "users",
            "referenced_columns": ["id"],
        }
    ]
    
    result = detector.compare_schemas(current, base_schema, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert any("Foreign key added: fk_test" in change for change in result["changes"])


def test_compare_schemas_foreign_key_removed(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of removed foreign key."""
    previous = base_schema.copy()
    previous["foreign_keys"] = [
        {
            "constraint_name": "fk_test",
            "columns": ["user_id"],
            "referenced_schema": "public",
            "referenced_table": "users",
            "referenced_columns": ["id"],
        }
    ]
    
    result = detector.compare_schemas(base_schema, previous, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert any("Foreign key removed: fk_test" in change for change in result["changes"])


def test_compare_schemas_index_added(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of added index."""
    current = base_schema.copy()
    current["indexes"] = [{"name": "idx_name", "definition": "CREATE INDEX...", "columns": ["name"]}]
    
    result = detector.compare_schemas(current, base_schema, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert any("Index added: idx_name" in change for change in result["changes"])


def test_compare_schemas_index_removed(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of removed index."""
    previous = base_schema.copy()
    previous["indexes"] = [{"name": "idx_name", "definition": "CREATE INDEX...", "columns": ["name"]}]
    
    result = detector.compare_schemas(base_schema, previous, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert any("Index removed: idx_name" in change for change in result["changes"])


def test_compare_schemas_fail_on_drift(strict_detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test that strict detector raises exception on drift."""
    current = copy.deepcopy(base_schema)
    current["columns"].append({"name": "email", "data_type": "text", "is_nullable": True})
    
    with pytest.raises(VerificationError, match="Schema drift detected"):
        strict_detector.compare_schemas(current, base_schema, "test_db", "test_table")


def test_compare_schemas_multiple_changes(detector: SchemaDriftDetector, base_schema: dict) -> None:
    """Test detection of multiple changes."""
    current = copy.deepcopy(base_schema)
    current["columns"].append({"name": "email", "data_type": "text", "is_nullable": True})
    current["columns"][1]["data_type"] = "varchar"
    current["primary_key"] = {"constraint_name": "pk_test2", "columns": ["id", "name"]}
    
    result = detector.compare_schemas(current, base_schema, "test_db", "test_table")
    
    assert result["has_drift"] is True
    assert len(result["changes"]) >= 3
    assert len(result["column_additions"]) == 1
    assert len(result["column_type_changes"]) == 1
    assert len(result["constraint_changes"]) == 1


def test_compare_schemas_empty_schemas(detector: SchemaDriftDetector) -> None:
    """Test comparison with empty schemas."""
    empty_schema = {
        "columns": [],
        "primary_key": None,
        "foreign_keys": [],
        "indexes": [],
        "check_constraints": [],
        "unique_constraints": [],
    }
    
    result = detector.compare_schemas(empty_schema, empty_schema, "test_db", "test_table")
    
    assert result["has_drift"] is False


def test_compare_schemas_missing_keys(detector: SchemaDriftDetector) -> None:
    """Test comparison with schemas missing some keys."""
    schema1 = {"columns": [{"name": "id", "data_type": "bigint"}]}
    schema2 = {
        "columns": [{"name": "id", "data_type": "bigint"}],
        "primary_key": None,
        "foreign_keys": [],
        "indexes": [],
        "check_constraints": [],
        "unique_constraints": [],
    }
    
    # Should handle missing keys gracefully
    result = detector.compare_schemas(schema1, schema2, "test_db", "test_table")
    
    # Should not crash, but may detect drift due to missing keys
    assert isinstance(result["has_drift"], bool)
