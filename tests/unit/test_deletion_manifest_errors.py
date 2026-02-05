"""Unit tests for deletion manifest error paths."""

import json

import pytest

from archiver.deletion_manifest import DeletionManifestGenerator


@pytest.fixture
def generator() -> DeletionManifestGenerator:
    """Create a DeletionManifestGenerator instance."""
    return DeletionManifestGenerator()


def test_generate_manifest_count_mismatch(generator: DeletionManifestGenerator) -> None:
    """Test generate_manifest with count mismatch."""
    manifest = generator.generate_manifest(
        database_name="db1",
        table_name="table1",
        schema_name="public",
        batch_number=1,
        batch_id="batch-1",
        primary_key_column="id",
        primary_keys=[1, 2, 3],
        deleted_count=2,  # Mismatch
    )

    assert "warning" in manifest["deletion_info"]
    assert manifest["deletion_info"]["expected_count"] == 3
    assert manifest["deletion_info"]["deleted_count"] == 2


def test_generate_manifest_count_match(generator: DeletionManifestGenerator) -> None:
    """Test generate_manifest with matching counts."""
    manifest = generator.generate_manifest(
        database_name="db1",
        table_name="table1",
        schema_name="public",
        batch_number=1,
        batch_id="batch-1",
        primary_key_column="id",
        primary_keys=[1, 2, 3],
        deleted_count=3,
    )

    assert "warning" not in manifest["deletion_info"]


def test_manifest_to_json(generator: DeletionManifestGenerator) -> None:
    """Test manifest_to_json."""
    manifest = {
        "version": "1.0",
        "manifest_info": {"database": "db1"},
        "primary_keys": [1, 2, 3],
    }

    json_str = generator.manifest_to_json(manifest)

    assert isinstance(json_str, str)
    parsed = json.loads(json_str)
    assert parsed["version"] == "1.0"


def test_manifest_from_json_valid(generator: DeletionManifestGenerator) -> None:
    """Test manifest_from_json with valid JSON."""
    manifest_dict = {
        "version": "1.0",
        "manifest_info": {"database": "db1"},
        "primary_keys": [1, 2, 3],
    }
    json_str = json.dumps(manifest_dict)

    result = generator.manifest_from_json(json_str)

    assert result["version"] == "1.0"
    assert result["primary_keys"] == [1, 2, 3]


def test_manifest_from_json_invalid(generator: DeletionManifestGenerator) -> None:
    """Test manifest_from_json with invalid JSON."""
    invalid_json = "{ invalid json }"

    with pytest.raises(ValueError, match="Invalid manifest JSON"):
        generator.manifest_from_json(invalid_json)


def test_verify_manifest_match(generator: DeletionManifestGenerator) -> None:
    """Test verify_manifest with matching keys."""
    manifest = {
        "primary_keys": [1, 2, 3, 4, 5],
    }
    expected = [1, 2, 3, 4, 5]

    result = generator.verify_manifest(manifest, expected)
    assert result is True


def test_verify_manifest_mismatch(generator: DeletionManifestGenerator) -> None:
    """Test verify_manifest with mismatched keys."""
    manifest = {
        "primary_keys": [1, 2, 3],
    }
    expected = [1, 2, 3, 4, 5]  # Missing 4, 5

    result = generator.verify_manifest(manifest, expected)
    assert result is False


def test_verify_manifest_extra_keys(generator: DeletionManifestGenerator) -> None:
    """Test verify_manifest with extra keys in manifest."""
    manifest = {
        "primary_keys": [1, 2, 3, 4, 5],
    }
    expected = [1, 2, 3]  # Manifest has extra keys

    result = generator.verify_manifest(manifest, expected)
    assert result is False


def test_verify_manifest_empty(generator: DeletionManifestGenerator) -> None:
    """Test verify_manifest with empty lists."""
    manifest = {"primary_keys": []}
    expected = []

    result = generator.verify_manifest(manifest, expected)
    assert result is True


def test_verify_manifest_missing_key(generator: DeletionManifestGenerator) -> None:
    """Test verify_manifest when manifest is missing primary_keys."""
    manifest = {}  # Missing primary_keys
    expected = [1, 2, 3]

    result = generator.verify_manifest(manifest, expected)
    assert result is False
