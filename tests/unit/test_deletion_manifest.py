"""Unit tests for deletion manifest generation."""

import json
from datetime import datetime, timezone

import pytest

from archiver.deletion_manifest import DeletionManifestGenerator
from utils.checksum import ChecksumCalculator


def test_generate_manifest():
    """Test deletion manifest generation."""
    generator = DeletionManifestGenerator()

    primary_keys = [1, 2, 3, 4, 5]

    manifest = generator.generate_manifest(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=1,
        batch_id="batch_123",
        primary_key_column="id",
        primary_keys=primary_keys,
        deleted_count=5,
    )

    assert manifest["version"] == "1.0"
    assert manifest["manifest_info"]["database"] == "test_db"
    assert manifest["manifest_info"]["table"] == "test_table"
    assert manifest["manifest_info"]["schema"] == "public"
    assert manifest["manifest_info"]["batch_number"] == 1
    assert manifest["manifest_info"]["batch_id"] == "batch_123"
    assert manifest["manifest_info"]["primary_key_column"] == "id"
    assert manifest["deletion_info"]["primary_keys_count"] == 5
    assert manifest["deletion_info"]["deleted_count"] == 5
    assert "primary_keys" in manifest
    assert "deleted_at" in manifest["manifest_info"]


def test_generate_manifest_checksum():
    """Test that manifest includes primary keys."""
    generator = DeletionManifestGenerator()

    primary_keys = [1, 2, 3]

    manifest = generator.generate_manifest(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=1,
        batch_id="batch_123",
        primary_key_column="id",
        primary_keys=primary_keys,
        deleted_count=3,
    )

    # Verify primary keys are included
    assert manifest["primary_keys"] == primary_keys
    assert manifest["deletion_info"]["primary_keys_count"] == 3


def test_manifest_to_json():
    """Test manifest JSON serialization."""
    generator = DeletionManifestGenerator()

    manifest = generator.generate_manifest(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=1,
        batch_id="batch_123",
        primary_key_column="id",
        primary_keys=[1, 2, 3],
        deleted_count=3,
    )

    json_str = generator.manifest_to_json(manifest)

    # Should be valid JSON
    parsed = json.loads(json_str)
    assert parsed["manifest_info"]["database"] == "test_db"
    assert parsed["manifest_info"]["table"] == "test_table"
    assert parsed["deletion_info"]["primary_keys_count"] == 3
    assert parsed["deletion_info"]["deleted_count"] == 3

