"""Deletion manifest generation and management."""

import json
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from utils.logging import get_logger


class DeletionManifestGenerator:
    """Generates deletion manifests for archived batches."""

    def __init__(self, logger: Optional[structlog.BoundLogger] = None) -> None:
        """Initialize deletion manifest generator.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("deletion_manifest")

    def generate_manifest(
        self,
        database_name: str,
        table_name: str,
        schema_name: str,
        batch_number: int,
        batch_id: str,
        primary_key_column: str,
        primary_keys: list[Any],
        deleted_count: int,
        deleted_at: Optional[datetime] = None,
    ) -> dict[str, Any]:
        """Generate deletion manifest dictionary.

        Args:
            database_name: Database name
            table_name: Table name
            schema_name: Schema name
            batch_number: Batch number
            batch_id: Unique batch identifier
            primary_key_column: Name of primary key column
            primary_keys: List of primary key values that were deleted
            deleted_count: Number of records actually deleted
            deleted_at: Timestamp when deletion occurred

        Returns:
            Deletion manifest dictionary
        """
        if deleted_at is None:
            deleted_at = datetime.now(timezone.utc)

        manifest = {
            "version": "1.0",
            "manifest_info": {
                "database": database_name,
                "schema": schema_name,
                "table": table_name,
                "batch_number": batch_number,
                "batch_id": batch_id,
                "primary_key_column": primary_key_column,
                "deleted_at": deleted_at.isoformat(),
            },
            "deletion_info": {
                "expected_count": len(primary_keys),
                "deleted_count": deleted_count,
                "primary_keys_count": len(primary_keys),
            },
            "primary_keys": primary_keys,  # Full list of deleted primary keys
        }

        if deleted_count != len(primary_keys):
            manifest["deletion_info"]["warning"] = (
                f"Deleted count ({deleted_count}) does not match "
                f"primary keys count ({len(primary_keys)})"
            )

        return manifest

    def manifest_to_json(self, manifest: dict[str, Any]) -> str:
        """Convert manifest dictionary to JSON string.

        Args:
            manifest: Manifest dictionary

        Returns:
            JSON string
        """
        return json.dumps(manifest, indent=2, default=str)

    def manifest_from_json(self, json_str: str) -> dict[str, Any]:
        """Parse manifest from JSON string.

        Args:
            json_str: JSON string

        Returns:
            Manifest dictionary

        Raises:
            ValueError: If JSON is invalid
        """
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid manifest JSON: {e}") from e

    def verify_manifest(
        self,
        manifest: dict[str, Any],
        expected_primary_keys: list[Any],
    ) -> bool:
        """Verify that manifest contains expected primary keys.

        Args:
            manifest: Deletion manifest dictionary
            expected_primary_keys: Expected list of primary keys

        Returns:
            True if manifest matches expected keys, False otherwise
        """
        manifest_keys = set(manifest.get("primary_keys", []))
        expected_keys = set(expected_primary_keys)

        if manifest_keys != expected_keys:
            missing = expected_keys - manifest_keys
            extra = manifest_keys - expected_keys

            self.logger.error(
                "Manifest verification failed",
                missing_count=len(missing),
                extra_count=len(extra),
                missing_sample=list(missing)[:10],
                extra_sample=list(extra)[:10],
            )
            return False

        self.logger.debug(
            "Manifest verification passed",
            key_count=len(manifest_keys),
        )
        return True

