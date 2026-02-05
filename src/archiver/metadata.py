"""Metadata file generation and management for archived batches."""

import json
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from utils.logging import get_logger


class MetadataGenerator:
    """Generates metadata files for archived batches."""

    def __init__(self, logger: Optional[structlog.BoundLogger] = None) -> None:
        """Initialize metadata generator.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("metadata")

    def generate_batch_metadata(
        self,
        database_name: str,
        table_name: str,
        schema_name: str,
        batch_number: int,
        batch_id: str,
        record_count: int,
        jsonl_checksum: str,
        compressed_checksum: str,
        uncompressed_size: int,
        compressed_size: int,
        primary_keys: list[Any],
        timestamp_range: Optional[dict[str, Optional[datetime]]] = None,
        archived_at: Optional[datetime] = None,
        table_schema: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Generate metadata dictionary for a batch.

        Args:
            database_name: Database name
            table_name: Table name
            schema_name: Schema name
            batch_number: Batch number
            batch_id: Unique batch identifier
            record_count: Number of records in batch
            jsonl_checksum: SHA-256 checksum of JSONL data (before compression)
            compressed_checksum: SHA-256 checksum of compressed data
            uncompressed_size: Size of uncompressed JSONL data in bytes
            compressed_size: Size of compressed data in bytes
            primary_keys: List of primary key values in this batch
            timestamp_range: Optional dict with 'min' and 'max' timestamp values
            archived_at: Timestamp when archival occurred

        Returns:
            Metadata dictionary
        """
        if archived_at is None:
            archived_at = datetime.now(timezone.utc)

        metadata = {
            "version": "1.0",
            "batch_info": {
                "database": database_name,
                "schema": schema_name,
                "table": table_name,
                "batch_number": batch_number,
                "batch_id": batch_id,
                "archived_at": archived_at.isoformat(),
            },
            "data_info": {
                "record_count": record_count,
                "uncompressed_size_bytes": uncompressed_size,
                "compressed_size_bytes": compressed_size,
                "compression_ratio": (
                    (1 - compressed_size / uncompressed_size) * 100 if uncompressed_size > 0 else 0
                ),
            },
            "checksums": {
                "jsonl_sha256": jsonl_checksum,
                "compressed_sha256": compressed_checksum,
            },
            "primary_keys": {
                "count": len(primary_keys),
                "sample": primary_keys[:10] if len(primary_keys) > 10 else primary_keys,
            },
        }

        if timestamp_range:
            metadata["timestamp_range"] = {
                "min": (timestamp_range["min"].isoformat() if timestamp_range.get("min") else None),
                "max": (timestamp_range["max"].isoformat() if timestamp_range.get("max") else None),
            }

        # Include schema information (only in first batch or if schema changed)
        if table_schema:
            metadata["table_schema"] = table_schema

        return metadata

    def metadata_to_json(self, metadata: dict[str, Any]) -> str:
        """Convert metadata dictionary to JSON string.

        Args:
            metadata: Metadata dictionary

        Returns:
            JSON string
        """
        return json.dumps(metadata, indent=2, default=str)

    def metadata_from_json(self, json_str: str) -> dict[str, Any]:
        """Parse metadata from JSON string.

        Args:
            json_str: JSON string

        Returns:
            Metadata dictionary

        Raises:
            ValueError: If JSON is invalid
        """
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid metadata JSON: {e}") from e
