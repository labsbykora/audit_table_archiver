"""Serialization of PostgreSQL rows to JSONL format."""

import base64
import json
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

import structlog

from archiver.exceptions import ArchiverError
from utils.logging import get_logger


class PostgreSQLSerializer:
    """Serializes PostgreSQL rows to JSONL format."""

    def __init__(self, logger: Optional[structlog.BoundLogger] = None) -> None:
        """Initialize serializer.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("serializer")

    def serialize_row(
        self,
        row: dict[str, Any],
        batch_id: str,
        database_name: str,
        table_name: str,
        archived_at: datetime,
    ) -> dict[str, Any]:
        """Serialize a single database row to JSON-serializable dict.

        Args:
            row: Database row as dictionary
            batch_id: Unique batch identifier
            database_name: Source database name
            table_name: Source table name
            archived_at: Timestamp of archival

        Returns:
            Serialized row as dictionary
        """
        serialized: dict[str, Any] = {}

        for key, value in row.items():
            serialized[key] = self._serialize_value(value)

        # Add metadata
        serialized["_archived_at"] = archived_at.isoformat() + "Z"
        serialized["_batch_id"] = batch_id
        serialized["_source_database"] = database_name
        serialized["_source_table"] = table_name

        return serialized

    def _serialize_value(self, value: Any) -> Any:
        """Serialize a single value based on its type.

        Args:
            value: Value to serialize

        Returns:
            Serialized value
        """
        if value is None:
            return None

        # Handle Python types that need special conversion
        if isinstance(value, (datetime, date, time)):
            return self._serialize_datetime(value)
        elif isinstance(value, Decimal):
            # Preserve precision as string
            return str(value)
        elif isinstance(value, UUID):
            return str(value)
        elif isinstance(value, bytes):
            # BYTEA: encode as base64
            return base64.b64encode(value).decode("utf-8")
        elif isinstance(value, (list, tuple)):
            # Arrays: convert to JSON array
            return [self._serialize_value(item) for item in value]
        elif isinstance(value, dict):
            # JSON/JSONB: preserve as nested JSON
            return {k: self._serialize_value(v) for k, v in value.items()}
        elif isinstance(value, (int, float, str, bool)):
            # Primitives: pass through
            return value
        else:
            # Fallback: convert to string
            self.logger.warning(
                "Unknown type, converting to string",
                type=type(value).__name__,
                value=str(value)[:100],
            )
            return str(value)

    def _serialize_datetime(self, value: datetime | date | time) -> str:
        """Serialize datetime/date/time to ISO 8601 format.

        Args:
            value: Datetime, date, or time value

        Returns:
            ISO 8601 formatted string
        """
        if isinstance(value, datetime):
            # Ensure timezone-aware (assume UTC if naive)
            if value.tzinfo is None:
                value = value.replace(tzinfo=None)  # Keep naive, add Z suffix
                return value.isoformat() + "Z"
            else:
                return value.isoformat()
        elif isinstance(value, date):
            return value.isoformat()
        elif isinstance(value, time):
            return value.isoformat()
        else:
            return str(value)

    def to_jsonl(self, rows: list[dict[str, Any]]) -> bytes:
        """Convert list of serialized rows to JSONL format.

        Args:
            rows: List of serialized row dictionaries

        Returns:
            JSONL formatted bytes (one JSON object per line)
        """
        lines: list[str] = []
        for row in rows:
            json_str = json.dumps(row, ensure_ascii=False, default=str)
            lines.append(json_str)

        return "\n".join(lines).encode("utf-8")

    def count_jsonl_lines(self, jsonl_data: bytes) -> int:
        """Count lines in JSONL data.

        Args:
            jsonl_data: JSONL formatted bytes

        Returns:
            Number of lines (records)
        """
        return jsonl_data.count(b"\n") + (1 if jsonl_data else 0)


class SerializationError(ArchiverError):
    """Serialization-related errors."""

    pass
