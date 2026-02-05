"""Batch processing logic for selecting and processing records."""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import asyncpg
import structlog

from archiver.config import DatabaseConfig, TableConfig
from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError
from utils import safe_identifier
from utils.logging import get_logger


class BatchProcessor:
    """Handles batch selection and processing of records."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        db_config: DatabaseConfig,
        table_config: TableConfig,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize batch processor.

        Args:
            db_manager: Database manager instance
            db_config: Database configuration
            table_config: Table configuration
            logger: Optional logger instance
        """
        self.db_manager = db_manager
        self.db_config = db_config
        self.table_config = table_config
        self.logger = logger or get_logger("batch_processor")

    async def _is_timestamp_column_timezone_aware(self) -> bool:
        """Check if the timestamp column is timezone-aware (TIMESTAMPTZ).

        Returns:
            True if column is TIMESTAMPTZ, False if TIMESTAMP
        """
        schema = self.table_config.schema_name
        table = self.table_config.name
        timestamp_col = self.table_config.timestamp_column

        query = """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = $2
              AND column_name = $3
        """

        try:
            result = await self.db_manager.fetchval(query, schema, table, timestamp_col)
            # TIMESTAMPTZ is timezone-aware, TIMESTAMP is timezone-naive
            return result == "timestamp with time zone"
        except Exception as e:
            # Default to timezone-naive if we can't determine (safer)
            self.logger.warning(
                "Could not determine timestamp column type, assuming timezone-naive",
                schema=schema,
                table=table,
                column=timestamp_col,
                error=str(e),
            )
            return False

    def calculate_cutoff_date(self, safety_buffer_days: int = 1) -> datetime:
        """Calculate cutoff date for archival (current_date - retention_days - buffer).

        Args:
            safety_buffer_days: Additional safety buffer in days (default: 1)

        Returns:
            Cutoff datetime in UTC
        """
        # Use database server time, not client time
        # For now, we'll use Python's UTC time and verify with database
        retention_days = self.table_config.retention_days or 90
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days + safety_buffer_days)
        return cutoff

    async def calculate_cutoff_date_for_query(self, safety_buffer_days: int = 1) -> datetime:
        """Calculate cutoff date adjusted for the timestamp column's timezone awareness.

        This method checks if the timestamp column is timezone-aware (TIMESTAMPTZ)
        or timezone-naive (TIMESTAMP) and returns an appropriately formatted datetime.

        Args:
            safety_buffer_days: Additional safety buffer in days (default: 1)

        Returns:
            Cutoff datetime (timezone-aware if column is TIMESTAMPTZ, naive if TIMESTAMP)
        """
        cutoff = self.calculate_cutoff_date(safety_buffer_days)

        # Check if column is timezone-aware
        is_tz_aware = await self._is_timestamp_column_timezone_aware()

        if is_tz_aware:
            # Column is TIMESTAMPTZ, keep timezone-aware
            return cutoff
        else:
            # Column is TIMESTAMP (naive), convert to naive datetime
            # Remove timezone info but keep the UTC time value
            return cutoff.replace(tzinfo=None)

    async def count_eligible_records(self) -> int:
        """Count records eligible for archival.

        Returns:
            Number of eligible records
        """
        cutoff = await self.calculate_cutoff_date_for_query()
        schema = safe_identifier(self.table_config.schema_name)
        table = safe_identifier(self.table_config.name)
        timestamp_col = safe_identifier(self.table_config.timestamp_column)

        query = f"""
            SELECT COUNT(*)
            FROM {schema}.{table}
            WHERE {timestamp_col} < $1
        """

        try:
            count = await self.db_manager.fetchval(query, cutoff)
            return count or 0
        except Exception as e:
            raise DatabaseError(
                f"Failed to count eligible records: {e}",
                context={
                    "database": self.db_config.name,
                    "table": table,
                    "schema": schema,
                },
            ) from e

    async def select_batch(
        self,
        batch_size: int,
        last_timestamp: Optional[datetime] = None,
        last_primary_key: Optional[Any] = None,
    ) -> list[asyncpg.Record]:
        """Select a batch of records using cursor-based pagination.

        Args:
            batch_size: Number of records to select
            last_timestamp: Last processed timestamp (for cursor)
            last_primary_key: Last processed primary key (for cursor)

        Returns:
            List of records

        Raises:
            DatabaseError: If selection fails
        """
        schema = safe_identifier(self.table_config.schema_name)
        table = safe_identifier(self.table_config.name)
        timestamp_col = safe_identifier(self.table_config.timestamp_column)
        primary_key = safe_identifier(self.table_config.primary_key)
        cutoff = await self.calculate_cutoff_date_for_query()

        # Build query with cursor-based pagination
        if last_timestamp and last_primary_key is not None:
            # Continue from last position
            # Allow records with timestamp > last_timestamp (new records inserted after watermark)
            # as long as they're still before the cutoff
            query = f"""
                SELECT *
                FROM {schema}.{table}
                WHERE {timestamp_col} < $1
                  AND (
                    {timestamp_col} > $2
                    OR ({timestamp_col} = $2 AND {primary_key} > $3)
                  )
                ORDER BY {timestamp_col}, {primary_key}
                LIMIT $4
                FOR UPDATE SKIP LOCKED
            """
            params = (cutoff, last_timestamp, last_primary_key, batch_size)
        else:
            # First batch
            query = f"""
                SELECT *
                FROM {schema}.{table}
                WHERE {timestamp_col} < $1
                ORDER BY {timestamp_col}, {primary_key}
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            """
            params = (cutoff, batch_size)

        try:
            records = await self.db_manager.fetch(query, *params)
            self.logger.debug(
                "Batch selected",
                database=self.db_config.name,
                table=table,
                count=len(records),
                batch_size=batch_size,
            )
            return records

        except Exception as e:
            raise DatabaseError(
                f"Failed to select batch: {e}",
                context={
                    "database": self.db_config.name,
                    "table": table,
                    "schema": schema,
                    "batch_size": batch_size,
                },
            ) from e

    def records_to_dicts(self, records: list[asyncpg.Record]) -> list[dict[str, Any]]:
        """Convert asyncpg records to dictionaries.

        Args:
            records: List of asyncpg records

        Returns:
            List of dictionaries
        """
        return [dict(record) for record in records]

    def extract_primary_keys(self, records: list[dict[str, Any]]) -> list[Any]:
        """Extract primary keys from records.

        Args:
            records: List of record dictionaries

        Returns:
            List of primary key values
        """
        pk_col = self.table_config.primary_key
        return [record[pk_col] for record in records]

    def get_last_cursor(
        self, records: list[dict[str, Any]]
    ) -> tuple[Optional[datetime], Optional[Any]]:
        """Get cursor position from last record in batch.

        Args:
            records: List of record dictionaries

        Returns:
            Tuple of (last_timestamp, last_primary_key) or (None, None) if empty
        """
        if not records:
            return None, None

        last_record = records[-1]
        timestamp_col = self.table_config.timestamp_column
        primary_key = self.table_config.primary_key

        last_timestamp = last_record.get(timestamp_col)
        last_pk = last_record.get(primary_key)

        # Ensure timestamp is datetime
        if isinstance(last_timestamp, str):
            # Try to parse if it's a string
            try:
                last_timestamp = datetime.fromisoformat(last_timestamp.replace("Z", "+00:00"))
            except Exception:
                self.logger.warning("Could not parse timestamp", timestamp=last_timestamp)

        return last_timestamp, last_pk

    def get_timestamp_range(self, records: list[dict[str, Any]]) -> dict[str, Optional[datetime]]:
        """Get min and max timestamp from records.

        Args:
            records: List of record dictionaries

        Returns:
            Dictionary with 'min' and 'max' timestamp values, or None if empty
        """
        if not records:
            return {"min": None, "max": None}

        timestamp_col = self.table_config.timestamp_column
        timestamps = []

        for record in records:
            ts = record.get(timestamp_col)
            if ts is not None:
                # Ensure timestamp is datetime
                if isinstance(ts, str):
                    try:
                        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except Exception:
                        self.logger.warning("Could not parse timestamp", timestamp=ts)
                        continue
                if isinstance(ts, datetime):
                    timestamps.append(ts)

        if not timestamps:
            return {"min": None, "max": None}

        return {"min": min(timestamps), "max": max(timestamps)}
