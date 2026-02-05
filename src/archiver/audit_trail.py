"""Immutable audit trail for compliance and governance."""

import json
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import structlog

from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError
from archiver.s3_client import S3Client
from utils.logging import get_logger


class AuditEventType(Enum):
    """Types of audit events."""

    ARCHIVE_START = "archive_start"
    ARCHIVE_SUCCESS = "archive_success"
    ARCHIVE_FAILURE = "archive_failure"
    RESTORE_START = "restore_start"
    RESTORE_SUCCESS = "restore_success"
    RESTORE_FAILURE = "restore_failure"
    CONFIGURATION_CHANGE = "configuration_change"
    ERROR = "error"


class AuditTrail:
    """Manages immutable audit trail for compliance."""

    def __init__(
        self,
        storage_type: str = "s3",  # "s3" or "database"
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize audit trail.

        Args:
            storage_type: Storage type for audit log ("s3" or "database")
            logger: Optional logger instance
        """
        if storage_type not in ("s3", "database"):
            raise ValueError(f"Invalid storage_type: {storage_type}. Must be 's3' or 'database'")

        self.storage_type = storage_type
        self.logger = logger or get_logger("audit_trail")

    async def log_event(
        self,
        event_type: AuditEventType,
        database_name: str,
        table_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        record_count: Optional[int] = None,
        s3_path: Optional[str] = None,
        status: str = "success",
        duration_seconds: Optional[float] = None,
        operator: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        s3_client: Optional[S3Client] = None,
        db_manager: Optional[DatabaseManager] = None,
    ) -> None:
        """Log an audit event.

        Args:
            event_type: Type of audit event
            database_name: Database name
            table_name: Table name (optional)
            schema_name: Schema name (optional)
            record_count: Number of records affected (optional)
            s3_path: S3 path to archived data (optional)
            status: Event status (success, failure, etc.)
            duration_seconds: Duration of operation in seconds (optional)
            operator: Operator/user who triggered the event (optional)
            error_message: Error message if event failed (optional)
            metadata: Additional metadata (optional)
            s3_client: S3 client (required if storage_type is "s3")
            db_manager: Database manager (required if storage_type is "database")
        """
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type.value,
            "database": database_name,
            "table": table_name,
            "schema": schema_name,
            "record_count": record_count,
            "s3_path": s3_path,
            "status": status,
            "duration_seconds": duration_seconds,
            "operator": operator or "system",
            "error_message": error_message,
            "metadata": metadata or {},
        }

        if self.storage_type == "s3":
            if s3_client is None:
                raise ValueError("s3_client is required for S3 storage type")
            await self._log_to_s3(event, s3_client, database_name, table_name)
        else:  # database
            if db_manager is None:
                raise ValueError("db_manager is required for database storage type")
            await self._log_to_database(event, db_manager)

        self.logger.debug(
            "Audit event logged",
            event_type=event_type.value,
            database=database_name,
            table=table_name,
            status=status,
        )

    async def _log_to_s3(
        self,
        event: dict[str, Any],
        s3_client: S3Client,
        database_name: str,
        table_name: Optional[str],
    ) -> None:
        """Log audit event to S3.

        Args:
            event: Audit event dictionary
            s3_client: S3 client
            database_name: Database name
            table_name: Table name (optional)
        """
        import tempfile

        # Generate S3 key: {prefix}/audit/{year}/{month}/{day}/{timestamp}_{event_type}.json
        timestamp = datetime.now(timezone.utc)
        date_partition = timestamp.strftime("year=%Y/month=%m/day=%d")
        event_id = timestamp.strftime("%Y%m%dT%H%M%S.%fZ")
        filename = f"{event_id}_{event['event_type']}.json"

        # Build S3 key
        prefix = s3_client.config.prefix.rstrip("/") if s3_client.config.prefix else ""
        if prefix:
            s3_key = f"{prefix}/audit/{date_partition}/{filename}"
        else:
            s3_key = f"audit/{date_partition}/{filename}"

        # Write event to temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json", encoding="utf-8"
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
            json.dump(event, tmp_file, indent=2, default=str)

        try:
            # Upload to S3
            s3_client.upload_file(tmp_path, s3_key)
            self.logger.debug(
                "Audit event uploaded to S3",
                s3_key=s3_key,
                event_type=event["event_type"],
            )
        finally:
            try:
                tmp_path.unlink()
            except Exception as e:
                self.logger.warning(
                    "Failed to delete temporary audit file",
                    path=str(tmp_path),
                    error=str(e),
                )

    async def _log_to_database(self, event: dict[str, Any], db_manager: DatabaseManager) -> None:
        """Log audit event to database.

        Args:
            event: Audit event dictionary
            db_manager: Database manager
        """
        # Create audit log table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS archiver_audit_log (
                id BIGSERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                event_type TEXT NOT NULL,
                database_name TEXT NOT NULL,
                table_name TEXT,
                schema_name TEXT,
                record_count INTEGER,
                s3_path TEXT,
                status TEXT NOT NULL,
                duration_seconds FLOAT,
                operator TEXT,
                error_message TEXT,
                metadata JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

        # Create index on timestamp for efficient queries
        create_index_query = """
            CREATE INDEX IF NOT EXISTS idx_archiver_audit_log_timestamp
            ON archiver_audit_log(timestamp DESC)
        """

        # Insert audit event
        insert_query = """
            INSERT INTO archiver_audit_log (
                timestamp, event_type, database_name, table_name, schema_name,
                record_count, s3_path, status, duration_seconds, operator,
                error_message, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """

        try:
            # Create table if needed
            await db_manager.execute(create_table_query)
            await db_manager.execute(create_index_query)

            # Insert event
            await db_manager.execute(
                insert_query,
                event["timestamp"],
                event["event_type"],
                event["database"],
                event.get("table"),
                event.get("schema"),
                event.get("record_count"),
                event.get("s3_path"),
                event["status"],
                event.get("duration_seconds"),
                event.get("operator"),
                event.get("error_message"),
                json.dumps(event.get("metadata", {})),
            )

            self.logger.debug(
                "Audit event logged to database",
                event_type=event["event_type"],
                database=event["database"],
            )

        except Exception as e:
            raise DatabaseError(
                f"Failed to log audit event to database: {e}",
                context={
                    "event_type": event["event_type"],
                    "database": event["database"],
                },
            ) from e
