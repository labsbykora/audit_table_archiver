"""Restore watermark management for tracking which archives have been restored."""

import json
import re
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError, S3Error
from archiver.s3_client import S3Client
from utils.logging import get_logger


class RestoreWatermark:
    """Represents a restore watermark."""

    def __init__(
        self,
        database_name: str,
        table_name: str,
        last_restored_date: datetime,
        last_restored_s3_key: str,
        total_archives_restored: int = 0,
        updated_at: Optional[datetime] = None,
    ) -> None:
        """Initialize restore watermark.

        Args:
            database_name: Database name
            table_name: Table name
            last_restored_date: Date of the last restored archive
            last_restored_s3_key: S3 key of the last restored archive
            total_archives_restored: Total number of archives restored
            updated_at: When watermark was last updated
        """
        self.database_name = database_name
        self.table_name = table_name
        self.last_restored_date = last_restored_date
        self.last_restored_s3_key = last_restored_s3_key
        self.total_archives_restored = total_archives_restored
        self.updated_at = updated_at or datetime.now(timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        """Convert watermark to dictionary."""
        return {
            "version": "1.0",
            "database": self.database_name,
            "table": self.table_name,
            "last_restored_date": self.last_restored_date.isoformat(),
            "last_restored_s3_key": self.last_restored_s3_key,
            "total_archives_restored": self.total_archives_restored,
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RestoreWatermark":
        """Create watermark from dictionary.

        Args:
            data: Dictionary containing watermark data

        Returns:
            RestoreWatermark object
        """
        last_restored_date = datetime.fromisoformat(
            data["last_restored_date"].replace("Z", "+00:00")
        )
        updated_at = datetime.fromisoformat(
            data.get("updated_at", datetime.now(timezone.utc).isoformat()).replace("Z", "+00:00")
        )

        return cls(
            database_name=data["database"],
            table_name=data["table"],
            last_restored_date=last_restored_date,
            last_restored_s3_key=data["last_restored_s3_key"],
            total_archives_restored=data.get("total_archives_restored", 0),
            updated_at=updated_at,
        )


class RestoreWatermarkManager:
    """Manages restore watermarks for tracking which archives have been restored."""

    def __init__(
        self,
        storage_type: str = "s3",  # "s3" or "database" or "both"
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize restore watermark manager.

        Args:
            storage_type: Storage type for watermarks ("s3", "database", or "both")
            logger: Optional logger instance
        """
        if storage_type not in ("s3", "database", "both"):
            raise ValueError(f"Invalid storage_type: {storage_type}. Must be 's3', 'database', or 'both'")

        self.storage_type = storage_type
        self.logger = logger or get_logger("restore_watermark_manager")

    async def load_watermark(
        self,
        database_name: str,
        table_name: str,
        s3_client: Optional[S3Client] = None,
        db_manager: Optional[DatabaseManager] = None,
    ) -> Optional[RestoreWatermark]:
        """Load restore watermark for a table.

        Args:
            database_name: Database name
            table_name: Table name
            s3_client: S3 client (required if storage_type is "s3" or "both")
            db_manager: Database manager (required if storage_type is "database" or "both")

        Returns:
            RestoreWatermark object or None if not found
        """
        watermark = None

        # Try S3 first if enabled
        if self.storage_type in ("s3", "both"):
            if s3_client is None:
                raise ValueError("s3_client is required for S3 storage type")
            watermark = await self._load_watermark_from_s3(s3_client, database_name, table_name)

        # Try database if S3 didn't return a watermark and database is enabled
        if watermark is None and self.storage_type in ("database", "both"):
            if db_manager is None:
                if self.storage_type == "database":
                    raise ValueError("db_manager is required for database storage type")
                # If "both", database is optional - just log warning
                self.logger.debug("Database manager not provided, skipping database watermark load")
            else:
                db_watermark = await self._load_watermark_from_database(
                    db_manager, database_name, table_name
                )
                if db_watermark:
                    watermark = db_watermark

        return watermark

    async def save_watermark(
        self,
        database_name: str,
        table_name: str,
        last_restored_date: datetime,
        last_restored_s3_key: str,
        total_archives_restored: int,
        s3_client: Optional[S3Client] = None,
        db_manager: Optional[DatabaseManager] = None,
    ) -> None:
        """Save restore watermark for a table.

        Args:
            database_name: Database name
            table_name: Table name
            last_restored_date: Date of the last restored archive
            last_restored_s3_key: S3 key of the last restored archive
            total_archives_restored: Total number of archives restored
            s3_client: S3 client (required if storage_type is "s3" or "both")
            db_manager: Database manager (required if storage_type is "database" or "both")
        """
        watermark = RestoreWatermark(
            database_name=database_name,
            table_name=table_name,
            last_restored_date=last_restored_date,
            last_restored_s3_key=last_restored_s3_key,
            total_archives_restored=total_archives_restored,
        )

        # Save to S3 if enabled
        if self.storage_type in ("s3", "both"):
            if s3_client is None:
                raise ValueError("s3_client is required for S3 storage type")
            await self._save_watermark_to_s3(s3_client, watermark)

        # Save to database if enabled
        if self.storage_type in ("database", "both"):
            if db_manager is None:
                if self.storage_type == "database":
                    raise ValueError("db_manager is required for database storage type")
                # If "both", database is optional - just log warning
                self.logger.debug("Database manager not provided, skipping database watermark save")
            else:
                await self._save_watermark_to_database(db_manager, watermark)

    def extract_date_from_s3_key(self, s3_key: str) -> Optional[datetime]:
        """Extract date from S3 key path.

        Supports both formats:
        - Hive-style: year=2026/month=01/day=06/...
        - Simple: 2026/01/06/...

        Args:
            s3_key: S3 key path

        Returns:
            Datetime object or None if date cannot be extracted
        """
        # Try Hive-style partitioning first: year=2026/month=01/day=06
        hive_pattern = r"year=(\d{4})/month=(\d{2})/day=(\d{2})"
        match = re.search(hive_pattern, s3_key)
        if match:
            year, month, day = match.groups()
            try:
                return datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
            except ValueError:
                pass

        # Try simple format: 2026/01/06
        simple_pattern = r"(\d{4})/(\d{2})/(\d{2})"
        match = re.search(simple_pattern, s3_key)
        if match:
            year, month, day = match.groups()
            try:
                return datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
            except ValueError:
                pass

        # Try ISO timestamp in filename: 20260106T114123Z
        iso_pattern = r"(\d{4})(\d{2})(\d{2})T"
        match = re.search(iso_pattern, s3_key)
        if match:
            year, month, day = match.groups()
            try:
                return datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
            except ValueError:
                pass

        self.logger.warning(
            "Could not extract date from S3 key",
            s3_key=s3_key,
        )
        return None

    def should_restore_archive(self, s3_key: str, watermark: Optional[RestoreWatermark]) -> bool:
        """Determine if an archive should be restored based on watermark.

        Args:
            s3_key: S3 key of the archive
            watermark: Restore watermark (None if no watermark exists)

        Returns:
            True if archive should be restored, False if already restored
        """
        if watermark is None:
            # No watermark = first restore, process all
            return True

        # Extract date from S3 key
        archive_date = self.extract_date_from_s3_key(s3_key)
        if archive_date is None:
            # Can't extract date - restore to be safe
            self.logger.warning(
                "Could not extract date from archive, will restore",
                s3_key=s3_key,
            )
            return True

        # Compare dates (restore if archive date is newer than last restored date)
        if archive_date > watermark.last_restored_date:
            return True

        # If dates are equal, check if this specific key was already restored
        if archive_date == watermark.last_restored_date:
            # If this is the same key, skip it
            if s3_key == watermark.last_restored_s3_key:
                return False
            # If different key but same date, restore (might be multiple archives per day)
            # We'll rely on conflict detection to handle duplicates
            return True

        # Archive date is older than last restored date - skip
        return False

    async def _load_watermark_from_s3(
        self,
        s3_client: S3Client,
        database_name: str,
        table_name: str,
    ) -> Optional[RestoreWatermark]:
        """Load restore watermark from S3.

        Args:
            s3_client: S3 client
            database_name: Database name
            table_name: Table name

        Returns:
            RestoreWatermark object or None if not found
        """
        # Watermark key: {prefix}/{database}/{table}/.restore_watermark.json
        watermark_key = f"{database_name}/{table_name}/.restore_watermark.json"

        # Add prefix if configured
        if s3_client.config.prefix:
            prefix = s3_client.config.prefix.rstrip("/")
            watermark_key = f"{prefix}/{watermark_key}"

        try:
            watermark_data = s3_client.get_object_bytes(watermark_key)
            watermark_json = watermark_data.decode("utf-8")
            watermark_dict = json.loads(watermark_json)

            watermark = RestoreWatermark.from_dict(watermark_dict)

            self.logger.debug(
                "Restore watermark loaded from S3",
                database=database_name,
                table=table_name,
                key=watermark_key,
                last_restored_date=watermark.last_restored_date.isoformat(),
            )

            return watermark

        except S3Error as e:
            # Watermark not found - this is OK for first restore
            if "NoSuchKey" in str(e) or "404" in str(e):
                self.logger.debug(
                    "Restore watermark not found in S3 (first restore or not yet created)",
                    database=database_name,
                    table=table_name,
                    key=watermark_key,
                )
                return None
            # Other S3 errors should be logged but not fail
            self.logger.warning(
                "Failed to load restore watermark from S3",
                database=database_name,
                table=table_name,
                key=watermark_key,
                error=str(e),
            )
            return None
        except Exception as e:
            self.logger.warning(
                "Failed to parse restore watermark from S3",
                database=database_name,
                table=table_name,
                key=watermark_key,
                error=str(e),
            )
            return None

    async def _save_watermark_to_s3(
        self,
        s3_client: S3Client,
        watermark: RestoreWatermark,
    ) -> None:
        """Save restore watermark to S3.

        Args:
            s3_client: S3 client
            watermark: RestoreWatermark object
        """
        import tempfile
        from pathlib import Path

        watermark_json = json.dumps(watermark.to_dict(), indent=2, default=str)
        watermark_key = f"{watermark.database_name}/{watermark.table_name}/.restore_watermark.json"

        # Add prefix if configured
        if s3_client.config.prefix:
            prefix = s3_client.config.prefix.rstrip("/")
            watermark_key = f"{prefix}/{watermark_key}"

        # Write to temporary file and upload
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json", encoding="utf-8") as tmp_file:
            tmp_path = Path(tmp_file.name)
            tmp_file.write(watermark_json)

        try:
            s3_client.upload_file(tmp_path, watermark_key)
            self.logger.debug(
                "Restore watermark saved to S3",
                database=watermark.database_name,
                table=watermark.table_name,
                key=watermark_key,
                last_restored_date=watermark.last_restored_date.isoformat(),
            )
        finally:
            try:
                tmp_path.unlink()
            except Exception as e:
                self.logger.warning(
                    "Failed to delete temporary restore watermark file",
                    path=str(tmp_path),
                    error=str(e),
                )

    async def _load_watermark_from_database(
        self,
        db_manager: DatabaseManager,
        database_name: str,
        table_name: str,
    ) -> Optional[RestoreWatermark]:
        """Load restore watermark from database.

        Args:
            db_manager: Database manager
            database_name: Database name
            table_name: Table name

        Returns:
            RestoreWatermark object or None if not found
        """
        # Use a system table to store restore watermarks
        # Table: restore_watermarks (created on first use)
        query = """
            SELECT last_restored_date, last_restored_s3_key, total_archives_restored, updated_at
            FROM restore_watermarks
            WHERE database_name = $1 AND table_name = $2
        """

        try:
            row = await db_manager.fetchone(query, database_name, table_name)
            if not row:
                return None

            watermark = RestoreWatermark(
                database_name=database_name,
                table_name=table_name,
                last_restored_date=row["last_restored_date"],
                last_restored_s3_key=row["last_restored_s3_key"],
                total_archives_restored=row.get("total_archives_restored", 0),
                updated_at=row.get("updated_at"),
            )

            self.logger.debug(
                "Restore watermark loaded from database",
                database=database_name,
                table=table_name,
                last_restored_date=watermark.last_restored_date.isoformat(),
            )

            return watermark

        except Exception as e:
            # Table might not exist yet - this is OK for first restore
            self.logger.debug(
                "Restore watermark not found in database (first restore or table not created)",
                database=database_name,
                table=table_name,
                error=str(e),
            )
            return None

    async def _save_watermark_to_database(
        self,
        db_manager: DatabaseManager,
        watermark: RestoreWatermark,
    ) -> None:
        """Save restore watermark to database.

        Args:
            db_manager: Database manager
            watermark: RestoreWatermark object
        """
        # Create watermark table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS restore_watermarks (
                database_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                last_restored_date TIMESTAMPTZ NOT NULL,
                last_restored_s3_key TEXT NOT NULL,
                total_archives_restored INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (database_name, table_name)
            )
        """

        # Upsert watermark
        upsert_query = """
            INSERT INTO restore_watermarks (
                database_name, table_name, last_restored_date,
                last_restored_s3_key, total_archives_restored, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (database_name, table_name)
            DO UPDATE SET
                last_restored_date = EXCLUDED.last_restored_date,
                last_restored_s3_key = EXCLUDED.last_restored_s3_key,
                total_archives_restored = EXCLUDED.total_archives_restored,
                updated_at = NOW()
        """

        try:
            # Create table if needed
            await db_manager.execute(create_table_query)

            # Upsert watermark
            await db_manager.execute(
                upsert_query,
                watermark.database_name,
                watermark.table_name,
                watermark.last_restored_date,
                watermark.last_restored_s3_key,
                watermark.total_archives_restored,
            )

            self.logger.debug(
                "Restore watermark saved to database",
                database=watermark.database_name,
                table=watermark.table_name,
                last_restored_date=watermark.last_restored_date.isoformat(),
            )

        except Exception as e:
            raise DatabaseError(
                f"Failed to save restore watermark to database: {e}",
                context={
                    "database": watermark.database_name,
                    "table": watermark.table_name,
                },
            ) from e

