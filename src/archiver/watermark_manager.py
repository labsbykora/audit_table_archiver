"""Watermark management for incremental archival."""

import json
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError
from archiver.s3_client import S3Client
from utils.logging import get_logger


class WatermarkManager:
    """Manages watermarks for tracking archival progress."""

    def __init__(
        self,
        storage_type: str = "s3",  # "s3" or "database"
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize watermark manager.

        Args:
            storage_type: Storage type for watermarks ("s3" or "database")
            logger: Optional logger instance
        """
        if storage_type not in ("s3", "database"):
            raise ValueError(f"Invalid storage_type: {storage_type}. Must be 's3' or 'database'")

        self.storage_type = storage_type
        self.logger = logger or get_logger("watermark_manager")

    async def load_watermark(
        self,
        database_name: str,
        table_name: str,
        s3_client: Optional[S3Client] = None,
        db_manager: Optional[DatabaseManager] = None,
    ) -> Optional[dict[str, Any]]:
        """Load watermark for a table.

        Args:
            database_name: Database name
            table_name: Table name
            s3_client: S3 client (required if storage_type is "s3")
            db_manager: Database manager (required if storage_type is "database")

        Returns:
            Watermark dictionary with 'last_timestamp' and 'last_primary_key', or None if not found
        """
        if self.storage_type == "s3":
            if s3_client is None:
                raise ValueError("s3_client is required for S3 storage type")
            return await self._load_watermark_from_s3(s3_client, database_name, table_name)
        else:  # database
            if db_manager is None:
                raise ValueError("db_manager is required for database storage type")
            return await self._load_watermark_from_database(db_manager, database_name, table_name)

    async def save_watermark(
        self,
        database_name: str,
        table_name: str,
        last_timestamp: datetime,
        last_primary_key: Any,
        s3_client: Optional[S3Client] = None,
        db_manager: Optional[DatabaseManager] = None,
    ) -> None:
        """Save watermark for a table.

        Args:
            database_name: Database name
            table_name: Table name
            last_timestamp: Last archived timestamp
            last_primary_key: Last archived primary key
            s3_client: S3 client (required if storage_type is "s3")
            db_manager: Database manager (required if storage_type is "database")
        """
        if self.storage_type == "s3":
            if s3_client is None:
                raise ValueError("s3_client is required for S3 storage type")
            await self._save_watermark_to_s3(
                s3_client, database_name, table_name, last_timestamp, last_primary_key
            )
        else:  # database
            if db_manager is None:
                raise ValueError("db_manager is required for database storage type")
            await self._save_watermark_to_database(
                db_manager, database_name, table_name, last_timestamp, last_primary_key
            )

    async def _load_watermark_from_s3(
        self,
        s3_client: S3Client,
        database_name: str,
        table_name: str,
    ) -> Optional[dict[str, Any]]:
        """Load watermark from S3.

        Args:
            s3_client: S3 client
            database_name: Database name
            table_name: Table name

        Returns:
            Watermark dictionary or None if not found
        """
        # Watermark key: {prefix}/{database}/{table}/.watermark.json
        watermark_key = f"{database_name}/{table_name}/.watermark.json"

        try:
            watermark_data = s3_client.get_object_bytes(watermark_key)
            watermark_json = watermark_data.decode("utf-8")
            watermark = json.loads(watermark_json)

            # Parse timestamp
            if watermark.get("last_timestamp"):
                watermark["last_timestamp"] = datetime.fromisoformat(
                    watermark["last_timestamp"].replace("Z", "+00:00")
                )

            self.logger.debug(
                "Watermark loaded from S3",
                database=database_name,
                table=table_name,
                key=watermark_key,
            )

            return watermark

        except Exception as e:
            # Watermark not found or invalid - this is OK for first run
            self.logger.debug(
                "Watermark not found in S3 (first run or not yet created)",
                database=database_name,
                table=table_name,
                error=str(e),
            )
            return None

    async def _save_watermark_to_s3(
        self,
        s3_client: S3Client,
        database_name: str,
        table_name: str,
        last_timestamp: datetime,
        last_primary_key: Any,
    ) -> None:
        """Save watermark to S3.

        Args:
            s3_client: S3 client
            database_name: Database name
            table_name: Table name
            last_timestamp: Last archived timestamp
            last_primary_key: Last archived primary key
        """
        import tempfile
        from pathlib import Path

        watermark = {
            "database": database_name,
            "table": table_name,
            "last_timestamp": last_timestamp.isoformat(),
            "last_primary_key": last_primary_key,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        watermark_json = json.dumps(watermark, indent=2, default=str)
        watermark_key = f"{database_name}/{table_name}/.watermark.json"

        # Write to temporary file and upload
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json", encoding="utf-8"
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
            tmp_file.write(watermark_json)

        try:
            s3_client.upload_file(tmp_path, watermark_key)
            self.logger.debug(
                "Watermark saved to S3",
                database=database_name,
                table=table_name,
                key=watermark_key,
            )
        finally:
            try:
                tmp_path.unlink()
            except Exception as e:
                self.logger.warning(
                    "Failed to delete temporary watermark file",
                    path=str(tmp_path),
                    error=str(e),
                )

    async def _load_watermark_from_database(
        self,
        db_manager: DatabaseManager,
        database_name: str,
        table_name: str,
    ) -> Optional[dict[str, Any]]:
        """Load watermark from database.

        Args:
            db_manager: Database manager
            database_name: Database name
            table_name: Table name

        Returns:
            Watermark dictionary or None if not found
        """
        # Use a system table to store watermarks
        # Table: archiver_watermarks (created on first use)
        query = """
            SELECT last_timestamp, last_primary_key, updated_at
            FROM archiver_watermarks
            WHERE database_name = $1 AND table_name = $2
        """

        try:
            row = await db_manager.fetchone(query, database_name, table_name)
            if not row:
                return None

            watermark = {
                "last_timestamp": row["last_timestamp"],
                "last_primary_key": row["last_primary_key"],
                "updated_at": row["updated_at"],
            }

            self.logger.debug(
                "Watermark loaded from database",
                database=database_name,
                table=table_name,
            )

            return watermark

        except Exception as e:
            # Table might not exist yet - this is OK for first run
            self.logger.debug(
                "Watermark not found in database (first run or table not created)",
                database=database_name,
                table=table_name,
                error=str(e),
            )
            return None

    async def _save_watermark_to_database(
        self,
        db_manager: DatabaseManager,
        database_name: str,
        table_name: str,
        last_timestamp: datetime,
        last_primary_key: Any,
    ) -> None:
        """Save watermark to database.

        Args:
            db_manager: Database manager
            database_name: Database name
            table_name: Table name
            last_timestamp: Last archived timestamp
            last_primary_key: Last archived primary key
        """
        # Create watermark table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS archiver_watermarks (
                database_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                last_timestamp TIMESTAMPTZ NOT NULL,
                last_primary_key TEXT,  -- Store as text for flexibility
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (database_name, table_name)
            )
        """

        # Upsert watermark
        upsert_query = """
            INSERT INTO archiver_watermarks (database_name, table_name, last_timestamp, last_primary_key, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (database_name, table_name)
            DO UPDATE SET
                last_timestamp = EXCLUDED.last_timestamp,
                last_primary_key = EXCLUDED.last_primary_key,
                updated_at = NOW()
        """

        try:
            # Create table if needed
            await db_manager.execute(create_table_query)

            # Convert primary key to string for storage
            pk_str = str(last_primary_key) if last_primary_key is not None else None

            # Upsert watermark
            await db_manager.execute(
                upsert_query, database_name, table_name, last_timestamp, pk_str
            )

            self.logger.debug(
                "Watermark saved to database",
                database=database_name,
                table=table_name,
            )

        except Exception as e:
            raise DatabaseError(
                f"Failed to save watermark to database: {e}",
                context={
                    "database": database_name,
                    "table": table_name,
                },
            ) from e
