"""Restore engine for bulk loading archived data into PostgreSQL."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import asyncpg
import structlog

from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError
from archiver.schema_detector import SchemaDetector
from restore.conflict_resolver import ConflictDetector, ConflictResolver
from restore.s3_reader import ArchiveFile
from restore.schema_migrator import SchemaMigrator
from utils.logging import get_logger


class RestoreEngine:
    """Engine for restoring archived data to PostgreSQL."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize restore engine.

        Args:
            db_manager: Database manager instance
            logger: Optional logger instance
        """
        self.db_manager = db_manager
        self.logger = logger or get_logger("restore_engine")
        self.schema_detector = SchemaDetector(logger=self.logger)
        self.schema_migrator = SchemaMigrator(logger=self.logger)
        self.conflict_detector = ConflictDetector(logger=self.logger) if ConflictDetector else None

    async def restore_archive(
        self,
        archive: ArchiveFile,
        conflict_strategy: str = "skip",
        batch_size: int = 1000,
        drop_indexes: bool = False,
        commit_frequency: int = 1,
        dry_run: bool = False,
        schema_migration_strategy: str = "lenient",
        detect_conflicts: bool = True,
        table_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """Restore an archive file to PostgreSQL.

        Args:
            archive: ArchiveFile object to restore
            conflict_strategy: How to handle conflicts (skip, overwrite, fail, merge, upsert)
            batch_size: Number of records per batch for COPY
            drop_indexes: If True, temporarily drop indexes before restore
            commit_frequency: Commit every N batches (1 = commit per batch)
            dry_run: If True, don't actually restore
            schema_migration_strategy: Schema migration strategy
            detect_conflicts: Whether to detect conflicts
            table_name: Table name override (fallback if archive doesn't have it)
            schema_name: Schema name override (fallback if archive doesn't have it)

        Returns:
            Dictionary with restore statistics

        Raises:
            DatabaseError: If restore fails
        """
        # Use provided table/schema names as fallback if archive doesn't have them
        target_table_name = table_name or archive.table_name
        target_schema_name = schema_name or archive.schema_name or "public"

        if not target_table_name:
            raise DatabaseError(
                "Table name is required but not found in archive metadata or CLI arguments",
                context={"archive_table": archive.table_name, "provided_table": table_name},
            )

        self.logger.debug(
            "Starting restore",
            database=archive.database_name,
            table=target_table_name,
            schema=target_schema_name,
            records=archive.record_count,
            conflict_strategy=conflict_strategy,
            dry_run=dry_run,
        )

        if dry_run:
            records = archive.parse_records()
            return {
                "records_processed": len(records),
                "records_restored": 0,
                "records_skipped": 0,
                "records_failed": 0,
                "dry_run": True,
            }

        # Parse records
        records = archive.parse_records()
        if not records:
            self.logger.warning("No records to restore")
            return {
                "records_processed": 0,
                "records_restored": 0,
                "records_skipped": 0,
                "records_failed": 0,
            }

        # Store original record count BEFORE any filtering
        original_record_count = len(records)

        # Get table schema from metadata if available
        archived_schema = archive.table_schema

        # Detect current database schema
        current_schema = None
        if not dry_run:
            try:
                current_schema = await self.schema_detector.detect_table_schema(
                    self.db_manager,
                    target_schema_name,
                    target_table_name,
                )
                self.logger.debug(
                    "Current schema detected",
                    schema=target_schema_name,
                    table=target_table_name,
                    columns=len(current_schema.get("columns", [])),
                )
            except Exception as e:
                self.logger.warning(
                    "Failed to detect current schema, proceeding without migration",
                    error=str(e),
                )

        # Compare schemas and generate diff report if both are available
        if archived_schema and current_schema:
            diff = self.schema_migrator.compare_schemas(archived_schema, current_schema)
            if diff.has_changes:
                report = self.schema_migrator.generate_diff_report(diff)
                self.logger.warning(
                    "Schema differences detected",
                    schema=target_schema_name,
                    table=target_table_name,
                    report=report,
                )

        records_failed_count = 0

        # Transform records if schema migration is needed
        if archived_schema and current_schema and schema_migration_strategy != "none":
            transformed_records = []
            for record in records:
                try:
                    transformed = self.schema_migrator.transform_record(
                        record,
                        archived_schema,
                        current_schema,
                        strategy=schema_migration_strategy,
                    )
                    transformed_records.append(transformed)
                except Exception as e:
                    if schema_migration_strategy == "strict":
                        raise DatabaseError(
                            f"Schema migration failed: {e}",
                            context={
                                "database": archive.database_name,
                                "table": target_table_name,
                                "strategy": schema_migration_strategy,
                            },
                        ) from e
                    else:
                        self.logger.warning(
                            "Record transformation failed, skipping",
                            error=str(e),
                        )
                        records_failed_count += 1
                        continue
            records = transformed_records

        # Get table columns (exclude metadata columns)
        metadata_columns = {"_archived_at", "_batch_id", "_source_database", "_source_table"}
        data_columns = [col for col in records[0].keys() if col not in metadata_columns]

        # Get table schema from metadata if available
        table_schema = archive.table_schema
        if table_schema:
            # Use schema from metadata to determine column types
            column_types = {
                col["name"]: col.get("type") or col.get("data_type") or "TEXT"
                for col in table_schema.get("columns", [])
                if col.get("name") in data_columns
            }
        else:
            # Infer types from data (fallback)
            column_types = self._infer_column_types(records, data_columns)

        # Connect to database
        if not self.db_manager.pool:
            await self.db_manager.connect()

        # Get primary key from schema
        primary_key = None
        if current_schema and current_schema.get("primary_key"):
            primary_key = current_schema["primary_key"].get("columns", [None])[0]
        elif archived_schema and archived_schema.get("primary_key"):
            primary_key = archived_schema["primary_key"].get("columns", [None])[0]

        # Detect conflicts if enabled and primary key is available
        conflict_report = None
        if ConflictResolver:
            conflict_resolver = ConflictResolver(strategy=conflict_strategy, logger=self.logger)
        else:
            conflict_resolver = None

        if (
            detect_conflicts
            and primary_key
            and not dry_run
            and self.conflict_detector
            and conflict_resolver
        ):
            try:
                conflict_report = await self.conflict_detector.detect_conflicts(
                    records=records,
                    primary_key=primary_key,
                    schema=target_schema_name,
                    table=target_table_name,
                    db_manager=self.db_manager,
                )

                if conflict_report.has_conflicts:
                    self.logger.debug(
                        "Conflicts detected",
                        schema=target_schema_name,
                        table=target_table_name,
                        total_conflicts=conflict_report.total_conflicts,
                        conflict_types=conflict_report.conflict_types,
                    )

                    # Validate strategy can handle conflicts
                    conflict_resolver.validate_strategy(conflict_report)

                    # Filter conflicting records if strategy is "skip"
                    if conflict_strategy == "skip":
                        records = conflict_resolver.filter_conflicting_records(
                            records, conflict_report, primary_key
                        )
                        self.logger.debug(
                            "Filtered conflicting records",
                            original_count=len(records) + conflict_report.total_conflicts,
                            filtered_count=len(records),
                        )

            except Exception as e:
                self.logger.warning(
                    "Conflict detection failed, proceeding without detection",
                    error=str(e),
                )

        # Calculate records skipped (before filtering, if conflicts were detected)
        records_skipped_count = 0
        if conflict_report and conflict_report.has_conflicts and conflict_strategy == "skip":
            # Records were filtered, so skipped = original - filtered
            records_skipped_count = original_record_count - len(records)

        # Safely extract conflict types
        conflict_types_dict = {}
        if conflict_report and hasattr(conflict_report, "conflict_types"):
            conflict_types_dict = conflict_report.conflict_types or {}

        stats = {
            "records_processed": original_record_count,  # Use original count, not filtered
            "records_restored": 0,
            "records_skipped": records_skipped_count,
            "records_failed": records_failed_count,
            "conflicts_detected": conflict_report.total_conflicts if conflict_report else 0,
            "conflict_types": conflict_types_dict,
            "skip_reason": (
                "conflict"
                if conflict_report and conflict_report.has_conflicts and conflict_strategy == "skip"
                else None
            ),
        }

        # Update data_columns after transformation (in case columns were added/removed)
        if records:
            metadata_columns = {"_archived_at", "_batch_id", "_source_database", "_source_table"}
            data_columns = [col for col in records[0].keys() if col not in metadata_columns]

        try:
            async with self.db_manager.pool.acquire() as conn:
                # Drop indexes if requested
                indexes_dropped = []
                if drop_indexes:
                    indexes_dropped = await self._drop_indexes(
                        conn, target_schema_name, target_table_name
                    )

                try:
                    # Restore using COPY FROM for performance
                    if conflict_strategy == "skip":
                        restored = await self._restore_with_copy_skip(
                            conn,
                            target_schema_name,
                            target_table_name,
                            records,
                            data_columns,
                            column_types,
                            batch_size,
                            commit_frequency,
                            current_schema,
                        )
                    elif conflict_strategy == "overwrite":
                        restored = await self._restore_with_copy_overwrite(
                            conn,
                            target_schema_name,
                            target_table_name,
                            records,
                            data_columns,
                            column_types,
                            batch_size,
                            commit_frequency,
                            current_schema,
                        )
                    elif conflict_strategy == "upsert":
                        restored = await self._restore_with_upsert(
                            conn,
                            target_schema_name,
                            target_table_name,
                            records,
                            data_columns,
                            column_types,
                            batch_size,
                            commit_frequency,
                            current_schema,
                        )
                    elif conflict_strategy == "fail":
                        restored = await self._restore_with_copy_fail(
                            conn,
                            target_schema_name,
                            target_table_name,
                            records,
                            data_columns,
                            column_types,
                            batch_size,
                            commit_frequency,
                            current_schema,
                        )
                    else:
                        raise ValueError(f"Unknown conflict strategy: {conflict_strategy}")

                    stats["records_restored"] = restored
                    # Update skipped records count (for skip strategy, already calculated above)
                    # If all records were filtered out due to conflicts, this is already set
                    # Otherwise, update based on actual restored count
                    if conflict_strategy == "skip" and not (
                        conflict_report and conflict_report.has_conflicts
                    ):
                        # Only update if conflicts weren't already handled in filtering
                        stats["records_skipped"] = original_record_count - restored

                finally:
                    # Restore indexes if dropped
                    if indexes_dropped:
                        await self._restore_indexes(conn, indexes_dropped)

        except Exception as e:
            self.logger.error(
                "Restore failed",
                database=archive.database_name,
                table=target_table_name,
                error=str(e),
                exc_info=True,
            )
            stats["records_failed"] = len(records)
            raise DatabaseError(
                f"Restore failed: {e}",
                context={
                    "database": archive.database_name,
                    "table": target_table_name,
                    "schema": target_schema_name,
                },
            ) from e

        self.logger.debug(
            "Restore completed",
            database=archive.database_name,
            table=target_table_name,
            **stats,
        )

        return stats

    def _infer_column_types(
        self, records: list[dict[str, Any]], columns: list[str]
    ) -> dict[str, str]:
        """Infer PostgreSQL column types from record data.

        Args:
            records: Sample records
            columns: Column names

        Returns:
            Dictionary mapping column names to PostgreSQL types
        """
        # Simple type inference - in production, use schema from metadata
        types = {}
        if not records:
            return types

        sample = records[0]
        for col in columns:
            value = sample.get(col)
            if value is None:
                types[col] = "TEXT"  # Default to TEXT for NULL
            elif isinstance(value, bool):
                types[col] = "BOOLEAN"
            elif isinstance(value, int):
                types[col] = "BIGINT"
            elif isinstance(value, float):
                types[col] = "DOUBLE PRECISION"
            elif isinstance(value, (datetime, str)):
                # Check if it's a datetime string
                if isinstance(value, str) and ("T" in value or value.endswith("Z")):
                    types[col] = "TIMESTAMPTZ"
                else:
                    types[col] = "TEXT"
            elif isinstance(value, list):
                types[col] = "JSONB"
            elif isinstance(value, dict):
                types[col] = "JSONB"
            else:
                types[col] = "TEXT"

        return types

    async def _drop_indexes(
        self, conn: asyncpg.Connection, schema: str, table: str
    ) -> list[dict[str, str]]:
        """Temporarily drop indexes on a table.

        Args:
            conn: Database connection
            schema: Schema name
            table: Table name

        Returns:
            List of dropped index information (for restoration)
        """
        query = """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = $1 AND tablename = $2
            AND indexname NOT LIKE '%_pkey'
        """
        indexes = await conn.fetch(query, schema, table)

        dropped = []
        for idx in indexes:
            try:
                await conn.execute(f"DROP INDEX IF EXISTS {schema}.{idx['indexname']}")
                dropped.append({"name": idx["indexname"], "def": idx["indexdef"]})
                self.logger.debug(
                    "Dropped index", schema=schema, table=table, index=idx["indexname"]
                )
            except Exception as e:
                self.logger.warning(
                    "Failed to drop index",
                    schema=schema,
                    table=table,
                    index=idx["indexname"],
                    error=str(e),
                )

        return dropped

    async def _restore_indexes(
        self, conn: asyncpg.Connection, indexes: list[dict[str, str]]
    ) -> None:
        """Restore dropped indexes.

        Args:
            conn: Database connection
            indexes: List of index information
        """
        for idx in indexes:
            try:
                await conn.execute(idx["def"])
                self.logger.debug("Restored index", index=idx["name"])
            except Exception as e:
                self.logger.warning("Failed to restore index", index=idx["name"], error=str(e))

    async def _restore_with_copy_skip(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        records: list[dict[str, Any]],
        columns: list[str],
        column_types: dict[str, str],
        batch_size: int,
        commit_frequency: int,
        current_schema: Optional[dict[str, Any]] = None,
    ) -> int:
        """Restore using INSERT with ON CONFLICT DO NOTHING.

        Args:
            conn: Database connection
            schema: Schema name
            table: Table name
            records: Records to restore
            columns: Column names
            column_types: Column type mapping
            batch_size: Batch size for inserts
            commit_frequency: Commit every N batches

        Returns:
            Number of records restored
        """
        # Build INSERT query with ON CONFLICT DO NOTHING
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        insert_query = f"""
            INSERT INTO {schema}.{table} ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """

        restored_count = 0
        batch_num = 0
        for i in range(0, len(records), batch_size):
            batch_num += 1
            batch = records[i : i + batch_size]
            batch_values = [
                self._prepare_record_values(record, columns, current_schema) for record in batch
            ]

            # Use transaction for each batch (or group of batches based on commit_frequency)
            async with conn.transaction():
                # Use executemany for batch insert
                result = await conn.executemany(insert_query, batch_values)
                # Parse result to get inserted count
                # asyncpg executemany returns "INSERT 0 N" format
                if result and "INSERT" in result:
                    try:
                        batch_restored = int(result.split()[-1])
                    except (ValueError, IndexError):
                        batch_restored = len(batch)
                else:
                    batch_restored = len(batch)

                restored_count += batch_restored

                self.logger.debug(
                    "Batch restored",
                    batch=batch_num,
                    restored=batch_restored,
                    total=restored_count,
                )

        return restored_count

    async def _restore_with_copy_overwrite(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        records: list[dict[str, Any]],
        columns: list[str],
        column_types: dict[str, str],
        batch_size: int,
        commit_frequency: int,
        current_schema: Optional[dict[str, Any]] = None,
    ) -> int:
        """Restore using INSERT with ON CONFLICT DO UPDATE.

        Args:
            conn: Database connection
            schema: Schema name
            table: Table name
            records: Records to restore
            columns: Column names
            column_types: Column type mapping
            batch_size: Batch size for inserts
            commit_frequency: Commit every N batches

        Returns:
            Number of records restored (inserted + updated)
        """
        # Build UPDATE clause for ON CONFLICT
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        insert_query = f"""
            INSERT INTO {schema}.{table} ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT DO UPDATE SET {update_clause}
        """

        restored_count = 0
        batch_num = 0
        for i in range(0, len(records), batch_size):
            batch_num += 1
            batch = records[i : i + batch_size]
            batch_values = [
                self._prepare_record_values(record, columns, current_schema) for record in batch
            ]

            async with conn.transaction():
                result = await conn.executemany(insert_query, batch_values)
                if result and "INSERT" in result:
                    try:
                        batch_restored = int(result.split()[-1])
                    except (ValueError, IndexError):
                        batch_restored = len(batch)
                else:
                    batch_restored = len(batch)

                restored_count += batch_restored

        return restored_count

    async def _restore_with_copy_fail(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        records: list[dict[str, Any]],
        columns: list[str],
        column_types: dict[str, str],
        batch_size: int,
        commit_frequency: int,
        current_schema: Optional[dict[str, Any]] = None,
    ) -> int:
        """Restore using INSERT, fail on conflicts.

        Args:
            conn: Database connection
            schema: Schema name
            table: Table name
            records: Records to restore
            columns: Column names
            column_types: Column type mapping
            batch_size: Batch size for inserts
            commit_frequency: Commit every N batches

        Returns:
            Number of records restored

        Raises:
            DatabaseError: If conflict detected
        """
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        insert_query = f"""
            INSERT INTO {schema}.{table} ({', '.join(columns)})
            VALUES ({placeholders})
        """

        restored_count = 0
        batch_num = 0
        for i in range(0, len(records), batch_size):
            batch_num += 1
            batch = records[i : i + batch_size]
            batch_values = [
                self._prepare_record_values(record, columns, current_schema) for record in batch
            ]

            async with conn.transaction():
                try:
                    result = await conn.executemany(insert_query, batch_values)
                    if result and "INSERT" in result:
                        try:
                            batch_restored = int(result.split()[-1])
                        except (ValueError, IndexError):
                            batch_restored = len(batch)
                    else:
                        batch_restored = len(batch)
                    restored_count += batch_restored
                except asyncpg.UniqueViolationError as e:
                    raise DatabaseError(
                        f"Conflict detected during restore: {e}",
                        context={"schema": schema, "table": table, "batch": batch_num},
                    ) from e

        return restored_count

    async def _restore_with_upsert(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        records: list[dict[str, Any]],
        columns: list[str],
        column_types: dict[str, str],
        batch_size: int,
        commit_frequency: int,
        current_schema: Optional[dict[str, Any]] = None,
    ) -> int:
        """Restore using upsert (same as overwrite for now).

        Args:
            conn: Database connection
            schema: Schema name
            table: Table name
            records: Records to restore
            columns: Column names
            column_types: Column type mapping
            batch_size: Batch size for COPY
            commit_frequency: Commit every N batches
            current_schema: Current database schema (for timezone handling)

        Returns:
            Number of records restored
        """
        # Upsert is same as overwrite for now
        return await self._restore_with_copy_overwrite(
            conn,
            schema,
            table,
            records,
            columns,
            column_types,
            batch_size,
            commit_frequency,
            current_schema,
        )

    def _is_column_timezone_aware(
        self, column_name: str, current_schema: Optional[dict[str, Any]]
    ) -> bool:
        """Check if a column is timezone-aware (TIMESTAMPTZ).

        Args:
            column_name: Column name
            current_schema: Current database schema dictionary

        Returns:
            True if column is TIMESTAMPTZ, False if TIMESTAMP or unknown
        """
        if not current_schema or not current_schema.get("columns"):
            return False  # Default to timezone-naive if schema not available

        for col in current_schema["columns"]:
            if col.get("name") == column_name:
                data_type = col.get("data_type", "").lower()
                # TIMESTAMPTZ is "timestamp with time zone", TIMESTAMP is "timestamp without time zone"
                return "timestamp with time zone" in data_type

        return False  # Default to timezone-naive if column not found

    def _prepare_record_values(
        self,
        record: dict[str, Any],
        columns: list[str],
        current_schema: Optional[dict[str, Any]] = None,
    ) -> list[Any]:
        """Prepare record values for INSERT.

        Args:
            record: Record dictionary
            columns: Column names
            current_schema: Current database schema (optional, for timezone handling)

        Returns:
            List of values in column order
        """
        values = []
        for col in columns:
            value = record.get(col)
            # Convert to appropriate Python type for asyncpg
            if value is None:
                values.append(None)
            elif isinstance(value, (dict, list)):
                # JSON/JSONB - asyncpg handles dict/list automatically
                values.append(value)
            elif isinstance(value, str):
                # Check if it's a datetime string
                if "T" in value or value.endswith("Z"):
                    try:
                        # Parse ISO format datetime
                        dt_str = value.replace("Z", "+00:00")
                        dt = datetime.fromisoformat(dt_str)

                        # Check if column is timezone-aware or timezone-naive
                        is_tz_aware = self._is_column_timezone_aware(col, current_schema)
                        if not is_tz_aware and dt.tzinfo is not None:
                            # Column is TIMESTAMP (naive), but datetime is timezone-aware
                            # Convert to naive by removing timezone info (keeping UTC time)
                            dt = dt.replace(tzinfo=None)

                        values.append(dt)
                    except (ValueError, AttributeError):
                        values.append(value)
                else:
                    values.append(value)
            elif isinstance(value, datetime):
                # Handle datetime objects directly (from parsed JSON)
                is_tz_aware = self._is_column_timezone_aware(col, current_schema)
                if not is_tz_aware and value.tzinfo is not None:
                    # Column is TIMESTAMP (naive), but datetime is timezone-aware
                    # Convert to naive by removing timezone info (keeping UTC time)
                    value = value.replace(tzinfo=None)
                elif is_tz_aware and value.tzinfo is None:
                    # Column is TIMESTAMPTZ (aware), but datetime is naive
                    # Assume naive datetime is in UTC and make it timezone-aware
                    value = value.replace(tzinfo=timezone.utc)
                values.append(value)
            elif isinstance(value, (int, float, bool)):
                values.append(value)
            elif isinstance(value, Decimal):
                values.append(float(value))  # Convert Decimal to float for asyncpg
            else:
                values.append(str(value))

        return values
