"""Vacuum manager for reclaiming space after archival operations."""

import asyncio
import time
from typing import Any, Optional

import structlog

from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError
from utils.logging import get_logger


class VacuumManager:
    """Manages VACUUM operations to reclaim space after archival."""

    def __init__(
        self,
        logger: Optional[structlog.BoundLogger] = None,
        vacuum_timeout_seconds: int = 7200,  # 2 hours default
        progress_update_interval: int = 30,  # 30 seconds
    ) -> None:
        """Initialize vacuum manager.

        Args:
            logger: Optional logger instance
            vacuum_timeout_seconds: Maximum time for vacuum operation (default: 7200 = 2 hours)
            progress_update_interval: Interval in seconds for progress updates (default: 30)
        """
        self.logger = logger or get_logger("vacuum_manager")
        self.vacuum_timeout_seconds = vacuum_timeout_seconds
        self.progress_update_interval = progress_update_interval

    async def get_table_size(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
    ) -> dict[str, int]:
        """Get table size information.

        Args:
            db_manager: Database manager instance
            schema_name: Schema name
            table_name: Table name

        Returns:
            Dictionary with size information:
            - total_size_bytes: Total table size including indexes and TOAST
            - table_size_bytes: Table size only (excluding indexes)
            - indexes_size_bytes: Indexes size
        """
        # Use PostgreSQL's quote_ident for safe identifier quoting
        # This prevents SQL injection by properly escaping identifiers
        async with db_manager.acquire_connection() as conn:
            # Use parameterized query with quote_ident function
            size_query = """
                SELECT
                    pg_total_relation_size(
                        (quote_ident($1) || '.' || quote_ident($2))::regclass
                    ) AS total_size_bytes,
                    pg_relation_size(
                        (quote_ident($1) || '.' || quote_ident($2))::regclass
                    ) AS table_size_bytes,
                    pg_indexes_size(
                        (quote_ident($1) || '.' || quote_ident($2))::regclass
                    ) AS indexes_size_bytes
            """

            try:
                result = await conn.fetchrow(size_query, schema_name, table_name)

                if not result:
                    raise DatabaseError(
                        f"Failed to get table size for {schema_name}.{table_name}",
                        context={"schema": schema_name, "table": table_name},
                    )

                # asyncpg records support attribute access
                return {
                    "total_size_bytes": result.total_size_bytes or 0,
                    "table_size_bytes": result.table_size_bytes or 0,
                    "indexes_size_bytes": result.indexes_size_bytes or 0,
                }
            except Exception as e:
                self.logger.error(
                    "Failed to get table size",
                    schema=schema_name,
                    table=table_name,
                    error=str(e),
                )
                raise DatabaseError(
                    f"Failed to get table size: {e}",
                    context={"schema": schema_name, "table": table_name},
                ) from e

    async def _monitor_vacuum_progress(
        self,
        conn: Any,
        schema_name: str,
        table_name: str,
        start_time: float,
    ) -> None:
        """Monitor vacuum progress using pg_stat_progress_vacuum.

        Args:
            conn: Database connection
            schema_name: Schema name
            table_name: Table name
            start_time: Start time of vacuum operation
        """
        last_log_time = start_time

        while True:
            await asyncio.sleep(self.progress_update_interval)

            try:
                # Query pg_stat_progress_vacuum for progress information
                # This view is available in PostgreSQL 12+
                progress_query = """
                    SELECT
                        pid,
                        phase,
                        heap_blks_total,
                        heap_blks_scanned,
                        heap_blks_vacuumed,
                        index_vacuum_count,
                        max_dead_tuples,
                        num_dead_tuples
                    FROM pg_stat_progress_vacuum
                    WHERE datname = current_database()
                    AND relid = (quote_ident($1) || '.' || quote_ident($2))::regclass
                """

                result = await conn.fetchrow(progress_query, schema_name, table_name)

                if result:
                    elapsed = time.time() - start_time
                    heap_progress = (
                        (result.heap_blks_scanned / result.heap_blks_total * 100)
                        if result.heap_blks_total > 0
                        else 0
                    )

                    self.logger.info(
                        "Vacuum progress",
                        schema=schema_name,
                        table=table_name,
                        phase=result.phase,
                        heap_blocks_scanned=result.heap_blks_scanned,
                        heap_blocks_total=result.heap_blks_total,
                        heap_progress_percent=round(heap_progress, 2),
                        heap_blocks_vacuumed=result.heap_blks_vacuumed,
                        index_vacuum_count=result.index_vacuum_count,
                        dead_tuples=result.num_dead_tuples,
                        elapsed_seconds=round(elapsed, 1),
                    )
                else:
                    # Vacuum may have completed or not started yet
                    break

            except Exception as e:
                # Progress monitoring is non-critical, log and continue
                self.logger.debug(
                    "Failed to get vacuum progress (non-critical)",
                    schema=schema_name,
                    table=table_name,
                    error=str(e),
                )
                # If it's a "relation does not exist" error, vacuum may have completed
                if "does not exist" in str(e).lower():
                    break

    async def run_vacuum(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
        vacuum_type: str = "analyze",
        dry_run: bool = False,
        timeout_seconds: Optional[int] = None,
    ) -> dict[str, Any]:
        """Run VACUUM operation on a table.

        Args:
            db_manager: Database manager instance
            schema_name: Schema name
            table_name: Table name
            vacuum_type: Type of vacuum to run:
                - "none": No vacuum
                - "analyze": VACUUM ANALYZE (lightweight, updates statistics)
                - "standard": VACUUM (lightweight, reclaims space)
                - "full": VACUUM FULL (aggressive, locks table, reclaims all space)
            dry_run: If True, don't actually run vacuum
            timeout_seconds: Optional timeout override (default: uses instance timeout)

        Returns:
            Dictionary with vacuum results:
            - vacuum_type: Type of vacuum that was run
            - duration_seconds: Duration of vacuum operation
            - success: Whether vacuum succeeded
            - progress_monitored: Whether progress was monitored
        """
        if vacuum_type.lower() == "none":
            self.logger.debug(
                "Vacuum disabled",
                schema=schema_name,
                table=table_name,
            )
            return {
                "vacuum_type": "none",
                "duration_seconds": 0.0,
                "success": True,
                "skipped": True,
            }

        # Map vacuum_type to SQL command
        vacuum_commands = {
            "analyze": "VACUUM ANALYZE",
            "standard": "VACUUM",
            "full": "VACUUM FULL",
        }

        vacuum_command = vacuum_commands.get(vacuum_type.lower(), "VACUUM ANALYZE")

        if dry_run:
            self.logger.info(
                "DRY RUN: Would run vacuum",
                schema=schema_name,
                table=table_name,
                vacuum_type=vacuum_command,
            )
            return {
                "vacuum_type": vacuum_command,
                "duration_seconds": 0.0,
                "success": True,
                "dry_run": True,
            }

        self.logger.info(
            "Starting vacuum operation",
            schema=schema_name,
            table=table_name,
            vacuum_type=vacuum_command,
            timeout_seconds=timeout_seconds or self.vacuum_timeout_seconds,
        )

        start_time = time.time()
        timeout = timeout_seconds or self.vacuum_timeout_seconds
        progress_monitored = False

        try:
            # Use direct connection for VACUUM (utility commands work better this way)
            async with db_manager.acquire_connection() as conn:
                # First, get properly quoted identifiers using PostgreSQL's quote_ident
                # This prevents SQL injection by properly escaping identifiers
                quote_query = "SELECT quote_ident($1) AS schema_quoted, quote_ident($2) AS table_quoted"
                quote_result = await conn.fetchrow(quote_query, schema_name, table_name)

                if not quote_result:
                    raise DatabaseError(
                        f"Failed to quote identifiers for {schema_name}.{table_name}",
                        context={"schema": schema_name, "table": table_name},
                    )

                schema_quoted = quote_result.schema_quoted
                table_quoted = quote_result.table_quoted
                table_identifier = f"{schema_quoted}.{table_quoted}"

                # Set statement timeout for this connection
                await conn.execute(
                    f"SET LOCAL statement_timeout = {timeout * 1000}"  # milliseconds
                )

                # Build VACUUM command with properly quoted identifiers
                vacuum_sql = f"{vacuum_command} {table_identifier}"

                # Start progress monitoring task (non-blocking)
                progress_task: Optional[asyncio.Task[None]] = None
                try:
                    # Try to start progress monitoring (PostgreSQL 12+)
                    progress_task = asyncio.create_task(
                        self._monitor_vacuum_progress(
                            conn, schema_name, table_name, start_time
                        )
                    )
                    progress_monitored = True
                except Exception:
                    # Progress monitoring not available (PostgreSQL < 12 or error)
                    self.logger.debug(
                        "Progress monitoring not available",
                        schema=schema_name,
                        table=table_name,
                    )

                # Execute vacuum with timeout protection
                try:
                    await asyncio.wait_for(
                        conn.execute(vacuum_sql),
                        timeout=timeout,
                    )
                except asyncio.TimeoutError:
                    # Cancel progress monitoring if still running
                    if progress_task and not progress_task.done():
                        progress_task.cancel()
                        try:
                            await progress_task
                        except asyncio.CancelledError:
                            pass

                    raise DatabaseError(
                        f"Vacuum operation timed out after {timeout} seconds",
                        context={
                            "schema": schema_name,
                            "table": table_name,
                            "vacuum_type": vacuum_command,
                            "timeout_seconds": timeout,
                        },
                    )

                # Cancel progress monitoring if still running
                if progress_task and not progress_task.done():
                    progress_task.cancel()
                    try:
                        await progress_task
                    except asyncio.CancelledError:
                        pass

            duration = time.time() - start_time

            self.logger.info(
                "Vacuum completed successfully",
                schema=schema_name,
                table=table_name,
                vacuum_type=vacuum_command,
                duration_seconds=round(duration, 2),
                progress_monitored=progress_monitored,
            )

            return {
                "vacuum_type": vacuum_command,
                "duration_seconds": duration,
                "success": True,
                "progress_monitored": progress_monitored,
            }

        except asyncio.TimeoutError:
            duration = time.time() - start_time
            error_msg = f"Vacuum operation timed out after {timeout} seconds"
            self.logger.error(
                "Vacuum timed out",
                schema=schema_name,
                table=table_name,
                vacuum_type=vacuum_command,
                duration_seconds=round(duration, 2),
                timeout_seconds=timeout,
            )
            return {
                "vacuum_type": vacuum_command,
                "duration_seconds": duration,
                "success": False,
                "error": error_msg,
                "timeout": True,
            }

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(
                "Vacuum failed",
                schema=schema_name,
                table=table_name,
                vacuum_type=vacuum_command,
                duration_seconds=round(duration, 2),
                error=str(e),
            )
            return {
                "vacuum_type": vacuum_command,
                "duration_seconds": duration,
                "success": False,
                "error": str(e),
            }

    async def vacuum_table_after_archive(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
        vacuum_type: str = "VACUUM ANALYZE",
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run vacuum after archival with space tracking.

        This method:
        1. Gets table size before vacuum
        2. Runs vacuum
        3. Gets table size after vacuum
        4. Calculates space reclaimed

        Args:
            db_manager: Database manager instance
            schema_name: Schema name
            table_name: Table name
            vacuum_type: Type of vacuum to run
            dry_run: If True, don't actually run vacuum

        Returns:
            Dictionary with vacuum results including space reclaimed
        """
        if vacuum_type.lower() == "none":
            return await self.run_vacuum(
                db_manager, schema_name, table_name, vacuum_type, dry_run
            )

        # Get size before vacuum
        try:
            size_before = await self.get_table_size(db_manager, schema_name, table_name)
        except Exception as e:
            self.logger.warning(
                "Failed to get table size before vacuum (non-critical)",
                schema=schema_name,
                table=table_name,
                error=str(e),
            )
            size_before = None

        # Run vacuum
        vacuum_result = await self.run_vacuum(
            db_manager, schema_name, table_name, vacuum_type, dry_run
        )

        # Get size after vacuum (only if vacuum succeeded and we got size before)
        size_after = None
        if vacuum_result.get("success") and size_before:
            try:
                size_after = await self.get_table_size(db_manager, schema_name, table_name)
            except Exception as e:
                self.logger.warning(
                    "Failed to get table size after vacuum (non-critical)",
                    schema=schema_name,
                    table=table_name,
                    error=str(e),
                )

        # Calculate space reclaimed
        space_reclaimed = None
        if size_before and size_after:
            space_reclaimed = {
                "total_size_bytes": size_before["total_size_bytes"]
                - size_after["total_size_bytes"],
                "table_size_bytes": size_before["table_size_bytes"]
                - size_after["table_size_bytes"],
                "indexes_size_bytes": size_before["indexes_size_bytes"]
                - size_after["indexes_size_bytes"],
            }

            # Calculate percentage reclaimed
            if size_before["total_size_bytes"] > 0:
                reclaim_percentage = (
                    space_reclaimed["total_size_bytes"]
                    / size_before["total_size_bytes"]
                    * 100
                )
            else:
                reclaim_percentage = 0.0

            space_reclaimed["reclaim_percentage"] = reclaim_percentage

            # Log space reclaimed
            if space_reclaimed["total_size_bytes"] > 0:
                self.logger.info(
                    "Space reclaimed after vacuum",
                    schema=schema_name,
                    table=table_name,
                    total_size_reclaimed_mb=round(
                        space_reclaimed["total_size_bytes"] / (1024 * 1024), 2
                    ),
                    table_size_reclaimed_mb=round(
                        space_reclaimed["table_size_bytes"] / (1024 * 1024), 2
                    ),
                    indexes_size_reclaimed_mb=round(
                        space_reclaimed["indexes_size_bytes"] / (1024 * 1024), 2
                    ),
                    reclaim_percentage=round(reclaim_percentage, 2),
                )

                # Warn if vacuum was ineffective (<10% space reclaimed)
                if reclaim_percentage < 10.0 and size_before["total_size_bytes"] > 100 * 1024 * 1024:  # > 100MB
                    self.logger.warning(
                        "Vacuum may be ineffective - less than 10% space reclaimed",
                        schema=schema_name,
                        table=table_name,
                        reclaim_percentage=round(reclaim_percentage, 2),
                        total_size_before_mb=round(
                            size_before["total_size_bytes"] / (1024 * 1024), 2
                        ),
                        suggestion="Consider running VACUUM FULL during maintenance window if significant space reclamation is needed",
                    )
            else:
                self.logger.debug(
                    "No space reclaimed (table may have been recently vacuumed)",
                    schema=schema_name,
                    table=table_name,
                )

        # Add size information to result
        vacuum_result["size_before"] = size_before
        vacuum_result["size_after"] = size_after
        vacuum_result["space_reclaimed"] = space_reclaimed

        return vacuum_result

    @staticmethod
    def _safe_identifier(identifier: str) -> str:
        """Safely quote a PostgreSQL identifier.

        Note: This method is kept for backward compatibility but is no longer
        used in critical paths. We now use PostgreSQL's quote_ident() function
        for proper SQL injection prevention.

        Args:
            identifier: Identifier to quote

        Returns:
            Quoted identifier
        """
        # Remove any existing quotes
        identifier = identifier.strip('"')

        # If identifier contains special characters or is a reserved word, quote it
        # PostgreSQL identifiers can contain letters, digits, underscore, and dollar sign
        # when unquoted, but must be quoted if they contain other characters
        if not identifier.replace("_", "").replace("$", "").isalnum():
            return f'"{identifier}"'
        return identifier

