"""Main archiver orchestrator that coordinates all components."""

import asyncio
import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import structlog
from prometheus_client import CollectorRegistry

from archiver.audit_trail import AuditEventType, AuditTrail
from archiver.batch_processor import BatchProcessor
from archiver.checkpoint import Checkpoint, CheckpointManager
from archiver.compressor import Compressor
from archiver.config import ArchiverConfig, DatabaseConfig, MonitoringConfig, TableConfig
from archiver.database import DatabaseManager
from archiver.deletion_manifest import DeletionManifestGenerator
from archiver.exceptions import ConfigurationError, VerificationError
from archiver.health_check import HealthChecker
from archiver.health_server import HealthCheckServer
from archiver.legal_hold import LegalHoldChecker
from archiver.locking import LockError, LockManager
from archiver.metadata import MetadataGenerator
from archiver.metrics import ArchiverMetrics
from archiver.multipart_cleanup import MultipartCleanup
from archiver.notification_manager import EnhancedNotificationManager
from archiver.progress_tracker import ProgressTracker
from archiver.retention_policy import RetentionPolicyEnforcer
from archiver.s3_client import S3Client
from archiver.sample_verifier import SampleVerifier
from archiver.schema_detector import SchemaDetector
from archiver.schema_drift import SchemaDriftDetector
from archiver.serializer import PostgreSQLSerializer
from archiver.verifier import Verifier
from archiver.watermark_manager import WatermarkManager
from utils import safe_identifier
from utils.checksum import ChecksumCalculator
from utils.logging import get_logger


class Archiver:
    """Main archiver that orchestrates the archival process."""

    def __init__(
        self,
        config: ArchiverConfig,
        dry_run: bool = False,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize archiver.

        Args:
            config: Archiver configuration
            dry_run: If True, don't make any changes
            logger: Optional logger instance
        """
        self.config = config
        self.dry_run = dry_run
        self.logger = logger or get_logger("archiver")
        defaults = config.defaults
        self.serializer = PostgreSQLSerializer(logger=self.logger)
        self.compressor = Compressor(
            compression_level=defaults.compression_level, logger=self.logger
        )
        self.verifier = Verifier(logger=self.logger)
        self.checksum_calculator = ChecksumCalculator(logger=self.logger)
        self.metadata_generator = MetadataGenerator(logger=self.logger)
        self.manifest_generator = DeletionManifestGenerator(logger=self.logger)
        self.sample_verifier = SampleVerifier(logger=self.logger)
        self.schema_detector = SchemaDetector(logger=self.logger)
        self.schema_drift_detector = SchemaDriftDetector(
            fail_on_drift=defaults.fail_on_schema_drift,
            logger=self.logger,
        )
        self.watermark_manager = WatermarkManager(
            storage_type=defaults.watermark_storage_type,
            logger=self.logger,
        )
        self.lock_manager = LockManager(
            lock_type=defaults.lock_type,
            logger=self.logger,
        )
        self.checkpoint_manager = CheckpointManager(
            storage_type=defaults.checkpoint_storage_type,
            checkpoint_interval=defaults.checkpoint_interval,
            logger=self.logger,
        )

        # Initialize legal hold checker if configured
        if config.legal_holds:
            self.legal_hold_checker = LegalHoldChecker(
                enabled=config.legal_holds.enabled,
                check_table=config.legal_holds.check_table,
                check_database=config.legal_holds.check_database,
                api_endpoint=config.legal_holds.api_endpoint,
                api_timeout=config.legal_holds.api_timeout,
                logger=self.logger,
            )
        else:
            self.legal_hold_checker = LegalHoldChecker(enabled=False, logger=self.logger)

        # Initialize retention policy enforcer if configured
        if config.compliance:
            self.retention_enforcer = RetentionPolicyEnforcer(
                compliance_config=config.compliance,
                logger=self.logger,
            )
        else:
            self.retention_enforcer = RetentionPolicyEnforcer(logger=self.logger)

        # Initialize audit trail
        self.audit_trail = AuditTrail(
            storage_type=defaults.audit_trail_storage_type,
            logger=self.logger,
        )

        # Initialize monitoring components
        monitoring_config = config.monitoring or MonitoringConfig()
        self.metrics = (
            ArchiverMetrics(logger=self.logger, registry=CollectorRegistry())
            if monitoring_config.metrics_enabled
            else None
        )
        self.progress_tracker = (
            ProgressTracker(
                quiet=monitoring_config.quiet_mode,
                update_interval=monitoring_config.progress_update_interval,
                logger=self.logger,
            )
            if monitoring_config.progress_enabled
            else None
        )
        self.health_checker = (
            HealthChecker(logger=self.logger) if monitoring_config.health_check_enabled else None
        )
        self.health_server: Optional[HealthCheckServer] = None

        # Start metrics server if enabled
        if self.metrics and monitoring_config.metrics_enabled:
            try:
                self.metrics.start_metrics_server(port=monitoring_config.metrics_port)
            except Exception as e:
                self.logger.warning(
                    "Failed to start metrics server (non-critical)",
                    port=monitoring_config.metrics_port,
                    error=str(e),
                )

        # Initialize notification manager if configured
        if config.notifications and config.notifications.enabled:
            self.notification_manager = EnhancedNotificationManager(
                config=config.notifications,
                logger=self.logger,
            )
        else:
            self.notification_manager = None

    async def archive(self) -> dict[str, Any]:
        """Run archival process for all configured databases and tables.

        Returns:
            Dictionary with archival statistics
        """
        self.logger.info("Starting archival process", dry_run=self.dry_run)

        stats = {
            "databases_processed": 0,
            "databases_failed": 0,
            "tables_processed": 0,
            "tables_failed": 0,
            "records_archived": 0,  # This run only (for backward compatibility)
            "records_archived_this_run": 0,  # This run only
            "records_archived_total": 0,  # Overall (including checkpoint)
            "batches_processed": 0,
            "start_time": datetime.now(timezone.utc).isoformat(),
            "database_stats": [],  # Per-database statistics
        }

        # Check if parallel processing is enabled
        if self.config.defaults.parallel_databases:
            max_parallel = self.config.defaults.max_parallel_databases
            self.logger.info(
                "Parallel database processing enabled",
                max_parallel=max_parallel,
                total_databases=len(self.config.databases),
            )
            await self._archive_databases_parallel(self.config.databases, stats, max_parallel)
        else:
            self.logger.info(
                "Sequential database processing",
                total_databases=len(self.config.databases),
            )
            for db_config in self.config.databases:
                await self._archive_database_with_stats(db_config, stats)

        stats["end_time"] = datetime.now(timezone.utc).isoformat()

        # Determine final status
        if stats["databases_failed"] == 0 and stats["tables_failed"] == 0:
            final_status = "success"
        elif stats["databases_processed"] > 0 or stats["tables_processed"] > 0:
            final_status = "partial"
        else:
            final_status = "failure"

        # Record run status in metrics
        if self.metrics:
            self.metrics.record_run_status(final_status)

        # Finish progress tracking
        if self.progress_tracker:
            self.progress_tracker.finish(success=(final_status == "success"))

        # Stop health check server
        if self.health_server:
            try:
                await self.health_server.stop()
            except Exception as e:
                self.logger.warning(
                    "Failed to stop health check server",
                    error=str(e),
                )

        # Send digest notification if enabled
        if (
            self.notification_manager
            and self.config.notifications
            and self.config.notifications.digest_mode
        ):
            try:
                await self.notification_manager.send_digest()
            except Exception as e:
                self.logger.warning(
                    "Failed to send digest notification (non-critical)",
                    error=str(e),
                )

        # Close notification manager
        if self.notification_manager:
            try:
                await self.notification_manager.close()
            except Exception as e:
                self.logger.warning(
                    "Failed to close notification manager",
                    error=str(e),
                )

        self.logger.info("Archival process completed", **stats)

        # Print formatted summary if not in JSON log mode
        # Check if console format is being used (not JSON)
        try:
            from utils.output import print_summary

            # Only print if we're likely in console mode (not JSON)
            # This is a simple heuristic - in production, you might want to pass a flag
            print_summary(stats, title="Archival Summary")
        except Exception:
            # If output formatting fails, just log normally
            pass

        return stats

    async def _archive_database_with_stats(
        self, db_config: DatabaseConfig, stats: dict[str, Any]
    ) -> None:
        """Archive a database and update statistics.

        Args:
            db_config: Database configuration
            stats: Statistics dictionary to update
        """
        db_stats = {
            "database": db_config.name,
            "tables_processed": 0,
            "tables_failed": 0,
            "records_archived": 0,
            "batches_processed": 0,
            "start_time": datetime.now(timezone.utc).isoformat(),
            "success": False,
        }

        try:
            await self._archive_database(db_config, stats, db_stats)
            db_stats["success"] = True
            stats["databases_processed"] += 1
        except Exception as e:
            db_stats["error"] = str(e)
            stats["databases_failed"] += 1
            self.logger.error(
                "Database archival failed",
                database=db_config.name,
                error=str(e),
                exc_info=True,
            )
        finally:
            db_stats["end_time"] = datetime.now(timezone.utc).isoformat()
            stats["database_stats"].append(db_stats)

    async def _archive_databases_parallel(
        self,
        databases: list[DatabaseConfig],
        stats: dict[str, Any],
        max_parallel: int,
    ) -> None:
        """Archive multiple databases in parallel with concurrency limit.

        Args:
            databases: List of database configurations
            stats: Statistics dictionary to update
            max_parallel: Maximum number of databases to process in parallel
        """
        semaphore = asyncio.Semaphore(max_parallel)

        async def archive_with_semaphore(db_config: DatabaseConfig) -> None:
            async with semaphore:
                await self._archive_database_with_stats(db_config, stats)

        tasks = [archive_with_semaphore(db_config) for db_config in databases]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _archive_database(
        self,
        db_config: DatabaseConfig,
        stats: dict[str, Any],
        db_stats: Optional[dict[str, Any]] = None,
    ) -> None:
        """Archive all tables in a database.

        Args:
            db_config: Database configuration
            stats: Global statistics dictionary to update
            db_stats: Per-database statistics dictionary to update (optional)
        """
        self.logger.debug("Processing database", database=db_config.name)

        # Get connection pool size (database-specific or global default)
        pool_size = db_config.connection_pool_size or self.config.defaults.connection_pool_size

        db_manager = DatabaseManager(db_config, pool_size=pool_size, logger=self.logger)
        s3_client = S3Client(self.config.s3, logger=self.logger)

        # Acquire database-level lock to prevent concurrent runs
        lock: Optional[Any] = None
        try:
            # Connect to database
            await db_manager.connect()

            # Acquire lock for this database (if not dry run)
            if not self.dry_run:
                lock_key = f"database:{db_config.name}"
                try:
                    lock = await self.lock_manager.acquire_lock(
                        lock_key=lock_key,
                        db_manager=db_manager,
                    )
                    # Start heartbeat to keep lock alive
                    await self.lock_manager.start_heartbeat(
                        lock=lock,
                        db_manager=db_manager,
                    )
                    self.logger.debug(
                        "Database lock acquired",
                        database=db_config.name,
                        lock_id=lock.lock_id,
                    )
                except LockError as e:
                    self.logger.error(
                        "Failed to acquire database lock (another instance may be running)",
                        database=db_config.name,
                        error=str(e),
                    )
                    raise

            # Validate S3 bucket
            if not self.dry_run:
                s3_client.validate_bucket()

            # Process each table
            for table_config in db_config.tables:
                table_start_time = datetime.now(timezone.utc)
                records_archived = 0
                try:
                    await self._archive_table(
                        db_manager, s3_client, db_config, table_config, stats, db_stats
                    )
                    stats["tables_processed"] += 1
                    if db_stats is not None:
                        db_stats["tables_processed"] += 1

                    # Get records archived from stats
                    records_archived = stats.get("records_archived", 0)
                    duration = (datetime.now(timezone.utc) - table_start_time).total_seconds()

                    # Log success to audit trail
                    await self.audit_trail.log_event(
                        event_type=AuditEventType.ARCHIVE_SUCCESS,
                        database_name=db_config.name,
                        table_name=table_config.name,
                        schema_name=table_config.schema_name,
                        record_count=records_archived,
                        status="success",
                        duration_seconds=duration,
                        s3_client=s3_client,
                    )
                except Exception as e:
                    stats["tables_failed"] += 1
                    if db_stats is not None:
                        db_stats["tables_failed"] += 1
                    self.logger.error(
                        "Table archival failed",
                        database=db_config.name,
                        table=table_config.name,
                        error=str(e),
                        exc_info=True,
                    )

                    # Log failure to audit trail
                    duration = (datetime.now(timezone.utc) - table_start_time).total_seconds()
                    records_archived_before_failure = stats.get("records_archived", 0)
                    batches_processed_before_failure = stats.get("batches_processed", 0)

                    await self.audit_trail.log_event(
                        event_type=AuditEventType.ARCHIVE_FAILURE,
                        database_name=db_config.name,
                        table_name=table_config.name,
                        schema_name=table_config.schema_name,
                        record_count=records_archived_before_failure,
                        status="failed",
                        duration_seconds=duration,
                        error_message=str(e),
                        s3_client=s3_client,
                    )

                    # Send failure notification
                    if self.notification_manager:
                        try:
                            await self.notification_manager.notify_archive_failure(
                                database=db_config.name,
                                table=table_config.name,
                                schema=table_config.schema_name,
                                error_message=str(e),
                                records_archived=records_archived_before_failure,
                                batches_processed=batches_processed_before_failure,
                            )
                        except Exception as e_notif:
                            self.logger.warning(
                                "Failed to send failure notification (non-critical)",
                                database=db_config.name,
                                table=table_config.name,
                                error=str(e_notif),
                            )

        finally:
            # Release lock
            if lock:
                try:
                    await self.lock_manager.release_lock(lock, db_manager=db_manager)
                    self.logger.debug(
                        "Database lock released",
                        database=db_config.name,
                    )
                except Exception as e:
                    self.logger.warning(
                        "Failed to release database lock",
                        database=db_config.name,
                        error=str(e),
                    )

            if db_manager:
                await db_manager.disconnect()

    async def _archive_table(
        self,
        db_manager: DatabaseManager,
        s3_client: S3Client,
        db_config: DatabaseConfig,
        table_config: TableConfig,
        stats: dict[str, Any],
        db_stats: Optional[dict[str, Any]] = None,
    ) -> None:
        """Archive a single table.

        Args:
            db_manager: Database manager
            s3_client: S3 client
            db_config: Database configuration
            table_config: Table configuration
            stats: Statistics dictionary to update
        """
        self.logger.debug(
            "Processing table",
            database=db_config.name,
            table=table_config.name,
            schema=table_config.schema_name,
        )

        # Check for legal hold before processing
        legal_hold = await self.legal_hold_checker.check_legal_hold(
            database_name=db_config.name,
            table_name=table_config.name,
            schema_name=table_config.schema_name,
            db_manager=db_manager,
        )

        if legal_hold and legal_hold.is_active():
            self.logger.warning(
                "Table has active legal hold - skipping archival",
                database=db_config.name,
                table=table_config.name,
                schema=table_config.schema_name,
                reason=legal_hold.reason,
                requestor=legal_hold.requestor,
                expiration_date=legal_hold.expiration_date,
            )
            stats["tables_skipped"] = stats.get("tables_skipped", 0) + 1

            # Log to audit trail
            await self.audit_trail.log_event(
                event_type=AuditEventType.ARCHIVE_FAILURE,
                database_name=db_config.name,
                table_name=table_config.name,
                schema_name=table_config.schema_name,
                status="skipped",
                error_message=f"Legal hold active: {legal_hold.reason}",
                metadata={
                    "legal_hold": {"reason": legal_hold.reason, "requestor": legal_hold.requestor}
                },
                s3_client=s3_client,
            )
            return

        # Validate retention policy
        try:
            self.retention_enforcer.validate_retention(table_config)
        except Exception as e:
            self.logger.error(
                "Retention policy validation failed",
                database=db_config.name,
                table=table_config.name,
                error=str(e),
            )
            await self.audit_trail.log_event(
                event_type=AuditEventType.ARCHIVE_FAILURE,
                database_name=db_config.name,
                table_name=table_config.name,
                schema_name=table_config.schema_name,
                status="failed",
                error_message=str(e),
                s3_client=s3_client,
            )
            raise

        # Validate encryption for sensitive tables
        if self.config.compliance and self.config.compliance.enforce_encryption:
            if table_config.critical:
                # Critical tables must have encryption enabled
                if self.config.s3.encryption.lower() == "none":
                    error_msg = "Encryption is required for critical tables but is set to 'none'"
                    self.logger.error(
                        "Encryption validation failed",
                        database=db_config.name,
                        table=table_config.name,
                        error=error_msg,
                    )
                    await self.audit_trail.log_event(
                        event_type=AuditEventType.ARCHIVE_FAILURE,
                        database_name=db_config.name,
                        table_name=table_config.name,
                        schema_name=table_config.schema_name,
                        status="failed",
                        error_message=error_msg,
                        s3_client=s3_client,
                    )
                    raise ConfigurationError(
                        error_msg,
                        context={
                            "database": db_config.name,
                            "table": table_config.name,
                        },
                    )

        # Log archive start
        archive_start_time = datetime.now(timezone.utc)
        await self.audit_trail.log_event(
            event_type=AuditEventType.ARCHIVE_START,
            database_name=db_config.name,
            table_name=table_config.name,
            schema_name=table_config.schema_name,
            status="started",
            s3_client=s3_client,
        )

        batch_processor = BatchProcessor(db_manager, db_config, table_config, logger=self.logger)

        # Detect table schema (for first batch or schema tracking)
        table_schema = None
        try:
            table_schema = await self.schema_detector.detect_table_schema(
                db_manager=db_manager,
                schema_name=table_config.schema_name,
                table_name=table_config.name,
            )
            self.logger.debug(
                "Table schema detected",
                database=db_config.name,
                table=table_config.name,
                column_count=len(table_schema.get("columns", [])),
            )
        except Exception as e:
            # Log but don't fail - schema detection is informational
            self.logger.warning(
                "Failed to detect table schema (non-critical)",
                database=db_config.name,
                table=table_config.name,
                error=str(e),
            )

        # Count eligible records
        eligible_count = await batch_processor.count_eligible_records()
        self.logger.debug(
            "Records eligible for archival",
            database=db_config.name,
            table=table_config.name,
            count=eligible_count,
        )

        # Load checkpoint for resuming interrupted runs (before progress tracking)
        checkpoint: Optional[Checkpoint] = None
        if not self.dry_run:
            try:
                checkpoint = await self.checkpoint_manager.load_checkpoint(
                    database_name=db_config.name,
                    table_name=table_config.name,
                    s3_client=s3_client,
                )
                if checkpoint:
                    self.logger.debug(
                        "Checkpoint loaded - resuming from interrupted run",
                        database=db_config.name,
                        table=table_config.name,
                        batch_number=checkpoint.batch_number,
                        records_archived=checkpoint.records_archived,
                    )

                    # Clean up orphaned multipart uploads when resuming from checkpoint
                    try:
                        multipart_cleanup = MultipartCleanup(
                            s3_client=s3_client,
                            stale_threshold_hours=1,  # Clean up uploads older than 1 hour
                            logger=self.logger,
                        )
                        cleanup_stats = await multipart_cleanup.cleanup_for_database_table(
                            database_name=db_config.name,
                            table_name=table_config.name,
                            dry_run=False,
                        )
                        if cleanup_stats["total_found"] > 0:
                            self.logger.debug(
                                "Cleaned up orphaned multipart uploads",
                                database=db_config.name,
                                table=table_config.name,
                                aborted=cleanup_stats["aborted"],
                                failed=cleanup_stats["failed"],
                            )
                    except Exception as e:
                        # Log but don't fail - multipart cleanup is non-critical
                        self.logger.warning(
                            "Failed to cleanup orphaned multipart uploads (non-critical)",
                            database=db_config.name,
                            table=table_config.name,
                            error=str(e),
                        )
            except Exception as e:
                # Log but don't fail - checkpoint loading is optional
                self.logger.warning(
                    "Failed to load checkpoint (non-critical)",
                    database=db_config.name,
                    table=table_config.name,
                    error=str(e),
                )

        # Calculate total records for progress tracking
        # When resuming from checkpoint, total = current eligible + already archived
        # This ensures percentage doesn't exceed 100% when resuming
        records_total_for_progress = eligible_count
        initial_records_from_checkpoint = 0
        if checkpoint:
            records_total_for_progress = eligible_count + checkpoint.records_archived
            initial_records_from_checkpoint = checkpoint.records_archived

        # Set eligible records in metrics
        if self.metrics:
            self.metrics.set_records_eligible(
                database=db_config.name,
                table=table_config.name,
                schema=table_config.schema_name,
                count=eligible_count,
            )

        # Start progress tracking
        if self.progress_tracker:
            self.progress_tracker.start(
                database=db_config.name,
                table=table_config.name,
                schema=table_config.schema_name,
                records_total=records_total_for_progress,  # Overall total
                records_total_this_run=eligible_count,  # This run only
                initial_records_processed=initial_records_from_checkpoint,
            )

        # Send notification on archive start (if enabled)
        if self.notification_manager:
            try:
                await self.notification_manager.notify_archive_start(
                    database=db_config.name,
                    table=table_config.name,
                    schema=table_config.schema_name,
                    records_eligible=eligible_count,
                )
            except Exception as e:
                self.logger.warning(
                    "Failed to send archive start notification (non-critical)",
                    database=db_config.name,
                    table=table_config.name,
                    error=str(e),
                )

        if eligible_count == 0 and not checkpoint:
            self.logger.info(
                "No records to archive",
                database=db_config.name,
                table=table_config.name,
            )
            if self.progress_tracker:
                self.progress_tracker.finish(success=True)
            return

        # Load watermark for incremental archival
        watermark = None
        if not self.dry_run:
            try:
                watermark = await self.watermark_manager.load_watermark(
                    database_name=db_config.name,
                    table_name=table_config.name,
                    s3_client=s3_client,
                    db_manager=db_manager,
                )
                if watermark:
                    self.logger.debug(
                        "Watermark loaded for incremental archival",
                        database=db_config.name,
                        table=table_config.name,
                        last_timestamp=watermark.get("last_timestamp"),
                    )
            except Exception as e:
                # Log but don't fail - watermark loading is optional
                self.logger.warning(
                    "Failed to load watermark (non-critical)",
                    database=db_config.name,
                    table=table_config.name,
                    error=str(e),
                )

        # Process batches
        # Use checkpoint if available (takes precedence), otherwise use watermark, otherwise start from beginning
        if checkpoint:
            last_timestamp = checkpoint.last_timestamp
            last_primary_key = checkpoint.last_primary_key
            batch_number = checkpoint.batch_number
            records_archived_so_far = checkpoint.records_archived
            batches_processed_so_far = checkpoint.batches_processed
            self.logger.debug(
                "Resuming from checkpoint",
                database=db_config.name,
                table=table_config.name,
                batch_number=batch_number,
                last_timestamp=last_timestamp,
            )
        elif watermark:
            last_timestamp = watermark.get("last_timestamp")
            last_primary_key = watermark.get("last_primary_key")
            batch_number = 0
            records_archived_so_far = 0
            batches_processed_so_far = 0
        else:
            last_timestamp = None
            last_primary_key = None
            batch_number = 0
            records_archived_so_far = 0
            batches_processed_so_far = 0

        # Track records archived in this run separately
        records_archived_this_run = 0

        is_first_batch = True  # Track first batch for schema inclusion
        s3_key = None  # Track S3 key for notifications

        while True:
            batch_number += 1
            self.logger.debug(
                "Processing batch",
                database=db_config.name,
                table=table_config.name,
                batch=batch_number,
            )

            try:
                # Select batch
                records = await batch_processor.select_batch(
                    batch_size=table_config.batch_size or self.config.defaults.batch_size,
                    last_timestamp=last_timestamp,
                    last_primary_key=last_primary_key,
                )

                if not records:
                    self.logger.debug(
                        "No more records to process",
                        database=db_config.name,
                        table=table_config.name,
                    )
                    # Store totals for summary display
                    # stats["records_archived"] already contains the sum across all tables for this run
                    # records_archived_this_run is for this table only in this run
                    # records_archived_so_far is the overall for this table (including checkpoint)
                    # Accumulate this run's count (stats["records_archived"] is already the sum, but we track separately for clarity)
                    stats["records_archived_this_run"] = (
                        stats.get("records_archived_this_run", 0) + records_archived_this_run
                    )
                    # Use the maximum overall across all tables to show total progress
                    if records_archived_so_far > stats.get("records_archived_total", 0):
                        stats["records_archived_total"] = records_archived_so_far

                    # Delete checkpoint on successful completion
                    if not self.dry_run:
                        try:
                            await self.checkpoint_manager.delete_checkpoint(
                                database_name=db_config.name,
                                table_name=table_config.name,
                                s3_client=s3_client,
                            )
                            self.logger.debug(
                                "Checkpoint deleted after successful completion",
                                database=db_config.name,
                                table=table_config.name,
                            )
                        except Exception as e:
                            # Log but don't fail - checkpoint deletion is non-critical
                            self.logger.warning(
                                "Failed to delete checkpoint (non-critical)",
                                database=db_config.name,
                                table=table_config.name,
                                error=str(e),
                            )

                    # Finish progress tracking
                    if self.progress_tracker:
                        self.progress_tracker.finish(success=True)

                    # Log completion to audit trail (will be logged again at table level, but this is for batch-level tracking)
                    duration = (datetime.now(timezone.utc) - archive_start_time).total_seconds()
                    await self.audit_trail.log_event(
                        event_type=AuditEventType.ARCHIVE_SUCCESS,
                        database_name=db_config.name,
                        table_name=table_config.name,
                        schema_name=table_config.schema_name,
                        record_count=records_archived_so_far,  # Overall count
                        status="success",
                        duration_seconds=duration,
                        s3_client=s3_client,
                    )

                    # Send success notification
                    if self.notification_manager:
                        try:
                            # Build S3 path from last batch's S3 key (if available)
                            s3_path = None
                            if s3_key:
                                s3_path = f"s3://{self.config.s3.bucket}/{s3_key}"
                            await self.notification_manager.notify_archive_success(
                                database=db_config.name,
                                table=table_config.name,
                                schema=table_config.schema_name,
                                records_archived=records_archived_so_far,  # Overall count
                                batches_processed=batches_processed_so_far,
                                duration_seconds=duration,
                                s3_path=s3_path,
                            )
                        except Exception as e:
                            self.logger.warning(
                                "Failed to send success notification (non-critical)",
                                database=db_config.name,
                                table=table_config.name,
                                error=str(e),
                            )

                    break

                # Process batch
                s3_key = await self._process_batch(
                    db_manager,
                    s3_client,
                    batch_processor,
                    db_config,
                    table_config,
                    records,
                    batch_number,
                    stats,
                    table_schema=(
                        table_schema if is_first_batch else None
                    ),  # Pass schema for first batch
                )

                # Update cursor for next batch
                record_dicts = batch_processor.records_to_dicts(records)
                last_timestamp, last_primary_key = batch_processor.get_last_cursor(record_dicts)

                stats["batches_processed"] += 1
                stats["records_archived"] += len(records)
                records_archived_so_far += len(records)
                records_archived_this_run += len(records)  # Track this run separately
                batches_processed_so_far += 1

                is_first_batch = False  # Mark that first batch is done

                # Update per-database stats if provided
                if db_stats is not None:
                    db_stats["batches_processed"] += 1
                    db_stats["records_archived"] += len(records)

                # Update progress tracker with both current run and overall
                if self.progress_tracker:
                    self.progress_tracker.update(
                        records_processed=records_archived_so_far,  # Overall (includes checkpoint)
                        batches_completed=batches_processed_so_far,
                        batches_total=None,  # We don't know total batches ahead of time
                        records_processed_this_run=records_archived_this_run,  # This run only
                    )

                # Update metrics batch progress
                if self.metrics and eligible_count > 0:
                    progress = records_archived_so_far / eligible_count
                    self.metrics.set_batch_progress(
                        database=db_config.name,
                        table=table_config.name,
                        schema=table_config.schema_name,
                        progress=progress,
                    )

                # Save checkpoint every N batches (for resume capability)
                if not self.dry_run and self.checkpoint_manager.should_save_checkpoint(
                    batch_number
                ):
                    try:
                        checkpoint = Checkpoint(
                            database_name=db_config.name,
                            table_name=table_config.name,
                            schema_name=table_config.schema_name,
                            batch_number=batch_number,
                            last_timestamp=last_timestamp,
                            last_primary_key=last_primary_key,
                            records_archived=records_archived_so_far,
                            batches_processed=batches_processed_so_far,
                            checkpoint_time=datetime.now(timezone.utc),
                            batch_id=record_dicts[-1].get("_batch_id") if record_dicts else None,
                        )
                        await self.checkpoint_manager.save_checkpoint(
                            checkpoint=checkpoint,
                            s3_client=s3_client,
                        )
                        self.logger.debug(
                            "Checkpoint saved",
                            database=db_config.name,
                            table=table_config.name,
                            batch_number=batch_number,
                        )
                    except Exception as e:
                        # Log but don't fail - checkpoint save is non-critical
                        self.logger.warning(
                            "Failed to save checkpoint (non-critical)",
                            database=db_config.name,
                            table=table_config.name,
                            batch_number=batch_number,
                            error=str(e),
                        )

                # Update watermark after successful batch (for incremental archival)
                if not self.dry_run and record_dicts:
                    # Get the last record's timestamp and primary key
                    last_record = record_dicts[-1]
                    new_last_timestamp = last_record.get(table_config.timestamp_column)
                    new_last_primary_key = last_record.get(table_config.primary_key)

                    # Ensure timestamp is datetime
                    if isinstance(new_last_timestamp, str):
                        try:
                            new_last_timestamp = datetime.fromisoformat(
                                new_last_timestamp.replace("Z", "+00:00")
                            )
                        except Exception:
                            self.logger.warning(
                                "Could not parse timestamp for watermark",
                                timestamp=new_last_timestamp,
                            )
                            new_last_timestamp = None

                    if new_last_timestamp and new_last_primary_key is not None:
                        try:
                            await self.watermark_manager.save_watermark(
                                database_name=db_config.name,
                                table_name=table_config.name,
                                last_timestamp=new_last_timestamp,
                                last_primary_key=new_last_primary_key,
                                s3_client=s3_client,
                                db_manager=db_manager,
                            )
                            self.logger.debug(
                                "Watermark updated",
                                database=db_config.name,
                                table=table_config.name,
                                timestamp=new_last_timestamp,
                            )
                        except Exception as e:
                            # Log but don't fail - watermark update is non-critical
                            self.logger.warning(
                                "Failed to update watermark (non-critical)",
                                database=db_config.name,
                                table=table_config.name,
                                error=str(e),
                            )

            except Exception as e:
                self.logger.error(
                    "Batch processing failed",
                    database=db_config.name,
                    table=table_config.name,
                    batch=batch_number,
                    error=str(e),
                    exc_info=True,
                )
                raise

    async def _process_batch(
        self,
        db_manager: DatabaseManager,
        s3_client: S3Client,
        batch_processor: BatchProcessor,
        db_config: DatabaseConfig,
        table_config: TableConfig,
        records: list,
        batch_number: int,
        stats: dict[str, Any],
        table_schema: Optional[dict[str, Any]] = None,
    ) -> Optional[str]:
        """Process a single batch following verify-then-delete pattern.

        Pattern: FETCH → UPLOAD → VERIFY → DELETE → COMMIT

        Args:
            db_manager: Database manager
            s3_client: S3 client
            batch_processor: Batch processor
            db_config: Database configuration
            table_config: Table configuration
            records: Records to process
            batch_number: Batch number
            stats: Statistics dictionary
        """
        # Generate batch ID
        batch_id = self._generate_batch_id(db_config.name, table_config.name, batch_number)

        # Convert records to dictionaries
        record_dicts = batch_processor.records_to_dicts(records)
        memory_count = len(record_dicts)

        # Extract primary keys for verification
        primary_keys = batch_processor.extract_primary_keys(record_dicts)

        # Count records in database that match this batch's primary keys
        # This is the actual count we're about to delete
        pk_col = safe_identifier(table_config.primary_key)
        schema = safe_identifier(table_config.schema_name)
        table = safe_identifier(table_config.name)

        # Count records with these primary keys (for verification)
        count_query = f"""
            SELECT COUNT(*)
            FROM {schema}.{table}
            WHERE {pk_col} = ANY($1)
        """
        db_count = await db_manager.fetchval(count_query, primary_keys)

        # Serialize to JSONL
        archived_at = datetime.now(timezone.utc)
        serialized_rows = [
            self.serializer.serialize_row(
                row=row,
                batch_id=batch_id,
                database_name=db_config.name,
                table_name=table_config.name,
                archived_at=archived_at,
            )
            for row in record_dicts
        ]

        jsonl_data = self.serializer.to_jsonl(serialized_rows)
        s3_count = self.serializer.count_jsonl_lines(jsonl_data)

        # Record serialize duration
        if self.metrics:
            self.metrics.stop_phase_timer(
                database=db_config.name,
                table=table_config.name,
                schema=table_config.schema_name,
                phase="serialize",
            )

        # Calculate checksum of JSONL data (before compression)
        jsonl_checksum = self.checksum_calculator.calculate_sha256(jsonl_data)
        self.logger.debug(
            "JSONL checksum calculated",
            checksum=jsonl_checksum,
            database=db_config.name,
            table=table_config.name,
            batch=batch_number,
        )

        # Compress
        if self.metrics:
            self.metrics.start_phase_timer("compress")
        compressed_data, uncompressed_size, compressed_size = self.compressor.compress(jsonl_data)
        if self.metrics:
            self.metrics.stop_phase_timer(
                database=db_config.name,
                table=table_config.name,
                schema=table_config.schema_name,
                phase="compress",
            )

        # Calculate checksum of compressed data
        compressed_checksum = self.checksum_calculator.calculate_sha256(compressed_data)
        self.logger.debug(
            "Compressed data checksum calculated",
            checksum=compressed_checksum,
            database=db_config.name,
            table=table_config.name,
            batch=batch_number,
        )

        # Get timestamp range for metadata
        timestamp_range = batch_processor.get_timestamp_range(record_dicts)

        # Generate metadata
        metadata = self.metadata_generator.generate_batch_metadata(
            database_name=db_config.name,
            table_name=table_config.name,
            schema_name=table_config.schema_name,
            batch_number=batch_number,
            batch_id=batch_id,
            record_count=memory_count,
            jsonl_checksum=jsonl_checksum,
            compressed_checksum=compressed_checksum,
            uncompressed_size=uncompressed_size,
            compressed_size=compressed_size,
            primary_keys=primary_keys,
            timestamp_range=timestamp_range,
            archived_at=archived_at,
            table_schema=table_schema,  # Will be None after first batch
        )
        metadata_json = self.metadata_generator.metadata_to_json(metadata)

        # Upload to S3 (if not dry run)
        s3_key = None
        metadata_key = None
        if not self.dry_run:
            # Generate S3 key
            date_partition = archived_at.strftime("year=%Y/month=%m/day=%d")
            filename = (
                f"{table_config.name}_{archived_at.strftime('%Y%m%dT%H%M%SZ')}_"
                f"batch_{batch_number:03d}.jsonl.gz"
            )
            # Build S3 key with prefix if configured
            if self.config.s3.prefix:
                prefix = self.config.s3.prefix.rstrip("/")
                s3_key = (
                    f"{prefix}/{db_config.name}/{table_config.name}/{date_partition}/{filename}"
                )
            else:
                s3_key = f"{db_config.name}/{table_config.name}/{date_partition}/{filename}"

            # Upload data file to temporary file first, then to S3
            with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
                tmp_path = Path(tmp_file.name)
                tmp_path.write_bytes(compressed_data)

            # Close the file handle before uploading (fixes Windows file locking issue)
            try:
                if self.metrics:
                    self.metrics.start_phase_timer("upload")
                _ = s3_client.upload_file(tmp_path, s3_key)
                if self.metrics:
                    self.metrics.stop_phase_timer(
                        database=db_config.name,
                        table=table_config.name,
                        schema=table_config.schema_name,
                        phase="upload",
                    )
                self.logger.debug(
                    "File uploaded to S3",
                    bucket=self.config.s3.bucket,
                    key=s3_key,
                    size=compressed_size,
                )
            finally:
                # Delete temp file after upload completes
                try:
                    tmp_path.unlink()
                except Exception as e:
                    self.logger.warning(
                        "Failed to delete temporary file",
                        path=str(tmp_path),
                        error=str(e),
                    )

            # Upload metadata file
            metadata_filename = filename.replace(".jsonl.gz", ".metadata.json")
            # Build metadata key with same prefix as data file
            if self.config.s3.prefix:
                prefix = self.config.s3.prefix.rstrip("/")
                metadata_key = f"{prefix}/{db_config.name}/{table_config.name}/{date_partition}/{metadata_filename}"
            else:
                metadata_key = (
                    f"{db_config.name}/{table_config.name}/{date_partition}/{metadata_filename}"
                )

            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, suffix=".json", encoding="utf-8"
            ) as tmp_meta_file:
                tmp_meta_path = Path(tmp_meta_file.name)
                tmp_meta_file.write(metadata_json)

            try:
                s3_client.upload_file(tmp_meta_path, metadata_key)
                self.logger.debug(
                    "Metadata file uploaded to S3",
                    bucket=self.config.s3.bucket,
                    key=metadata_key,
                )
            finally:
                try:
                    tmp_meta_path.unlink()
                except Exception as e:
                    self.logger.warning(
                        "Failed to delete temporary metadata file",
                        path=str(tmp_meta_path),
                        error=str(e),
                    )

        # Verify counts
        if self.metrics:
            self.metrics.start_phase_timer("verify")
        self.verifier.verify_counts(
            db_count=db_count,
            memory_count=memory_count,
            s3_count=s3_count,
            context={
                "database": db_config.name,
                "table": table_config.name,
                "batch": batch_number,
            },
        )
        if self.metrics:
            self.metrics.stop_phase_timer(
                database=db_config.name,
                table=table_config.name,
                schema=table_config.schema_name,
                phase="verify",
            )

        # Delete from database (if not dry run)
        # primary_keys already extracted above
        if not self.dry_run:
            # Use transaction for safety
            if self.metrics:
                self.metrics.start_phase_timer("delete")
            async with db_manager.transaction() as conn:
                # Delete records
                pk_col = safe_identifier(table_config.primary_key)
                schema = safe_identifier(table_config.schema_name)
                table = safe_identifier(table_config.name)

                # Build delete query - handle different PK types
                # For MVP, assume integer types. In Phase 2, detect type dynamically
                delete_query = f"""
                    DELETE FROM {schema}.{table}
                    WHERE {pk_col} = ANY($1)
                """

                # Execute delete
                result = await conn.execute(delete_query, primary_keys)

                # Parse deleted count from result (format: "DELETE <count>")
                deleted_count = 0
                if result and result.startswith("DELETE"):
                    try:
                        deleted_count = int(result.split()[-1])
                    except (ValueError, IndexError):
                        pass

                if deleted_count != len(primary_keys):
                    raise VerificationError(
                        f"Delete count mismatch: expected {len(primary_keys)}, got {deleted_count}",
                        context={
                            "database": db_config.name,
                            "table": table_config.name,
                            "batch": batch_number,
                        },
                    )

                self.logger.debug(
                    "Records deleted",
                    database=db_config.name,
                    table=table_config.name,
                    count=deleted_count,
                )
            if self.metrics:
                self.metrics.stop_phase_timer(
                    database=db_config.name,
                    table=table_config.name,
                    schema=table_config.schema_name,
                    phase="delete",
                )

            # Record archived records and bytes in metrics
            if self.metrics:
                self.metrics.record_archived(
                    database=db_config.name,
                    table=table_config.name,
                    schema=table_config.schema_name,
                    count=memory_count,
                    bytes_uploaded=compressed_size,
                )

                # Record batch processing (duration is already recorded per phase)
                # We can calculate total batch duration from the phase durations
                # For now, we'll use a simple approach: record batch with estimated duration
                # The actual duration per phase is already recorded in histograms
                # Note: We don't have a single "total batch duration" metric, but we have per-phase durations
                # which is more useful for analysis

                # Verify primary keys
                self.verifier.verify_primary_keys(
                    fetched_pks=primary_keys,
                    delete_pks=primary_keys,
                    context={
                        "database": db_config.name,
                        "table": table_config.name,
                        "batch": batch_number,
                    },
                )

            # Generate deletion manifest (outside transaction, but still in dry_run check)
            deleted_at = datetime.now(timezone.utc)
            manifest = self.manifest_generator.generate_manifest(
                database_name=db_config.name,
                table_name=table_config.name,
                schema_name=table_config.schema_name,
                batch_number=batch_number,
                batch_id=batch_id,
                primary_key_column=table_config.primary_key,
                primary_keys=primary_keys,
                deleted_count=deleted_count,
                deleted_at=deleted_at,
            )
            manifest_json = self.manifest_generator.manifest_to_json(manifest)

            # Upload deletion manifest to S3
            manifest_filename = filename.replace(".jsonl.gz", ".manifest.json")
            if self.config.s3.prefix:
                prefix = self.config.s3.prefix.rstrip("/")
                manifest_key = f"{prefix}/{db_config.name}/{table_config.name}/{date_partition}/{manifest_filename}"
            else:
                manifest_key = (
                    f"{db_config.name}/{table_config.name}/{date_partition}/{manifest_filename}"
                )

            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, suffix=".json", encoding="utf-8"
            ) as tmp_manifest_file:
                tmp_manifest_path = Path(tmp_manifest_file.name)
                tmp_manifest_file.write(manifest_json)

            try:
                s3_client.upload_file(tmp_manifest_path, manifest_key)
                self.logger.debug(
                    "Deletion manifest uploaded to S3",
                    bucket=self.config.s3.bucket,
                    key=manifest_key,
                )
            finally:
                try:
                    tmp_manifest_path.unlink()
                except Exception as e:
                    self.logger.warning(
                        "Failed to delete temporary manifest file",
                        path=str(tmp_manifest_path),
                        error=str(e),
                    )

            # Perform sample verification (after deletion)
            # Select random samples from archived records
            sample_pks = self.sample_verifier.select_samples(
                records=record_dicts,
                primary_key_column=table_config.primary_key,
            )

            if sample_pks:
                try:
                    # Download the archived file from S3 to verify samples
                    s3_data = s3_client.get_object_bytes(s3_key)

                    # Extract sample records from S3 data
                    _ = self.sample_verifier.extract_samples_from_s3(
                        s3_data=s3_data,
                        primary_key_column=table_config.primary_key,
                        sample_pks=sample_pks,
                    )

                    # Verify samples are not in database (should be deleted)
                    await self.sample_verifier.verify_samples_not_in_database(
                        db_manager=db_manager,
                        table_schema=table_config.schema_name,
                        table_name=table_config.name,
                        primary_key_column=table_config.primary_key,
                        sample_pks=sample_pks,
                    )

                    self.logger.debug(
                        "Sample verification completed",
                        database=db_config.name,
                        table=table_config.name,
                        batch=batch_number,
                        sample_count=len(sample_pks),
                    )

                except Exception as e:
                    # Log but don't fail the batch if sample verification fails
                    # This is a safety check, not a critical operation
                    self.logger.warning(
                        "Sample verification failed (non-critical)",
                        database=db_config.name,
                        table=table_config.name,
                        batch=batch_number,
                        error=str(e),
                        exc_info=True,
                    )

        else:
            self.logger.debug(
                "DRY RUN: Would delete records",
                database=db_config.name,
                table=table_config.name,
                count=len(primary_keys),
            )

        self.logger.debug(
            "Batch processed successfully",
            database=db_config.name,
            table=table_config.name,
            batch=batch_number,
            records=len(records),
        )

        # Return S3 key for tracking at table level
        return s3_key

    def _generate_batch_id(self, database: str, table: str, batch_number: int) -> str:
        """Generate deterministic batch ID.

        Args:
            database: Database name
            table: Table name
            batch_number: Batch number

        Returns:
            Batch ID (SHA-256 hash)
        """
        # For MVP, use simple hash
        # In Phase 2, will include timestamp range
        content = f"{database}_{table}_{batch_number}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    async def _load_previous_schema(
        self,
        s3_client: S3Client,
        database_name: str,
        table_name: str,
    ) -> Optional[dict[str, Any]]:
        """Load previous schema from most recent metadata file in S3.

        Args:
            s3_client: S3 client
            database_name: Database name
            table_name: Table name

        Returns:
            Previous schema dictionary or None if not found
        """
        try:
            # Search for metadata files for this table
            # Pattern: {prefix}/{database}/{table}/**/*.metadata.json
            search_prefix = f"{database_name}/{table_name}/"
            objects = s3_client.list_objects(prefix=search_prefix)

            # Filter for metadata files
            metadata_files = [obj for obj in objects if obj["key"].endswith(".metadata.json")]

            if not metadata_files:
                self.logger.debug(
                    "No previous metadata files found",
                    database=database_name,
                    table=table_name,
                )
                return None

            # Sort by last_modified (most recent first)
            metadata_files.sort(key=lambda x: x["last_modified"], reverse=True)

            # Load the most recent metadata file
            most_recent_key = metadata_files[0]["key"]
            metadata_json = s3_client.get_object_bytes(most_recent_key)
            metadata_str = metadata_json.decode("utf-8")

            metadata = self.metadata_generator.metadata_from_json(metadata_str)

            # Extract schema from metadata
            previous_schema = metadata.get("table_schema")
            if previous_schema:
                self.logger.debug(
                    "Previous schema loaded from metadata",
                    database=database_name,
                    table=table_name,
                    metadata_key=most_recent_key,
                )

            return previous_schema

        except Exception as e:
            # Log but don't fail - previous schema loading is optional
            self.logger.warning(
                "Failed to load previous schema (non-critical)",
                database=database_name,
                table=table_name,
                error=str(e),
            )
            return None
