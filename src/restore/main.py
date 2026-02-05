"""CLI entry point for restore utility."""

import asyncio
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import click
import structlog

from archiver.config import ArchiverConfig, DatabaseConfig, load_config
from archiver.database import DatabaseManager
from archiver.s3_client import S3Client
from restore.restore_engine import RestoreEngine
from restore.restore_watermark import RestoreWatermarkManager
from restore.s3_reader import S3ArchiveReader
from utils.logging import configure_logging, get_logger


@click.command()
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file",
)
@click.option(
    "--s3-key",
    type=str,
    help="S3 key of archive file to restore (e.g., archives/db/table/year=2026/month=01/day=04/file.jsonl.gz). If not provided, will restore all batches for the table.",
)
@click.option(
    "--restore-all",
    is_flag=True,
    help="Restore all batches for the specified database/table (ignores --s3-key if set)",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Start date (YYYY-MM-DD) for date-filtered restore (only with --restore-all)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="End date (YYYY-MM-DD) for date-filtered restore (only with --restore-all)",
)
@click.option(
    "--database",
    type=str,
    help="Database name (overrides config)",
)
@click.option(
    "--table",
    type=str,
    help="Table name (overrides config)",
)
@click.option(
    "--schema",
    type=str,
    default="public",
    help="Schema name (default: public)",
)
@click.option(
    "--conflict-strategy",
    type=click.Choice(["skip", "overwrite", "fail", "upsert"], case_sensitive=False),
    default="skip",
    help="Conflict resolution strategy (default: skip)",
)
@click.option(
    "--schema-migration-strategy",
    type=click.Choice(["strict", "lenient", "transform", "none"], case_sensitive=False),
    default="lenient",
    help="Schema migration strategy (default: lenient)",
)
@click.option(
    "--batch-size",
    type=int,
    default=1000,
    help="Batch size for restore (default: 1000)",
)
@click.option(
    "--drop-indexes",
    is_flag=True,
    help="Temporarily drop indexes before restore for better performance",
)
@click.option(
    "--commit-frequency",
    type=int,
    default=1,
    help="Commit every N batches (default: 1)",
)
@click.option(
    "--no-validate-checksum",
    is_flag=True,
    help="Skip checksum validation",
)
@click.option(
    "--no-detect-conflicts",
    is_flag=True,
    help="Skip conflict detection",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Dry run mode (don't actually restore)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Verbose output",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    help="Log level (default: INFO)",
)
@click.option(
    "--log-format",
    default="console",
    type=click.Choice(["console", "json"], case_sensitive=False),
    help="Log format: 'console' for human-readable output, 'json' for structured logs (default: console)",
)
@click.option(
    "--ignore-watermark",
    is_flag=True,
    help="Ignore restore watermark and restore all archives (even if already restored)",
)
def main(
    config: Optional[Path],
    s3_key: Optional[str],
    database: Optional[str],
    table: Optional[str],
    schema: str,
    conflict_strategy: str,
    schema_migration_strategy: str,
    batch_size: int,
    drop_indexes: bool,
    commit_frequency: int,
    no_validate_checksum: bool,
    no_detect_conflicts: bool,
    dry_run: bool,
    verbose: bool,
    log_level: str,
    restore_all: bool,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    ignore_watermark: bool,
    log_format: str = "console",
) -> None:
    """Restore archived data from S3 to PostgreSQL.

    Examples:

    \b
    # Restore from specific S3 key
    restore --config config.yaml --s3-key archives/db/table/year=2026/month=01/day=04/file.jsonl.gz

    \b
    # Restore with overwrite strategy
    restore --config config.yaml --s3-key archives/db/table/year=2026/month=01/day=04/file.jsonl.gz \\
        --conflict-strategy overwrite

    \b
    # Dry run to see what would be restored
    restore --config config.yaml --s3-key archives/db/table/year=2026/month=01/day=04/file.jsonl.gz \\
        --dry-run
    """
    # Configure logging
    # Verbose mode automatically enables DEBUG level for more detailed output
    effective_log_level = "DEBUG" if verbose else log_level

    # For list command, suppress logs for cleaner output
    is_list_command = not s3_key and not restore_all and database and table
    if is_list_command:
        # For list command, suppress most logs (unless verbose is explicitly set)
        if verbose:
            log_level_override = "DEBUG"
        else:
            log_level_override = "WARNING"  # Only show warnings/errors
        log_format_override = "console"  # Always use console for list command
    else:
        log_level_override = effective_log_level.upper()
        log_format_override = log_format  # Use provided format (default: console)

    configure_logging(
        log_level=log_level_override,
        log_format=log_format_override,
    )

    # Suppress noisy third-party library logs in verbose mode
    # Keep boto3/botocore at WARNING level to reduce noise while keeping our DEBUG logs
    if verbose:
        import logging as std_logging
        std_logging.getLogger("boto3").setLevel(std_logging.WARNING)
        std_logging.getLogger("botocore").setLevel(std_logging.WARNING)
        std_logging.getLogger("urllib3").setLevel(std_logging.WARNING)

    logger = get_logger("restore_cli")

    try:
        # Load configuration
        if not config:
            logger.error("Configuration file is required")
            sys.exit(1)

        archiver_config = load_config(config)
        logger.debug("Configuration loaded", config_file=str(config))

        # Handle list command
        if not s3_key and not restore_all:
            # List available archives
            if database and table:
                logger.debug("Listing available archives", database=database, table=table)
                asyncio.run(_list_archives(archiver_config, database, table, logger))
                sys.exit(0)
            else:
                logger.error("S3 key is required (--s3-key) or use --restore-all with --database and --table to restore all batches")
                sys.exit(1)

        # Determine database and table from S3 key or options
        if not database or not table:
            if s3_key:
                # Try to extract from S3 key path
                # Format: {prefix}/{database}/{table}/year=YYYY/month=MM/day=DD/{file}
                parts = s3_key.split("/")
                if len(parts) >= 3:
                    # Skip prefix if present
                    prefix = archiver_config.s3.prefix.rstrip("/") if archiver_config.s3.prefix else ""
                    if prefix and s3_key.startswith(prefix):
                        # Remove prefix
                        relative_key = s3_key[len(prefix) + 1 :] if s3_key.startswith(prefix + "/") else s3_key[len(prefix):]
                        parts = relative_key.split("/")

                    if not database and len(parts) >= 1:
                        database = parts[0]
                    if not table and len(parts) >= 2:
                        table = parts[1]

        if not database or not table:
            logger.error(
                "Database and table must be specified (--database, --table) or extractable from S3 key",
                s3_key=s3_key,
                restore_all=restore_all,
            )
            sys.exit(1)

        logger.debug(
            "Restore parameters",
            s3_key=s3_key if not restore_all else None,
            database=database,
            table=table,
            schema=schema,
            conflict_strategy=conflict_strategy,
            restore_all=restore_all,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
            dry_run=dry_run,
            ignore_watermark=ignore_watermark,
        )
        if ignore_watermark:
            logger.warning(
                "Watermark ignored - all archives will be restored regardless of previous restore status",
                database=database,
                table=table,
            )

        # Run restore
        if restore_all or not s3_key:
            # Restore all batches for the table
            asyncio.run(
                _restore_all(
                    archiver_config=archiver_config,
                    database_name=database,
                    table_name=table,
                    schema_name=schema,
                    conflict_strategy=conflict_strategy,
                    batch_size=batch_size,
                    drop_indexes=drop_indexes,
                    commit_frequency=commit_frequency,
                    validate_checksum=not no_validate_checksum,
                    dry_run=dry_run,
                    schema_migration_strategy=schema_migration_strategy,
                    detect_conflicts=not no_detect_conflicts,
                    start_date=start_date,
                    end_date=end_date,
                    logger=logger,
                    verbose=verbose,
                    ignore_watermark=ignore_watermark,
                )
            )
        else:
            # Restore single file
            asyncio.run(
                _restore(
                    archiver_config=archiver_config,
                    s3_key=s3_key,
                    database_name=database,
                    table_name=table,
                    schema_name=schema,
                    conflict_strategy=conflict_strategy,
                    batch_size=batch_size,
                    drop_indexes=drop_indexes,
                    commit_frequency=commit_frequency,
                    validate_checksum=not no_validate_checksum,
                    dry_run=dry_run,
                    schema_migration_strategy=schema_migration_strategy,
                    detect_conflicts=not no_detect_conflicts,
                    logger=logger,
                )
            )

    except KeyboardInterrupt:
        logger.warning("Restore interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error("Restore failed", error=str(e), exc_info=True)
        sys.exit(1)


async def _list_archives(
    archiver_config: ArchiverConfig,
    database_name: str,
    table_name: str,
    logger: structlog.BoundLogger,
) -> None:
    """List available archives in S3.

    Args:
        archiver_config: Archiver configuration
        database_name: Database name
        table_name: Table name
        logger: Logger instance
    """
    s3_reader = S3ArchiveReader(archiver_config.s3, logger=logger)
    archives = await s3_reader.list_archives(
        database_name=database_name,
        table_name=table_name,
    )

    # Suppress JSON logs for cleaner output (already using console format)
    if archives:
        # Format output nicely
        click.echo()
        click.echo(click.style("=" * 70, fg="cyan"))
        click.echo(click.style(f"Available Archives: {database_name}.{table_name}", fg="cyan", bold=True))
        click.echo(click.style("=" * 70, fg="cyan"))
        click.echo()
        click.echo(click.style(f"Found {len(archives)} archive(s):", fg="green", bold=True))
        click.echo()

        # Group archives by date for better readability
        archives_by_date: dict[str, list[str]] = defaultdict(list)
        for archive_key in sorted(archives):
            # Try to extract date from key
            date_str = "Unknown"
            if "year=" in archive_key:
                try:
                    # Extract year=YYYY/month=MM/day=DD
                    match = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", archive_key)
                    if match:
                        date_str = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
                except Exception:
                    pass
            archives_by_date[date_str].append(archive_key)

        # Display grouped by date
        for date_str in sorted(archives_by_date.keys()):
            if date_str != "Unknown":
                click.echo(click.style(f"  {date_str}:", fg="yellow", bold=True))
            for archive_key in archives_by_date[date_str]:
                # Extract just the filename for cleaner display
                filename = archive_key.split("/")[-1] if "/" in archive_key else archive_key
                click.echo(f"    • {filename}")
                # Show full path in dim text if verbose
                if len(archive_key) > 80:
                    click.echo(click.style(f"      {archive_key[:77]}...", dim=True))

        click.echo()
        click.echo(click.style("-" * 70, dim=True))
        click.echo()
        click.echo(click.style("Quick Commands:", fg="cyan", bold=True))
        click.echo()
        click.echo(click.style("  Restore all batches:", fg="white"))
        click.echo(
            click.style(
                f"    python -m restore.main --config config.yaml --restore-all --database {database_name} --table {table_name}",
                fg="green",
            )
        )
        click.echo()
        click.echo(click.style("  Restore a specific file:", fg="white"))
        click.echo(
            click.style(
                f"    python -m restore.main --config config.yaml --s3-key \"<key>\" --database {database_name} --table {table_name}",
                fg="green",
            )
        )
        click.echo()
    else:
        click.echo()
        click.echo(click.style("=" * 70, fg="yellow"))
        click.echo(click.style("No Archives Found", fg="yellow", bold=True))
        click.echo(click.style("=" * 70, fg="yellow"))
        click.echo()
        click.echo(click.style("Database: ", fg="white") + click.style(database_name, fg="cyan", bold=True))
        click.echo(click.style("Table: ", fg="white") + click.style(table_name, fg="cyan", bold=True))
        click.echo()
        click.echo(click.style("No archived files found for this table.", dim=True))
        click.echo(click.style("Make sure archives exist in S3 and the path is correct.", dim=True))
        click.echo()


async def _restore(
    archiver_config: ArchiverConfig,
    s3_key: str,
    database_name: str,
    table_name: str,
    schema_name: str,
    conflict_strategy: str,
    batch_size: int,
    drop_indexes: bool,
    commit_frequency: int,
    validate_checksum: bool,
    dry_run: bool,
    schema_migration_strategy: str,
    detect_conflicts: bool,
    logger: structlog.BoundLogger,
) -> None:
    """Perform restore operation.

    Args:
        archiver_config: Archiver configuration
        s3_key: S3 key of archive file
        database_name: Database name
        table_name: Table name
        schema_name: Schema name
        conflict_strategy: Conflict resolution strategy
        batch_size: Batch size
        drop_indexes: Whether to drop indexes
        commit_frequency: Commit frequency
        validate_checksum: Whether to validate checksums
        dry_run: Whether this is a dry run
        logger: Logger instance
    """
    # Find database config
    db_config: Optional[DatabaseConfig] = None
    for db in archiver_config.databases:
        if db.name == database_name:
            db_config = db
            break

    if not db_config:
        error_msg = f"Database '{database_name}' not found in configuration"
        logger.error(error_msg, database=database_name)
        raise ValueError(error_msg)

    # Initialize S3 reader
    s3_reader = S3ArchiveReader(archiver_config.s3, logger=logger)

    # Read archive
    logger.debug("Reading archive from S3", s3_key=s3_key)
    archive = await s3_reader.read_archive(s3_key, validate_checksum=validate_checksum)
    logger.debug(
        "Archive read",
        records=archive.record_count,
        database=archive.database_name,
        table=archive.table_name,
    )

    if dry_run:
        records = archive.parse_records()
        logger.debug(
            "DRY RUN: Would restore",
            records=len(records),
            database=database_name,
            table=table_name,
            schema=schema_name,
        )
        if records:
            logger.debug("Sample record columns", columns=list(records[0].keys())[:10])
        return

    # Initialize database manager
    db_manager = DatabaseManager(db_config, logger=logger)
    await db_manager.connect()

    try:
        # Initialize restore engine
        restore_engine = RestoreEngine(db_manager, logger=logger)

        # Perform restore
        stats = await restore_engine.restore_archive(
            archive=archive,
            conflict_strategy=conflict_strategy,
            batch_size=batch_size,
            drop_indexes=drop_indexes,
            commit_frequency=commit_frequency,
            dry_run=False,
            schema_migration_strategy=schema_migration_strategy,
            detect_conflicts=detect_conflicts,
            table_name=table_name,
            schema_name=schema_name,
        )

        logger.debug(
            "Restore completed",
            **stats,
            database=database_name,
            table=table_name,
            schema=schema_name,
        )

    finally:
        await db_manager.disconnect()


async def _restore_all(
    archiver_config: ArchiverConfig,
    database_name: str,
    table_name: str,
    schema_name: str,
    conflict_strategy: str,
    batch_size: int,
    drop_indexes: bool,
    commit_frequency: int,
    validate_checksum: bool,
    dry_run: bool,
    schema_migration_strategy: str,
    detect_conflicts: bool,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    logger: structlog.BoundLogger,
    verbose: bool = False,
    ignore_watermark: bool = False,
) -> None:
    """Restore all batches for a table.

    Args:
        archiver_config: Archiver configuration
        database_name: Database name
        table_name: Table name
        schema_name: Schema name
        conflict_strategy: Conflict resolution strategy
        batch_size: Batch size for restore operations
        drop_indexes: Whether to drop indexes
        commit_frequency: Commit frequency
        validate_checksum: Whether to validate checksums
        dry_run: Whether this is a dry run
        schema_migration_strategy: Schema migration strategy
        detect_conflicts: Whether to detect conflicts
        start_date: Optional start date filter
        end_date: Optional end date filter
        logger: Logger instance
    """
    # Find database config
    db_config: Optional[DatabaseConfig] = None
    for db in archiver_config.databases:
        if db.name == database_name:
            db_config = db
            break

    if not db_config:
        error_msg = f"Database '{database_name}' not found in configuration"
        logger.error(error_msg, database=database_name)
        raise ValueError(error_msg)

    # Initialize S3 reader and client
    s3_reader = S3ArchiveReader(archiver_config.s3, logger=logger)
    s3_client = S3Client(archiver_config.s3, logger=logger)

    # Initialize restore watermark manager if enabled
    # Note: We initialize it even when ignoring watermark, so we can still update it at the end
    restore_watermark_manager: Optional[RestoreWatermarkManager] = None
    watermark = None
    if archiver_config.restore_watermark and archiver_config.restore_watermark.enabled:
        restore_watermark_manager = RestoreWatermarkManager(
            storage_type=archiver_config.restore_watermark.storage_type,
            logger=logger,
        )
        # Only load existing watermark if not ignoring it (for filtering purposes)
        if not ignore_watermark:
            try:
                watermark = await restore_watermark_manager.load_watermark(
                    database_name=database_name,
                    table_name=table_name,
                    s3_client=s3_client,
                    db_manager=None,  # Will be set later if database storage is needed
                )
                if watermark:
                    logger.debug(
                        "Restore watermark loaded",
                        database=database_name,
                        table=table_name,
                        last_restored_date=watermark.last_restored_date.isoformat(),
                        total_archives_restored=watermark.total_archives_restored,
                    )
            except Exception as e:
                logger.warning(
                    "Failed to load restore watermark, will process all archives",
                    database=database_name,
                    table=table_name,
                    error=str(e),
                )
        else:
            logger.debug(
                "Watermark loading skipped (--ignore-watermark flag set)",
                database=database_name,
                table=table_name,
            )

    # List all archives for this table
    logger.debug(
        "Listing all archives for restore",
        database=database_name,
        table=table_name,
        start_date=start_date.isoformat() if start_date else None,
        end_date=end_date.isoformat() if end_date else None,
    )
    all_archive_keys = await s3_reader.list_archives(
        database_name=database_name,
        table_name=table_name,
        start_date=start_date,
        end_date=end_date,
    )

    # Filter out already-restored archives if watermark is enabled and not ignoring watermark
    if restore_watermark_manager and watermark and not ignore_watermark:
        archive_keys = [
            key
            for key in all_archive_keys
            if restore_watermark_manager.should_restore_archive(key, watermark)
        ]
        skipped_count = len(all_archive_keys) - len(archive_keys)
        if skipped_count > 0:
            logger.debug(
                "Filtered already-restored archives",
                database=database_name,
                table=table_name,
                total_archives=len(all_archive_keys),
                archives_to_restore=len(archive_keys),
                archives_skipped=skipped_count,
                last_restored_date=watermark.last_restored_date.isoformat(),
            )
    else:
        archive_keys = all_archive_keys

    if not archive_keys:
        logger.warning("No archives found to restore", database=database_name, table=table_name)
        if restore_watermark_manager and watermark and not ignore_watermark:
            click.echo(f"\nNo new archives to restore for database '{database_name}', table '{table_name}'")
            click.echo(f"Last restored: {watermark.last_restored_date.isoformat()}")
            click.echo(f"Total archives already restored: {watermark.total_archives_restored}")
            click.echo("\nTip: Use --ignore-watermark to restore all archives regardless of watermark")
        else:
            click.echo(f"\nNo archives found for database '{database_name}', table '{table_name}'")
        return

    logger.debug(
        "Found archives to restore",
        count=len(archive_keys),
        database=database_name,
        table=table_name,
    )

    if dry_run:
        click.echo(f"\nDRY RUN: Would restore {len(archive_keys)} archive file(s):")
        for key in archive_keys[:10]:  # Show first 10
            click.echo(f"  - {key}")
        if len(archive_keys) > 10:
            click.echo(f"  ... and {len(archive_keys) - 10} more")
        if restore_watermark_manager and watermark and not ignore_watermark:
            click.echo(f"\nNote: {len(all_archive_keys) - len(archive_keys)} archive(s) already restored (skipped)")
        if ignore_watermark:
            click.echo(f"\nNote: Watermark ignored - all {len(all_archive_keys)} archive(s) will be processed")
        return

    # Initialize database manager (reuse connection for all restores)
    db_manager = DatabaseManager(db_config, logger=logger)
    await db_manager.connect()

    try:
        # Re-load watermark with database manager if database storage is enabled (only if not ignoring watermark)
        if restore_watermark_manager and not ignore_watermark and archiver_config.restore_watermark.storage_type in ("database", "both"):
            try:
                watermark = await restore_watermark_manager.load_watermark(
                    database_name=database_name,
                    table_name=table_name,
                    s3_client=s3_client,
                    db_manager=db_manager,
                )
            except Exception as e:
                logger.warning(
                    "Failed to reload restore watermark from database (non-critical)",
                    database=database_name,
                    table=table_name,
                    error=str(e),
                )

        # Initialize restore engine
        restore_engine = RestoreEngine(db_manager, logger=logger)

        # Restore each archive file
        total_stats = {
            "files_processed": 0,
            "files_failed": 0,
            "records_processed": 0,
            "records_restored": 0,
            "records_skipped": 0,
            "records_failed": 0,
            "conflicts_detected": 0,
        }

        # Track detailed failure information
        failed_files: list[dict[str, Any]] = []

        # Track skip reasons
        skip_reasons: dict[str, int] = {}

        # Track the latest restored archive for watermark update
        latest_restored_date: Optional[datetime] = None
        latest_restored_key: Optional[str] = None

        # Log start of bulk restore
        total_files = len(archive_keys)
        from utils.output import print_info as output_print_info
        output_print_info(f"Restoring {total_files} archive file(s)...")
        logger.debug(
            "Starting bulk restore",
            database=database_name,
            table=table_name,
            total_files=total_files,
            total_archives=len(all_archive_keys),
        )

        # Calculate progress update interval (every 10% or every 25 files, whichever is more frequent)
        # For small batches, show every file; for large batches, show periodic updates
        if total_files <= 10:
            progress_interval = 1  # Show every file
        elif total_files <= 100:
            progress_interval = 10  # Show every 10 files
        else:
            progress_interval = max(10, total_files // 10)  # Show ~10 updates

        for idx, archive_key in enumerate(sorted(archive_keys), 1):
            try:
                logger.debug(
                    "Restoring archive",
                    file_number=idx,
                    total_files=len(archive_keys),
                    s3_key=archive_key,
                )

                # Read archive
                archive = await s3_reader.read_archive(archive_key, validate_checksum=validate_checksum)

                # Perform restore
                stats = await restore_engine.restore_archive(
                    archive=archive,
                    conflict_strategy=conflict_strategy,
                    batch_size=batch_size,
                    drop_indexes=drop_indexes if idx == 1 else False,  # Only drop indexes for first file
                    commit_frequency=commit_frequency,
                    dry_run=False,
                    schema_migration_strategy=schema_migration_strategy,
                    detect_conflicts=detect_conflicts,
                    table_name=table_name,
                    schema_name=schema_name,
                )

                # Aggregate stats
                total_stats["files_processed"] += 1
                total_stats["records_processed"] += stats.get("records_processed", 0)
                total_stats["records_restored"] += stats.get("records_restored", 0)
                total_stats["records_skipped"] += stats.get("records_skipped", 0)
                total_stats["records_failed"] += stats.get("records_failed", 0)
                total_stats["conflicts_detected"] += stats.get("conflicts_detected", 0)

                # Track latest restored archive for watermark update
                # Always track (even when ignoring watermark) so we can update watermark at the end
                if restore_watermark_manager:
                    archive_date = restore_watermark_manager.extract_date_from_s3_key(archive_key)
                    if archive_date:
                        if latest_restored_date is None or archive_date > latest_restored_date:
                            latest_restored_date = archive_date
                            latest_restored_key = archive_key

                        # Update watermark after each archive if configured (only if not ignoring watermark)
                        if not ignore_watermark and archiver_config.restore_watermark.update_after_each_archive:
                            try:
                                await restore_watermark_manager.save_watermark(
                                    database_name=database_name,
                                    table_name=table_name,
                                    last_restored_date=archive_date,
                                    last_restored_s3_key=archive_key,
                                    total_archives_restored=(watermark.total_archives_restored if watermark else 0) + total_stats["files_processed"],
                                    s3_client=s3_client,
                                    db_manager=db_manager if archiver_config.restore_watermark.storage_type in ("database", "both") else None,
                                )
                                logger.debug(
                                    "Restore watermark updated",
                                    database=database_name,
                                    table=table_name,
                                    archive_key=archive_key,
                                    archive_date=archive_date.isoformat(),
                                )
                            except Exception as e:
                                logger.warning(
                                    "Failed to update restore watermark (non-critical)",
                                    database=database_name,
                                    table=table_name,
                                    archive_key=archive_key,
                                    error=str(e),
                                )

                # Track skip reasons
                # Only track conflict types if we have detailed breakdown
                # Otherwise, use the generic skip reason
                if stats.get("records_skipped", 0) > 0:
                    conflict_types = stats.get("conflict_types", {})
                    if conflict_types and isinstance(conflict_types, dict) and conflict_types:
                        # Use detailed conflict type breakdown
                        for conflict_type, count in conflict_types.items():
                            if conflict_type:  # Ensure conflict_type is not None/empty
                                key = f"conflict_{conflict_type}"
                                skip_reasons[key] = skip_reasons.get(key, 0) + count
                    else:
                        # Fall back to generic skip reason
                        reason = stats.get("skip_reason", "conflict")
                        if reason:
                            skip_reasons[reason] = skip_reasons.get(reason, 0) + stats.get("records_skipped", 0)
                        else:
                            skip_reasons["conflict"] = skip_reasons.get("conflict", 0) + stats.get("records_skipped", 0)

                logger.debug(
                    "Archive restored successfully",
                    file_number=idx,
                    total_files=total_files,
                    s3_key=archive_key,
                    records_restored=stats.get("records_restored", 0),
                    records_skipped=stats.get("records_skipped", 0),
                )

                # Show progress periodically
                if idx % progress_interval == 0 or idx == total_files:
                    percent_complete = (idx / total_files) * 100
                    output_print_info(
                        f"Progress: {idx}/{total_files} files ({percent_complete:.1f}%) | "
                        f"Restored: {total_stats['records_restored']:,} | "
                        f"Skipped: {total_stats['records_skipped']:,}"
                    )
                    logger.debug(
                        "Restore progress",
                        files_processed=idx,
                        total_files=total_files,
                        percent_complete=f"{percent_complete:.1f}%",
                        records_restored_so_far=total_stats["records_restored"],
                        records_skipped_so_far=total_stats["records_skipped"],
                    )

            except Exception as e:
                total_stats["files_failed"] += 1
                error_type = type(e).__name__
                error_msg = str(e)

                # Store failure details
                failed_files.append({
                    "file": archive_key,
                    "file_number": idx,
                    "error_type": error_type,
                    "error": error_msg,
                })

                logger.error(
                    "Failed to restore archive",
                    file_number=idx,
                    total_files=len(archive_keys),
                    s3_key=archive_key,
                    error_type=error_type,
                    error=error_msg,
                    exc_info=True,
                )
                # Continue with next file instead of failing completely
                continue

        # Update watermark at the end if not updating after each archive
        # Always update watermark at the end (even when ignoring it) to track what was restored
        if restore_watermark_manager and latest_restored_date and latest_restored_key:
            if not archiver_config.restore_watermark.update_after_each_archive:
                try:
                    # When ignoring watermark, we still update it but with the new restore info
                    # Load current watermark to preserve total count if it exists
                    current_watermark = watermark
                    if ignore_watermark:
                        # Try to load existing watermark to preserve total count
                        try:
                            current_watermark = await restore_watermark_manager.load_watermark(
                                database_name=database_name,
                                table_name=table_name,
                                s3_client=s3_client,
                                db_manager=db_manager if archiver_config.restore_watermark.storage_type in ("database", "both") else None,
                            )
                        except Exception:
                            current_watermark = None

                    await restore_watermark_manager.save_watermark(
                        database_name=database_name,
                        table_name=table_name,
                        last_restored_date=latest_restored_date,
                        last_restored_s3_key=latest_restored_key,
                        total_archives_restored=(current_watermark.total_archives_restored if current_watermark else 0) + total_stats["files_processed"],
                        s3_client=s3_client,
                        db_manager=db_manager if archiver_config.restore_watermark.storage_type in ("database", "both") else None,
                    )
                    if ignore_watermark:
                        logger.debug(
                            "Restore watermark updated (watermark was ignored for filtering, but updated to track this restore)",
                            database=database_name,
                            table=table_name,
                            last_restored_date=latest_restored_date.isoformat(),
                            total_archives_restored=(current_watermark.total_archives_restored if current_watermark else 0) + total_stats["files_processed"],
                        )
                    else:
                        logger.debug(
                            "Restore watermark updated",
                            database=database_name,
                            table=table_name,
                            last_restored_date=latest_restored_date.isoformat(),
                            total_archives_restored=(current_watermark.total_archives_restored if current_watermark else 0) + total_stats["files_processed"],
                        )
                except Exception as e:
                    logger.warning(
                        "Failed to update restore watermark (non-critical)",
                        database=database_name,
                        table=table_name,
                        error=str(e),
                    )

        # Final summary log (debug level since we have formatted summary)
        logger.debug(
            "Bulk restore completed",
            **total_stats,
            database=database_name,
            table=table_name,
            schema=schema_name,
            failed_files_count=len(failed_files),
        )

        # Use formatted output utilities
        from utils.output import (
            print_header,
            print_info,
            print_key_value,
            print_section,
        )

        print_header("Restore Summary")

        print_section("Files")
        print_key_value("Processed", total_stats['files_processed'])
        print_key_value("Failed", total_stats['files_failed'], value_color="red" if total_stats['files_failed'] > 0 else "white")
        print_key_value("Total", len(archive_keys))
        if restore_watermark_manager and watermark and len(all_archive_keys) > len(archive_keys) and not ignore_watermark:
            print_key_value("Already Restored (Skipped)", len(all_archive_keys) - len(archive_keys), value_color="yellow")
        if ignore_watermark:
            print_info("Watermark ignored - all archives processed")

        print_section("Records")
        print_key_value("Processed", f"{total_stats['records_processed']:,}")
        print_key_value("Restored", f"{total_stats['records_restored']:,}", value_color="green")
        print_key_value("Skipped", f"{total_stats['records_skipped']:,}", value_color="yellow" if total_stats['records_skipped'] > 0 else "white")
        print_key_value("Failed", f"{total_stats['records_failed']:,}", value_color="red" if total_stats['records_failed'] > 0 else "white")

        # Show skip reasons if any
        if skip_reasons:
            print_section("Skip Reasons", color="yellow")
            for reason, count in sorted(skip_reasons.items(), key=lambda x: x[1], reverse=True):
                reason_display = reason.replace("_", " ").title()
                print_key_value(reason_display, f"{count:,}", value_color="yellow")

        # Show conflicts if any
        if total_stats.get("conflicts_detected", 0) > 0:
            print_section("Conflicts", color="yellow")
            print_key_value("Total Conflicts Detected", f"{total_stats['conflicts_detected']:,}")
            print_key_value("Conflict Strategy", conflict_strategy)

        # Show failed files if any
        if failed_files:
            print_section(f"Failed Files ({len(failed_files)})", color="red")
            # Group failures by error type
            failures_by_type: dict[str, list[dict[str, Any]]] = {}
            for failure in failed_files:
                error_type = failure["error_type"]
                if error_type not in failures_by_type:
                    failures_by_type[error_type] = []
                failures_by_type[error_type].append(failure)

            # Show summary by error type
            for error_type, failures in sorted(failures_by_type.items(), key=lambda x: len(x[1]), reverse=True):
                click.echo(f"\n  {error_type}: {len(failures)} file(s)")
                # Show first 3 examples of each error type
                for failure in failures[:3]:
                    file_name = failure["file"].split("/")[-1] if "/" in failure["file"] else failure["file"]
                    error_msg = failure["error"][:80] + "..." if len(failure["error"]) > 80 else failure["error"]
                    click.echo(f"    • {file_name}: {error_msg}")
                if len(failures) > 3:
                    click.echo(f"    ... and {len(failures) - 3} more file(s) with the same error")

            # Optionally show all failed files if verbose
            if verbose:
                click.echo("\n  All failed files:")
                for failure in failed_files:
                    click.echo(f"    [{failure['file_number']}/{len(archive_keys)}] {failure['file']}")
                    click.echo(f"      Error: {failure['error_type']}: {failure['error']}")

        click.echo()

    finally:
        await db_manager.disconnect()


if __name__ == "__main__":
    main()

