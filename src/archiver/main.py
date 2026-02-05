"""Main entry point for the archiver CLI."""

import sys
from pathlib import Path
from typing import Optional

import click

from archiver.config import load_config
from archiver.exceptions import ConfigurationError

# Import from utils package (sibling to archiver)
# Add src to path for utils import
src_path = Path(__file__).parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from utils.logging import configure_logging  # noqa: E402


@click.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file (YAML)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show what would be archived without making changes",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Enable verbose output",
)
@click.option(
    "--database",
    help="Process only specified database",
)
@click.option(
    "--table",
    help="Process only specified table (requires --database)",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]),
    help="Log level",
)
@click.option(
    "--log-format",
    default="console",
    type=click.Choice(["console", "json"], case_sensitive=False),
    help="Log format: 'console' for human-readable output, 'json' for structured logs (default: console)",
)
def main(
    config: Path,
    dry_run: bool,
    verbose: bool,
    database: Optional[str],
    table: Optional[str],
    log_level: str,
    log_format: str,
) -> None:
    """Archive PostgreSQL audit tables to S3-compatible storage.

    This tool archives historical audit table data from PostgreSQL databases
    to S3-compatible object storage while maintaining zero data loss guarantees.
    """
    # Configure logging
    # Use console format by default for better readability
    # JSON format is available for log aggregation systems (ELK, Splunk, etc.)
    # Verbose mode automatically enables DEBUG level for more detailed output
    effective_log_level = "DEBUG" if verbose else log_level
    logger = configure_logging(log_level=effective_log_level, log_format=log_format)

    # Suppress noisy third-party library logs in verbose mode
    # Keep boto3/botocore at WARNING level to reduce noise while keeping our DEBUG logs
    if verbose:
        import logging as std_logging
        std_logging.getLogger("boto3").setLevel(std_logging.WARNING)
        std_logging.getLogger("botocore").setLevel(std_logging.WARNING)
        std_logging.getLogger("urllib3").setLevel(std_logging.WARNING)
    logger = logger.bind(component="main")

    try:
        # Load configuration (only log in verbose mode)
        if verbose:
            logger.info("Loading configuration", config_path=str(config))
        archiver_config = load_config(config)
        if verbose:
            logger.info("Configuration loaded successfully", version=archiver_config.version)

        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")

        # Filter configuration if specified
        if database or table:
            filtered_databases = []
            for db in archiver_config.databases:
                if database and db.name != database:
                    continue
                if table:
                    db.tables = [t for t in db.tables if t.name == table]
                    if not db.tables:
                        continue
                filtered_databases.append(db)
            archiver_config.databases = filtered_databases

        if not archiver_config.databases:
            logger.warning("No databases/tables to process after filtering")
            return

        # Import archiver (avoid circular imports)
        from archiver.archiver import Archiver

        # Create and run archiver
        archiver = Archiver(archiver_config, dry_run=dry_run, logger=logger)

        import asyncio

        try:
            stats = asyncio.run(archiver.archive())
            # Summary is already printed by archiver if in console mode
            if not verbose:
                # In non-verbose mode, suppress the JSON log for completion
                pass
            else:
                logger.info("Archival completed successfully", **stats)
        except Exception as e:
            logger.exception("Archival failed", error=str(e))
            sys.exit(1)

        if verbose:
            logger.info("Archiver execution completed")

    except ConfigurationError as e:
        logger.error("Configuration error", error=str(e), correlation_id=e.correlation_id)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

