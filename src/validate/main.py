"""CLI for archive validation."""

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import click
import structlog

from archiver.config import ArchiverConfig, load_config
from archiver.exceptions import ConfigurationError
from validate.archive_validator import ArchiveValidator

# Import from utils package
src_path = Path(__file__).parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from utils.logging import configure_logging, get_logger


@click.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file (YAML)",
)
@click.option(
    "--database",
    help="Database name to validate (if not specified, validates all)",
)
@click.option(
    "--table",
    help="Table name to validate (if not specified, validates all)",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    help="Start date for validation (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    help="End date for validation (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)",
)
@click.option(
    "--s3-key",
    help="Specific S3 key to validate (if specified, only this archive is validated)",
)
@click.option(
    "--no-validate-checksum",
    is_flag=True,
    default=False,
    help="Skip checksum validation (NOT RECOMMENDED)",
)
@click.option(
    "--no-validate-record-count",
    is_flag=True,
    default=False,
    help="Skip record count validation",
)
@click.option(
    "--output-format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    help="Output format (default: text)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Enable verbose output",
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
    database: Optional[str],
    table: Optional[str],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    s3_key: Optional[str],
    no_validate_checksum: bool,
    no_validate_record_count: bool,
    output_format: str,
    verbose: bool,
    log_level: str,
    log_format: str,
) -> None:
    """Validate archive integrity in S3.

    Examples:

    \b
    # Validate all archives
    validate --config config.yaml

    \b
    # Validate archives for specific database/table
    validate --config config.yaml --database mydb --table audit_logs

    \b
    # Validate archives in date range
    validate --config config.yaml --start-date 2026-01-01 --end-date 2026-01-31

    \b
    # Validate specific archive
    validate --config config.yaml --s3-key archives/db/table/year=2026/month=01/day=04/file.jsonl.gz
    """
    # Configure logging
    configure_logging(
        log_level=log_level if not verbose else "DEBUG",
        log_format=log_format,
    )
    logger = get_logger("validate_cli")

    try:
        archiver_config = load_config(config)

        # Initialize validator
        validator = ArchiveValidator(archiver_config.s3, logger=logger)

        # Run validation
        if s3_key:
            # Validate single archive
            result_dict = asyncio.run(
                validator.validate_archive(
                    s3_key,
                    validate_checksum=not no_validate_checksum,
                    validate_record_count=not no_validate_record_count,
                )
            )

            if output_format == "json":
                import json

                click.echo(json.dumps(result_dict, indent=2))
            else:
                from utils.output import print_header, print_success, print_error, print_key_value
                
                print_header("Archive Validation", color="cyan" if result_dict["valid"] else "red")
                print_key_value("S3 Key", s3_key)
                print_key_value("Status", "Valid" if result_dict["valid"] else "Invalid", 
                              value_color="green" if result_dict["valid"] else "red")
                
                if result_dict.get("record_count"):
                    print_key_value("Record Count", f"{result_dict['record_count']:,}")
                if result_dict.get("checksum"):
                    print_key_value("Checksum", result_dict["checksum"][:16] + "...")
                
                if not result_dict["valid"]:
                    click.echo()
                    print_error("Validation Errors:")
                    for error in result_dict["errors"]:
                        click.echo(f"  • {error}")
                else:
                    click.echo()
                    print_success("Archive validation passed")
                click.echo()

            sys.exit(0 if result_dict["valid"] else 1)
        else:
            # Validate multiple archives
            result = asyncio.run(
                validator.validate_archives(
                    database_name=database,
                    table_name=table,
                    start_date=start_date,
                    end_date=end_date,
                    validate_checksum=not no_validate_checksum,
                    validate_record_count=not no_validate_record_count,
                )
            )

            if output_format == "json":
                import json

                click.echo(json.dumps(result.to_dict(), indent=2))
            else:
                from utils.output import print_header, print_section, print_key_value, print_success, print_error, print_table
                
                # Format validation results nicely
                print_header("Archive Validation Results", 
                           color="green" if result.is_valid else "red")
                
                print_section("Overview")
                print_key_value("Total Archives", result.total_archives)
                print_key_value("Valid", result.valid_archives, value_color="green")
                print_key_value("Invalid", result.invalid_archives, value_color="red")
                
                if result.orphaned_files:
                    print_section("Orphaned Files", color="yellow")
                    for file in result.orphaned_files[:10]:  # Show first 10
                        click.echo(f"  • {file}")
                    if len(result.orphaned_files) > 10:
                        click.echo(f"  ... and {len(result.orphaned_files) - 10} more")
                
                if result.checksum_failures:
                    print_section("Checksum Failures", color="red")
                    for file in result.checksum_failures[:10]:
                        click.echo(f"  • {file}")
                    if len(result.checksum_failures) > 10:
                        click.echo(f"  ... and {len(result.checksum_failures) - 10} more")
                
                if result.record_count_mismatches:
                    print_section("Record Count Mismatches", color="red")
                    for file in result.record_count_mismatches[:10]:
                        click.echo(f"  • {file}")
                    if len(result.record_count_mismatches) > 10:
                        click.echo(f"  ... and {len(result.record_count_mismatches) - 10} more")
                
                click.echo()
                if result.is_valid:
                    print_success("All archives validated successfully")
                else:
                    print_error(f"Validation failed: {result.invalid_archives} archive(s) invalid")
                click.echo()

            sys.exit(0 if result.is_valid else 1)

    except KeyboardInterrupt:
        logger.warning("Validation interrupted by user")
        sys.exit(130)
    except ConfigurationError as e:
        logger.error("Configuration error", error=str(e))
        sys.exit(1)
    except Exception as e:
        logger.error("Validation failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

