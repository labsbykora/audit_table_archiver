"""CLI for interactive configuration wizard."""

import sys
from pathlib import Path
from typing import Optional

import click
import yaml

from archiver.config import ArchiverConfig
from archiver.exceptions import ConfigurationError
from wizard.config_wizard import ConfigWizard

# Import from utils package
src_path = Path(__file__).parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from utils.logging import configure_logging, get_logger  # noqa: E402


@click.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path("config.yaml"),
    help="Output configuration file path (default: config.yaml)",
)
@click.option(
    "--non-interactive",
    is_flag=True,
    default=False,
    help="Non-interactive mode (use defaults, requires all options)",
)
@click.option(
    "--database-host",
    help="Database host (required in non-interactive mode)",
)
@click.option(
    "--database-port",
    type=int,
    default=5432,
    help="Database port (default: 5432)",
)
@click.option(
    "--database-name",
    help="Database name (required in non-interactive mode)",
)
@click.option(
    "--database-user",
    help="Database user (required in non-interactive mode)",
)
@click.option(
    "--database-password",
    help="Database password (required in non-interactive mode, or use DB_PASSWORD env var)",
)
@click.option(
    "--s3-bucket",
    help="S3 bucket name (required in non-interactive mode)",
)
@click.option(
    "--s3-endpoint",
    help="S3 endpoint URL (optional, for S3-compatible storage)",
)
@click.option(
    "--s3-region",
    default="us-east-1",
    help="S3 region (default: us-east-1)",
)
@click.option(
    "--s3-prefix",
    default="archives/",
    help="S3 key prefix (default: archives/)",
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
    output: Path,
    non_interactive: bool,
    database_host: Optional[str],
    database_port: int,
    database_name: Optional[str],
    database_user: Optional[str],
    database_password: Optional[str],
    s3_bucket: Optional[str],
    s3_endpoint: Optional[str],
    s3_region: str,
    s3_prefix: str,
    verbose: bool,
    log_level: str,
    log_format: str,
) -> None:
    """Interactive configuration wizard for archiver setup.

    This wizard helps you create a configuration file by:
    - Auto-detecting tables in your database
    - Suggesting retention periods based on data age
    - Validating configuration before saving

    Examples:

    \b
    # Interactive mode (recommended)
    wizard

    \b
    # Non-interactive mode
    wizard --non-interactive \\
        --database-host localhost \\
        --database-name mydb \\
        --database-user postgres \\
        --s3-bucket my-archive-bucket
    """
    # Configure logging
    configure_logging(
        log_level=log_level if not verbose else "DEBUG",
        log_format=log_format,
    )
    logger = get_logger("wizard_cli")

    try:
        wizard = ConfigWizard(logger=logger)

        if non_interactive:
            # Non-interactive mode - use provided options
            if not all([database_host, database_name, database_user, s3_bucket]):
                raise ConfigurationError(
                    "In non-interactive mode, --database-host, --database-name, "
                    "--database-user, and --s3-bucket are required"
                )

            # Use password from option or environment variable
            password = database_password or None
            if not password:
                import os

                password = os.getenv("DB_PASSWORD")
                if not password:
                    raise ConfigurationError(
                        "Database password required (--database-password or DB_PASSWORD env var)"
                    )

            # Auto-detect tables
            import asyncio

            tables = asyncio.run(
                wizard.detect_tables(
                    host=database_host,
                    port=database_port,
                    database=database_name,
                    user=database_user,
                    password=password,
                )
            )

            # Create configuration with detected tables
            databases = [
                {
                    "name": database_name,
                    "host": database_host,
                    "port": database_port,
                    "user": database_user,
                    "password_env": "DB_PASSWORD",
                    "tables": [
                        {
                            "name": table["name"],
                            "schema_name": table["schema"],
                            "timestamp_column": table["suggested_timestamp"] or table["timestamp_columns"][0] if table["timestamp_columns"] else "created_at",
                            "primary_key": table["primary_key"] or "id",
                        }
                        for table in tables
                    ],
                }
            ]

            s3_config = {
                "bucket": s3_bucket,
                "endpoint": s3_endpoint,
                "region": s3_region,
                "prefix": s3_prefix,
            }

            defaults = {
                "retention_days": 365,
                "batch_size": 1000,
            }

            config = wizard.generate_config(databases, s3_config, defaults)

        else:
            # Interactive mode
            click.echo("=== Audit Archiver Configuration Wizard ===\n")

            # Database connection info
            click.echo("Database Configuration:")
            database_host = click.prompt("Database host", default="localhost")
            database_port = click.prompt("Database port", type=int, default=5432)
            database_name = click.prompt("Database name")
            database_user = click.prompt("Database user", default="postgres")
            database_password = click.prompt(
                "Database password", hide_input=True, default=""
            )
            if not database_password:
                import os

                database_password = os.getenv("DB_PASSWORD", "")

            # Detect tables
            click.echo("\nDetecting tables...")
            import asyncio

            tables = asyncio.run(
                wizard.detect_tables(
                    host=database_host,
                    port=database_port,
                    database=database_name,
                    user=database_user,
                    password=database_password,
                )
            )

            if not tables:
                click.echo("No tables found in database.")
                sys.exit(1)

            click.echo(f"Found {len(tables)} table(s)\n")

            # Select tables to archive
            click.echo("Select tables to archive:")
            for i, table in enumerate(tables, 1):
                has_timestamp = table["suggested_timestamp"] is not None
                has_pk = table["primary_key"] is not None
                status = "✓" if (has_timestamp and has_pk) else "⚠"
                click.echo(
                    f"  {i}. {status} {table['schema']}.{table['name']} "
                    f"(timestamp: {table['suggested_timestamp'] or 'N/A'}, "
                    f"PK: {table['primary_key'] or 'N/A'})"
                )

            table_indices = click.prompt(
                "\nEnter table numbers (comma-separated, or 'all')",
                default="all",
            )

            if table_indices.lower() == "all":
                selected_indices = list(range(len(tables)))
            else:
                selected_indices = [
                    int(x.strip()) - 1
                    for x in table_indices.split(",")
                    if x.strip().isdigit()
                ]

            # Configure selected tables
            configured_tables = []
            for idx in selected_indices:
                if idx < 0 or idx >= len(tables):
                    continue

                table = tables[idx]
                click.echo(f"\nConfiguring {table['schema']}.{table['name']}:")

                # Timestamp column
                if table["suggested_timestamp"]:
                    timestamp_col = click.prompt(
                        "Timestamp column",
                        default=table["suggested_timestamp"],
                    )
                elif table["timestamp_columns"]:
                    timestamp_col = click.prompt(
                        "Timestamp column",
                        default=table["timestamp_columns"][0],
                    )
                else:
                    timestamp_col = click.prompt("Timestamp column")

                # Primary key
                if table["primary_key"]:
                    primary_key = click.prompt(
                        "Primary key column", default=table["primary_key"]
                    )
                elif table["id_columns"]:
                    primary_key = click.prompt(
                        "Primary key column", default=table["id_columns"][0]
                    )
                else:
                    primary_key = click.prompt("Primary key column")

                # Estimate records and suggest retention
                try:
                    estimates = asyncio.run(
                        wizard.estimate_record_count(
                            host=database_host,
                            port=database_port,
                            database=database_name,
                            user=database_user,
                            password=database_password,
                            schema=table["schema"],
                            table=table["name"],
                            timestamp_column=timestamp_col,
                            retention_days=365,
                        )
                    )

                    click.echo(
                        f"  Total records: {estimates['total_records']:,}"
                    )
                    if estimates.get("age_days"):
                        click.echo(f"  Data age: {estimates['age_days']} days")
                    if estimates.get("suggested_retention_days"):
                        click.echo(
                            f"  Suggested retention: {estimates['suggested_retention_days']} days"
                        )

                    suggested_retention = estimates.get("suggested_retention_days", 365)
                except Exception as e:
                    logger.warning("Failed to estimate records", error=str(e))
                    suggested_retention = 365

                retention_days = click.prompt(
                    "Retention period (days)", type=int, default=suggested_retention
                )

                configured_tables.append({
                    "name": table["name"],
                    "schema_name": table["schema"],
                    "timestamp_column": timestamp_col,
                    "primary_key": primary_key,
                    "retention_days": retention_days,
                })

            # S3 configuration
            click.echo("\nS3 Configuration:")
            s3_bucket = click.prompt("S3 bucket name")
            s3_endpoint = click.prompt(
                "S3 endpoint URL (optional, press Enter for AWS S3)", default=""
            )
            s3_region = click.prompt("S3 region", default="us-east-1")
            s3_prefix = click.prompt("S3 key prefix", default="archives/")

            # Defaults
            click.echo("\nDefault Settings:")
            default_retention = click.prompt(
                "Default retention period (days)", type=int, default=365
            )
            default_batch_size = click.prompt(
                "Default batch size", type=int, default=1000
            )

            databases = [
                {
                    "name": database_name,
                    "host": database_host,
                    "port": database_port,
                    "user": database_user,
                    "password_env": "DB_PASSWORD",
                    "tables": configured_tables,
                }
            ]

            s3_config = {
                "bucket": s3_bucket,
                "endpoint": s3_endpoint if s3_endpoint else None,
                "region": s3_region,
                "prefix": s3_prefix,
            }

            defaults = {
                "retention_days": default_retention,
                "batch_size": default_batch_size,
            }

            config = wizard.generate_config(databases, s3_config, defaults)

        # Validate configuration
        click.echo("\nValidating configuration...")
        try:
            # Try to create config object (this validates)
            _ = ArchiverConfig.model_validate(config.model_dump())
            click.echo("✓ Configuration is valid")
        except Exception as e:
            click.echo(f"✗ Configuration validation failed: {e}")
            sys.exit(1)

        # Save configuration
        click.echo(f"\nSaving configuration to {output}...")
        config_dict = config.model_dump(mode="json", exclude_none=True)

        # Convert to YAML-friendly format
        with open(output, "w") as f:
            yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

        click.echo(f"✓ Configuration saved to {output}")
        click.echo("\nNext steps:")
        click.echo("  1. Set environment variable: export DB_PASSWORD='your_password'")
        click.echo(f"  2. Test configuration: archiver --config {output} --dry-run")
        click.echo(f"  3. Run archiver: archiver --config {output}")

    except KeyboardInterrupt:
        logger.warning("Wizard interrupted by user")
        sys.exit(130)
    except ConfigurationError as e:
        logger.error("Configuration error", error=str(e))
        sys.exit(1)
    except Exception as e:
        logger.error("Wizard failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

