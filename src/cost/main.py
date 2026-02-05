"""CLI for S3 storage cost estimation."""

import sys
from pathlib import Path
from typing import Optional

import click

from archiver.config import load_config
from archiver.exceptions import ConfigurationError
from cost.cost_estimator import CostEstimator, StorageClass

# Import from utils package
src_path = Path(__file__).parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from utils.logging import configure_logging, get_logger  # noqa: E402


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file (YAML) - used to estimate costs for configured tables",
)
@click.option(
    "--size-gb",
    type=float,
    help="Uncompressed data size in GB (alternative to --config)",
)
@click.option(
    "--records",
    type=int,
    help="Number of records (requires --avg-record-size)",
)
@click.option(
    "--avg-record-size",
    type=float,
    help="Average record size in bytes (required with --records)",
)
@click.option(
    "--storage-class",
    type=click.Choice([sc.value for sc in StorageClass], case_sensitive=False),
    default=StorageClass.STANDARD_IA.value,
    help="S3 storage class (default: STANDARD_IA)",
)
@click.option(
    "--region",
    default="us-east-1",
    help="AWS region (default: us-east-1)",
)
@click.option(
    "--compression-ratio",
    type=float,
    help="Compression ratio (0.0-1.0, default: 0.3 = 70% compression)",
)
@click.option(
    "--retrieval-percentage",
    type=float,
    help="Percentage of data retrieved per month (0.0-1.0, default: 0.05 = 5%)",
)
@click.option(
    "--compare",
    is_flag=True,
    default=False,
    help="Compare costs across all storage classes",
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
    config: Optional[Path],
    size_gb: Optional[float],
    records: Optional[int],
    avg_record_size: Optional[float],
    storage_class: str,
    region: str,
    compression_ratio: Optional[float],
    retrieval_percentage: Optional[float],
    compare: bool,
    output_format: str,
    verbose: bool,
    log_level: str,
    log_format: str,
) -> None:
    """Estimate S3 storage costs for archived data.

    This tool helps you estimate the monthly and annual costs of storing
    archived data in S3, considering compression ratios and storage classes.

    Examples:

    \b
    # Estimate from configuration file
    cost-estimate --config config.yaml

    \b
    # Estimate from data size
    cost-estimate --size-gb 100 --storage-class STANDARD_IA

    \b
    # Estimate from record count
    cost-estimate --records 1000000 --avg-record-size 1024

    \b
    # Compare all storage classes
    cost-estimate --size-gb 100 --compare
    """
    # Configure logging
    configure_logging(
        log_level=log_level if not verbose else "DEBUG",
        log_format=log_format,
    )
    logger = get_logger("cost_estimate_cli")

    try:
        estimator = CostEstimator(logger=logger)

        # Determine input method
        if config:
            # Load configuration and estimate for all tables
            archiver_config = load_config(config)
            storage_class_enum = StorageClass(storage_class.upper())

            click.echo("Estimating costs for configured tables...\n")

            total_uncompressed_gb = 0.0
            for db in archiver_config.databases:
                for table in db.tables:
                    # Estimate based on retention period and batch size
                    # This is a rough estimate - actual size depends on data
                    # For now, we'll use a placeholder that users can refine
                    click.echo(
                        f"Table: {db.name}.{table.schema_name}.{table.name} "
                        f"(retention: {table.retention_days or archiver_config.defaults.retention_days} days)"
                    )
                    click.echo(
                        "  Note: Actual size depends on data. Use --size-gb for precise estimates.\n"
                    )

            # Use S3 config from archiver config

            if not size_gb:
                click.echo(
                    "Warning: Cannot estimate from config alone. "
                    "Please provide --size-gb or use --records with --avg-record-size."
                )
                sys.exit(1)

        elif size_gb:
            # Direct size input
            storage_class_enum = StorageClass(storage_class.upper())
            total_uncompressed_gb = size_gb

        elif records and avg_record_size:
            # Record count input
            storage_class_enum = StorageClass(storage_class.upper())
            estimate = estimator.estimate_from_records(
                record_count=records,
                avg_record_size_bytes=avg_record_size,
                storage_class=storage_class_enum,
                region=region,
                compression_ratio=compression_ratio,
                retrieval_percentage=retrieval_percentage,
            )
            total_uncompressed_gb = estimate.total_size_gb

        else:
            raise ConfigurationError(
                "Must provide either --config, --size-gb, or --records with --avg-record-size"
            )

        # Generate estimate(s)
        if compare:
            # Compare all storage classes
            comparisons = estimator.compare_storage_classes(
                uncompressed_size_gb=total_uncompressed_gb,
                region=region,
                compression_ratio=compression_ratio,
                retrieval_percentage=retrieval_percentage,
            )

            if output_format == "json":
                import json

                result = {sc: est.to_dict() for sc, est in comparisons.items()}
                click.echo(json.dumps(result, indent=2))
            else:
                from utils.output import print_header, print_key_value, print_section, print_table

                print_header(f"Cost Comparison ({total_uncompressed_gb:,.2f} GB uncompressed)")

                # Create table
                headers = ["Storage Class", "Monthly Cost", "Annual Cost"]
                rows = []
                for sc_name, estimate in sorted(
                    comparisons.items(), key=lambda x: x[1].annual_total_cost
                ):
                    rows.append(
                        [
                            sc_name,
                            f"${estimate.monthly_total_cost:,.2f}",
                            f"${estimate.annual_total_cost:,.2f}",
                        ]
                    )
                print_table(headers, rows)
                click.echo()
        else:
            # Single estimate
            estimate = estimator.estimate_cost(
                uncompressed_size_gb=total_uncompressed_gb,
                storage_class=storage_class_enum,
                region=region,
                compression_ratio=compression_ratio,
                retrieval_percentage=retrieval_percentage,
            )

            if output_format == "json":
                import json

                click.echo(json.dumps(estimate.to_dict(), indent=2))
            else:
                from utils.output import print_header, print_key_value, print_section

                print_header("Cost Estimate")
                print_section("Storage")
                print_key_value("Uncompressed Size", f"{estimate.total_size_gb:,.2f} GB")
                print_key_value("Compressed Size", f"{estimate.compressed_size_gb:,.2f} GB")
                print_key_value("Storage Class", storage_class_enum.value)
                print_key_value("Region", region)

                print_section("Costs")
                print_key_value("Monthly Storage", f"${estimate.monthly_storage_cost:,.2f}")
                if estimate.monthly_retrieval_cost > 0:
                    print_key_value("Monthly Retrieval", f"${estimate.monthly_retrieval_cost:,.2f}")
                print_key_value(
                    "Monthly Total", f"${estimate.monthly_total_cost:,.2f}", value_color="green"
                )
                print_key_value(
                    "Annual Total", f"${estimate.annual_total_cost:,.2f}", value_color="green"
                )
                click.echo()

    except KeyboardInterrupt:
        logger.warning("Cost estimation interrupted by user")
        sys.exit(130)
    except ConfigurationError as e:
        logger.error("Configuration error", error=str(e))
        sys.exit(1)
    except Exception as e:
        logger.error("Cost estimation failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
