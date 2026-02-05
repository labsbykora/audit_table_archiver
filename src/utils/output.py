"""Utility functions for formatted CLI output."""

from typing import Any

import click


def print_header(title: str, width: int = 70, color: str = "cyan") -> None:
    """Print a formatted header.

    Args:
        title: Header title
        width: Header width
        color: Header color
    """
    click.echo()
    click.echo(click.style("=" * width, fg=color))
    click.echo(click.style(title, fg=color, bold=True))
    click.echo(click.style("=" * width, fg=color))
    click.echo()


def print_section(title: str, color: str = "yellow") -> None:
    """Print a section title.

    Args:
        title: Section title
        color: Section color
    """
    click.echo(click.style(f"\n{title}:", fg=color, bold=True))


def print_key_value(
    key: str, value: Any, key_color: str = "white", value_color: str = "cyan"
) -> None:
    """Print a key-value pair.

    Args:
        key: Key name
        value: Value
        key_color: Key color
        value_color: Value color
    """
    click.echo(
        click.style(f"  {key}: ", fg=key_color) + click.style(str(value), fg=value_color, bold=True)
    )


def print_success(message: str) -> None:
    """Print a success message.

    Args:
        message: Success message
    """
    click.echo(click.style(f"✓ {message}", fg="green", bold=True))


def print_error(message: str) -> None:
    """Print an error message.

    Args:
        message: Error message
    """
    click.echo(click.style(f"✗ {message}", fg="red", bold=True))


def print_warning(message: str) -> None:
    """Print a warning message.

    Args:
        message: Warning message
    """
    click.echo(click.style(f"⚠ {message}", fg="yellow", bold=True))


def print_info(message: str) -> None:
    """Print an info message.

    Args:
        message: Info message
    """
    click.echo(click.style(f"ℹ {message}", fg="blue"))


def print_table(headers: list[str], rows: list[list[Any]], header_color: str = "cyan") -> None:
    """Print a formatted table.

    Args:
        headers: Table headers
        rows: Table rows
        header_color: Header color
    """
    if not rows:
        return

    # Calculate column widths
    col_widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    # Print header
    header_row = " | ".join(str(h).ljust(col_widths[i]) for i, h in enumerate(headers))
    click.echo(click.style(header_row, fg=header_color, bold=True))
    click.echo(click.style("-" * len(header_row), dim=True))

    # Print rows
    for row in rows:
        row_str = " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row))
        click.echo(row_str)


def print_summary(stats: dict[str, Any], title: str = "Summary") -> None:
    """Print a formatted summary from statistics.

    Args:
        stats: Statistics dictionary
        title: Summary title
    """
    print_header(title)

    # Group statistics by category
    if "databases_processed" in stats:
        # Archiver summary
        print_section("Databases")
        print_key_value("Processed", stats.get("databases_processed", 0))
        print_key_value("Failed", stats.get("databases_failed", 0))

        print_section("Tables")
        print_key_value("Processed", stats.get("tables_processed", 0))
        print_key_value("Failed", stats.get("tables_failed", 0))
        print_key_value("Skipped", stats.get("tables_skipped", 0))

        print_section("Records")
        # Show both current run and overall if available
        # Use records_archived_this_run if explicitly set, otherwise fall back to records_archived
        records_archived_this_run = stats.get("records_archived_this_run")
        if records_archived_this_run is None:
            records_archived_this_run = stats.get("records_archived", 0)
        records_archived_total = stats.get("records_archived_total")

        if (
            records_archived_total is not None
            and records_archived_total > records_archived_this_run
        ):
            # Both metrics available and different - show both
            print_key_value("Archived (This Run)", f"{records_archived_this_run:,}")
            print_key_value("Archived (Total)", f"{records_archived_total:,}")
        else:
            # Only this run metric available (no checkpoint or first run)
            print_key_value("Archived", f"{records_archived_this_run:,}")
        print_key_value("Failed", stats.get("records_failed", 0))

        print_section("Batches")
        print_key_value("Processed", stats.get("batches_processed", 0))
        print_key_value("Failed", stats.get("batches_failed", 0))

        if stats.get("start_time") and stats.get("end_time"):
            from datetime import datetime

            start = datetime.fromisoformat(stats["start_time"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(stats["end_time"].replace("Z", "+00:00"))
            duration = (end - start).total_seconds()
            hours = int(duration // 3600)
            minutes = int((duration % 3600) // 60)
            seconds = int(duration % 60)
            duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            print_section("Duration")
            print_key_value("Total Time", duration_str)

    elif "files_processed" in stats:
        # Restore summary
        print_section("Files")
        print_key_value("Processed", stats.get("files_processed", 0))
        print_key_value("Failed", stats.get("files_failed", 0))

        print_section("Records")
        print_key_value("Processed", f"{stats.get('records_processed', 0):,}")
        print_key_value("Restored", f"{stats.get('records_restored', 0):,}")
        print_key_value("Skipped", f"{stats.get('records_skipped', 0):,}")
        print_key_value("Failed", stats.get("records_failed", 0))

    click.echo()


def print_separator(width: int = 70, color: str = "cyan") -> None:
    """Print a separator line.

    Args:
        width: Separator width
        color: Separator color
    """
    click.echo(click.style("=" * width, fg=color))


def print_delimiter(width: int = 70) -> None:
    """Print a delimiter line.

    Args:
        width: Delimiter width
    """
    click.echo(click.style("-" * width, dim=True))
