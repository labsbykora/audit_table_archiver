"""Audit Table Archiver - Shared utilities."""

import re


def safe_identifier(name: str) -> str:
    """Validate and quote a PostgreSQL identifier to prevent SQL injection.

    Ensures the name is a valid SQL identifier, then double-quotes it.
    Rejects anything that isn't alphanumeric/underscores (plus dots for schema.table).

    Args:
        name: SQL identifier (table name, column name, schema name)

    Returns:
        Safely quoted identifier (e.g., '"public"."my_table"')

    Raises:
        ValueError: If the identifier contains invalid characters
    """
    # Handle schema-qualified names (e.g., "public.my_table")
    if "." in name:
        parts = name.split(".", 1)
        return f"{safe_identifier(parts[0])}.{safe_identifier(parts[1])}"

    # Validate: only allow alphanumeric, underscores, and must start with letter/underscore
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError(
            f"Invalid SQL identifier: {name!r}. "
            "Only letters, digits, and underscores are allowed."
        )

    # Double-quote to handle reserved words and preserve case
    return f'"{name}"'
