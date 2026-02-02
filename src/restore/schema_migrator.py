"""Schema migration utilities for restore operations."""

from typing import Any, Optional

import structlog

from archiver.exceptions import ArchiverError
from utils.logging import get_logger


class SchemaDiff:
    """Represents differences between two schemas."""

    def __init__(
        self,
        added_columns: list[dict[str, Any]],
        removed_columns: list[dict[str, Any]],
        type_changes: list[dict[str, Any]],
        nullable_changes: list[dict[str, Any]],
    ) -> None:
        """Initialize schema diff.

        Args:
            added_columns: Columns present in current but not in archived
            removed_columns: Columns present in archived but not in current
            type_changes: Columns with type changes
            nullable_changes: Columns with nullable constraint changes
        """
        self.added_columns = added_columns
        self.removed_columns = removed_columns
        self.type_changes = type_changes
        self.nullable_changes = nullable_changes

    @property
    def has_changes(self) -> bool:
        """Check if there are any schema changes."""
        return bool(
            self.added_columns
            or self.removed_columns
            or self.type_changes
            or self.nullable_changes
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "added_columns": self.added_columns,
            "removed_columns": self.removed_columns,
            "type_changes": self.type_changes,
            "nullable_changes": self.nullable_changes,
        }


class SchemaMigrator:
    """Handles schema migration during restore operations."""

    def __init__(
        self,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize schema migrator.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("schema_migrator")

    def compare_schemas(
        self,
        archived_schema: dict[str, Any],
        current_schema: dict[str, Any],
    ) -> SchemaDiff:
        """Compare archived schema with current schema.

        Args:
            archived_schema: Schema from archived metadata
            current_schema: Current database schema

        Returns:
            SchemaDiff object with differences
        """
        # Normalize column format (handle both 'type' and 'data_type')
        archived_columns = {}
        for col in archived_schema.get("columns", []):
            normalized_col = col.copy()
            # Normalize type field
            if "type" in normalized_col and "data_type" not in normalized_col:
                normalized_col["data_type"] = normalized_col["type"]
            # Normalize nullable field
            if "nullable" in normalized_col and "is_nullable" not in normalized_col:
                normalized_col["is_nullable"] = normalized_col["nullable"]
            archived_columns[col["name"]] = normalized_col

        current_columns = {}
        for col in current_schema.get("columns", []):
            normalized_col = col.copy()
            # Normalize type field
            if "type" in normalized_col and "data_type" not in normalized_col:
                normalized_col["data_type"] = normalized_col["type"]
            # Normalize nullable field
            if "nullable" in normalized_col and "is_nullable" not in normalized_col:
                normalized_col["is_nullable"] = normalized_col["nullable"]
            current_columns[col["name"]] = normalized_col

        added_columns = [
            current_columns[col_name]
            for col_name in current_columns
            if col_name not in archived_columns
        ]

        removed_columns = [
            archived_columns[col_name]
            for col_name in archived_columns
            if col_name not in current_columns
        ]

        type_changes = []
        nullable_changes = []

        for col_name in archived_columns:
            if col_name in current_columns:
                archived_col = archived_columns[col_name]
                current_col = current_columns[col_name]

                # Check type changes (compare data_type, fallback to type)
                archived_type = archived_col.get("data_type") or archived_col.get("type")
                current_type = current_col.get("data_type") or current_col.get("type")
                if archived_type != current_type:
                    type_changes.append({
                        "column": col_name,
                        "archived_type": archived_type,
                        "current_type": current_type,
                    })

                # Check nullable changes (handle both is_nullable and nullable)
                archived_nullable = archived_col.get("is_nullable", archived_col.get("nullable", True))
                current_nullable = current_col.get("is_nullable", current_col.get("nullable", True))
                if archived_nullable != current_nullable:
                    nullable_changes.append({
                        "column": col_name,
                        "archived_nullable": archived_nullable,
                        "current_nullable": current_nullable,
                    })

        return SchemaDiff(
            added_columns=added_columns,
            removed_columns=removed_columns,
            type_changes=type_changes,
            nullable_changes=nullable_changes,
        )

    def generate_diff_report(self, diff: SchemaDiff) -> str:
        """Generate human-readable schema diff report.

        Args:
            diff: SchemaDiff object

        Returns:
            Formatted report string
        """
        lines = ["Schema Differences Detected:", "=" * 50]

        if diff.added_columns:
            lines.append(f"\nAdded Columns ({len(diff.added_columns)}):")
            for col in diff.added_columns:
                lines.append(f"  + {col['name']} ({col.get('type', 'unknown')})")

        if diff.removed_columns:
            lines.append(f"\nRemoved Columns ({len(diff.removed_columns)}):")
            for col in diff.removed_columns:
                lines.append(f"  - {col['name']} ({col.get('type', 'unknown')})")

        if diff.type_changes:
            lines.append(f"\nType Changes ({len(diff.type_changes)}):")
            for change in diff.type_changes:
                lines.append(
                    f"  ~ {change['column']}: "
                    f"{change['archived_type']} -> {change['current_type']}"
                )

        if diff.nullable_changes:
            lines.append(f"\nNullable Constraint Changes ({len(diff.nullable_changes)}):")
            for change in diff.nullable_changes:
                lines.append(
                    f"  ~ {change['column']}: "
                    f"nullable={change['archived_nullable']} -> nullable={change['current_nullable']}"
                )

        if not diff.has_changes:
            lines.append("\nNo schema differences detected.")

        return "\n".join(lines)

    def transform_record(
        self,
        record: dict[str, Any],
        archived_schema: dict[str, Any],
        current_schema: dict[str, Any],
        strategy: str = "lenient",
    ) -> dict[str, Any]:
        """Transform a record to match current schema.

        Args:
            record: Record from archive
            archived_schema: Schema from archived metadata
            current_schema: Current database schema
            strategy: Migration strategy (strict, lenient, transform)

        Returns:
            Transformed record

        Raises:
            ArchiverError: If transformation fails (strict mode) or incompatible changes
        """
        diff = self.compare_schemas(archived_schema, current_schema)
        transformed = record.copy()

        # Handle removed columns
        if diff.removed_columns:
            if strategy == "strict":
                raise ArchiverError(
                    f"Cannot restore: columns removed from table: {[c['name'] for c in diff.removed_columns]}",
                    context={"strategy": strategy, "removed_columns": [c["name"] for c in diff.removed_columns]},
                )
            elif strategy == "lenient":
                # Remove columns that no longer exist
                for col in diff.removed_columns:
                    transformed.pop(col["name"], None)
                    self.logger.warning(
                        "Removed column from record",
                        column=col["name"],
                        strategy=strategy,
                    )
            # transform strategy: same as lenient for removed columns

        # Handle added columns
        if diff.added_columns:
            for col in diff.added_columns:
                col_name = col["name"]
                if col_name not in transformed:
                    # Set default value based on nullable and type
                    is_nullable = col.get("is_nullable", col.get("nullable", True))
                    if not is_nullable:
                        # Non-nullable column - use type-specific default
                        col_type = col.get("data_type") or col.get("type")
                        default_value = self._get_default_value(col_type)
                        transformed[col_name] = default_value
                        self.logger.debug(
                            "Added default value for new column",
                            column=col_name,
                            default=default_value,
                        )
                    else:
                        # Nullable column - set to None
                        transformed[col_name] = None

        # Handle type changes
        if diff.type_changes:
            if strategy == "strict":
                raise ArchiverError(
                    f"Cannot restore: type changes detected: {[c['column'] for c in diff.type_changes]}",
                    context={"strategy": strategy, "type_changes": diff.type_changes},
                )
            elif strategy in ("lenient", "transform"):
                # Attempt type conversion
                for change in diff.type_changes:
                    col_name = change["column"]
                    if col_name in transformed:
                        try:
                            transformed[col_name] = self._convert_type(
                                transformed[col_name],
                                change["archived_type"],
                                change["current_type"],
                            )
                            self.logger.debug(
                                "Converted column type",
                                column=col_name,
                                from_type=change["archived_type"],
                                to_type=change["current_type"],
                            )
                        except (ValueError, TypeError) as e:
                            if strategy == "strict":
                                raise ArchiverError(
                                    f"Type conversion failed for {col_name}: {e}",
                                    context={"column": col_name, "change": change},
                                ) from e
                            else:
                                # Lenient: set to None if conversion fails
                                self.logger.warning(
                                    "Type conversion failed, setting to None",
                                    column=col_name,
                                    error=str(e),
                                )
                                transformed[col_name] = None

        # Handle nullable changes (warn only, don't transform)
        if diff.nullable_changes:
            for change in diff.nullable_changes:
                col_name = change["column"]
                if col_name in transformed and transformed[col_name] is None:
                    if not change["current_nullable"]:
                        # Column is now NOT NULL but value is None
                        if strategy == "strict":
                            raise ArchiverError(
                                f"Cannot restore: NULL value in non-nullable column {col_name}",
                                context={"column": col_name, "change": change},
                            )
                        else:
                            # Set default value
                            default_value = self._get_default_value(
                                current_schema.get("columns", {})
                                .get(col_name, {})
                                .get("type", "TEXT")
                            )
                            transformed[col_name] = default_value
                            self.logger.warning(
                                "Replaced NULL with default for non-nullable column",
                                column=col_name,
                                default=default_value,
                            )

        return transformed

    def _get_default_value(self, column_type: Optional[str]) -> Any:
        """Get default value for a column type.

        Args:
            column_type: PostgreSQL column type

        Returns:
            Default value
        """
        if not column_type:
            return None

        type_lower = column_type.upper()
        if "INT" in type_lower or "SERIAL" in type_lower:
            return 0
        elif "FLOAT" in type_lower or "DOUBLE" in type_lower or "REAL" in type_lower:
            return 0.0
        elif "BOOL" in type_lower:
            return False
        elif "TIMESTAMP" in type_lower or "DATE" in type_lower:
            return None  # Will need to be handled by database default
        elif "JSON" in type_lower or "JSONB" in type_lower:
            return {}
        elif "ARRAY" in type_lower:
            return []
        else:
            return ""

    def _convert_type(
        self,
        value: Any,
        from_type: str,
        to_type: str,
    ) -> Any:
        """Convert value from one type to another.

        Args:
            value: Value to convert
            from_type: Source type
            to_type: Target type

        Returns:
            Converted value

        Raises:
            ValueError: If conversion is not possible
        """
        if value is None:
            return None

        from_type_upper = from_type.upper()
        to_type_upper = to_type.upper()

        # Same type or compatible
        if from_type_upper == to_type_upper:
            return value

        # Numeric conversions
        if "INT" in from_type_upper and "INT" in to_type_upper:
            return int(value) if value is not None else None
        if "NUMERIC" in from_type_upper or "DECIMAL" in from_type_upper:
            if "INT" in to_type_upper:
                return int(float(value)) if value is not None else None
            if "FLOAT" in to_type_upper or "DOUBLE" in to_type_upper or "REAL" in to_type_upper:
                return float(value) if value is not None else None

        # String conversions
        if "TEXT" in to_type_upper or "VARCHAR" in to_type_upper or "CHAR" in to_type_upper:
            return str(value) if value is not None else None

        # JSON conversions
        if "JSON" in to_type_upper or "JSONB" in to_type_upper:
            if isinstance(value, (dict, list)):
                return value
            if isinstance(value, str):
                import json
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value

        # If no conversion found, return as-is (lenient mode)
        return value

