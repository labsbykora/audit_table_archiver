"""Schema drift detection and comparison."""

from typing import Any, Optional

import structlog

from archiver.exceptions import VerificationError
from utils.logging import get_logger


class SchemaDriftDetector:
    """Detects schema changes between archival runs."""

    def __init__(
        self,
        fail_on_drift: bool = False,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize schema drift detector.

        Args:
            fail_on_drift: If True, raise exception on schema drift (default: False)
            logger: Optional logger instance
        """
        self.fail_on_drift = fail_on_drift
        self.logger = logger or get_logger("schema_drift")

    def compare_schemas(
        self,
        current_schema: dict[str, Any],
        previous_schema: Optional[dict[str, Any]],
        database_name: str,
        table_name: str,
    ) -> dict[str, Any]:
        """Compare current schema with previous schema and detect changes.

        Args:
            current_schema: Current table schema
            previous_schema: Previous table schema (from metadata) or None
            database_name: Database name for logging
            table_name: Table name for logging

        Returns:
            Dictionary with drift information:
            - has_drift: bool
            - changes: list of change descriptions
            - column_additions: list of added columns
            - column_removals: list of removed columns
            - column_type_changes: list of type changes
            - constraint_changes: list of constraint changes

        Raises:
            VerificationError: If fail_on_drift is True and drift is detected
        """
        if previous_schema is None:
            self.logger.info(
                "No previous schema found (first archival run)",
                database=database_name,
                table=table_name,
            )
            return {
                "has_drift": False,
                "changes": [],
                "column_additions": [],
                "column_removals": [],
                "column_type_changes": [],
                "constraint_changes": [],
            }

        changes = []
        column_additions = []
        column_removals = []
        column_type_changes = []
        constraint_changes = []

        # Compare columns
        current_columns = {col["name"]: col for col in current_schema.get("columns", [])}
        previous_columns = {col["name"]: col for col in previous_schema.get("columns", [])}

        # Find added columns
        for col_name in current_columns:
            if col_name not in previous_columns:
                column_additions.append(col_name)
                changes.append(f"Column added: {col_name}")

        # Find removed columns
        for col_name in previous_columns:
            if col_name not in current_columns:
                column_removals.append(col_name)
                changes.append(f"Column removed: {col_name}")

        # Find type changes
        for col_name in current_columns:
            if col_name in previous_columns:
                current_col = current_columns[col_name]
                previous_col = previous_columns[col_name]

                # Compare data type
                if current_col.get("data_type") != previous_col.get("data_type"):
                    column_type_changes.append({
                        "column": col_name,
                        "previous_type": previous_col.get("data_type"),
                        "current_type": current_col.get("data_type"),
                    })
                    changes.append(
                        f"Column type changed: {col_name} "
                        f"({previous_col.get('data_type')} -> {current_col.get('data_type')})"
                    )

                # Compare nullable
                if current_col.get("is_nullable") != previous_col.get("is_nullable"):
                    changes.append(
                        f"Column nullable changed: {col_name} "
                        f"({previous_col.get('is_nullable')} -> {current_col.get('is_nullable')})"
                    )

        # Compare primary key
        current_pk = current_schema.get("primary_key")
        previous_pk = previous_schema.get("primary_key")

        if current_pk != previous_pk:
            constraint_changes.append({
                "type": "primary_key",
                "previous": previous_pk,
                "current": current_pk,
            })
            changes.append(
                f"Primary key changed: {previous_pk} -> {current_pk}"
            )

        # Compare foreign keys
        current_fks = {fk["constraint_name"]: fk for fk in current_schema.get("foreign_keys", [])}
        previous_fks = {fk["constraint_name"]: fk for fk in previous_schema.get("foreign_keys", [])}

        # Find added/removed foreign keys
        for fk_name in current_fks:
            if fk_name not in previous_fks:
                constraint_changes.append({
                    "type": "foreign_key_added",
                    "constraint": fk_name,
                    "details": current_fks[fk_name],
                })
                changes.append(f"Foreign key added: {fk_name}")

        for fk_name in previous_fks:
            if fk_name not in current_fks:
                constraint_changes.append({
                    "type": "foreign_key_removed",
                    "constraint": fk_name,
                    "details": previous_fks[fk_name],
                })
                changes.append(f"Foreign key removed: {fk_name}")

        # Compare indexes
        current_indexes = {idx["name"]: idx for idx in current_schema.get("indexes", [])}
        previous_indexes = {idx["name"]: idx for idx in previous_schema.get("indexes", [])}

        for idx_name in current_indexes:
            if idx_name not in previous_indexes:
                changes.append(f"Index added: {idx_name}")

        for idx_name in previous_indexes:
            if idx_name not in current_indexes:
                changes.append(f"Index removed: {idx_name}")

        has_drift = len(changes) > 0

        drift_info = {
            "has_drift": has_drift,
            "changes": changes,
            "column_additions": column_additions,
            "column_removals": column_removals,
            "column_type_changes": column_type_changes,
            "constraint_changes": constraint_changes,
        }

        if has_drift:
            self.logger.warning(
                "Schema drift detected",
                database=database_name,
                table=table_name,
                change_count=len(changes),
                column_additions=len(column_additions),
                column_removals=len(column_removals),
                type_changes=len(column_type_changes),
                constraint_changes=len(constraint_changes),
                changes=changes[:10],  # First 10 changes
            )

            if self.fail_on_drift:
                raise VerificationError(
                    f"Schema drift detected: {len(changes)} change(s) found",
                    context={
                        "database": database_name,
                        "table": table_name,
                        "changes": changes,
                        "column_additions": column_additions,
                        "column_removals": column_removals,
                        "column_type_changes": column_type_changes,
                    },
                )

        else:
            self.logger.debug(
                "No schema drift detected",
                database=database_name,
                table=table_name,
            )

        return drift_info

