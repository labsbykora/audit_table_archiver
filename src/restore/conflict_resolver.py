"""Conflict detection and resolution for restore operations."""

from typing import Any, Optional

import structlog

from archiver.exceptions import ArchiverError
from utils.logging import get_logger


class ConflictReport:
    """Represents a conflict detection report."""

    def __init__(
        self,
        conflicts: list[dict[str, Any]],
        total_conflicts: int,
        conflict_types: dict[str, int],
    ) -> None:
        """Initialize conflict report.

        Args:
            conflicts: List of conflict details
            total_conflicts: Total number of conflicts
            conflict_types: Count of conflicts by type
        """
        self.conflicts = conflicts
        self.total_conflicts = total_conflicts
        self.conflict_types = conflict_types

    @property
    def has_conflicts(self) -> bool:
        """Check if there are any conflicts."""
        return self.total_conflicts > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_conflicts": self.total_conflicts,
            "conflict_types": self.conflict_types,
            "conflicts": self.conflicts,
        }

    def to_string(self) -> str:
        """Generate human-readable report."""
        lines = [f"Conflict Report: {self.total_conflicts} conflict(s) detected"]
        lines.append("=" * 60)

        if self.conflict_types:
            lines.append("\nConflict Types:")
            for conflict_type, count in self.conflict_types.items():
                lines.append(f"  {conflict_type}: {count}")

        if self.conflicts:
            lines.append("\nSample Conflicts (first 10):")
            for i, conflict in enumerate(self.conflicts[:10], 1):
                lines.append(
                    f"  {i}. {conflict.get('type', 'unknown')}: {conflict.get('description', 'N/A')}"
                )

        if len(self.conflicts) > 10:
            lines.append(f"\n... and {len(self.conflicts) - 10} more conflicts")

        return "\n".join(lines)


class ConflictDetector:
    """Detects conflicts before restore operations."""

    def __init__(
        self,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize conflict detector.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("conflict_detector")

    async def detect_conflicts(
        self,
        records: list[dict[str, Any]],
        primary_key: str,
        schema: str,
        table: str,
        db_manager: Any,  # DatabaseManager
    ) -> ConflictReport:
        """Detect conflicts between records to restore and existing database records.

        Args:
            records: Records to restore
            primary_key: Primary key column name
            schema: Schema name
            table: Table name
            db_manager: Database manager instance

        Returns:
            ConflictReport with detected conflicts
        """
        if not records:
            return ConflictReport(conflicts=[], total_conflicts=0, conflict_types={})

        # Extract primary key values from records
        pk_values = [record.get(primary_key) for record in records if primary_key in record]
        if not pk_values:
            self.logger.warning(
                "No primary key values found in records",
                schema=schema,
                table=table,
                primary_key=primary_key,
            )
            return ConflictReport(conflicts=[], total_conflicts=0, conflict_types={})

        # Query database for existing records with these primary keys
        query = f"""
            SELECT {primary_key}
            FROM {schema}.{table}
            WHERE {primary_key} = ANY($1)
        """

        try:
            existing_pks = await db_manager.fetch(query, pk_values)
            existing_pk_set = {row[primary_key] for row in existing_pks if primary_key in row}

            conflicts = []
            conflict_types: dict[str, int] = {}

            for record in records:
                pk_value = record.get(primary_key)
                if pk_value in existing_pk_set:
                    conflict = {
                        "type": "primary_key_exists",
                        "primary_key": primary_key,
                        "primary_key_value": pk_value,
                        "description": f"Record with {primary_key}={pk_value} already exists",
                    }
                    conflicts.append(conflict)
                    conflict_types["primary_key_exists"] = (
                        conflict_types.get("primary_key_exists", 0) + 1
                    )

            self.logger.debug(
                "Conflict detection completed",
                schema=schema,
                table=table,
                total_conflicts=len(conflicts),
                total_records=len(records),
            )

            return ConflictReport(
                conflicts=conflicts,
                total_conflicts=len(conflicts),
                conflict_types=conflict_types,
            )

        except Exception as e:
            self.logger.error(
                "Failed to detect conflicts",
                schema=schema,
                table=table,
                error=str(e),
            )
            # Return empty report on error (non-fatal)
            return ConflictReport(conflicts=[], total_conflicts=0, conflict_types={})


class ConflictResolver:
    """Resolves conflicts during restore operations."""

    def __init__(
        self,
        strategy: str = "skip",
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize conflict resolver.

        Args:
            strategy: Conflict resolution strategy (skip, overwrite, fail, merge, upsert)
            logger: Optional logger instance
        """
        if strategy not in ("skip", "overwrite", "fail", "merge", "upsert"):
            raise ValueError(f"Unknown conflict strategy: {strategy}")

        self.strategy = strategy
        self.logger = logger or get_logger("conflict_resolver")

    def should_skip(self, conflict_report: ConflictReport) -> bool:
        """Determine if restore should be skipped based on conflict report.

        Args:
            conflict_report: Conflict detection report

        Returns:
            True if restore should be skipped, False otherwise
        """
        if not conflict_report.has_conflicts:
            return False

        if self.strategy == "fail":
            return True  # Fail means skip the restore
        elif self.strategy == "skip":
            # Skip strategy means skip conflicting records, not the whole restore
            return False
        else:
            # Other strategies (overwrite, merge, upsert) proceed with restore
            return False

    def filter_conflicting_records(
        self,
        records: list[dict[str, Any]],
        conflict_report: ConflictReport,
        primary_key: str,
    ) -> list[dict[str, Any]]:
        """Filter out conflicting records based on strategy.

        Args:
            records: Records to restore
            conflict_report: Conflict detection report
            primary_key: Primary key column name

        Returns:
            Filtered list of records
        """
        if not conflict_report.has_conflicts or self.strategy != "skip":
            return records

        # Extract conflicting primary key values
        conflicting_pks = {
            conflict["primary_key_value"]
            for conflict in conflict_report.conflicts
            if conflict.get("type") == "primary_key_exists"
        }

        # Filter out conflicting records
        filtered = [record for record in records if record.get(primary_key) not in conflicting_pks]

        self.logger.debug(
            "Filtered conflicting records",
            original_count=len(records),
            filtered_count=len(filtered),
            conflicts_skipped=len(conflicting_pks),
        )

        return filtered

    def validate_strategy(
        self,
        conflict_report: ConflictReport,
    ) -> None:
        """Validate that the conflict resolution strategy can handle detected conflicts.

        Args:
            conflict_report: Conflict detection report

        Raises:
            ArchiverError: If strategy cannot handle conflicts
        """
        if not conflict_report.has_conflicts:
            return

        if self.strategy == "fail":
            raise ArchiverError(
                f"Conflicts detected and strategy is 'fail': {conflict_report.total_conflicts} conflict(s)",
                context={
                    "total_conflicts": conflict_report.total_conflicts,
                    "conflict_types": conflict_report.conflict_types,
                },
            )

        # Other strategies can proceed
        self.logger.debug(
            "Strategy validation passed",
            strategy=self.strategy,
            conflicts=conflict_report.total_conflicts,
        )
