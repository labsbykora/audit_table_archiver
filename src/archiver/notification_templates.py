"""Notification templates for archival events."""

from datetime import datetime, timezone
from typing import Any, Optional


class NotificationTemplate:
    """Template generator for notifications."""

    @staticmethod
    def archive_success(
        database: str,
        table: str,
        schema: str,
        records_archived: int,
        batches_processed: int,
        duration_seconds: float,
        s3_path: Optional[str] = None,
    ) -> tuple[str, str, dict[str, Any]]:
        """Generate notification for successful archival.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            records_archived: Number of records archived
            batches_processed: Number of batches processed
            duration_seconds: Duration in seconds
            s3_path: S3 path where data was archived

        Returns:
            Tuple of (subject, message, metadata)
        """
        subject = f"✅ Archive Success: {database}.{schema}.{table}"
        message = f"""
Archival completed successfully for table {schema}.{table} in database {database}.

**Summary:**
- Records archived: {records_archived:,}
- Batches processed: {batches_processed}
- Duration: {duration_seconds:.1f} seconds
- Rate: {records_archived / duration_seconds:.0f} records/second
"""
        if s3_path:
            message += f"- S3 path: {s3_path}\n"

        metadata = {
            "database": database,
            "table": table,
            "schema": schema,
            "records_archived": records_archived,
            "batches_processed": batches_processed,
            "duration_seconds": duration_seconds,
            "s3_path": s3_path,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return subject, message.strip(), metadata

    @staticmethod
    def archive_failure(
        database: str,
        table: str,
        schema: str,
        error_message: str,
        records_archived: int = 0,
        batches_processed: int = 0,
    ) -> tuple[str, str, dict[str, Any]]:
        """Generate notification for failed archival.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            error_message: Error message
            records_archived: Number of records archived before failure
            batches_processed: Number of batches processed before failure

        Returns:
            Tuple of (subject, message, metadata)
        """
        subject = f"❌ Archive Failure: {database}.{schema}.{table}"
        message = f"""
Archival failed for table {schema}.{table} in database {database}.

**Error:**
{error_message}

**Progress before failure:**
- Records archived: {records_archived:,}
- Batches processed: {batches_processed}
"""
        metadata = {
            "database": database,
            "table": table,
            "schema": schema,
            "error_message": error_message,
            "records_archived": records_archived,
            "batches_processed": batches_processed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return subject, message.strip(), metadata

    @staticmethod
    def archive_start(
        database: str,
        table: str,
        schema: str,
        records_eligible: int,
    ) -> tuple[str, str, dict[str, Any]]:
        """Generate notification for archival start.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            records_eligible: Number of records eligible for archival

        Returns:
            Tuple of (subject, message, metadata)
        """
        subject = f"🔄 Archive Started: {database}.{schema}.{table}"
        message = f"""
Archival started for table {schema}.{table} in database {database}.

**Eligible records:** {records_eligible:,}
"""
        metadata = {
            "database": database,
            "table": table,
            "schema": schema,
            "records_eligible": records_eligible,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return subject, message.strip(), metadata

    @staticmethod
    def threshold_violation(
        metric: str,
        threshold: float,
        actual_value: float,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> tuple[str, str, dict[str, Any]]:
        """Generate notification for threshold violation.

        Args:
            metric: Metric name (e.g., "error_rate", "duration")
            threshold: Threshold value
            actual_value: Actual value that violated threshold
            database: Database name (optional)
            table: Table name (optional)

        Returns:
            Tuple of (subject, message, metadata)
        """
        subject = f"⚠️ Threshold Violation: {metric}"
        message = f"""
Threshold violation detected for metric: {metric}

**Threshold:** {threshold}
**Actual Value:** {actual_value}
"""
        if database:
            message += f"**Database:** {database}\n"
        if table:
            message += f"**Table:** {table}\n"

        metadata = {
            "metric": metric,
            "threshold": threshold,
            "actual_value": actual_value,
            "database": database,
            "table": table,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return subject, message.strip(), metadata

    @staticmethod
    def digest_summary(
        total_runs: int,
        successful_runs: int,
        failed_runs: int,
        total_records_archived: int,
        total_duration_seconds: float,
        databases: list[str],
        errors: Optional[list[dict[str, Any]]] = None,
    ) -> tuple[str, str, dict[str, Any]]:
        """Generate daily digest summary notification.

        Args:
            total_runs: Total number of archival runs
            successful_runs: Number of successful runs
            failed_runs: Number of failed runs
            total_records_archived: Total records archived
            total_duration_seconds: Total duration in seconds
            databases: List of databases processed
            errors: List of error details (optional)

        Returns:
            Tuple of (subject, message, metadata)
        """
        subject = f"📊 Daily Archive Summary"
        message = f"""
Daily archival summary for {datetime.now(timezone.utc).strftime('%Y-%m-%d')}

**Runs:**
- Total: {total_runs}
- Successful: {successful_runs}
- Failed: {failed_runs}
- Success Rate: {(successful_runs / total_runs * 100) if total_runs > 0 else 0:.1f}%

**Performance:**
- Total records archived: {total_records_archived:,}
- Total duration: {total_duration_seconds:.1f} seconds
- Average rate: {(total_records_archived / total_duration_seconds) if total_duration_seconds > 0 else 0:.0f} records/second

**Databases processed:** {len(databases)}
"""
        if errors:
            message += f"\n**Errors ({len(errors)}):**\n"
            for i, error in enumerate(errors[:5], 1):  # Show first 5 errors
                message += f"{i}. {error.get('database', 'unknown')}.{error.get('table', 'unknown')}: {error.get('error', 'Unknown error')}\n"
            if len(errors) > 5:
                message += f"... and {len(errors) - 5} more errors\n"

        metadata = {
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "total_records_archived": total_records_archived,
            "total_duration_seconds": total_duration_seconds,
            "databases": databases,
            "errors": errors or [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return subject, message.strip(), metadata

