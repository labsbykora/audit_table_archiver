"""Unit tests for notification templates."""

from datetime import datetime, timezone

import pytest

from archiver.notification_templates import NotificationTemplate


class TestNotificationTemplate:
    """Tests for NotificationTemplate."""

    def test_archive_success(self):
        """Test archive success template."""
        subject, message, metadata = NotificationTemplate.archive_success(
            database="test_db",
            table="test_table",
            schema="public",
            records_archived=1000,
            batches_processed=5,
            duration_seconds=120.5,
            s3_path="s3://bucket/path",
        )

        assert "✅" in subject
        assert "test_db" in subject
        assert "test_table" in subject
        assert ("1000" in message or "1,000" in message)  # May be formatted with commas
        assert "5" in message
        assert "120.5" in message
        assert "s3://bucket/path" in message
        assert metadata["database"] == "test_db"
        assert metadata["records_archived"] == 1000
        assert metadata["batches_processed"] == 5
        assert metadata["duration_seconds"] == 120.5

    def test_archive_success_no_s3_path(self):
        """Test archive success template without S3 path."""
        subject, message, metadata = NotificationTemplate.archive_success(
            database="test_db",
            table="test_table",
            schema="public",
            records_archived=1000,
            batches_processed=5,
            duration_seconds=120.5,
        )

        assert "s3://" not in message
        assert metadata["s3_path"] is None

    def test_archive_failure(self):
        """Test archive failure template."""
        subject, message, metadata = NotificationTemplate.archive_failure(
            database="test_db",
            table="test_table",
            schema="public",
            error_message="Connection timeout",
            records_archived=500,
            batches_processed=2,
        )

        assert "❌" in subject
        assert "test_db" in subject
        assert "Connection timeout" in message
        assert "500" in message
        assert "2" in message
        assert metadata["error_message"] == "Connection timeout"
        assert metadata["records_archived"] == 500

    def test_archive_start(self):
        """Test archive start template."""
        subject, message, metadata = NotificationTemplate.archive_start(
            database="test_db",
            table="test_table",
            schema="public",
            records_eligible=5000,
        )

        assert "🔄" in subject
        assert "test_db" in subject
        assert ("5000" in message or "5,000" in message)  # May be formatted with commas
        assert metadata["records_eligible"] == 5000

    def test_threshold_violation(self):
        """Test threshold violation template."""
        subject, message, metadata = NotificationTemplate.threshold_violation(
            metric="error_rate",
            threshold=0.05,
            actual_value=0.15,
            database="test_db",
            table="test_table",
        )

        assert "⚠️" in subject
        assert "error_rate" in subject
        assert "0.05" in message
        assert "0.15" in message
        assert "test_db" in message
        assert metadata["metric"] == "error_rate"
        assert metadata["threshold"] == 0.05
        assert metadata["actual_value"] == 0.15

    def test_threshold_violation_no_db_table(self):
        """Test threshold violation template without database/table."""
        subject, message, metadata = NotificationTemplate.threshold_violation(
            metric="error_rate",
            threshold=0.05,
            actual_value=0.15,
        )

        assert "test_db" not in message
        assert metadata["database"] is None
        assert metadata["table"] is None

    def test_digest_summary(self):
        """Test digest summary template."""
        subject, message, metadata = NotificationTemplate.digest_summary(
            total_runs=10,
            successful_runs=8,
            failed_runs=2,
            total_records_archived=50000,
            total_duration_seconds=600.0,
            databases=["db1", "db2"],
            errors=[
                {"database": "db1", "table": "table1", "error": "Error 1"},
                {"database": "db2", "table": "table2", "error": "Error 2"},
            ],
        )

        assert "📊" in subject
        assert "10" in message
        assert "8" in message
        assert "2" in message
        assert ("50000" in message or "50,000" in message)  # May be formatted with commas
        assert "600.0" in message
        assert "Error 1" in message
        assert "Error 2" in message
        assert metadata["total_runs"] == 10
        assert metadata["successful_runs"] == 8
        assert metadata["failed_runs"] == 2
        assert len(metadata["databases"]) == 2
        assert len(metadata["errors"]) == 2

    def test_digest_summary_no_errors(self):
        """Test digest summary template without errors."""
        subject, message, metadata = NotificationTemplate.digest_summary(
            total_runs=10,
            successful_runs=10,
            failed_runs=0,
            total_records_archived=50000,
            total_duration_seconds=600.0,
            databases=["db1"],
        )

        assert "Error" not in message
        assert metadata["errors"] == []

    def test_digest_summary_many_errors(self):
        """Test digest summary template with many errors (should show first 5)."""
        errors = [
            {"database": f"db{i}", "table": f"table{i}", "error": f"Error {i}"}
            for i in range(10)
        ]

        subject, message, metadata = NotificationTemplate.digest_summary(
            total_runs=10,
            successful_runs=0,
            failed_runs=10,
            total_records_archived=0,
            total_duration_seconds=1.0,  # Use non-zero to avoid division by zero
            databases=["db1"],
            errors=errors,
        )

        # Should show first 5 errors and mention "and 5 more"
        assert "Error 0" in message
        assert "Error 4" in message
        assert "Error 5" not in message  # Should not show 6th error
        assert "and 5 more" in message
        assert len(metadata["errors"]) == 10  # All errors in metadata

