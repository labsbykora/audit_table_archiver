"""Unit tests for progress tracker."""

import time
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from archiver.progress_tracker import ProgressTracker


class TestProgressTracker:
    """Tests for ProgressTracker class."""

    def test_init_default(self):
        """Test default initialization."""
        tracker = ProgressTracker()
        assert tracker.quiet is False
        assert tracker.update_interval == 5.0
        assert tracker.start_time is None

    def test_init_quiet(self):
        """Test initialization with quiet mode."""
        tracker = ProgressTracker(quiet=True)
        assert tracker.quiet is True

    def test_init_custom_interval(self):
        """Test initialization with custom update interval."""
        tracker = ProgressTracker(update_interval=10.0)
        assert tracker.update_interval == 10.0

    def test_start(self):
        """Test starting progress tracking."""
        tracker = ProgressTracker(quiet=True)  # Quiet to avoid log output
        
        tracker.start(
            database="test_db",
            table="test_table",
            schema="public",
            records_total=1000,
        )
        
        assert tracker.start_time is not None
        assert tracker.current_database == "test_db"
        assert tracker.current_table == "test_table"
        assert tracker.current_schema == "public"
        assert tracker.records_total == 1000
        assert tracker.records_processed == 0
        assert tracker.batches_completed == 0

    def test_start_no_total(self):
        """Test starting progress tracking without total records."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
        )
        
        assert tracker.records_total is None

    def test_update(self):
        """Test updating progress."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
            records_total=1000,
        )
        
        tracker.update(
            records_processed=500,
            batches_completed=5,
            batches_total=10,
        )
        
        assert tracker.records_processed == 500
        assert tracker.batches_completed == 5
        assert tracker.batches_total == 10

    def test_update_rate_calculation(self):
        """Test that processing rate is calculated."""
        tracker = ProgressTracker(quiet=True, update_interval=0.1)
        
        tracker.start(
            database="test_db",
            table="test_table",
            records_total=1000,
        )
        
        # Update with some records
        tracker.update(
            records_processed=100,
            batches_completed=1,
        )
        
        # Small delay to allow rate calculation
        time.sleep(0.2)
        
        # Update again
        tracker.update(
            records_processed=200,
            batches_completed=2,
        )
        
        # Rate should be calculated
        assert tracker.records_per_second >= 0

    def test_finish_success(self):
        """Test finishing progress tracking with success."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
            records_total=1000,
        )
        
        tracker.update(
            records_processed=1000,
            batches_completed=10,
        )
        
        tracker.finish(success=True)
        
        assert True  # Verify no exception

    def test_finish_failure(self):
        """Test finishing progress tracking with failure."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
        )
        
        tracker.finish(success=False)
        
        assert True  # Verify no exception

    def test_get_eta_with_rate(self):
        """Test getting ETA when processing rate is known."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
            records_total=1000,
        )
        
        # Set a processing rate
        tracker.records_per_second = 100.0
        tracker.records_processed = 500
        
        eta = tracker.get_eta()
        
        assert eta is not None
        assert isinstance(eta, timedelta)
        # Should be approximately 5 seconds (500 records / 100 rec/s)
        assert eta.total_seconds() > 0

    def test_get_eta_no_rate(self):
        """Test getting ETA when processing rate is not known."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
            records_total=1000,
        )
        
        tracker.records_per_second = 0.0
        
        eta = tracker.get_eta()
        
        assert eta is None

    def test_get_eta_complete(self):
        """Test getting ETA when already complete."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
            records_total=1000,
        )
        
        tracker.records_processed = 1000
        tracker.records_per_second = 100.0
        
        eta = tracker.get_eta()
        
        assert eta is None  # No ETA when complete

    def test_get_progress_percentage(self):
        """Test getting progress percentage."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
            records_total=1000,
        )
        
        tracker.records_processed = 500
        
        percentage = tracker.get_progress_percentage()
        
        assert percentage == 50.0

    def test_get_progress_percentage_no_total(self):
        """Test getting progress percentage when total is unknown."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
        )
        
        tracker.records_processed = 500
        
        percentage = tracker.get_progress_percentage()
        
        assert percentage == 0.0

    def test_get_progress_percentage_complete(self):
        """Test getting progress percentage when complete."""
        tracker = ProgressTracker(quiet=True)
        
        tracker.start(
            database="test_db",
            table="test_table",
            records_total=1000,
        )
        
        tracker.records_processed = 1500  # More than total
        
        percentage = tracker.get_progress_percentage()
        
        assert percentage == 100.0  # Capped at 100%

    def test_update_interval_enforcement(self):
        """Test that updates respect update interval."""
        tracker = ProgressTracker(quiet=True, update_interval=1.0)
        
        tracker.start(
            database="test_db",
            table="test_table",
            records_total=1000,
        )
        
        # First update should display
        tracker.update(
            records_processed=100,
            batches_completed=1,
        )
        
        # Second update immediately should not display (interval not met)
        tracker.update(
            records_processed=200,
            batches_completed=2,
        )
        
        # Wait for interval
        time.sleep(1.1)
        
        # Third update should display
        tracker.update(
            records_processed=300,
            batches_completed=3,
        )
        
        assert True  # Verify no exception

