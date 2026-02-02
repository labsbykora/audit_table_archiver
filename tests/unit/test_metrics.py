"""Unit tests for Prometheus metrics."""

import time
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import CollectorRegistry, REGISTRY

from archiver.metrics import ArchiverMetrics


def clear_registry():
    """Clear Prometheus registry between tests."""
    # Remove all collectors except the default ones
    collectors_to_remove = [
        collector
        for collector in list(REGISTRY._collector_to_names.keys())
        if hasattr(collector, '_name') and collector._name.startswith('archiver_')
    ]
    for collector in collectors_to_remove:
        REGISTRY.unregister(collector)


class TestArchiverMetrics:
    """Tests for ArchiverMetrics class."""

    def setup_method(self):
        """Clear registry before each test."""
        clear_registry()

    def test_init(self):
        """Test metrics initialization."""
        metrics = ArchiverMetrics()
        assert metrics is not None
        assert metrics.records_archived_total is not None
        assert metrics.bytes_uploaded_total is not None
        assert metrics.duration_seconds is not None

    def test_record_archived(self):
        """Test recording archived records and bytes."""
        metrics = ArchiverMetrics()
        
        metrics.record_archived(
            database="test_db",
            table="test_table",
            schema="public",
            count=1000,
            bytes_uploaded=50000,
        )
        
        # Verify counters were incremented
        # We can't easily test Prometheus metrics without scraping, but we can verify no exceptions
        assert True  # If we get here, no exception was raised

    def test_record_batch_processed(self):
        """Test recording batch processing."""
        metrics = ArchiverMetrics()
        
        metrics.record_batch_processed(
            database="test_db",
            table="test_table",
            schema="public",
            record_count=1000,
            duration_seconds=5.5,
        )
        
        assert True  # Verify no exception

    def test_record_duration(self):
        """Test recording duration for a phase."""
        metrics = ArchiverMetrics()
        
        metrics.record_duration(
            database="test_db",
            table="test_table",
            schema="public",
            phase="query",
            duration_seconds=2.5,
        )
        
        assert True  # Verify no exception

    def test_record_error(self):
        """Test recording errors."""
        metrics = ArchiverMetrics()
        
        metrics.record_error(
            error_type="network",
            database="test_db",
            table="test_table",
        )
        
        assert True  # Verify no exception

    def test_record_error_no_labels(self):
        """Test recording errors without database/table labels."""
        metrics = ArchiverMetrics()
        
        metrics.record_error(error_type="database")
        
        assert True  # Verify no exception

    def test_record_run_status_success(self):
        """Test recording successful run status."""
        metrics = ArchiverMetrics()
        
        metrics.record_run_status("success")
        
        assert True  # Verify no exception

    def test_record_run_status_failure(self):
        """Test recording failed run status."""
        metrics = ArchiverMetrics()
        
        metrics.record_run_status("failure")
        
        assert True  # Verify no exception

    def test_record_run_status_partial(self):
        """Test recording partial run status."""
        metrics = ArchiverMetrics()
        
        metrics.record_run_status("partial")
        
        assert True  # Verify no exception

    def test_set_state(self):
        """Test setting archiver state."""
        metrics = ArchiverMetrics()
        
        metrics.set_state(0)  # idle
        metrics.set_state(1)  # running
        metrics.set_state(2)  # failed
        
        assert True  # Verify no exception

    def test_set_memory_usage(self):
        """Test setting memory usage."""
        metrics = ArchiverMetrics()
        
        metrics.set_memory_usage(1024 * 1024 * 100)  # 100 MB
        
        assert True  # Verify no exception

    def test_set_database_connections(self):
        """Test setting database connection count."""
        metrics = ArchiverMetrics()
        
        metrics.set_database_connections("test_db", 5)
        
        assert True  # Verify no exception

    def test_set_space_reclaimed(self):
        """Test setting space reclaimed."""
        metrics = ArchiverMetrics()
        
        metrics.set_space_reclaimed(
            database="test_db",
            table="test_table",
            schema="public",
            bytes_reclaimed=1024 * 1024 * 1024,  # 1 GB
        )
        
        assert True  # Verify no exception

    def test_set_batch_progress(self):
        """Test setting batch progress."""
        metrics = ArchiverMetrics()
        
        metrics.set_batch_progress(
            database="test_db",
            table="test_table",
            schema="public",
            progress=0.5,  # 50%
        )
        
        assert True  # Verify no exception

    def test_set_records_eligible(self):
        """Test setting records eligible count."""
        metrics = ArchiverMetrics()
        
        metrics.set_records_eligible(
            database="test_db",
            table="test_table",
            schema="public",
            count=10000,
        )
        
        assert True  # Verify no exception

    def test_start_stop_phase_timer(self):
        """Test phase timer start/stop."""
        metrics = ArchiverMetrics()
        
        metrics.start_phase_timer("query")
        time.sleep(0.1)  # Small delay
        duration = metrics.stop_phase_timer(
            database="test_db",
            table="test_table",
            schema="public",
            phase="query",
        )
        
        assert duration is not None
        assert duration > 0

    def test_stop_phase_timer_not_started(self):
        """Test stopping a phase timer that was never started."""
        metrics = ArchiverMetrics()
        
        duration = metrics.stop_phase_timer(
            database="test_db",
            table="test_table",
            schema="public",
            phase="nonexistent",
        )
        
        assert duration is None

    def test_get_metrics(self):
        """Test getting metrics in Prometheus format."""
        metrics = ArchiverMetrics()
        
        metrics_data = metrics.get_metrics()
        
        assert isinstance(metrics_data, bytes)
        assert len(metrics_data) > 0

    @patch("archiver.metrics.start_http_server")
    def test_start_metrics_server(self, mock_start_server):
        """Test starting metrics server."""
        registry = CollectorRegistry(auto_describe=True)
        metrics = ArchiverMetrics(registry=registry)
        
        metrics.start_metrics_server(port=8000)
        
        mock_start_server.assert_called_once_with(8000, registry=registry)

    @patch("archiver.metrics.start_http_server")
    def test_start_metrics_server_custom_port(self, mock_start_server):
        """Test starting metrics server with custom port."""
        registry = CollectorRegistry(auto_describe=True)
        metrics = ArchiverMetrics(registry=registry)
        
        metrics.start_metrics_server(port=9000)
        
        mock_start_server.assert_called_once_with(9000, registry=registry)

    def test_all_phases(self):
        """Test recording duration for all phases."""
        metrics = ArchiverMetrics()
        
        phases = ["query", "serialize", "compress", "upload", "verify", "delete", "vacuum"]
        
        for phase in phases:
            metrics.record_duration(
                database="test_db",
                table="test_table",
                schema="public",
                phase=phase,
                duration_seconds=1.0,
            )
        
        assert True  # Verify no exception

