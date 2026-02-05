"""Prometheus metrics for monitoring archival operations."""

import time
from typing import Optional

import structlog
from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    start_http_server,
)

from utils.logging import get_logger


class ArchiverMetrics:
    """Prometheus metrics for the archiver."""

    def __init__(
        self,
        logger: Optional[structlog.BoundLogger] = None,
        registry: Optional[CollectorRegistry] = None,
    ) -> None:
        """Initialize metrics.

        Args:
            logger: Optional logger instance
            registry: Optional Prometheus registry (defaults to global REGISTRY)
        """
        self.logger = logger or get_logger("metrics")
        self.registry = registry or REGISTRY

        # Counters: Total counts (monotonically increasing)
        self.records_archived_total = Counter(
            "archiver_records_archived_total",
            "Total number of records archived",
            ["database", "table", "schema"],
            registry=self.registry,
        )

        self.bytes_uploaded_total = Counter(
            "archiver_bytes_uploaded_total",
            "Total bytes uploaded to S3",
            ["database", "table", "schema"],
            registry=self.registry,
        )

        self.batches_processed_total = Counter(
            "archiver_batches_processed_total",
            "Total number of batches processed",
            ["database", "table", "schema"],
            registry=self.registry,
        )

        self.runs_total = Counter(
            "archiver_runs_total",
            "Total number of archival runs",
            ["status"],  # success, failure, partial
            registry=self.registry,
        )

        self.errors_total = Counter(
            "archiver_errors_total",
            "Total number of errors",
            ["type", "database", "table"],  # type: network, database, s3, verification, etc.
            registry=self.registry,
        )

        # Histograms: Duration measurements
        self.duration_seconds = Histogram(
            "archiver_duration_seconds",
            "Duration of operations in seconds",
            ["database", "table", "schema", "phase"],  # phase: query, serialize, compress, upload, verify, delete, vacuum
            buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0],
            registry=self.registry,
        )

        self.batch_processing_rate = Histogram(
            "archiver_batch_processing_rate",
            "Batch processing rate (records per second)",
            ["database", "table", "schema"],
            buckets=[100, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000],
            registry=self.registry,
        )

        # Gauges: Current state (can go up or down)
        self.current_state = Gauge(
            "archiver_current_state",
            "Current archiver state (0=idle, 1=running, 2=failed)",
            registry=self.registry,
        )

        self.memory_usage_bytes = Gauge(
            "archiver_memory_usage_bytes",
            "Current memory usage in bytes",
            registry=self.registry,
        )

        self.database_connections = Gauge(
            "archiver_database_connections",
            "Current number of database connections",
            ["database"],
            registry=self.registry,
        )

        self.last_success_timestamp = Gauge(
            "archiver_last_success_timestamp",
            "Unix timestamp of last successful archival run",
            registry=self.registry,
        )

        self.space_reclaimed_bytes = Gauge(
            "archiver_space_reclaimed_bytes",
            "Space reclaimed in bytes",
            ["database", "table", "schema"],
            registry=self.registry,
        )

        self.current_batch_progress = Gauge(
            "archiver_current_batch_progress",
            "Current batch progress (0.0 to 1.0)",
            ["database", "table", "schema"],
            registry=self.registry,
        )

        self.records_eligible = Gauge(
            "archiver_records_eligible",
            "Number of records eligible for archival",
            ["database", "table", "schema"],
            registry=self.registry,
        )

        # Track timing for phases
        self._phase_timers: dict[str, float] = {}

    def record_archived(
        self,
        database: str,
        table: str,
        schema: str,
        count: int,
        bytes_uploaded: int,
    ) -> None:
        """Record archived records and bytes.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            count: Number of records archived
            bytes_uploaded: Bytes uploaded to S3
        """
        self.records_archived_total.labels(
            database=database, table=table, schema=schema
        ).inc(count)
        self.bytes_uploaded_total.labels(
            database=database, table=table, schema=schema
        ).inc(bytes_uploaded)

    def record_batch_processed(
        self,
        database: str,
        table: str,
        schema: str,
        record_count: int,
        duration_seconds: float,
    ) -> None:
        """Record batch processing.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            record_count: Number of records in batch
            duration_seconds: Time taken to process batch
        """
        self.batches_processed_total.labels(
            database=database, table=table, schema=schema
        ).inc()

        # Calculate and record processing rate
        if duration_seconds > 0:
            rate = record_count / duration_seconds
            self.batch_processing_rate.labels(
                database=database, table=table, schema=schema
            ).observe(rate)

    def record_duration(
        self,
        database: str,
        table: str,
        schema: str,
        phase: str,
        duration_seconds: float,
    ) -> None:
        """Record duration for a specific phase.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            phase: Phase name (query, serialize, compress, upload, verify, delete, vacuum)
            duration_seconds: Duration in seconds
        """
        self.duration_seconds.labels(
            database=database, table=table, schema=schema, phase=phase
        ).observe(duration_seconds)

    def record_error(
        self,
        error_type: str,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        """Record an error.

        Args:
            error_type: Error type (network, database, s3, verification, etc.)
            database: Database name (optional)
            table: Table name (optional)
        """
        self.errors_total.labels(
            type=error_type,
            database=database or "unknown",
            table=table or "unknown",
        ).inc()

    def record_run_status(self, status: str) -> None:
        """Record run status.

        Args:
            status: Run status (success, failure, partial)
        """
        self.runs_total.labels(status=status).inc()
        if status == "success":
            self.last_success_timestamp.set(time.time())
            self.current_state.set(0)  # idle
        elif status == "failure":
            self.current_state.set(2)  # failed
        elif status == "partial":
            self.current_state.set(1)  # running (partial success)

    def set_state(self, state: int) -> None:
        """Set current archiver state.

        Args:
            state: State (0=idle, 1=running, 2=failed)
        """
        self.current_state.set(state)

    def set_memory_usage(self, bytes_used: int) -> None:
        """Set current memory usage.

        Args:
            bytes_used: Memory usage in bytes
        """
        self.memory_usage_bytes.set(bytes_used)

    def set_database_connections(self, database: str, count: int) -> None:
        """Set current database connection count.

        Args:
            database: Database name
            count: Number of connections
        """
        self.database_connections.labels(database=database).set(count)

    def set_space_reclaimed(
        self, database: str, table: str, schema: str, bytes_reclaimed: int
    ) -> None:
        """Set space reclaimed.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            bytes_reclaimed: Bytes reclaimed
        """
        self.space_reclaimed_bytes.labels(
            database=database, table=table, schema=schema
        ).set(bytes_reclaimed)

    def set_batch_progress(
        self, database: str, table: str, schema: str, progress: float
    ) -> None:
        """Set current batch progress.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            progress: Progress (0.0 to 1.0)
        """
        self.current_batch_progress.labels(
            database=database, table=table, schema=schema
        ).set(progress)

    def set_records_eligible(
        self, database: str, table: str, schema: str, count: int
    ) -> None:
        """Set number of records eligible for archival.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            count: Number of eligible records
        """
        self.records_eligible.labels(
            database=database, table=table, schema=schema
        ).set(count)

    def start_phase_timer(self, phase: str) -> None:
        """Start timing a phase.

        Args:
            phase: Phase name
        """
        self._phase_timers[phase] = time.time()

    def stop_phase_timer(
        self, database: str, table: str, schema: str, phase: str
    ) -> Optional[float]:
        """Stop timing a phase and record the duration.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            phase: Phase name

        Returns:
            Duration in seconds, or None if timer was not started
        """
        if phase not in self._phase_timers:
            return None

        duration = time.time() - self._phase_timers[phase]
        del self._phase_timers[phase]

        self.record_duration(database, table, schema, phase, duration)
        return duration

    def get_metrics(self) -> bytes:
        """Get Prometheus metrics in text format.

        Returns:
            Metrics in Prometheus text format
        """
        return generate_latest(self.registry)

    def start_metrics_server(self, port: int = 8000) -> None:
        """Start HTTP server for Prometheus metrics.

        Args:
            port: Port to listen on (default: 8000)
        """
        try:
            start_http_server(port, registry=self.registry)
            self.logger.info(
                "Prometheus metrics server started",
                port=port,
                endpoint=f"http://localhost:{port}/metrics",
            )
        except Exception as e:
            self.logger.error(
                "Failed to start metrics server",
                port=port,
                error=str(e),
            )
            raise

