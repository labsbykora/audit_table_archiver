"""Real-time progress tracking and ETA calculation."""

import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import structlog

from utils.logging import get_logger


class ProgressTracker:
    """Tracks and displays real-time progress during archival."""

    def __init__(
        self,
        quiet: bool = False,
        update_interval: float = 5.0,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize progress tracker.

        Args:
            quiet: If True, suppress progress output (for cron)
            update_interval: Minimum seconds between progress updates
            logger: Optional logger instance
        """
        self.quiet = quiet
        self.update_interval = update_interval
        self.logger = logger or get_logger("progress")

        # Progress state
        self.start_time: Optional[datetime] = None
        self.last_update_time: Optional[float] = None
        self.current_database: Optional[str] = None
        self.current_table: Optional[str] = None
        self.current_schema: Optional[str] = None

        # Batch progress
        self.batches_completed: int = 0
        self.batches_total: Optional[int] = None
        self.records_processed: int = 0
        self.records_total: Optional[int] = None

        # Track both current run and overall progress
        self.records_processed_this_run: int = 0
        self.records_total_this_run: Optional[int] = None
        self.records_processed_total: int = 0
        self.records_total_overall: Optional[int] = None
        self.initial_records_from_checkpoint: int = 0

        # Rate tracking
        self.records_per_second: float = 0.0
        self._last_record_count: int = 0
        self._last_rate_calc_time: Optional[float] = None

    def start(
        self,
        database: Optional[str] = None,
        table: Optional[str] = None,
        schema: Optional[str] = None,
        records_total: Optional[int] = None,
        records_total_this_run: Optional[int] = None,
        initial_records_processed: int = 0,
    ) -> None:
        """Start tracking progress.

        Args:
            database: Current database name
            table: Current table name
            schema: Current schema name
            records_total: Total records to process overall (including checkpoint, optional)
            records_total_this_run: Total records to process in this run only (optional)
            initial_records_processed: Initial records processed (from checkpoint, for overall tracking)
        """
        self.start_time = datetime.now(timezone.utc)
        self.last_update_time = time.time()
        self.current_database = database
        self.current_table = table
        self.current_schema = schema
        self.records_total = records_total  # Overall total (for backward compatibility)
        self.records_total_overall = records_total
        self.records_total_this_run = records_total_this_run
        self.initial_records_from_checkpoint = initial_records_processed
        self.batches_completed = 0
        self.records_processed = initial_records_processed  # Overall (includes checkpoint)
        self.records_processed_total = initial_records_processed
        self.records_processed_this_run = 0  # This run only
        self.records_per_second = 0.0
        self._last_record_count = initial_records_processed
        self._last_rate_calc_time = time.time()

        if not self.quiet:
            self._display_start()

    def update(
        self,
        records_processed: int,
        batches_completed: int,
        batches_total: Optional[int] = None,
        records_processed_this_run: Optional[int] = None,
    ) -> None:
        """Update progress.

        Args:
            records_processed: Number of records processed so far (overall, including checkpoint)
            batches_completed: Number of batches completed
            batches_total: Total number of batches (optional)
            records_processed_this_run: Number of records processed in this run only (optional, calculated if not provided)
        """
        self.records_processed = records_processed
        self.records_processed_total = records_processed

        # Calculate this run's progress if not provided
        if records_processed_this_run is not None:
            self.records_processed_this_run = records_processed_this_run
        else:
            self.records_processed_this_run = records_processed - self.initial_records_from_checkpoint

        self.batches_completed = batches_completed
        if batches_total is not None:
            self.batches_total = batches_total

        # Calculate processing rate (based on overall progress)
        current_time = time.time()
        if self._last_rate_calc_time:
            time_delta = current_time - self._last_rate_calc_time
            if time_delta > 0:
                records_delta = records_processed - self._last_record_count
                self.records_per_second = records_delta / time_delta
                self._last_record_count = records_processed
                self._last_rate_calc_time = current_time

        # Update display if enough time has passed
        if not self.quiet:
            if (
                self.last_update_time is None
                or (current_time - self.last_update_time) >= self.update_interval
            ):
                self._display_progress()
                self.last_update_time = current_time

    def finish(self, success: bool = True) -> None:
        """Finish tracking and display final summary.

        Args:
            success: Whether the operation was successful
        """
        if not self.quiet:
            self._display_finish(success)

    def _display_start(self) -> None:
        """Display start message."""
        if self.current_table:
            self.logger.info(
                "Starting archival",
                database=self.current_database,
                table=self.current_table,
                schema=self.current_schema,
                records_total=self.records_total,
            )
        else:
            self.logger.info("Starting archival process")

    def _display_progress(self) -> None:
        """Display current progress."""
        if not self.start_time:
            return

        # Calculate percentages
        percentage_overall = 0.0
        if self.records_total_overall and self.records_total_overall > 0:
            percentage_overall = min(100.0, (self.records_processed_total / self.records_total_overall) * 100)

        percentage_this_run = 0.0
        if self.records_total_this_run and self.records_total_this_run > 0:
            percentage_this_run = min(100.0, (self.records_processed_this_run / self.records_total_this_run) * 100)

        # Calculate ETA (based on overall progress)
        eta_seconds: Optional[float] = None
        eta_str = "N/A"
        if self.records_per_second > 0 and self.records_total_overall:
            remaining = self.records_total_overall - self.records_processed_total
            if remaining > 0:
                eta_seconds = remaining / self.records_per_second
                eta = timedelta(seconds=int(eta_seconds))
                eta_str = str(eta)

        # Build progress message
        progress_parts = []
        if self.current_database:
            progress_parts.append(f"DB: {self.current_database}")
        if self.current_table:
            progress_parts.append(f"Table: {self.current_table}")

        # Show both current run and overall progress
        if self.records_total_this_run is not None and self.records_total_overall is not None:
            # Both metrics available - show both
            progress_parts.append(
                f"Current: {self.records_processed_this_run:,}/{self.records_total_this_run:,} ({percentage_this_run:.1f}%)"
            )
            progress_parts.append(
                f"Overall: {self.records_processed_total:,}/{self.records_total_overall:,} ({percentage_overall:.1f}%)"
            )
        elif self.records_total_overall is not None:
            # Only overall available (backward compatibility)
            progress_parts.append(f"Records: {self.records_processed_total:,}")
            progress_parts.append(f"/ {self.records_total_overall:,} ({percentage_overall:.1f}%)")
        else:
            # No totals available
            progress_parts.append(f"Records: {self.records_processed_total:,}")

        progress_parts.append(f"Batches: {self.batches_completed}")
        if self.batches_total:
            progress_parts.append(f"/ {self.batches_total}")
        progress_parts.append(f"Rate: {self.records_per_second:.0f} rec/s")
        progress_parts.append(f"ETA: {eta_str}")

        self.logger.info("Progress", message=" | ".join(progress_parts))

    def _display_finish(self, success: bool) -> None:
        """Display finish message."""
        if not self.start_time:
            return

        elapsed = datetime.now(timezone.utc) - self.start_time
        elapsed_str = str(elapsed).split(".")[0]  # Remove microseconds

        status = "completed" if success else "failed"
        self.logger.info(
            f"Archival {status}",
            database=self.current_database,
            table=self.current_table,
            records_processed=self.records_processed,
            batches_completed=self.batches_completed,
            elapsed=elapsed_str,
            average_rate=f"{self.records_per_second:.0f} rec/s" if self.records_per_second > 0 else "N/A",
        )

    def get_eta(self) -> Optional[timedelta]:
        """Get estimated time remaining.

        Returns:
            Estimated time remaining, or None if cannot calculate
        """
        if (
            not self.records_total
            or self.records_per_second <= 0
            or self.records_processed >= self.records_total
        ):
            return None

        remaining = self.records_total - self.records_processed
        eta_seconds = remaining / self.records_per_second
        return timedelta(seconds=int(eta_seconds))

    def get_progress_percentage(self) -> float:
        """Get current progress percentage.

        Returns:
            Progress percentage (0.0 to 100.0), or 0.0 if unknown
        """
        if not self.records_total or self.records_total == 0:
            return 0.0
        return min(100.0, (self.records_processed / self.records_total) * 100)

