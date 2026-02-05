"""Notification manager with digest mode and rate limiting."""

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import structlog

from archiver.config import NotificationConfig
from archiver.notification_templates import NotificationTemplate
from archiver.notifications import (
    EmailNotificationChannel,
    NotificationChannel,
    NotificationManager,
    SlackNotificationChannel,
    TeamsNotificationChannel,
)
from utils.logging import get_logger


class RateLimiter:
    """Rate limiter for notifications to prevent alert fatigue."""

    def __init__(self, rate_limit_hours: float = 4.0) -> None:
        """Initialize rate limiter.

        Args:
            rate_limit_hours: Minimum hours between notifications of the same type
        """
        self.rate_limit_hours = rate_limit_hours
        self._last_sent: dict[str, datetime] = {}

    def can_send(self, notification_type: str) -> bool:
        """Check if a notification can be sent (not rate limited).

        Args:
            notification_type: Type of notification (e.g., "archive_failure", "threshold_violation")

        Returns:
            True if notification can be sent, False if rate limited
        """
        if notification_type not in self._last_sent:
            return True

        last_sent = self._last_sent[notification_type]
        time_since_last = datetime.now(timezone.utc) - last_sent
        return time_since_last >= timedelta(hours=self.rate_limit_hours)

    def record_sent(self, notification_type: str) -> None:
        """Record that a notification was sent.

        Args:
            notification_type: Type of notification
        """
        self._last_sent[notification_type] = datetime.now(timezone.utc)


class DigestCollector:
    """Collects notifications for digest mode."""

    def __init__(self) -> None:
        """Initialize digest collector."""
        self.events: list[dict[str, Any]] = []
        self.start_time = datetime.now(timezone.utc)

    def add_event(
        self,
        event_type: str,
        subject: str,
        message: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add an event to the digest.

        Args:
            event_type: Type of event (success, failure, etc.)
            subject: Event subject
            message: Event message
            metadata: Event metadata
        """
        self.events.append(
            {
                "event_type": event_type,
                "subject": subject,
                "message": message,
                "metadata": metadata or {},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    def get_summary(self) -> dict[str, Any]:
        """Get digest summary statistics.

        Returns:
            Dictionary with summary statistics
        """
        total_runs = len([e for e in self.events if e["event_type"] in ("success", "failure")])
        successful_runs = len([e for e in self.events if e["event_type"] == "success"])
        failed_runs = len([e for e in self.events if e["event_type"] == "failure"])

        total_records = sum(
            e["metadata"].get("records_archived", 0) for e in self.events if "records_archived" in e.get("metadata", {})
        )

        total_duration = sum(
            e["metadata"].get("duration_seconds", 0) for e in self.events if "duration_seconds" in e.get("metadata", {})
        )

        databases = set()
        errors = []
        for event in self.events:
            if "database" in event.get("metadata", {}):
                databases.add(event["metadata"]["database"])
            if event["event_type"] == "failure":
                errors.append(
                    {
                        "database": event["metadata"].get("database"),
                        "table": event["metadata"].get("table"),
                        "error": event["metadata"].get("error_message", "Unknown error"),
                    }
                )

        return {
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "total_records_archived": total_records,
            "total_duration_seconds": total_duration,
            "databases": list(databases),
            "errors": errors,
        }

    def clear(self) -> None:
        """Clear collected events."""
        self.events = []
        self.start_time = datetime.now(timezone.utc)


class EnhancedNotificationManager:
    """Enhanced notification manager with digest mode, rate limiting, and quiet hours."""

    def __init__(
        self,
        config: NotificationConfig,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize enhanced notification manager.

        Args:
            config: Notification configuration
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or get_logger("notification_manager")
        self.rate_limiter = RateLimiter(rate_limit_hours=config.rate_limit_hours)
        self.digest_collector = DigestCollector() if config.digest_mode else None

        # Build notification channels
        self.channels: list[NotificationChannel] = []
        self._build_channels()

        # Create base notification manager
        self.notification_manager = NotificationManager(channels=self.channels, logger=self.logger)

    def _build_channels(self) -> None:
        """Build notification channels from configuration."""
        if not self.config.enabled:
            return

        # Email channel
        if self.config.email.enabled:
            smtp_password = None
            if self.config.email.smtp_password_env:
                smtp_password = os.getenv(self.config.email.smtp_password_env)
                if not smtp_password:
                    self.logger.warning(
                        "Email notifications enabled but SMTP password environment variable not set",
                        env_var=self.config.email.smtp_password_env,
                    )

            email_channel = EmailNotificationChannel(
                smtp_host=self.config.email.smtp_host,
                smtp_port=self.config.email.smtp_port,
                smtp_user=self.config.email.smtp_user,
                smtp_password=smtp_password,
                from_email=self.config.email.from_email,
                to_emails=self.config.email.to_emails,
                use_tls=self.config.email.use_tls,
                logger=self.logger,
            )
            self.channels.append(email_channel)

        # Slack channel
        if self.config.slack.enabled:
            slack_webhook_url = os.getenv(self.config.slack.webhook_url_env)
            if not slack_webhook_url:
                self.logger.warning(
                    "Slack notifications enabled but webhook URL environment variable not set",
                    env_var=self.config.slack.webhook_url_env,
                )
            else:
                slack_channel = SlackNotificationChannel(
                    webhook_url=slack_webhook_url,
                    channel=self.config.slack.channel,
                    username=self.config.slack.username,
                    logger=self.logger,
                )
                self.channels.append(slack_channel)

        # Teams channel
        if self.config.teams.enabled:
            teams_webhook_url = os.getenv(self.config.teams.webhook_url_env)
            if not teams_webhook_url:
                self.logger.warning(
                    "Teams notifications enabled but webhook URL environment variable not set",
                    env_var=self.config.teams.webhook_url_env,
                )
            else:
                teams_channel = TeamsNotificationChannel(
                    webhook_url=teams_webhook_url,
                    logger=self.logger,
                )
                self.channels.append(teams_channel)

    def _is_quiet_hours(self) -> bool:
        """Check if current time is within quiet hours.

        Returns:
            True if within quiet hours, False otherwise
        """
        if not self.config.quiet_hours_start or not self.config.quiet_hours_end:
            return False

        current_hour = datetime.now(timezone.utc).hour

        # Handle quiet hours that span midnight (e.g., 22:00 - 06:00)
        if self.config.quiet_hours_start > self.config.quiet_hours_end:
            return current_hour >= self.config.quiet_hours_start or current_hour < self.config.quiet_hours_end
        else:
            return self.config.quiet_hours_start <= current_hour < self.config.quiet_hours_end

    def _should_send_notification(self, notification_type: str) -> bool:
        """Check if notification should be sent based on configuration and rate limiting.

        Args:
            notification_type: Type of notification (success, failure, start, threshold)

        Returns:
            True if notification should be sent, False otherwise
        """
        if not self.config.enabled:
            return False

        # Check quiet hours
        if self._is_quiet_hours():
            self.logger.debug("Quiet hours active, skipping notification", notification_type=notification_type)
            return False

        # Check rate limiting
        if not self.rate_limiter.can_send(notification_type):
            self.logger.debug(
                "Notification rate limited",
                notification_type=notification_type,
                rate_limit_hours=self.config.rate_limit_hours,
            )
            return False

        # Check if notification type is enabled
        if notification_type == "success" and not self.config.send_on_success:
            return False
        if notification_type == "failure" and not self.config.send_on_failure:
            return False
        if notification_type == "start" and not self.config.send_on_start:
            return False
        if notification_type == "threshold" and not self.config.send_on_threshold_violation:
            return False

        return True

    async def notify_archive_success(
        self,
        database: str,
        table: str,
        schema: str,
        records_archived: int,
        batches_processed: int,
        duration_seconds: float,
        s3_path: Optional[str] = None,
    ) -> None:
        """Send notification for successful archival.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            records_archived: Number of records archived
            batches_processed: Number of batches processed
            duration_seconds: Duration in seconds
            s3_path: S3 path where data was archived
        """
        if self.config.digest_mode and self.digest_collector:
            # Add to digest instead of sending immediately
            subject, message, metadata = NotificationTemplate.archive_success(
                database=database,
                table=table,
                schema=schema,
                records_archived=records_archived,
                batches_processed=batches_processed,
                duration_seconds=duration_seconds,
                s3_path=s3_path,
            )
            self.digest_collector.add_event("success", subject, message, metadata)
            return

        if not self._should_send_notification("success"):
            return

        subject, message, metadata = NotificationTemplate.archive_success(
            database=database,
            table=table,
            schema=schema,
            records_archived=records_archived,
            batches_processed=batches_processed,
            duration_seconds=duration_seconds,
            s3_path=s3_path,
        )

        results = await self.notification_manager.send_notification(subject, message, metadata)
        if any(results.values()):
            self.rate_limiter.record_sent("success")

    async def notify_archive_failure(
        self,
        database: str,
        table: str,
        schema: str,
        error_message: str,
        records_archived: int = 0,
        batches_processed: int = 0,
    ) -> None:
        """Send notification for failed archival.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            error_message: Error message
            records_archived: Number of records archived before failure
            batches_processed: Number of batches processed before failure
        """
        if self.config.digest_mode and self.digest_collector:
            # Add to digest (failures are always included in digest)
            subject, message, metadata = NotificationTemplate.archive_failure(
                database=database,
                table=table,
                schema=schema,
                error_message=error_message,
                records_archived=records_archived,
                batches_processed=batches_processed,
            )
            self.digest_collector.add_event("failure", subject, message, metadata)
            return

        if not self._should_send_notification("failure"):
            return

        subject, message, metadata = NotificationTemplate.archive_failure(
            database=database,
            table=table,
            schema=schema,
            error_message=error_message,
            records_archived=records_archived,
            batches_processed=batches_processed,
        )

        results = await self.notification_manager.send_notification(subject, message, metadata)
        if any(results.values()):
            self.rate_limiter.record_sent("failure")

    async def notify_archive_start(
        self,
        database: str,
        table: str,
        schema: str,
        records_eligible: int,
    ) -> None:
        """Send notification when archival starts.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            records_eligible: Number of records eligible for archival
        """
        if not self._should_send_notification("start"):
            return

        subject, message, metadata = NotificationTemplate.archive_start(
            database=database,
            table=table,
            schema=schema,
            records_eligible=records_eligible,
        )

        await self.notification_manager.send_notification(subject, message, metadata)

    async def notify_threshold_violation(
        self,
        metric: str,
        threshold: float,
        actual_value: float,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        """Send notification for threshold violation.

        Args:
            metric: Metric name
            threshold: Threshold value
            actual_value: Actual value that violated threshold
            database: Database name (optional)
            table: Table name (optional)
        """
        if not self._should_send_notification("threshold"):
            return

        subject, message, metadata = NotificationTemplate.threshold_violation(
            metric=metric,
            threshold=threshold,
            actual_value=actual_value,
            database=database,
            table=table,
        )

        results = await self.notification_manager.send_notification(subject, message, metadata)
        if any(results.values()):
            self.rate_limiter.record_sent("threshold")

    async def send_digest(self) -> None:
        """Send daily digest summary (if digest mode is enabled).

        This should be called at the configured digest hour.
        """
        if not self.config.digest_mode or not self.digest_collector:
            return

        summary = self.digest_collector.get_summary()
        if summary["total_runs"] == 0:
            # No events to report
            return

        subject, message, metadata = NotificationTemplate.digest_summary(
            total_runs=summary["total_runs"],
            successful_runs=summary["successful_runs"],
            failed_runs=summary["failed_runs"],
            total_records_archived=summary["total_records_archived"],
            total_duration_seconds=summary["total_duration_seconds"],
            databases=summary["databases"],
            errors=summary["errors"],
        )

        await self.notification_manager.send_notification(subject, message, metadata)
        self.digest_collector.clear()

    async def close(self) -> None:
        """Close all notification channels."""
        await self.notification_manager.close()

