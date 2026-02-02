"""Notification system for alerting on archival events."""

import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
import structlog

from utils.logging import get_logger


class NotificationChannel(ABC):
    """Abstract base class for notification channels."""

    @abstractmethod
    async def send(
        self,
        subject: str,
        message: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Send a notification.

        Args:
            subject: Notification subject/title
            message: Notification message body
            metadata: Additional metadata for the notification

        Returns:
            True if notification was sent successfully, False otherwise
        """
        pass


class EmailNotificationChannel(NotificationChannel):
    """Email notification channel using SMTP."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        from_email: str = "archiver@example.com",
        to_emails: list[str] = None,
        use_tls: bool = True,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize email notification channel.

        Args:
            smtp_host: SMTP server hostname
            smtp_port: SMTP server port
            smtp_user: SMTP username (optional, for authentication)
            smtp_password: SMTP password (optional, for authentication)
            from_email: Sender email address
            to_emails: List of recipient email addresses
            use_tls: Use TLS encryption (default: True)
            logger: Optional logger instance
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_email = from_email
        self.to_emails = to_emails or []
        self.use_tls = use_tls
        self.logger = logger or get_logger("email_notifications")

    async def send(
        self,
        subject: str,
        message: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Send email notification.

        Args:
            subject: Email subject
            message: Email body
            metadata: Additional metadata

        Returns:
            True if email was sent successfully, False otherwise
        """
        if not self.to_emails:
            self.logger.warning("No email recipients configured, skipping email notification")
            return False

        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            # Create email message
            msg = MIMEMultipart()
            msg["From"] = self.from_email
            msg["To"] = ", ".join(self.to_emails)
            msg["Subject"] = subject

            # Add body
            body = message
            if metadata:
                body += "\n\nMetadata:\n" + json.dumps(metadata, indent=2, default=str)
            msg.attach(MIMEText(body, "plain"))

            # Send email (synchronous SMTP, but we're in async context)
            # In production, consider using aiofiles or running in executor
            import asyncio

            def send_sync():
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                if self.use_tls:
                    server.starttls()
                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
                server.quit()

            await asyncio.get_event_loop().run_in_executor(None, send_sync)

            self.logger.info(
                "Email notification sent",
                subject=subject,
                recipients=self.to_emails,
            )
            return True

        except Exception as e:
            self.logger.error(
                "Failed to send email notification",
                subject=subject,
                error=str(e),
                exc_info=True,
            )
            return False


class SlackNotificationChannel(NotificationChannel):
    """Slack notification channel using webhooks."""

    def __init__(
        self,
        webhook_url: str,
        channel: Optional[str] = None,
        username: str = "Audit Archiver",
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize Slack notification channel.

        Args:
            webhook_url: Slack webhook URL
            channel: Slack channel to post to (optional, can be set in webhook)
            username: Bot username (default: "Audit Archiver")
            logger: Optional logger instance
        """
        self.webhook_url = webhook_url
        self.channel = channel
        self.username = username
        self.logger = logger or get_logger("slack_notifications")
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp client session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close_session(self) -> None:
        """Close the aiohttp client session if it exists."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            self.logger.debug("aiohttp client session closed")

    async def send(
        self,
        subject: str,
        message: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Send Slack notification.

        Args:
            subject: Notification subject (used as title)
            message: Notification message
            metadata: Additional metadata

        Returns:
            True if notification was sent successfully, False otherwise
        """
        try:
            # Format Slack message
            slack_payload = {
                "text": subject,
                "username": self.username,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": subject,
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message,
                        },
                    },
                ],
            }

            if self.channel:
                slack_payload["channel"] = self.channel

            if metadata:
                # Add metadata as a code block
                metadata_text = "```\n" + json.dumps(metadata, indent=2, default=str) + "\n```"
                slack_payload["blocks"].append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Metadata:*\n{metadata_text}",
                        },
                    }
                )

            session = await self._get_session()
            async with session.post(
                self.webhook_url,
                json=slack_payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                response.raise_for_status()

            self.logger.info(
                "Slack notification sent",
                subject=subject,
                channel=self.channel,
            )
            return True

        except Exception as e:
            self.logger.error(
                "Failed to send Slack notification",
                subject=subject,
                error=str(e),
                exc_info=True,
            )
            return False


class TeamsNotificationChannel(NotificationChannel):
    """Microsoft Teams notification channel using webhooks."""

    def __init__(
        self,
        webhook_url: str,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize Teams notification channel.

        Args:
            webhook_url: Teams webhook URL
            logger: Optional logger instance
        """
        self.webhook_url = webhook_url
        self.logger = logger or get_logger("teams_notifications")
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp client session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close_session(self) -> None:
        """Close the aiohttp client session if it exists."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            self.logger.debug("aiohttp client session closed")

    async def send(
        self,
        subject: str,
        message: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Send Teams notification.

        Args:
            subject: Notification subject (used as title)
            message: Notification message
            metadata: Additional metadata

        Returns:
            True if notification was sent successfully, False otherwise
        """
        try:
            # Format Teams message (Office 365 Connector Card format)
            teams_payload = {
                "@type": "MessageCard",
                "@context": "https://schema.org/extensions",
                "summary": subject,
                "themeColor": "0078D4",  # Teams blue
                "title": subject,
                "text": message,
            }

            if metadata:
                # Add metadata as facts
                facts = []
                for key, value in metadata.items():
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value, indent=2, default=str)
                    facts.append({"name": str(key), "value": str(value)})
                teams_payload["sections"] = [{"facts": facts}]

            session = await self._get_session()
            async with session.post(
                self.webhook_url,
                json=teams_payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                response.raise_for_status()

            self.logger.info(
                "Teams notification sent",
                subject=subject,
            )
            return True

        except Exception as e:
            self.logger.error(
                "Failed to send Teams notification",
                subject=subject,
                error=str(e),
                exc_info=True,
            )
            return False


class NotificationManager:
    """Manages multiple notification channels."""

    def __init__(
        self,
        channels: list[NotificationChannel],
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize notification manager.

        Args:
            channels: List of notification channels
            logger: Optional logger instance
        """
        self.channels = channels
        self.logger = logger or get_logger("notification_manager")

    async def send_notification(
        self,
        subject: str,
        message: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, bool]:
        """Send notification to all channels.

        Args:
            subject: Notification subject
            message: Notification message
            metadata: Additional metadata

        Returns:
            Dictionary mapping channel class names to success status
        """
        results = {}
        for channel in self.channels:
            channel_name = channel.__class__.__name__
            try:
                success = await channel.send(subject, message, metadata)
                results[channel_name] = success
            except Exception as e:
                self.logger.error(
                    "Error sending notification via channel",
                    channel=channel_name,
                    error=str(e),
                    exc_info=True,
                )
                results[channel_name] = False

        return results

    async def close(self) -> None:
        """Close all notification channels (cleanup sessions)."""
        for channel in self.channels:
            if hasattr(channel, "close_session"):
                try:
                    await channel.close_session()
                except Exception as e:
                    self.logger.warning(
                        "Error closing notification channel session",
                        channel=channel.__class__.__name__,
                        error=str(e),
                    )

