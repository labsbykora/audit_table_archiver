"""Unit tests for notification channels."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from archiver.notifications import (
    EmailNotificationChannel,
    NotificationManager,
    SlackNotificationChannel,
    TeamsNotificationChannel,
)


class TestEmailNotificationChannel:
    """Tests for EmailNotificationChannel."""

    def test_init(self):
        """Test email channel initialization."""
        channel = EmailNotificationChannel(
            smtp_host="smtp.example.com",
            smtp_port=587,
            from_email="test@example.com",
            to_emails=["recipient@example.com"],
        )
        assert channel.smtp_host == "smtp.example.com"
        assert channel.smtp_port == 587
        assert channel.from_email == "test@example.com"
        assert len(channel.to_emails) == 1

    def test_init_no_recipients(self):
        """Test email channel initialization without recipients."""
        channel = EmailNotificationChannel(
            smtp_host="smtp.example.com",
            smtp_port=587,
        )
        assert channel.to_emails == []

    @pytest.mark.asyncio
    async def test_send_no_recipients(self):
        """Test sending email with no recipients."""
        channel = EmailNotificationChannel(
            smtp_host="smtp.example.com",
            smtp_port=587,
        )
        result = await channel.send("Test", "Message")
        assert result is False

    @pytest.mark.asyncio
    @patch("smtplib.SMTP")
    async def test_send_success(self, mock_smtp):
        """Test successful email send."""
        channel = EmailNotificationChannel(
            smtp_host="smtp.example.com",
            smtp_port=587,
            from_email="test@example.com",
            to_emails=["recipient@example.com"],
        )

        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        result = await channel.send("Test Subject", "Test Message")

        assert result is True
        mock_server.starttls.assert_called_once()
        mock_server.send_message.assert_called_once()
        mock_server.quit.assert_called_once()

    @pytest.mark.asyncio
    @patch("smtplib.SMTP")
    async def test_send_with_auth(self, mock_smtp):
        """Test email send with authentication."""
        channel = EmailNotificationChannel(
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_user="user",
            smtp_password="pass",
            from_email="test@example.com",
            to_emails=["recipient@example.com"],
        )

        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        result = await channel.send("Test Subject", "Test Message")

        assert result is True
        mock_server.login.assert_called_once_with("user", "pass")

    @pytest.mark.asyncio
    @patch("smtplib.SMTP")
    async def test_send_failure(self, mock_smtp):
        """Test email send failure."""
        channel = EmailNotificationChannel(
            smtp_host="smtp.example.com",
            smtp_port=587,
            from_email="test@example.com",
            to_emails=["recipient@example.com"],
        )

        mock_smtp.side_effect = Exception("SMTP error")

        result = await channel.send("Test Subject", "Test Message")

        assert result is False


class TestSlackNotificationChannel:
    """Tests for SlackNotificationChannel."""

    def test_init(self):
        """Test Slack channel initialization."""
        channel = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")
        assert channel.webhook_url == "https://hooks.slack.com/test"
        assert channel.username == "Audit Archiver"

    def test_init_with_channel(self):
        """Test Slack channel initialization with channel."""
        channel = SlackNotificationChannel(
            webhook_url="https://hooks.slack.com/test",
            channel="#test",
        )
        assert channel.channel == "#test"

    @pytest.mark.asyncio
    async def test_send_success(self):
        """Test successful Slack notification."""
        channel = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.status = 200
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        with patch("archiver.notifications.aiohttp.ClientSession", return_value=mock_session):
            result = await channel.send("Test Subject", "Test Message")

            assert result is True
            mock_session.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_with_metadata(self):
        """Test Slack notification with metadata."""
        channel = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.status = 200
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        with patch("archiver.notifications.aiohttp.ClientSession", return_value=mock_session):
            result = await channel.send(
                "Test Subject",
                "Test Message",
                metadata={"key": "value"},
            )

            assert result is True

    @pytest.mark.asyncio
    async def test_send_failure(self):
        """Test Slack notification failure."""
        channel = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")

        with patch("archiver.notifications.aiohttp.ClientSession") as mock_session_class:
            mock_session = AsyncMock()
            mock_session.post = AsyncMock(side_effect=Exception("Network error"))
            mock_session_class.return_value = mock_session

            result = await channel.send("Test Subject", "Test Message")

            assert result is False

    @pytest.mark.asyncio
    async def test_close_session(self):
        """Test closing Slack session."""
        channel = SlackNotificationChannel(webhook_url="https://hooks.slack.com/test")

        with patch("archiver.notifications.aiohttp.ClientSession") as mock_session_class:
            mock_session = AsyncMock()
            mock_session.closed = False
            mock_session.close = AsyncMock()
            mock_session_class.return_value = mock_session
            channel._session = mock_session

            await channel.close_session()

            mock_session.close.assert_called_once()


class TestTeamsNotificationChannel:
    """Tests for TeamsNotificationChannel."""

    def test_init(self):
        """Test Teams channel initialization."""
        channel = TeamsNotificationChannel(webhook_url="https://outlook.office.com/webhook/test")
        assert channel.webhook_url == "https://outlook.office.com/webhook/test"

    @pytest.mark.asyncio
    async def test_send_success(self):
        """Test successful Teams notification."""
        channel = TeamsNotificationChannel(webhook_url="https://outlook.office.com/webhook/test")

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.status = 200
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        with patch("archiver.notifications.aiohttp.ClientSession", return_value=mock_session):
            result = await channel.send("Test Subject", "Test Message")

            assert result is True
            mock_session.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_with_metadata(self):
        """Test Teams notification with metadata."""
        channel = TeamsNotificationChannel(webhook_url="https://outlook.office.com/webhook/test")

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.status = 200
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        with patch("archiver.notifications.aiohttp.ClientSession", return_value=mock_session):
            result = await channel.send(
                "Test Subject",
                "Test Message",
                metadata={"key": "value"},
            )

            assert result is True

    @pytest.mark.asyncio
    async def test_send_failure(self):
        """Test Teams notification failure."""
        channel = TeamsNotificationChannel(webhook_url="https://outlook.office.com/webhook/test")

        with patch("archiver.notifications.aiohttp.ClientSession") as mock_session_class:
            mock_session = AsyncMock()
            mock_session.post = AsyncMock(side_effect=Exception("Network error"))
            mock_session_class.return_value = mock_session

            result = await channel.send("Test Subject", "Test Message")

            assert result is False

    @pytest.mark.asyncio
    async def test_close_session(self):
        """Test closing Teams session."""
        channel = TeamsNotificationChannel(webhook_url="https://outlook.office.com/webhook/test")

        with patch("archiver.notifications.aiohttp.ClientSession") as mock_session_class:
            mock_session = AsyncMock()
            mock_session.closed = False
            mock_session.close = AsyncMock()
            mock_session_class.return_value = mock_session
            channel._session = mock_session

            await channel.close_session()

            mock_session.close.assert_called_once()


class TestNotificationManager:
    """Tests for NotificationManager."""

    @pytest.mark.asyncio
    async def test_send_notification_single_channel(self):
        """Test sending notification to single channel."""
        mock_channel = MagicMock()
        mock_channel.send = AsyncMock(return_value=True)
        mock_channel.__class__.__name__ = "TestChannel"

        manager = NotificationManager(channels=[mock_channel])

        results = await manager.send_notification("Subject", "Message")

        assert results["TestChannel"] is True
        mock_channel.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_notification_multiple_channels(self):
        """Test sending notification to multiple channels."""
        mock_channel1 = MagicMock()
        mock_channel1.send = AsyncMock(return_value=True)
        mock_channel1.__class__.__name__ = "Channel1"

        mock_channel2 = MagicMock()
        mock_channel2.send = AsyncMock(return_value=True)
        mock_channel2.__class__.__name__ = "Channel2"

        manager = NotificationManager(channels=[mock_channel1, mock_channel2])

        results = await manager.send_notification("Subject", "Message")

        assert results["Channel1"] is True
        assert results["Channel2"] is True

    @pytest.mark.asyncio
    async def test_send_notification_channel_failure(self):
        """Test notification when channel fails."""
        mock_channel = MagicMock()
        mock_channel.send = AsyncMock(return_value=False)
        mock_channel.__class__.__name__ = "TestChannel"

        manager = NotificationManager(channels=[mock_channel])

        results = await manager.send_notification("Subject", "Message")

        assert results["TestChannel"] is False

    @pytest.mark.asyncio
    async def test_send_notification_channel_exception(self):
        """Test notification when channel raises exception."""
        mock_channel = MagicMock()
        mock_channel.send = AsyncMock(side_effect=Exception("Channel error"))
        mock_channel.__class__.__name__ = "TestChannel"

        manager = NotificationManager(channels=[mock_channel])

        results = await manager.send_notification("Subject", "Message")

        assert results["TestChannel"] is False

    @pytest.mark.asyncio
    async def test_close(self):
        """Test closing notification manager."""
        mock_channel1 = MagicMock()
        mock_channel1.close_session = AsyncMock()

        mock_channel2 = MagicMock()
        # Channel 2 doesn't have close_session

        manager = NotificationManager(channels=[mock_channel1, mock_channel2])

        await manager.close()

        mock_channel1.close_session.assert_called_once()

