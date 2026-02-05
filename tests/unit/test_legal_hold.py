"""Unit tests for legal hold checking."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from archiver.database import DatabaseManager
from archiver.legal_hold import LegalHold, LegalHoldChecker


class TestLegalHold:
    """Tests for LegalHold class."""

    def test_legal_hold_is_active_current(self):
        """Test that legal hold is active when current time is within range."""
        start_date = datetime.now(timezone.utc) - timedelta(days=1)
        expiration_date = datetime.now(timezone.utc) + timedelta(days=1)

        hold = LegalHold(
            table_name="test_table",
            schema_name="public",
            reason="Test hold",
            start_date=start_date,
            expiration_date=expiration_date,
            requestor="test@example.com",
        )

        assert hold.is_active() is True

    def test_legal_hold_is_active_no_expiration(self):
        """Test that legal hold is active when there's no expiration date."""
        start_date = datetime.now(timezone.utc) - timedelta(days=1)

        hold = LegalHold(
            table_name="test_table",
            schema_name="public",
            reason="Test hold",
            start_date=start_date,
            expiration_date=None,
            requestor="test@example.com",
        )

        assert hold.is_active() is True

    def test_legal_hold_is_inactive_before_start(self):
        """Test that legal hold is inactive before start date."""
        start_date = datetime.now(timezone.utc) + timedelta(days=1)
        expiration_date = datetime.now(timezone.utc) + timedelta(days=2)

        hold = LegalHold(
            table_name="test_table",
            schema_name="public",
            reason="Test hold",
            start_date=start_date,
            expiration_date=expiration_date,
            requestor="test@example.com",
        )

        assert hold.is_active() is False

    def test_legal_hold_is_inactive_after_expiration(self):
        """Test that legal hold is inactive after expiration date."""
        start_date = datetime.now(timezone.utc) - timedelta(days=2)
        expiration_date = datetime.now(timezone.utc) - timedelta(days=1)

        hold = LegalHold(
            table_name="test_table",
            schema_name="public",
            reason="Test hold",
            start_date=start_date,
            expiration_date=expiration_date,
            requestor="test@example.com",
        )

        assert hold.is_active() is False


class TestLegalHoldChecker:
    """Tests for LegalHoldChecker class."""

    def test_init_enabled(self):
        """Test initialization with legal hold checking enabled."""
        checker = LegalHoldChecker(enabled=True, check_table="legal_holds")
        assert checker.enabled is True
        assert checker.check_table == "legal_holds"

    def test_init_disabled(self):
        """Test initialization with legal hold checking disabled."""
        checker = LegalHoldChecker(enabled=False)
        assert checker.enabled is False

    @pytest.mark.asyncio
    async def test_check_legal_hold_disabled(self):
        """Test that checking returns None when disabled."""
        checker = LegalHoldChecker(enabled=False)
        result = await checker.check_legal_hold(
            database_name="test_db",
            table_name="test_table",
            schema_name="public",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_check_legal_hold_database_table_found(self):
        """Test checking legal hold in database table when hold exists."""
        checker = LegalHoldChecker(
            enabled=True,
            check_table="legal_holds",
            check_database="test_db",
        )

        # Mock database manager
        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_row = {
            "table_name": "test_table",
            "schema_name": "public",
            "reason": "Legal case XYZ",
            "start_date": datetime.now(timezone.utc) - timedelta(days=1),
            "expiration_date": datetime.now(timezone.utc) + timedelta(days=1),
            "requestor": "legal@example.com",
            "where_clause": None,
        }
        mock_db_manager.fetchone = AsyncMock(return_value=mock_row)

        result = await checker.check_legal_hold(
            database_name="test_db",
            table_name="test_table",
            schema_name="public",
            db_manager=mock_db_manager,
        )

        assert result is not None
        assert isinstance(result, LegalHold)
        assert result.table_name == "test_table"
        assert result.reason == "Legal case XYZ"
        assert result.is_active() is True

    @pytest.mark.asyncio
    async def test_check_legal_hold_database_table_not_found(self):
        """Test checking legal hold when no hold exists."""
        checker = LegalHoldChecker(
            enabled=True,
            check_table="legal_holds",
        )

        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_db_manager.fetchone = AsyncMock(return_value=None)

        result = await checker.check_legal_hold(
            database_name="test_db",
            table_name="test_table",
            schema_name="public",
            db_manager=mock_db_manager,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_check_legal_hold_database_error(self):
        """Test that database errors are handled gracefully."""
        checker = LegalHoldChecker(
            enabled=True,
            check_table="legal_holds",
        )

        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_db_manager.fetchone = AsyncMock(side_effect=Exception("Database error"))

        # Should log warning but not raise exception
        result = await checker.check_legal_hold(
            database_name="test_db",
            table_name="test_table",
            schema_name="public",
            db_manager=mock_db_manager,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_check_legal_hold_api_success(self):
        """Test checking legal hold via API when hold exists."""
        checker = LegalHoldChecker(
            enabled=True,
            api_endpoint="https://api.example.com",
            api_timeout=5,
        )

        mock_response_data = {
            "has_hold": True,
            "table_name": "test_table",
            "schema_name": "public",
            "reason": "Legal case XYZ",
            "start_date": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
            "expiration_date": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
            "requestor": "legal@example.com",
            "where_clause": None,
        }

        # Mock aiohttp - it's imported inside the function, so we need to patch it there
        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=mock_response_data)
            mock_response.raise_for_status = MagicMock()

            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_response)
            mock_context.__aexit__ = AsyncMock(return_value=None)

            mock_session_instance = MagicMock()
            mock_session_instance.get = MagicMock(return_value=mock_context)
            mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session_instance.__aexit__ = AsyncMock(return_value=None)

            mock_session_class.return_value = mock_session_instance

            # Also need to mock ClientTimeout
            with patch("aiohttp.ClientTimeout") as mock_timeout:
                mock_timeout.return_value = MagicMock()

                result = await checker.check_legal_hold(
                    database_name="test_db",
                    table_name="test_table",
                    schema_name="public",
                )

                assert result is not None
                assert isinstance(result, LegalHold)
                assert result.table_name == "test_table"
                assert result.reason == "Legal case XYZ"

    @pytest.mark.asyncio
    async def test_check_legal_hold_api_not_found(self):
        """Test checking legal hold via API when no hold exists."""
        checker = LegalHoldChecker(
            enabled=True,
            api_endpoint="https://api.example.com",
        )

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_response = MagicMock()
            mock_response.status = 404

            mock_context = MagicMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_response)
            mock_context.__aexit__ = AsyncMock(return_value=None)

            mock_session_instance = MagicMock()
            mock_session_instance.get = MagicMock(return_value=mock_context)
            mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
            mock_session_instance.__aexit__ = AsyncMock(return_value=None)

            mock_session_class.return_value = mock_session_instance

            with patch("aiohttp.ClientTimeout"):
                result = await checker.check_legal_hold(
                    database_name="test_db",
                    table_name="test_table",
                    schema_name="public",
                )

                assert result is None

    @pytest.mark.asyncio
    async def test_check_legal_hold_api_error(self):
        """Test that API errors are handled gracefully."""
        checker = LegalHoldChecker(
            enabled=True,
            api_endpoint="https://api.example.com",
        )

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session_class.side_effect = Exception("Network error")

            result = await checker.check_legal_hold(
                database_name="test_db",
                table_name="test_table",
                schema_name="public",
            )

            assert result is None

