"""Unit tests for health check."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from archiver.database import DatabaseManager
from archiver.health_check import HealthChecker, HealthStatus
from archiver.s3_client import S3Client


class TestHealthStatus:
    """Tests for HealthStatus class."""

    def test_init_healthy(self):
        """Test initialization with healthy status."""
        status = HealthStatus(
            healthy=True,
            status="healthy",
            checks={"database": {"healthy": True}},
        )

        assert status.healthy is True
        assert status.status == "healthy"
        assert status.to_http_status() == 200

    def test_init_unhealthy(self):
        """Test initialization with unhealthy status."""
        status = HealthStatus(
            healthy=False,
            status="unhealthy",
            checks={"database": {"healthy": False}},
        )

        assert status.healthy is False
        assert status.status == "unhealthy"
        assert status.to_http_status() == 503

    def test_init_degraded(self):
        """Test initialization with degraded status."""
        status = HealthStatus(
            healthy=False,
            status="degraded",
            checks={"database": {"healthy": True}, "s3": {"healthy": False}},
        )

        assert status.healthy is False
        assert status.status == "degraded"
        assert status.to_http_status() == 503

    def test_to_dict(self):
        """Test converting to dictionary."""
        status = HealthStatus(
            healthy=True,
            status="healthy",
            checks={"test": {"healthy": True}},
        )

        result = status.to_dict()

        assert isinstance(result, dict)
        assert result["healthy"] is True
        assert result["status"] == "healthy"
        assert "timestamp" in result
        assert "checks" in result

    def test_to_dict_includes_timestamp(self):
        """Test that to_dict includes timestamp."""
        status = HealthStatus(
            healthy=True,
            status="healthy",
            checks={},
        )

        result = status.to_dict()

        assert "timestamp" in result
        assert isinstance(result["timestamp"], str)


class TestHealthChecker:
    """Tests for HealthChecker class."""

    def test_init(self):
        """Test initialization."""
        checker = HealthChecker()
        assert checker is not None

    @pytest.mark.asyncio
    async def test_check_health_no_components(self):
        """Test health check with no components configured."""
        checker = HealthChecker()

        status = await checker.check_health()

        assert isinstance(status, HealthStatus)
        assert status.healthy is True  # No components = healthy (nothing to check)
        assert "databases" in status.checks
        assert "s3" in status.checks

    @pytest.mark.asyncio
    async def test_check_health_database_healthy(self):
        """Test health check with healthy database."""
        checker = HealthChecker()

        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_db_manager.fetchval = AsyncMock(return_value=1)

        db_managers = {"test_db": mock_db_manager}

        status = await checker.check_health(db_managers=db_managers)

        assert isinstance(status, HealthStatus)
        assert "databases" in status.checks
        assert "test_db" in status.checks["databases"]
        assert status.checks["databases"]["test_db"]["healthy"] is True

    @pytest.mark.asyncio
    async def test_check_health_database_unhealthy(self):
        """Test health check with unhealthy database."""
        checker = HealthChecker()

        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_db_manager.fetchval = AsyncMock(side_effect=Exception("Connection failed"))

        db_managers = {"test_db": mock_db_manager}

        status = await checker.check_health(db_managers=db_managers)

        assert isinstance(status, HealthStatus)
        assert status.healthy is False
        assert "databases" in status.checks
        assert "test_db" in status.checks["databases"]
        assert status.checks["databases"]["test_db"]["healthy"] is False

    @pytest.mark.asyncio
    async def test_check_health_s3_healthy(self):
        """Test health check with healthy S3."""
        checker = HealthChecker()

        mock_s3_client = MagicMock(spec=S3Client)
        mock_s3_client.config = MagicMock()
        mock_s3_client.config.bucket = "test-bucket"
        mock_s3_client.client = MagicMock()
        mock_s3_client.client.list_objects_v2 = MagicMock(return_value={})

        status = await checker.check_health(s3_client=mock_s3_client)

        assert isinstance(status, HealthStatus)
        assert "s3" in status.checks
        assert status.checks["s3"]["healthy"] is True

    @pytest.mark.asyncio
    async def test_check_health_s3_unhealthy(self):
        """Test health check with unhealthy S3."""
        checker = HealthChecker()

        mock_s3_client = MagicMock(spec=S3Client)
        mock_s3_client.config = MagicMock()
        mock_s3_client.config.bucket = "test-bucket"
        mock_s3_client.client = MagicMock()
        mock_s3_client.client.list_objects_v2 = MagicMock(side_effect=Exception("S3 error"))

        status = await checker.check_health(s3_client=mock_s3_client)

        assert isinstance(status, HealthStatus)
        assert status.healthy is False
        assert "s3" in status.checks
        assert status.checks["s3"]["healthy"] is False

    @pytest.mark.asyncio
    async def test_check_health_mixed(self):
        """Test health check with mixed healthy/unhealthy components."""
        checker = HealthChecker()

        # Healthy database
        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_db_manager.fetchval = AsyncMock(return_value=1)

        # Unhealthy S3
        mock_s3_client = MagicMock(spec=S3Client)
        mock_s3_client.config = MagicMock()
        mock_s3_client.config.bucket = "test-bucket"
        mock_s3_client.client = MagicMock()
        mock_s3_client.client.list_objects_v2 = MagicMock(side_effect=Exception("S3 error"))

        status = await checker.check_health(
            db_managers={"test_db": mock_db_manager},
            s3_client=mock_s3_client,
        )

        assert isinstance(status, HealthStatus)
        assert status.healthy is False
        assert status.status == "degraded"  # Some components healthy, some not

    @pytest.mark.asyncio
    async def test_check_health_multiple_databases(self):
        """Test health check with multiple databases."""
        checker = HealthChecker()

        mock_db1 = MagicMock(spec=DatabaseManager)
        mock_db1.fetchval = AsyncMock(return_value=1)

        mock_db2 = MagicMock(spec=DatabaseManager)
        mock_db2.fetchval = AsyncMock(return_value=1)

        db_managers = {
            "db1": mock_db1,
            "db2": mock_db2,
        }

        status = await checker.check_health(db_managers=db_managers)

        assert isinstance(status, HealthStatus)
        assert "databases" in status.checks
        assert "db1" in status.checks["databases"]
        assert "db2" in status.checks["databases"]
        assert status.checks["databases"]["db1"]["healthy"] is True
        assert status.checks["databases"]["db2"]["healthy"] is True

    @pytest.mark.asyncio
    async def test_check_database_unexpected_result(self):
        """Test database check with unexpected query result."""
        checker = HealthChecker()

        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_db_manager.fetchval = AsyncMock(return_value=2)  # Not 1

        db_managers = {"test_db": mock_db_manager}

        status = await checker.check_health(db_managers=db_managers)

        assert isinstance(status, HealthStatus)
        assert status.healthy is False
        assert status.checks["databases"]["test_db"]["healthy"] is False
