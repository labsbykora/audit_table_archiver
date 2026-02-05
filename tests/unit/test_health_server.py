"""Unit tests for health check HTTP server."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import web

from archiver.database import DatabaseManager
from archiver.health_check import HealthStatus
from archiver.health_server import HealthCheckServer
from archiver.s3_client import S3Client


class TestHealthCheckServer:
    """Tests for HealthCheckServer class."""

    def test_init(self):
        """Test initialization."""
        server = HealthCheckServer(port=8001)
        assert server.port == 8001
        assert server.app is None
        assert server.runner is None

    def test_init_with_components(self):
        """Test initialization with database and S3 components."""
        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_s3_client = MagicMock(spec=S3Client)

        server = HealthCheckServer(
            port=8001,
            db_managers={"test_db": mock_db_manager},
            s3_client=mock_s3_client,
        )

        assert server.db_managers["test_db"] == mock_db_manager
        assert server.s3_client == mock_s3_client

    @pytest.mark.asyncio
    async def test_start(self):
        """Test starting the server."""
        server = HealthCheckServer(port=8001)

        with patch("archiver.health_server.web.AppRunner") as mock_runner_class:
            mock_runner = MagicMock()
            mock_runner.setup = AsyncMock()
            mock_runner_class.return_value = mock_runner

            mock_site = MagicMock()
            mock_site.start = AsyncMock()

            with patch("archiver.health_server.web.TCPSite", return_value=mock_site):
                await server.start()

            assert server.app is not None
            assert server.runner is not None
            assert server.site is not None
            mock_runner.setup.assert_called_once()
            mock_site.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop(self):
        """Test stopping the server."""
        server = HealthCheckServer(port=8001)

        # Set up mock components
        server.site = MagicMock()
        server.site.stop = AsyncMock()
        server.runner = MagicMock()
        server.runner.cleanup = AsyncMock()

        await server.stop()

        server.site.stop.assert_called_once()
        server.runner.cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_not_started(self):
        """Test stopping server that was never started."""
        server = HealthCheckServer(port=8001)

        # Should not raise exception
        await server.stop()

    @pytest.mark.asyncio
    async def test_handle_health_healthy(self):
        """Test handling health check request when healthy."""
        server = HealthCheckServer(port=8001)
        server.health_checker = MagicMock()

        mock_status = HealthStatus(
            healthy=True,
            status="healthy",
            checks={"test": {"healthy": True}},
        )
        server.health_checker.check_health = AsyncMock(return_value=mock_status)

        # Create a mock request
        request = MagicMock(spec=web.Request)

        response = await server._handle_health(request)

        assert response.status == 200
        assert response.content_type == "application/json"

        # Parse response body (text is a property, not a method)
        body = response.text
        data = json.loads(body)

        assert data["healthy"] is True
        assert data["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_handle_health_unhealthy(self):
        """Test handling health check request when unhealthy."""
        server = HealthCheckServer(port=8001)
        server.health_checker = MagicMock()

        mock_status = HealthStatus(
            healthy=False,
            status="unhealthy",
            checks={"test": {"healthy": False}},
        )
        server.health_checker.check_health = AsyncMock(return_value=mock_status)

        request = MagicMock(spec=web.Request)

        response = await server._handle_health(request)

        assert response.status == 503
        assert response.content_type == "application/json"

        body = response.text
        data = json.loads(body)

        assert data["healthy"] is False
        assert data["status"] == "unhealthy"

    @pytest.mark.asyncio
    async def test_handle_health_error(self):
        """Test handling health check request when error occurs."""
        server = HealthCheckServer(port=8001)
        server.health_checker = MagicMock()
        server.health_checker.check_health = AsyncMock(side_effect=Exception("Test error"))

        request = MagicMock(spec=web.Request)

        response = await server._handle_health(request)

        assert response.status == 503
        assert response.content_type == "application/json"

        body = response.text
        data = json.loads(body)

        assert data["healthy"] is False
        assert "error" in data

    @pytest.mark.asyncio
    async def test_handle_root(self):
        """Test handling root request."""
        server = HealthCheckServer(port=8001)

        request = MagicMock(spec=web.Request)

        response = await server._handle_root(request)

        assert response.status == 200
        assert response.content_type == "application/json"

        body = response.text
        data = json.loads(body)

        assert "service" in data
        assert data["service"] == "audit-archiver"
        assert "health_endpoint" in data
        assert "metrics_endpoint" in data
