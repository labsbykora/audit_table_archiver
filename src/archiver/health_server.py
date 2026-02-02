"""HTTP server for health check endpoint."""

import asyncio
import json
from typing import Optional

import aiohttp
from aiohttp import web
import structlog

from archiver.database import DatabaseManager
from archiver.health_check import HealthChecker, HealthStatus
from archiver.s3_client import S3Client
from utils.logging import get_logger


class HealthCheckServer:
    """HTTP server for health check endpoint."""

    def __init__(
        self,
        port: int = 8001,
        db_managers: Optional[dict[str, DatabaseManager]] = None,
        s3_client: Optional[S3Client] = None,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize health check server.

        Args:
            port: Port to listen on
            db_managers: Dictionary of database managers by database name
            s3_client: S3 client instance
            logger: Optional logger instance
        """
        self.port = port
        self.db_managers = db_managers or {}
        self.s3_client = s3_client
        self.logger = logger or get_logger("health_server")
        self.health_checker = HealthChecker(logger=self.logger)
        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None

    async def start(self) -> None:
        """Start the health check server."""
        self.app = web.Application()
        self.app.router.add_get("/health", self._handle_health)
        self.app.router.add_get("/", self._handle_root)

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()

        self.site = web.TCPSite(self.runner, "0.0.0.0", self.port)
        await self.site.start()

        self.logger.info(
            "Health check server started",
            port=self.port,
            endpoint=f"http://0.0.0.0:{self.port}/health",
        )

    async def stop(self) -> None:
        """Stop the health check server."""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        self.logger.info("Health check server stopped")

    async def _handle_health(self, request: web.Request) -> web.Response:
        """Handle health check request.

        Args:
            request: HTTP request

        Returns:
            HTTP response with health status
        """
        try:
            health_status = await self.health_checker.check_health(
                db_managers=self.db_managers,
                s3_client=self.s3_client,
            )

            status_code = health_status.to_http_status()
            response_data = health_status.to_dict()

            return web.Response(
                text=json.dumps(response_data, indent=2, default=str),
                status=status_code,
                content_type="application/json",
            )
        except Exception as e:
            self.logger.error("Error during health check", error=str(e), exc_info=True)
            return web.Response(
                text=json.dumps(
                    {
                        "healthy": False,
                        "status": "error",
                        "error": str(e),
                    }
                ),
                status=503,
                content_type="application/json",
            )

    async def _handle_root(self, request: web.Request) -> web.Response:
        """Handle root request (redirect to /health).

        Args:
            request: HTTP request

        Returns:
            HTTP response redirecting to /health
        """
        return web.Response(
            text=json.dumps(
                {
                    "service": "audit-archiver",
                    "health_endpoint": "/health",
                    "metrics_endpoint": "/metrics",
                }
            ),
            content_type="application/json",
        )

