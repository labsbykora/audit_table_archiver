"""Health check endpoint for monitoring."""

from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from archiver.database import DatabaseManager
from archiver.s3_client import S3Client
from utils.logging import get_logger


class HealthStatus:
    """Represents health check status."""

    def __init__(
        self,
        healthy: bool,
        status: str,
        checks: dict[str, Any],
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Initialize health status.

        Args:
            healthy: Whether the system is healthy
            status: Status string (healthy, degraded, unhealthy)
            checks: Dictionary of individual check results
            timestamp: Timestamp of health check
        """
        self.healthy = healthy
        self.status = status
        self.checks = checks
        self.timestamp = timestamp or datetime.now(timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON response.

        Returns:
            Dictionary representation
        """
        return {
            "healthy": self.healthy,
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
            "checks": self.checks,
        }

    def to_http_status(self) -> int:
        """Get HTTP status code for this health status.

        Returns:
            HTTP status code (200 for healthy, 503 for unhealthy)
        """
        return 200 if self.healthy else 503


class HealthChecker:
    """Performs health checks on archiver components."""

    def __init__(
        self,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize health checker.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("health_check")

    async def check_health(
        self,
        db_managers: Optional[dict[str, DatabaseManager]] = None,
        s3_client: Optional[S3Client] = None,
    ) -> HealthStatus:
        """Perform comprehensive health check.

        Args:
            db_managers: Dictionary of database managers by database name
            s3_client: S3 client instance

        Returns:
            HealthStatus object
        """
        checks: dict[str, Any] = {}
        all_healthy = True

        # Check databases
        if db_managers:
            db_checks = {}
            for db_name, db_manager in db_managers.items():
                db_health = await self._check_database(db_manager)
                db_checks[db_name] = db_health
                if not db_health.get("healthy", False):
                    all_healthy = False
            checks["databases"] = db_checks
        else:
            checks["databases"] = {"status": "not_configured"}

        # Check S3
        if s3_client:
            s3_health = await self._check_s3(s3_client)
            checks["s3"] = s3_health
            if not s3_health.get("healthy", False):
                all_healthy = False
        else:
            checks["s3"] = {"status": "not_configured"}

        # Determine overall status
        if all_healthy:
            status = "healthy"
        else:
            # Check if any component is healthy (degraded) or all are unhealthy
            has_healthy = False
            for check_value in checks.values():
                if isinstance(check_value, dict):
                    if check_value.get("healthy", False):
                        has_healthy = True
                        break
                    # Check nested dictionaries (like databases dict)
                    for nested_value in check_value.values():
                        if isinstance(nested_value, dict) and nested_value.get("healthy", False):
                            has_healthy = True
                            break
                    if has_healthy:
                        break

            status = "degraded" if has_healthy else "unhealthy"

        return HealthStatus(
            healthy=all_healthy,
            status=status,
            checks=checks,
        )

    async def _check_database(self, db_manager: DatabaseManager) -> dict[str, Any]:
        """Check database health.

        Args:
            db_manager: Database manager instance

        Returns:
            Dictionary with health check results
        """
        try:
            # Try to execute a simple query
            start_time = datetime.now(timezone.utc)
            result = await db_manager.fetchval("SELECT 1")
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            if result == 1:
                return {
                    "healthy": True,
                    "status": "ok",
                    "response_time_seconds": duration,
                }
            else:
                return {
                    "healthy": False,
                    "status": "error",
                    "error": "Unexpected query result",
                }
        except Exception as e:
            return {
                "healthy": False,
                "status": "error",
                "error": str(e),
            }

    async def _check_s3(self, s3_client: S3Client) -> dict[str, Any]:
        """Check S3 health.

        Args:
            s3_client: S3 client instance

        Returns:
            Dictionary with health check results
        """
        try:
            # Try to list bucket (lightweight operation)
            start_time = datetime.now(timezone.utc)
            s3_client.client.list_objects_v2(Bucket=s3_client.config.bucket, MaxKeys=1)
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            return {
                "healthy": True,
                "status": "ok",
                "bucket": s3_client.config.bucket,
                "response_time_seconds": duration,
            }
        except Exception as e:
            return {
                "healthy": False,
                "status": "error",
                "bucket": s3_client.config.bucket,
                "error": str(e),
            }
