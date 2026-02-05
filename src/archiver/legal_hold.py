"""Legal hold checking to prevent archival of held data."""

from datetime import datetime, timezone
from typing import Optional

import structlog

from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError
from utils.logging import get_logger


class LegalHold:
    """Represents a legal hold on a table or records."""

    def __init__(
        self,
        table_name: str,
        schema_name: str,
        reason: str,
        start_date: datetime,
        expiration_date: Optional[datetime],
        requestor: str,
        where_clause: Optional[str] = None,
    ) -> None:
        """Initialize legal hold.

        Args:
            table_name: Table name
            schema_name: Schema name
            reason: Reason for legal hold
            start_date: When the hold started
            expiration_date: When the hold expires (None for indefinite)
            requestor: Who requested the hold
            where_clause: Optional WHERE clause for record-level holds
        """
        self.table_name = table_name
        self.schema_name = schema_name
        self.reason = reason
        self.start_date = start_date
        self.expiration_date = expiration_date
        self.requestor = requestor
        self.where_clause = where_clause

    def is_active(self, current_time: Optional[datetime] = None) -> bool:
        """Check if legal hold is currently active.

        Args:
            current_time: Current time (defaults to now)

        Returns:
            True if hold is active, False otherwise
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        # Check if hold has started
        if current_time < self.start_date:
            return False

        # Check if hold has expired
        if self.expiration_date and current_time >= self.expiration_date:
            return False

        return True


class LegalHoldChecker:
    """Checks for legal holds before archival."""

    def __init__(
        self,
        enabled: bool = True,
        check_table: Optional[str] = None,
        check_database: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        api_timeout: int = 5,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize legal hold checker.

        Args:
            enabled: Whether legal hold checking is enabled
            check_table: Database table name containing legal holds (format: schema.table)
            check_database: Database name to check for legal holds
            api_endpoint: Optional API endpoint for legal hold checking
            api_timeout: API request timeout in seconds
            logger: Optional logger instance
        """
        self.enabled = enabled
        self.check_table = check_table
        self.check_database = check_database
        self.api_endpoint = api_endpoint
        self.api_timeout = api_timeout
        self.logger = logger or get_logger("legal_hold")

    async def check_legal_hold(
        self,
        database_name: str,
        table_name: str,
        schema_name: str,
        db_manager: Optional[DatabaseManager] = None,
    ) -> Optional[LegalHold]:
        """Check if a table has an active legal hold.

        Args:
            database_name: Database name
            table_name: Table name
            schema_name: Schema name
            db_manager: Database manager (required if using database table)

        Returns:
            LegalHold object if hold exists, None otherwise

        Raises:
            DatabaseError: If database check fails
        """
        if not self.enabled:
            return None

        # Try database table first
        if self.check_table and db_manager:
            try:
                hold = await self._check_database_table(
                    db_manager, database_name, table_name, schema_name
                )
                if hold:
                    return hold
            except Exception as e:
                self.logger.warning(
                    "Failed to check legal hold in database table",
                    database=database_name,
                    table=table_name,
                    error=str(e),
                )

        # Try API endpoint
        if self.api_endpoint:
            try:
                hold = await self._check_api(database_name, table_name, schema_name)
                if hold:
                    return hold
            except Exception as e:
                self.logger.warning(
                    "Failed to check legal hold via API",
                    database=database_name,
                    table=table_name,
                    error=str(e),
                )

        return None

    async def _check_database_table(
        self,
        db_manager: DatabaseManager,
        database_name: str,
        table_name: str,
        schema_name: str,
    ) -> Optional[LegalHold]:
        """Check legal hold in database table.

        Args:
            db_manager: Database manager
            database_name: Database name
            table_name: Table name
            schema_name: Schema name

        Returns:
            LegalHold if found, None otherwise

        Raises:
            DatabaseError: If query fails
        """
        # Parse table name (format: schema.table or just table)
        if "." in self.check_table:
            hold_schema, hold_table = self.check_table.split(".", 1)
        else:
            hold_schema = "public"
            hold_table = self.check_table

        # Query for active legal holds
        # Expected columns: table_name, schema_name, reason, start_date, expiration_date, requestor, where_clause
        query = f"""
            SELECT
                table_name,
                schema_name,
                reason,
                start_date,
                expiration_date,
                requestor,
                where_clause
            FROM {hold_schema}.{hold_table}
            WHERE table_name = $1
              AND schema_name = $2
              AND start_date <= NOW()
              AND (expiration_date IS NULL OR expiration_date > NOW())
            ORDER BY start_date DESC
            LIMIT 1
        """

        try:
            row = await db_manager.fetchone(query, table_name, schema_name)
            if not row:
                return None

            # Parse expiration date
            expiration_date = row.get("expiration_date")
            if expiration_date and isinstance(expiration_date, str):
                try:
                    expiration_date = datetime.fromisoformat(expiration_date.replace("Z", "+00:00"))
                except Exception:
                    expiration_date = None

            # Parse start date
            start_date = row.get("start_date")
            if isinstance(start_date, str):
                try:
                    start_date = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                except Exception:
                    start_date = datetime.now(timezone.utc)
            elif start_date is None:
                start_date = datetime.now(timezone.utc)

            hold = LegalHold(
                table_name=row.get("table_name", table_name),
                schema_name=row.get("schema_name", schema_name),
                reason=row.get("reason", "Legal hold"),
                start_date=start_date,
                expiration_date=expiration_date,
                requestor=row.get("requestor", "Unknown"),
                where_clause=row.get("where_clause"),
            )

            self.logger.info(
                "Legal hold found",
                database=database_name,
                table=table_name,
                schema=schema_name,
                reason=hold.reason,
                requestor=hold.requestor,
                expiration_date=hold.expiration_date,
            )

            return hold

        except Exception as e:
            raise DatabaseError(
                f"Failed to check legal hold in database table: {e}",
                context={
                    "database": database_name,
                    "table": table_name,
                    "schema": schema_name,
                    "check_table": self.check_table,
                },
            ) from e

    async def _check_api(
        self, database_name: str, table_name: str, schema_name: str
    ) -> Optional[LegalHold]:
        """Check legal hold via API endpoint.

        Args:
            database_name: Database name
            table_name: Table name
            schema_name: Schema name

        Returns:
            LegalHold if found, None otherwise
        """
        import aiohttp

        # API should return JSON with legal hold information
        # Expected format:
        # {
        #   "has_hold": true,
        #   "table_name": "audit_logs",
        #   "schema_name": "public",
        #   "reason": "Legal case XYZ",
        #   "start_date": "2024-01-01T00:00:00Z",
        #   "expiration_date": "2024-12-31T23:59:59Z",
        #   "requestor": "legal@example.com",
        #   "where_clause": "user_id = 123"
        # }

        url = f"{self.api_endpoint}/legal-holds/{database_name}/{schema_name}/{table_name}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=self.api_timeout)
                ) as response:
                    if response.status == 404:
                        return None

                    response.raise_for_status()
                    data = await response.json()

                    if not data.get("has_hold", False):
                        return None

                    # Parse dates
                    start_date_str = data.get("start_date")
                    expiration_date_str = data.get("expiration_date")

                    start_date = datetime.now(timezone.utc)
                    if start_date_str:
                        try:
                            start_date = datetime.fromisoformat(
                                start_date_str.replace("Z", "+00:00")
                            )
                        except Exception:
                            pass

                    expiration_date = None
                    if expiration_date_str:
                        try:
                            expiration_date = datetime.fromisoformat(
                                expiration_date_str.replace("Z", "+00:00")
                            )
                        except Exception:
                            pass

                    hold = LegalHold(
                        table_name=data.get("table_name", table_name),
                        schema_name=data.get("schema_name", schema_name),
                        reason=data.get("reason", "Legal hold"),
                        start_date=start_date,
                        expiration_date=expiration_date,
                        requestor=data.get("requestor", "Unknown"),
                        where_clause=data.get("where_clause"),
                    )

                    self.logger.info(
                        "Legal hold found via API",
                        database=database_name,
                        table=table_name,
                        schema=schema_name,
                        reason=hold.reason,
                        requestor=hold.requestor,
                    )

                    return hold

        except aiohttp.ClientError as e:
            self.logger.warning(
                "Failed to check legal hold via API",
                database=database_name,
                table=table_name,
                error=str(e),
            )
            return None
        except Exception as e:
            self.logger.warning(
                "Unexpected error checking legal hold via API",
                database=database_name,
                table=table_name,
                error=str(e),
            )
            return None
