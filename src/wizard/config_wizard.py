"""Interactive configuration wizard for archiver setup."""

from typing import Any, Optional

import asyncpg
import structlog

from archiver.config import ArchiverConfig, DatabaseConfig, S3Config, TableConfig
from archiver.exceptions import DatabaseError
from utils.logging import get_logger


class ConfigWizard:
    """Interactive wizard for generating archiver configuration."""

    def __init__(
        self,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize configuration wizard.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("config_wizard")

    async def detect_tables(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        schema: str = "public",
    ) -> list[dict[str, Any]]:
        """Auto-detect tables in a database.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            schema: Schema name (default: public)

        Returns:
            List of table information dictionaries
        """
        self.logger.debug("Detecting tables", database=database, schema=schema)

        try:
            conn = await asyncpg.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )

            try:
                # Query for tables with timestamp columns
                query = """
                    SELECT
                        t.table_name,
                        array_agg(c.column_name) FILTER (WHERE c.data_type IN ('timestamp without time zone', 'timestamp with time zone', 'timestamptz', 'timestamp')) as timestamp_columns,
                        array_agg(c.column_name) FILTER (WHERE c.column_name LIKE '%id' AND c.data_type IN ('bigint', 'integer', 'uuid')) as id_columns
                    FROM information_schema.tables t
                    LEFT JOIN information_schema.columns c ON t.table_schema = c.table_schema AND t.table_name = c.table_name
                    WHERE t.table_schema = $1
                      AND t.table_type = 'BASE TABLE'
                    GROUP BY t.table_name
                    ORDER BY t.table_name
                """

                rows = await conn.fetch(query, schema)

                tables = []
                for row in rows:
                    table_name = row["table_name"]
                    timestamp_cols = row["timestamp_columns"] or []
                    id_cols = row["id_columns"] or []

                    # Try to find primary key
                    pk_query = """
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                            ON tc.constraint_name = kcu.constraint_name
                            AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema = $1
                          AND tc.table_name = $2
                          AND tc.constraint_type = 'PRIMARY KEY'
                        ORDER BY kcu.ordinal_position
                        LIMIT 1
                    """
                    pk_row = await conn.fetchrow(pk_query, schema, table_name)
                    primary_key = pk_row["column_name"] if pk_row else (id_cols[0] if id_cols else None)

                    # Suggest timestamp column (prefer 'created_at', 'updated_at', 'timestamp')
                    suggested_timestamp = None
                    for col in timestamp_cols:
                        if col.lower() in ("created_at", "updated_at", "timestamp", "ts"):
                            suggested_timestamp = col
                            break
                    if not suggested_timestamp and timestamp_cols:
                        suggested_timestamp = timestamp_cols[0]

                    tables.append({
                        "name": table_name,
                        "schema": schema,
                        "timestamp_columns": timestamp_cols,
                        "suggested_timestamp": suggested_timestamp,
                        "id_columns": id_cols,
                        "primary_key": primary_key,
                    })

                self.logger.debug("Tables detected", database=database, count=len(tables))
                return tables

            finally:
                await conn.close()

        except Exception as e:
            raise DatabaseError(
                f"Failed to detect tables: {e}",
                context={"database": database, "host": host},
            ) from e

    async def estimate_record_count(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        schema: str,
        table: str,
        timestamp_column: str,
        retention_days: int,
    ) -> dict[str, Any]:
        """Estimate record count and suggest retention period.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            schema: Schema name
            table: Table name
            timestamp_column: Timestamp column name
            retention_days: Proposed retention period in days

        Returns:
            Dictionary with estimates and suggestions
        """
        self.logger.debug(
            "Estimating record count",
            database=database,
            table=table,
            retention_days=retention_days,
        )

        try:
            conn = await asyncpg.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )

            try:
                # Get total record count
                total_count = await conn.fetchval(
                    f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                )

                # Get count of records older than retention period
                cutoff_query = f"""
                    SELECT COUNT(*)
                    FROM "{schema}"."{table}"
                    WHERE "{timestamp_column}" < NOW() - INTERVAL '{retention_days} days'
                """
                eligible_count = await conn.fetchval(cutoff_query)

                # Get oldest and newest timestamps
                oldest_query = f'SELECT MIN("{timestamp_column}") FROM "{schema}"."{table}"'
                newest_query = f'SELECT MAX("{timestamp_column}") FROM "{schema}"."{table}"'
                oldest = await conn.fetchval(oldest_query)
                newest = await conn.fetchval(newest_query)

                # Calculate age of oldest record
                age_days = None
                if oldest:
                    age_query = f'SELECT EXTRACT(EPOCH FROM (NOW() - MIN("{timestamp_column}"))) / 86400 FROM "{schema}"."{table}"'
                    age_days = await conn.fetchval(age_query)

                # Suggest retention period based on data age
                suggested_retention = None
                if age_days:
                    # Suggest retention slightly less than oldest data age
                    # Convert Decimal to float for multiplication
                    age_days_float = float(age_days) if age_days else 0.0
                    suggested_retention = max(30, int(age_days_float * 0.9))  # At least 30 days

                return {
                    "total_records": total_count or 0,
                    "eligible_records": eligible_count or 0,
                    "oldest_timestamp": oldest.isoformat() if oldest else None,
                    "newest_timestamp": newest.isoformat() if newest else None,
                    "age_days": int(age_days) if age_days else None,
                    "suggested_retention_days": suggested_retention,
                }

            finally:
                await conn.close()

        except Exception as e:
            raise DatabaseError(
                f"Failed to estimate record count: {e}",
                context={"database": database, "table": table},
            ) from e

    def generate_config(
        self,
        databases: list[dict[str, Any]],
        s3_config: dict[str, Any],
        defaults: dict[str, Any],
    ) -> ArchiverConfig:
        """Generate ArchiverConfig from wizard inputs.

        Args:
            databases: List of database configurations
            s3_config: S3 configuration dictionary
            defaults: Default settings dictionary

        Returns:
            ArchiverConfig object
        """
        self.logger.debug("Generating configuration", database_count=len(databases))

        # Convert database dictionaries to DatabaseConfig objects
        db_configs = []
        for db_dict in databases:
            tables = []
            for table_dict in db_dict.get("tables", []):
                table_config = TableConfig(
                    name=table_dict["name"],
                    schema_name=table_dict.get("schema_name", table_dict.get("schema", "public")),
                    timestamp_column=table_dict["timestamp_column"],
                    primary_key=table_dict["primary_key"],
                    retention_days=table_dict.get("retention_days"),
                    batch_size=table_dict.get("batch_size"),
                )
                tables.append(table_config)

            db_config = DatabaseConfig(
                name=db_dict["name"],
                host=db_dict["host"],
                port=db_dict.get("port", 5432),
                user=db_dict["user"],
                password_env=db_dict.get("password_env", f"DB_PASSWORD_{db_dict['name'].upper()}"),
                tables=tables,
            )
            db_configs.append(db_config)

        # Create S3Config
        s3 = S3Config(**s3_config)

        # Create ArchiverConfig
        from archiver.config import DefaultsConfig

        defaults_config = DefaultsConfig(**defaults)

        config = ArchiverConfig(
            version="1.0",
            databases=db_configs,
            s3=s3,
            defaults=defaults_config,
        )

        return config

    def suggest_batch_size(self, estimated_records: int) -> int:
        """Suggest batch size based on estimated record count.

        Args:
            estimated_records: Estimated number of records to archive

        Returns:
            Suggested batch size
        """
        if estimated_records < 1000:
            return 100
        elif estimated_records < 10000:
            return 500
        elif estimated_records < 100000:
            return 1000
        elif estimated_records < 1000000:
            return 5000
        else:
            return 10000

