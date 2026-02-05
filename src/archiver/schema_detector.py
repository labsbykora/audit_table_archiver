"""Schema detection and management for archived tables."""

from typing import Any, Optional

import structlog

from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError
from utils.logging import get_logger


class SchemaDetector:
    """Detects and extracts table schema information from PostgreSQL."""

    def __init__(self, logger: Optional[structlog.BoundLogger] = None) -> None:
        """Initialize schema detector.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("schema_detector")

    async def detect_table_schema(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
    ) -> dict[str, Any]:
        """Detect complete table schema including columns, types, constraints, and indexes.

        Args:
            db_manager: Database manager instance
            schema_name: Schema name
            table_name: Table name

        Returns:
            Dictionary containing schema information

        Raises:
            DatabaseError: If schema detection fails
        """
        try:
            self.logger.debug(
                "Detecting table schema",
                schema=schema_name,
                table=table_name,
            )

            # Get columns
            columns = await self._get_columns(db_manager, schema_name, table_name)

            # Get primary key constraint
            primary_key = await self._get_primary_key(db_manager, schema_name, table_name)

            # Get foreign keys
            foreign_keys = await self._get_foreign_keys(db_manager, schema_name, table_name)

            # Get indexes
            indexes = await self._get_indexes(db_manager, schema_name, table_name)

            # Get check constraints
            check_constraints = await self._get_check_constraints(
                db_manager, schema_name, table_name
            )

            # Get unique constraints
            unique_constraints = await self._get_unique_constraints(
                db_manager, schema_name, table_name
            )

            schema_info = {
                "table_name": table_name,
                "schema_name": schema_name,
                "columns": columns,
                "primary_key": primary_key,
                "foreign_keys": foreign_keys,
                "indexes": indexes,
                "check_constraints": check_constraints,
                "unique_constraints": unique_constraints,
            }

            self.logger.debug(
                "Table schema detected",
                schema=schema_name,
                table=table_name,
                column_count=len(columns),
                index_count=len(indexes),
            )

            return schema_info

        except Exception as e:
            raise DatabaseError(
                f"Failed to detect table schema: {e}",
                context={
                    "schema": schema_name,
                    "table": table_name,
                },
            ) from e

    async def _get_columns(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Get column information for a table.

        Args:
            db_manager: Database manager
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of column dictionaries with name, type, nullable, default, etc.
        """
        query = """
            SELECT
                c.column_name,
                c.data_type,
                c.udt_name,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default,
                c.ordinal_position
            FROM information_schema.columns c
            WHERE c.table_schema = $1
              AND c.table_name = $2
            ORDER BY c.ordinal_position
        """

        rows = await db_manager.fetch(query, schema_name, table_name)

        columns = []
        for row in rows:
            columns.append(
                {
                    "name": row["column_name"],
                    "data_type": row["data_type"],
                    "udt_name": row["udt_name"],  # User-defined type name
                    "character_maximum_length": row["character_maximum_length"],
                    "numeric_precision": row["numeric_precision"],
                    "numeric_scale": row["numeric_scale"],
                    "is_nullable": row["is_nullable"] == "YES",
                    "default": row["column_default"],
                    "ordinal_position": row["ordinal_position"],
                }
            )

        return columns

    async def _get_primary_key(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
    ) -> Optional[dict[str, Any]]:
        """Get primary key constraint information.

        Args:
            db_manager: Database manager
            schema_name: Schema name
            table_name: Table name

        Returns:
            Dictionary with constraint name and column list, or None if no PK
        """
        query = """
            SELECT
                tc.constraint_name,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = $1
              AND tc.table_name = $2
              AND tc.constraint_type = 'PRIMARY KEY'
            GROUP BY tc.constraint_name
        """

        row = await db_manager.fetchone(query, schema_name, table_name)

        if not row:
            return None

        return {
            "constraint_name": row["constraint_name"],
            "columns": row["columns"],
        }

    async def _get_foreign_keys(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Get foreign key constraints.

        Args:
            db_manager: Database manager
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of foreign key dictionaries
        """
        query = """
            SELECT
                tc.constraint_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = $1
              AND tc.table_name = $2
            ORDER BY tc.constraint_name, kcu.ordinal_position
        """

        rows = await db_manager.fetch(query, schema_name, table_name)

        # Group by constraint name
        fks_by_constraint: dict[str, dict[str, Any]] = {}
        for row in rows:
            constraint_name = row["constraint_name"]
            if constraint_name not in fks_by_constraint:
                fks_by_constraint[constraint_name] = {
                    "constraint_name": constraint_name,
                    "columns": [],
                    "referenced_schema": row["foreign_table_schema"],
                    "referenced_table": row["foreign_table_name"],
                    "referenced_columns": [],
                }
            fks_by_constraint[constraint_name]["columns"].append(row["column_name"])
            fks_by_constraint[constraint_name]["referenced_columns"].append(
                row["foreign_column_name"]
            )

        return list(fks_by_constraint.values())

    async def _get_indexes(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Get index information.

        Args:
            db_manager: Database manager
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of index dictionaries
        """
        query = """
            SELECT
                i.indexname,
                i.indexdef,
                array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns,
                ix.indisunique,
                ix.indisprimary
            FROM pg_indexes i
            JOIN pg_class c ON c.relname = i.indexname
            JOIN pg_index ix ON ix.indexrelid = c.oid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE i.schemaname = $1
              AND i.tablename = $2
              AND NOT ix.indisprimary  -- Exclude primary key indexes
            GROUP BY i.indexname, i.indexdef, ix.indisunique, ix.indisprimary
            ORDER BY i.indexname
        """

        rows = await db_manager.fetch(query, schema_name, table_name)

        indexes = []
        for row in rows:
            indexes.append(
                {
                    "name": row["indexname"],
                    "definition": row["indexdef"],
                    "columns": row["columns"],
                    "is_unique": row["indisunique"],
                }
            )

        return indexes

    async def _get_check_constraints(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Get check constraints.

        Args:
            db_manager: Database manager
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of check constraint dictionaries
        """
        query = """
            SELECT
                constraint_name,
                check_clause
            FROM information_schema.check_constraints cc
            WHERE cc.constraint_name IN (
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_schema = $1
                  AND table_name = $2
                  AND constraint_type = 'CHECK'
            )
        """

        rows = await db_manager.fetch(query, schema_name, table_name)

        return [
            {
                "constraint_name": row["constraint_name"],
                "check_clause": row["check_clause"],
            }
            for row in rows
        ]

    async def _get_unique_constraints(
        self,
        db_manager: DatabaseManager,
        schema_name: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Get unique constraints (excluding primary key).

        Args:
            db_manager: Database manager
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of unique constraint dictionaries
        """
        query = """
            SELECT
                tc.constraint_name,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = $1
              AND tc.table_name = $2
              AND tc.constraint_type = 'UNIQUE'
            GROUP BY tc.constraint_name
        """

        rows = await db_manager.fetch(query, schema_name, table_name)

        return [
            {
                "constraint_name": row["constraint_name"],
                "columns": row["columns"],
            }
            for row in rows
        ]
