"""Query plan analysis for performance optimization."""

import re
from typing import Any, Optional

import structlog

from archiver.exceptions import DatabaseError
from utils.logging import get_logger


class QueryAnalyzer:
    """Analyzes PostgreSQL query plans for performance optimization."""

    def __init__(
        self,
        logger: Optional[structlog.BoundLogger] = None,
        slow_query_threshold: float = 2.0,
        warn_on_seq_scan: bool = True,
    ) -> None:
        """Initialize query analyzer.

        Args:
            logger: Optional logger instance
            slow_query_threshold: Threshold in seconds for slow query warnings
            warn_on_seq_scan: Whether to warn on sequential scans
        """
        self.logger = logger or get_logger("query_analyzer")
        self.slow_query_threshold = slow_query_threshold
        self.warn_on_seq_scan = warn_on_seq_scan

    async def analyze_query_plan(
        self,
        db_manager: Any,
        query: str,
        params: tuple[Any, ...],
        query_time: float,
        database: str,
        table: str,
    ) -> dict[str, Any]:
        """Analyze query execution plan using EXPLAIN ANALYZE.

        Args:
            db_manager: Database manager instance
            query: SQL query to analyze
            params: Query parameters
            query_time: Actual query execution time in seconds
            database: Database name
            table: Table name

        Returns:
            Dictionary with analysis results

        Raises:
            DatabaseError: If analysis fails
        """
        analysis = {
            "query_time": query_time,
            "is_slow": query_time > self.slow_query_threshold,
            "has_seq_scan": False,
            "has_index_scan": False,
            "plan_nodes": [],
            "suggestions": [],
        }

        # Only analyze if query is slow or seq scan warnings enabled
        if not analysis["is_slow"] and not self.warn_on_seq_scan:
            return analysis

        try:
            # Get query plan
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) {query}"
            plan_result = await db_manager.fetch(explain_query, *params)

            if not plan_result:
                return analysis

            # Parse JSON plan (asyncpg returns JSON as string or dict)
            import json

            plan_data = plan_result[0][0] if plan_result else None
            if plan_data is None:
                return analysis

            # Handle both string and dict formats
            if isinstance(plan_data, str):
                plan_json = json.loads(plan_data)
            else:
                plan_json = plan_data

            if not plan_json or not isinstance(plan_json, list) or not plan_json:
                return analysis

            # Extract plan from first element
            plan_entry = plan_json[0] if isinstance(plan_json[0], dict) else plan_json
            plan = plan_entry.get("Plan") if isinstance(plan_entry, dict) else None
            if not plan:
                return analysis

            # Analyze plan nodes recursively
            self._analyze_plan_node(plan, analysis)

            # Generate suggestions
            if analysis["has_seq_scan"] and self.warn_on_seq_scan:
                analysis["suggestions"].append(
                    f"Sequential scan detected on {table}. Consider creating an index on the timestamp column."
                )

            if analysis["is_slow"]:
                analysis["suggestions"].append(
                    f"Query took {query_time:.2f}s (threshold: {self.slow_query_threshold}s). "
                    "Consider optimizing query or reducing batch size."
                )

            # Log warnings
            if analysis["has_seq_scan"] and self.warn_on_seq_scan:
                self.logger.warning(
                    "Sequential scan detected - missing index",
                    database=database,
                    table=table,
                    query_time=query_time,
                    suggestion=analysis["suggestions"][0] if analysis["suggestions"] else None,
                )

            if analysis["is_slow"]:
                self.logger.warning(
                    "Slow query detected",
                    database=database,
                    table=table,
                    query_time=query_time,
                    threshold=self.slow_query_threshold,
                    has_seq_scan=analysis["has_seq_scan"],
                )

        except Exception as e:
            # Don't fail on analysis errors - it's optional
            self.logger.debug(
                "Query plan analysis failed (non-critical)",
                database=database,
                table=table,
                error=str(e),
            )

        return analysis

    def _analyze_plan_node(self, node: dict[str, Any], analysis: dict[str, Any]) -> None:
        """Recursively analyze query plan nodes.

        Args:
            node: Plan node dictionary
            analysis: Analysis results dictionary to update
        """
        node_type = node.get("Node Type", "").lower()

        # Check for sequential scan
        if "seq scan" in node_type:
            analysis["has_seq_scan"] = True
            analysis["plan_nodes"].append(
                {
                    "type": "Seq Scan",
                    "relation": node.get("Relation Name"),
                    "cost": node.get("Total Cost"),
                }
            )

        # Check for index scan
        if "index" in node_type and "scan" in node_type:
            analysis["has_index_scan"] = True
            analysis["plan_nodes"].append(
                {
                    "type": "Index Scan",
                    "index": node.get("Index Name"),
                    "cost": node.get("Total Cost"),
                }
            )

        # Recursively analyze child nodes
        if "Plans" in node:
            for child in node["Plans"]:
                self._analyze_plan_node(child, analysis)

    def suggest_indexes(
        self,
        database: str,
        table: str,
        schema: str,
        timestamp_column: str,
        primary_key: str,
    ) -> list[str]:
        """Suggest indexes based on query patterns.

        Args:
            database: Database name
            table: Table name
            schema: Schema name
            timestamp_column: Timestamp column name
            primary_key: Primary key column name

        Returns:
            List of suggested index creation SQL statements
        """
        suggestions = []

        # Composite index for timestamp + primary key (covers WHERE and ORDER BY)
        suggestions.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_{timestamp_column}_{primary_key} "
            f"ON {schema}.{table}({timestamp_column}, {primary_key});"
        )

        # Single column index on timestamp (if composite not used)
        suggestions.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_{timestamp_column} "
            f"ON {schema}.{table}({timestamp_column});"
        )

        return suggestions

