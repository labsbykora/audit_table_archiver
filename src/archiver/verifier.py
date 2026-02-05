"""Data verification utilities."""

from typing import Any, Optional

import structlog

from archiver.exceptions import VerificationError
from utils.logging import get_logger


class Verifier:
    """Handles data verification at multiple levels."""

    def __init__(self, logger: Optional[structlog.BoundLogger] = None) -> None:
        """Initialize verifier.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("verifier")

    def verify_counts(
        self,
        db_count: int,
        memory_count: int,
        s3_count: int,
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        """Verify that all three counts match.

        Args:
            db_count: Count from database query
            memory_count: Count from in-memory records
            s3_count: Count from S3 JSONL file
            context: Optional context for logging

        Raises:
            VerificationError: If counts don't match
        """
        context = context or {}
        self.logger.debug(
            "Verifying counts",
            db_count=db_count,
            memory_count=memory_count,
            s3_count=s3_count,
            **context,
        )

        if db_count != memory_count:
            raise VerificationError(
                f"Count mismatch: DB count ({db_count}) != Memory count ({memory_count})",
                context={
                    "db_count": db_count,
                    "memory_count": memory_count,
                    "s3_count": s3_count,
                    **context,
                },
            )

        if memory_count != s3_count:
            raise VerificationError(
                f"Count mismatch: Memory count ({memory_count}) != S3 count ({s3_count})",
                context={
                    "db_count": db_count,
                    "memory_count": memory_count,
                    "s3_count": s3_count,
                    **context,
                },
            )

        if db_count != s3_count:
            raise VerificationError(
                f"Count mismatch: DB count ({db_count}) != S3 count ({s3_count})",
                context={
                    "db_count": db_count,
                    "memory_count": memory_count,
                    "s3_count": s3_count,
                    **context,
                },
            )

        self.logger.debug(
            "Count verification passed",
            count=db_count,
            **context,
        )

    def verify_primary_keys(
        self,
        fetched_pks: list[Any],
        delete_pks: list[Any],
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        """Verify that primary keys match between fetched and delete operations.

        Args:
            fetched_pks: Primary keys from fetched records
            delete_pks: Primary keys used in delete operation
            context: Optional context for logging

        Raises:
            VerificationError: If primary keys don't match
        """
        context = context or {}

        # Convert to sets for comparison (order doesn't matter)
        fetched_set = set(fetched_pks)
        delete_set = set(delete_pks)

        if fetched_set != delete_set:
            missing_in_delete = fetched_set - delete_set
            extra_in_delete = delete_set - fetched_set

            raise VerificationError(
                f"Primary key mismatch: {len(missing_in_delete)} missing in delete, "
                f"{len(extra_in_delete)} extra in delete",
                context={
                    "fetched_count": len(fetched_pks),
                    "delete_count": len(delete_pks),
                    "missing_in_delete": list(missing_in_delete)[:10],  # First 10
                    "extra_in_delete": list(extra_in_delete)[:10],  # First 10
                    **context,
                },
            )

        self.logger.debug(
            "Primary key verification passed",
            count=len(fetched_pks),
            **context,
        )
