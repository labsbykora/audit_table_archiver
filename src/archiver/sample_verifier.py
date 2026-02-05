"""Sample verification for archived data."""

import json
import random
from typing import Any, Optional

import structlog

from archiver.compressor import Compressor
from archiver.exceptions import VerificationError
from archiver.serializer import PostgreSQLSerializer
from utils.logging import get_logger


class SampleVerifier:
    """Verifies archived data by sampling and checking database."""

    def __init__(
        self,
        sample_percentage: float = 0.01,
        min_samples: int = 10,
        max_samples: int = 1000,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize sample verifier.

        Args:
            sample_percentage: Percentage of records to sample (default: 0.01 = 1%)
            min_samples: Minimum number of samples (default: 10)
            max_samples: Maximum number of samples (default: 1000)
            logger: Optional logger instance
        """
        if not 0 < sample_percentage <= 1:
            raise ValueError(f"Sample percentage must be between 0 and 1, got {sample_percentage}")
        if min_samples < 1:
            raise ValueError(f"Min samples must be at least 1, got {min_samples}")
        if max_samples < min_samples:
            raise ValueError(f"Max samples ({max_samples}) must be >= min samples ({min_samples})")

        self.sample_percentage = sample_percentage
        self.min_samples = min_samples
        self.max_samples = max_samples
        self.logger = logger or get_logger("sample_verifier")
        self.serializer = PostgreSQLSerializer(logger=self.logger)
        self.compressor = Compressor(logger=self.logger)

    def select_samples(self, records: list[dict[str, Any]], primary_key_column: str) -> list[Any]:
        """Select random samples from records.

        Args:
            records: List of record dictionaries
            primary_key_column: Name of primary key column

        Returns:
            List of primary key values for sampled records
        """
        if not records:
            return []

        # Calculate sample size
        total_count = len(records)
        sample_size = max(
            self.min_samples,
            min(
                self.max_samples,
                int(total_count * self.sample_percentage),
            ),
        )

        # If we have fewer records than min_samples, sample all
        if total_count <= sample_size:
            sample_size = total_count

        # Select random samples
        sampled_records = random.sample(records, sample_size)
        sample_pks = [record[primary_key_column] for record in sampled_records]

        self.logger.debug(
            "Samples selected",
            total_count=total_count,
            sample_size=sample_size,
            sample_percentage=self.sample_percentage,
        )

        return sample_pks

    def extract_samples_from_s3(
        self,
        s3_data: bytes,
        primary_key_column: str,
        sample_pks: list[Any],
    ) -> list[dict[str, Any]]:
        """Extract sample records from S3 JSONL data.

        Args:
            s3_data: Compressed JSONL data from S3
            primary_key_column: Name of primary key column
            sample_pks: List of primary key values to extract

        Returns:
            List of sample record dictionaries
        """
        # Decompress
        try:
            jsonl_data = self.compressor.decompress(s3_data)
        except Exception as e:
            raise VerificationError(
                f"Failed to decompress S3 data: {e}",
                context={"compressed_size": len(s3_data)},
            ) from e

        # Parse JSONL
        sample_pk_set = set(sample_pks)
        samples = []

        for line in jsonl_data.decode("utf-8").splitlines():
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                pk_value = record.get(primary_key_column)

                if pk_value in sample_pk_set:
                    samples.append(record)
                    sample_pk_set.discard(pk_value)  # Remove to track missing

                    # Early exit if we found all samples
                    if not sample_pk_set:
                        break

            except json.JSONDecodeError as e:
                self.logger.warning(
                    "Failed to parse JSONL line",
                    error=str(e),
                    line_preview=line[:100],
                )
                continue

        if sample_pk_set:
            missing_count = len(sample_pk_set)
            self.logger.warning(
                "Some sample primary keys not found in S3 data",
                missing_count=missing_count,
                missing_sample=list(sample_pk_set)[:10],
            )

        self.logger.debug(
            "Samples extracted from S3",
            requested_count=len(sample_pks),
            found_count=len(samples),
        )

        return samples

    async def verify_samples_not_in_database(
        self,
        db_manager: Any,  # DatabaseManager
        table_schema: str,
        table_name: str,
        primary_key_column: str,
        sample_pks: list[Any],
    ) -> None:
        """Verify that sample primary keys are not in database.

        Args:
            db_manager: Database manager instance
            table_schema: Table schema name
            table_name: Table name
            primary_key_column: Primary key column name
            sample_pks: List of primary key values to verify

        Raises:
            VerificationError: If any samples are found in database
        """
        if not sample_pks:
            self.logger.warning("No samples to verify")
            return

        # Query database for sample primary keys
        query = f"""
            SELECT {primary_key_column}
            FROM {table_schema}.{table_name}
            WHERE {primary_key_column} = ANY($1)
        """

        try:
            found_pks = await db_manager.fetch(query, sample_pks)
            found_pk_set = {row[primary_key_column] for row in found_pks}

            if found_pk_set:
                missing_pks = set(sample_pks) - found_pk_set
                raise VerificationError(
                    f"Sample verification failed: {len(found_pk_set)} of {len(sample_pks)} "
                    f"sample primary keys found in database (should be 0)",
                    context={
                        "table": f"{table_schema}.{table_name}",
                        "total_samples": len(sample_pks),
                        "found_in_db": len(found_pk_set),
                        "found_pks": list(found_pk_set)[:10],
                        "missing_pks": list(missing_pks)[:10] if missing_pks else [],
                    },
                )

            self.logger.debug(
                "Sample verification passed",
                table=f"{table_schema}.{table_name}",
                sample_count=len(sample_pks),
            )

        except Exception as e:
            if isinstance(e, VerificationError):
                raise
            raise VerificationError(
                f"Failed to verify samples in database: {e}",
                context={
                    "table": f"{table_schema}.{table_name}",
                    "sample_count": len(sample_pks),
                },
            ) from e
