"""Archive validation utilities for verifying archive integrity."""

from datetime import datetime
from typing import Any, Optional

import structlog

from archiver.config import S3Config
from archiver.exceptions import S3Error, VerificationError
from restore.s3_reader import S3ArchiveReader
from utils.logging import get_logger


class ValidationResult:
    """Represents the result of archive validation."""

    def __init__(
        self,
        total_archives: int,
        valid_archives: int,
        invalid_archives: int,
        orphaned_files: list[str],
        missing_metadata: list[str],
        checksum_failures: list[str],
        record_count_mismatches: list[str],
        errors: list[dict[str, Any]],
    ) -> None:
        """Initialize validation result.

        Args:
            total_archives: Total number of archive files checked
            valid_archives: Number of valid archives
            invalid_archives: Number of invalid archives
            orphaned_files: List of orphaned file keys
            missing_metadata: List of archives with missing metadata
            checksum_failures: List of archives with checksum failures
            record_count_mismatches: List of archives with record count mismatches
            errors: List of error details
        """
        self.total_archives = total_archives
        self.valid_archives = valid_archives
        self.invalid_archives = invalid_archives
        self.orphaned_files = orphaned_files
        self.missing_metadata = missing_metadata
        self.checksum_failures = checksum_failures
        self.record_count_mismatches = record_count_mismatches
        self.errors = errors

    @property
    def is_valid(self) -> bool:
        """Check if all archives are valid."""
        return (
            self.invalid_archives == 0
            and len(self.orphaned_files) == 0
            and len(self.missing_metadata) == 0
            and len(self.checksum_failures) == 0
            and len(self.record_count_mismatches) == 0
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_archives": self.total_archives,
            "valid_archives": self.valid_archives,
            "invalid_archives": self.invalid_archives,
            "orphaned_files": self.orphaned_files,
            "missing_metadata": self.missing_metadata,
            "checksum_failures": self.checksum_failures,
            "record_count_mismatches": self.record_count_mismatches,
            "errors": self.errors,
            "is_valid": self.is_valid,
        }

    def to_string(self) -> str:
        """Generate human-readable report."""
        lines = [f"Archive Validation Report: {self.total_archives} archive(s) checked"]
        lines.append("=" * 70)

        lines.append(f"\nSummary:")
        lines.append(f"  Valid: {self.valid_archives}")
        lines.append(f"  Invalid: {self.invalid_archives}")
        lines.append(f"  Overall Status: {'✓ VALID' if self.is_valid else '✗ INVALID'}")

        if self.orphaned_files:
            lines.append(f"\nOrphaned Files ({len(self.orphaned_files)}):")
            for file in self.orphaned_files[:10]:
                lines.append(f"  - {file}")
            if len(self.orphaned_files) > 10:
                lines.append(f"  ... and {len(self.orphaned_files) - 10} more")

        if self.missing_metadata:
            lines.append(f"\nMissing Metadata ({len(self.missing_metadata)}):")
            for file in self.missing_metadata[:10]:
                lines.append(f"  - {file}")
            if len(self.missing_metadata) > 10:
                lines.append(f"  ... and {len(self.missing_metadata) - 10} more")

        if self.checksum_failures:
            lines.append(f"\nChecksum Failures ({len(self.checksum_failures)}):")
            for file in self.checksum_failures[:10]:
                lines.append(f"  - {file}")
            if len(self.checksum_failures) > 10:
                lines.append(f"  ... and {len(self.checksum_failures) - 10} more")

        if self.record_count_mismatches:
            lines.append(f"\nRecord Count Mismatches ({len(self.record_count_mismatches)}):")
            for file in self.record_count_mismatches[:10]:
                lines.append(f"  - {file}")
            if len(self.record_count_mismatches) > 10:
                lines.append(f"  ... and {len(self.record_count_mismatches) - 10} more")

        if self.errors:
            lines.append(f"\nErrors ({len(self.errors)}):")
            for error in self.errors[:5]:
                lines.append(f"  - {error.get('file', 'unknown')}: {error.get('error', 'unknown error')}")
            if len(self.errors) > 5:
                lines.append(f"  ... and {len(self.errors) - 5} more errors")

        return "\n".join(lines)


class ArchiveValidator:
    """Validates archive integrity in S3."""

    def __init__(
        self,
        s3_config: S3Config,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize archive validator.

        Args:
            s3_config: S3 configuration
            logger: Optional logger instance
        """
        self.s3_config = s3_config
        self.logger = logger or get_logger("archive_validator")
        self.s3_reader = S3ArchiveReader(s3_config, logger=self.logger)

    async def validate_archive(
        self,
        s3_key: str,
        validate_checksum: bool = True,
        validate_record_count: bool = True,
    ) -> dict[str, Any]:
        """Validate a single archive file.

        Args:
            s3_key: S3 key of the archive file
            validate_checksum: If True, validate checksums
            validate_record_count: If True, validate record counts

        Returns:
            Dictionary with validation results
        """
        self.logger.debug("Validating archive", s3_key=s3_key)

        result = {
            "s3_key": s3_key,
            "valid": True,
            "errors": [],
            "warnings": [],
        }

        try:
            # Read archive (this validates checksums if enabled)
            archive = await self.s3_reader.read_archive(
                s3_key, validate_checksum=validate_checksum
            )

            # Validate record count if requested
            if validate_record_count:
                try:
                    records = archive.parse_records()
                    actual_count = len(records)
                    expected_count = archive.record_count

                    if actual_count != expected_count:
                        result["valid"] = False
                        result["errors"].append(
                            f"Record count mismatch: expected {expected_count}, got {actual_count}"
                        )
                        self.logger.error(
                            "Record count mismatch",
                            s3_key=s3_key,
                            expected=expected_count,
                            actual=actual_count,
                        )
                    else:
                        self.logger.debug(
                            "Record count validated",
                            s3_key=s3_key,
                            count=actual_count,
                        )
                except Exception as e:
                    result["valid"] = False
                    result["errors"].append(f"Failed to parse records: {e}")
                    self.logger.error(
                        "Failed to parse records",
                        s3_key=s3_key,
                        error=str(e),
                    )

            # Validate metadata structure
            if not archive.metadata:
                result["valid"] = False
                result["errors"].append("Missing metadata")
            else:
                # Check required metadata fields
                required_fields = ["batch_info", "data_info", "checksums"]
                for field in required_fields:
                    if field not in archive.metadata:
                        result["valid"] = False
                        result["errors"].append(f"Missing metadata field: {field}")

        self.logger.debug(
            "Archive validation completed",
            s3_key=s3_key,
                valid=result["valid"],
                errors=len(result["errors"]),
            )

        except S3Error as e:
            result["valid"] = False
            result["errors"].append(f"S3 error: {e}")
            self.logger.error("S3 error during validation", s3_key=s3_key, error=str(e))
        except VerificationError as e:
            result["valid"] = False
            result["errors"].append(f"Verification error: {e}")
            self.logger.error(
                "Verification error during validation",
                s3_key=s3_key,
                error=str(e),
            )
        except Exception as e:
            result["valid"] = False
            result["errors"].append(f"Unexpected error: {e}")
            self.logger.error(
                "Unexpected error during validation",
                s3_key=s3_key,
                error=str(e),
                exc_info=True,
            )

        return result

    async def validate_archives(
        self,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        validate_checksum: bool = True,
        validate_record_count: bool = True,
    ) -> ValidationResult:
        """Validate multiple archives.

        Args:
            database_name: Optional database name filter
            table_name: Optional table name filter
            start_date: Optional start date filter
            end_date: Optional end date filter
            validate_checksum: If True, validate checksums
            validate_record_count: If True, validate record counts

        Returns:
            ValidationResult with validation statistics
        """
        self.logger.debug(
            "Starting archive validation",
            database=database_name,
            table=table_name,
            start_date=start_date,
            end_date=end_date,
        )

        # List all archives
        try:
            archive_keys = await self.s3_reader.list_archives(
                database_name=database_name,
                table_name=table_name,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as e:
            self.logger.error("Failed to list archives", error=str(e))
            return ValidationResult(
                total_archives=0,
                valid_archives=0,
                invalid_archives=0,
                orphaned_files=[],
                missing_metadata=[],
                checksum_failures=[],
                record_count_mismatches=[],
                errors=[{"error": f"Failed to list archives: {e}"}],
            )

        if not archive_keys:
            self.logger.warning("No archives found to validate")
            return ValidationResult(
                total_archives=0,
                valid_archives=0,
                invalid_archives=0,
                orphaned_files=[],
                missing_metadata=[],
                checksum_failures=[],
                record_count_mismatches=[],
                errors=[],
            )

        self.logger.debug("Found archives to validate", count=len(archive_keys))

        # Validate each archive
        valid_count = 0
        invalid_count = 0
        orphaned_files: list[str] = []
        missing_metadata: list[str] = []
        checksum_failures: list[str] = []
        record_count_mismatches: list[str] = []
        errors: list[dict[str, Any]] = []

        for s3_key in archive_keys:
            result = await self.validate_archive(
                s3_key,
                validate_checksum=validate_checksum,
                validate_record_count=validate_record_count,
            )

            if result["valid"]:
                valid_count += 1
            else:
                invalid_count += 1
                errors.append({"file": s3_key, "error": "; ".join(result["errors"])})

                # Categorize errors
                for error in result["errors"]:
                    if "checksum" in error.lower():
                        checksum_failures.append(s3_key)
                    elif "record count" in error.lower():
                        record_count_mismatches.append(s3_key)
                    elif "metadata" in error.lower() or "missing" in error.lower():
                        missing_metadata.append(s3_key)

        # Check for orphaned files (metadata files without data files, or vice versa)
        orphaned_files = await self._find_orphaned_files(archive_keys)

        self.logger.debug(
            "Archive validation completed",
            total=len(archive_keys),
            valid=valid_count,
            invalid=invalid_count,
        )

        return ValidationResult(
            total_archives=len(archive_keys),
            valid_archives=valid_count,
            invalid_archives=invalid_count,
            orphaned_files=orphaned_files,
            missing_metadata=missing_metadata,
            checksum_failures=checksum_failures,
            record_count_mismatches=record_count_mismatches,
            errors=errors,
        )

    async def _find_orphaned_files(self, archive_keys: list[str]) -> list[str]:
        """Find orphaned files (metadata without data, or data without metadata).

        Args:
            archive_keys: List of archive data file keys

        Returns:
            List of orphaned file keys
        """
        orphaned: list[str] = []

        # Check each archive has corresponding metadata
        for data_key in archive_keys:
            metadata_key = self.s3_reader._get_metadata_key(data_key)
            try:
                # Try to read metadata (this will fail if it doesn't exist)
                await self.s3_reader.read_archive(data_key, validate_checksum=False)
            except S3Error as e:
                if "metadata" in str(e).lower() or "not found" in str(e).lower():
                    orphaned.append(data_key)
                    self.logger.warning(
                        "Orphaned data file (missing metadata)",
                        data_key=data_key,
                        metadata_key=metadata_key,
                    )

        # Note: We could also check for orphaned metadata files, but that would require
        # listing all files in the bucket, which is more expensive. For now, we focus
        # on data files missing metadata.

        return orphaned

