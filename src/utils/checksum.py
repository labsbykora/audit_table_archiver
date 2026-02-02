"""Checksum utilities for data integrity verification."""

import hashlib
from typing import Optional

import structlog

from utils.logging import get_logger


class ChecksumCalculator:
    """Calculates and verifies SHA-256 checksums for data integrity."""

    def __init__(self, logger: Optional[structlog.BoundLogger] = None) -> None:
        """Initialize checksum calculator.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or get_logger("checksum")

    def calculate_sha256(self, data: bytes) -> str:
        """Calculate SHA-256 checksum of data.

        Args:
            data: Data to checksum

        Returns:
            Hexadecimal SHA-256 checksum (64 characters)
        """
        sha256_hash = hashlib.sha256(data)
        checksum = sha256_hash.hexdigest()

        self.logger.debug(
            "Checksum calculated",
            checksum=checksum,
            data_size=len(data),
        )

        return checksum

    def verify_checksum(self, data: bytes, expected_checksum: str) -> bool:
        """Verify data matches expected checksum.

        Args:
            data: Data to verify
            expected_checksum: Expected SHA-256 checksum (hexadecimal)

        Returns:
            True if checksum matches, False otherwise
        """
        actual_checksum = self.calculate_sha256(data)

        if actual_checksum.lower() != expected_checksum.lower():
            self.logger.error(
                "Checksum verification failed",
                expected=expected_checksum,
                actual=actual_checksum,
                data_size=len(data),
            )
            return False

        self.logger.debug(
            "Checksum verification passed",
            checksum=actual_checksum,
            data_size=len(data),
        )

        return True

    def verify_checksum_or_raise(self, data: bytes, expected_checksum: str) -> None:
        """Verify data matches expected checksum, raise exception if not.

        Args:
            data: Data to verify
            expected_checksum: Expected SHA-256 checksum (hexadecimal)

        Raises:
            ValueError: If checksum doesn't match
        """
        if not self.verify_checksum(data, expected_checksum):
            raise ValueError(
                f"Checksum mismatch: expected {expected_checksum}, "
                f"got {self.calculate_sha256(data)}"
            )

