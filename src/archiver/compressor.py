"""Compression utilities for archived data."""

import gzip
from io import BytesIO
from typing import Optional

import structlog

from archiver.exceptions import ArchiverError
from utils.logging import get_logger


class Compressor:
    """Handles gzip compression of data."""

    def __init__(
        self,
        compression_level: int = 6,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize compressor.

        Args:
            compression_level: Gzip compression level (1-9, default: 6)
            logger: Optional logger instance
        """
        if not 1 <= compression_level <= 9:
            raise ValueError(f"Compression level must be between 1 and 9, got {compression_level}")

        self.compression_level = compression_level
        self.logger = logger or get_logger("compressor")

    def compress(self, data: bytes) -> tuple[bytes, int, int]:
        """Compress data using gzip.

        Args:
            data: Uncompressed data

        Returns:
            Tuple of (compressed_data, uncompressed_size, compressed_size)
        """
        uncompressed_size = len(data)

        try:
            buffer = BytesIO()
            with gzip.GzipFile(
                fileobj=buffer,
                mode="wb",
                compresslevel=self.compression_level,
            ) as gz_file:
                gz_file.write(data)

            compressed_data = buffer.getvalue()
            compressed_size = len(compressed_data)

            compression_ratio = (
                (1 - compressed_size / uncompressed_size) * 100 if uncompressed_size > 0 else 0
            )

            self.logger.debug(
                "Compression completed",
                uncompressed_size=uncompressed_size,
                compressed_size=compressed_size,
                compression_ratio=f"{compression_ratio:.1f}%",
                level=self.compression_level,
            )

            return compressed_data, uncompressed_size, compressed_size

        except Exception as e:
            raise ArchiverError(
                f"Compression failed: {e}",
                context={"uncompressed_size": uncompressed_size},
            ) from e

    def decompress(self, compressed_data: bytes) -> bytes:
        """Decompress gzip data.

        Args:
            compressed_data: Compressed data

        Returns:
            Uncompressed data

        Raises:
            ArchiverError: If decompression fails
        """
        try:
            return gzip.decompress(compressed_data)
        except Exception as e:
            raise ArchiverError(
                f"Decompression failed: {e}",
                context={"compressed_size": len(compressed_data)},
            ) from e
