"""S3 reader for downloading and validating archived data."""

import gzip
import json
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from archiver.config import S3Config
from archiver.exceptions import S3Error
from archiver.s3_client import S3Client
from utils.checksum import ChecksumCalculator
from utils.logging import get_logger


class ArchiveFile:
    """Represents an archived data file with its metadata."""

    def __init__(
        self,
        s3_key: str,
        metadata: dict[str, Any],
        data: bytes,
        jsonl_data: bytes,
    ) -> None:
        """Initialize archive file.

        Args:
            s3_key: S3 key of the data file
            metadata: Metadata dictionary
            data: Compressed data bytes
            jsonl_data: Decompressed JSONL data bytes
        """
        self.s3_key = s3_key
        self.metadata = metadata
        self.data = data
        self.jsonl_data = jsonl_data

    @property
    def record_count(self) -> int:
        """Get record count from metadata."""
        return self.metadata.get("data_info", {}).get("record_count", 0)

    @property
    def batch_id(self) -> Optional[str]:
        """Get batch ID from metadata."""
        return self.metadata.get("batch_info", {}).get("batch_id")

    @property
    def database_name(self) -> Optional[str]:
        """Get database name from metadata."""
        batch_info = self.metadata.get("batch_info", {})
        # Try both "database" and "database_name" keys for compatibility
        return batch_info.get("database") or batch_info.get("database_name")

    @property
    def table_name(self) -> Optional[str]:
        """Get table name from metadata."""
        batch_info = self.metadata.get("batch_info", {})
        # Try both "table" and "table_name" keys for compatibility
        return batch_info.get("table") or batch_info.get("table_name")

    @property
    def schema_name(self) -> Optional[str]:
        """Get schema name from metadata."""
        return self.metadata.get("batch_info", {}).get("schema", "public")

    @property
    def table_schema(self) -> Optional[dict[str, Any]]:
        """Get table schema from metadata."""
        return self.metadata.get("table_schema")

    def parse_records(self) -> list[dict[str, Any]]:
        """Parse JSONL data into list of records.

        Returns:
            List of record dictionaries
        """
        records = []
        for line in self.jsonl_data.decode("utf-8").strip().split("\n"):
            if line.strip():
                records.append(json.loads(line))
        return records


class S3ArchiveReader:
    """Reads and validates archived data from S3."""

    def __init__(
        self,
        s3_config: S3Config,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize S3 archive reader.

        Args:
            s3_config: S3 configuration
            logger: Optional logger instance
        """
        self.s3_config = s3_config
        self.logger = logger or get_logger("s3_archive_reader")
        self.s3_client = S3Client(s3_config, logger=self.logger)
        self.checksum_calculator = ChecksumCalculator()

    def _get_metadata_key(self, data_key: str) -> str:
        """Get metadata key from data key.

        Args:
            data_key: S3 key of data file

        Returns:
            S3 key of metadata file
        """
        # Replace .jsonl.gz with .metadata.json
        if data_key.endswith(".jsonl.gz"):
            return data_key.replace(".jsonl.gz", ".metadata.json")
        # If no extension, append .metadata.json
        return f"{data_key}.metadata.json"

    async def read_archive(self, s3_key: str, validate_checksum: bool = True) -> ArchiveFile:
        """Read archive file from S3 and validate.

        Args:
            s3_key: S3 key of the data file
            validate_checksum: If True, validate checksums

        Returns:
            ArchiveFile object

        Raises:
            S3Error: If file cannot be read or checksum validation fails
        """
        self.logger.debug("Reading archive from S3", s3_key=s3_key)

        # Read metadata file
        metadata_key = self._get_metadata_key(s3_key)
        metadata = None
        try:
            metadata_bytes = self.s3_client.get_object_bytes(metadata_key)
            metadata = json.loads(metadata_bytes.decode("utf-8"))
            self.logger.debug("Metadata file read", metadata_key=metadata_key)
        except Exception:
            # Try alternative metadata key patterns
            alternative_keys = []

            # Try with schema name inserted: archives/db/public/table/...
            key_parts = s3_key.split("/")
            if len(key_parts) >= 3:
                # Insert "public" after database name
                db_idx = 1 if key_parts[0] == self.s3_config.prefix.rstrip("/") else 0
                if db_idx + 1 < len(key_parts):
                    alt_parts = key_parts.copy()
                    alt_parts.insert(db_idx + 1, "public")
                    alt_key = "/".join(alt_parts).replace(".jsonl.gz", ".metadata.json")
                    alternative_keys.append(alt_key)

            # Try without prefix (in case prefix was added twice)
            if self.s3_config.prefix:
                prefix = self.s3_config.prefix.rstrip("/")
                if s3_key.startswith(prefix + "/"):
                    alt_key = s3_key[len(prefix) + 1 :].replace(".jsonl.gz", ".metadata.json")
                    alternative_keys.append(alt_key)

            # Try each alternative
            for alt_key in alternative_keys:
                if alt_key and alt_key != metadata_key:
                    try:
                        self.logger.debug("Trying alternative metadata key", metadata_key=alt_key)
                        metadata_bytes = self.s3_client.get_object_bytes(alt_key)
                        metadata = json.loads(metadata_bytes.decode("utf-8"))
                        self.logger.debug(
                            "Metadata file found with alternative key", metadata_key=alt_key
                        )
                        metadata_key = alt_key  # Update metadata_key for consistency
                        break
                    except Exception:
                        continue

            if metadata is None:
                # Metadata is optional - we'll create minimal metadata after reading the data file
                self.logger.warning(
                    "Metadata file not found, will create minimal metadata from data file",
                    metadata_key=metadata_key,
                    s3_key=s3_key,
                    alternative_keys_tried=alternative_keys,
                )
                metadata = {}

        # Read data file
        compressed_data = None
        try:
            compressed_data = self.s3_client.get_object_bytes(s3_key)
            self.logger.debug("Data file read", s3_key=s3_key, size=len(compressed_data))
        except Exception as e:
            # Try alternative paths for data file
            alternative_keys = []

            # Try without prefix (in case prefix was added twice during upload)
            if self.s3_config.prefix:
                prefix = self.s3_config.prefix.rstrip("/")
                if s3_key.startswith(prefix + "/"):
                    alt_key = s3_key[len(prefix) + 1 :]
                    alternative_keys.append(alt_key)

            # Try with schema name inserted: archives/db/public/table/...
            key_parts = s3_key.split("/")
            if len(key_parts) >= 3:
                # Find database index (after prefix)
                db_idx = 1 if key_parts[0] == self.s3_config.prefix.rstrip("/") else 0
                if db_idx + 1 < len(key_parts) and key_parts[db_idx + 1] != "public":
                    alt_parts = key_parts.copy()
                    alt_parts.insert(db_idx + 1, "public")
                    alt_key = "/".join(alt_parts)
                    alternative_keys.append(alt_key)

            # Try each alternative
            for alt_key in alternative_keys:
                if alt_key != s3_key:
                    try:
                        self.logger.debug("Trying alternative data file key", s3_key=alt_key)
                        compressed_data = self.s3_client.get_object_bytes(alt_key)
                        self.logger.debug("Data file found with alternative key", s3_key=alt_key)
                        break
                    except Exception:
                        continue

            if compressed_data is None:
                # Provide helpful error message
                error_msg = f"Failed to read data file from S3. Tried key: {s3_key}"
                if alternative_keys:
                    error_msg += f" and alternatives: {alternative_keys}"
                # Try to extract database and table from key for helpful error message
                key_parts = s3_key.split("/")
                db_name = key_parts[1] if len(key_parts) > 1 else "DATABASE"
                table_name = key_parts[2] if len(key_parts) > 2 else "TABLE"
                # Skip schema if present
                if table_name == "public" and len(key_parts) > 3:
                    table_name = key_parts[3]

                error_msg += (
                    f"\n\nTo list available archives, use:\n"
                    f"  python -m restore.main --config <config> --database {db_name} --table {table_name}"
                )
                raise S3Error(
                    error_msg,
                    context={
                        "s3_key": s3_key,
                        "alternative_keys_tried": alternative_keys,
                        "bucket": self.s3_config.bucket,
                    },
                ) from e

        # Decompress
        try:
            jsonl_data = gzip.decompress(compressed_data)
            self.logger.debug("Data decompressed", uncompressed_size=len(jsonl_data))
        except Exception as e:
            raise S3Error(
                f"Failed to decompress data: {e}",
                context={"s3_key": s3_key},
            ) from e

        # Create minimal metadata if missing
        if not metadata:
            # Parse first record to extract basic info
            try:
                first_line = jsonl_data.split(b"\n")[0].decode("utf-8")
                first_record = json.loads(first_line)
                record_count = len(jsonl_data.split(b"\n")) - 1  # Subtract 1 for trailing newline

                metadata = {
                    "batch_info": {
                        "database_name": first_record.get("_source_database", "unknown"),
                        "table_name": first_record.get("_source_table", "unknown"),
                        "batch_id": first_record.get("_batch_id", "unknown"),
                    },
                    "data_info": {
                        "record_count": record_count,
                        "uncompressed_size": len(jsonl_data),
                        "compressed_size": len(compressed_data),
                    },
                    "checksums": {},  # No checksums available
                }
                self.logger.debug(
                    "Created minimal metadata from data file",
                    record_count=record_count,
                    s3_key=s3_key,
                )
            except Exception as e:
                self.logger.warning(
                    "Failed to create minimal metadata, using empty metadata",
                    error=str(e),
                    s3_key=s3_key,
                )
                metadata = {
                    "batch_info": {},
                    "data_info": {},
                    "checksums": {},
                }

        # Validate checksums if requested and available
        if validate_checksum and metadata.get("checksums"):
            self._validate_checksums(metadata, compressed_data, jsonl_data, s3_key)
        elif validate_checksum and not metadata.get("checksums"):
            self.logger.warning(
                "Checksum validation requested but no checksums in metadata",
                s3_key=s3_key,
            )

        return ArchiveFile(
            s3_key=s3_key,
            metadata=metadata,
            data=compressed_data,
            jsonl_data=jsonl_data,
        )

    def _validate_checksums(
        self,
        metadata: dict[str, Any],
        compressed_data: bytes,
        jsonl_data: bytes,
        s3_key: str,
    ) -> None:
        """Validate checksums from metadata.

        Args:
            metadata: Metadata dictionary
            compressed_data: Compressed data bytes
            jsonl_data: Decompressed JSONL data bytes
            s3_key: S3 key for error context

        Raises:
            S3Error: If checksum validation fails
        """
        checksums = metadata.get("checksums", {})
        expected_jsonl_checksum = checksums.get("jsonl_sha256")
        expected_compressed_checksum = checksums.get("compressed_sha256")

        if expected_jsonl_checksum:
            if not self.checksum_calculator.verify_checksum(jsonl_data, expected_jsonl_checksum):
                actual_checksum = self.checksum_calculator.calculate_sha256(jsonl_data)
                raise S3Error(
                    f"JSONL checksum mismatch: expected {expected_jsonl_checksum}, got {actual_checksum}",
                    context={"s3_key": s3_key, "type": "jsonl"},
                )

        if expected_compressed_checksum:
            if not self.checksum_calculator.verify_checksum(
                compressed_data, expected_compressed_checksum
            ):
                actual_checksum = self.checksum_calculator.calculate_sha256(compressed_data)
                raise S3Error(
                    f"Compressed checksum mismatch: expected {expected_compressed_checksum}, got {actual_checksum}",
                    context={"s3_key": s3_key, "type": "compressed"},
                )

        self.logger.debug("Checksums validated", s3_key=s3_key)

    async def list_archives(
        self,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> list[str]:
        """List archive files in S3.

        Args:
            database_name: Optional database name filter
            table_name: Optional table name filter
            start_date: Optional start date filter (inclusive)
            end_date: Optional end date filter (inclusive)

        Returns:
            List of S3 keys for data files
        """
        # Build prefix
        # S3 keys are structured as: prefix/database_name/table_name/... (no schema in path)
        # But we should try both patterns for compatibility
        prefixes_to_try = []

        if self.s3_config.prefix:
            base_prefix = self.s3_config.prefix.rstrip("/")
        else:
            base_prefix = ""

        if database_name and table_name:
            # Try without schema first (actual archiver format)
            prefix_parts = []
            if base_prefix:
                prefix_parts.append(base_prefix)
            prefix_parts.append(database_name)
            prefix_parts.append(table_name)
            prefixes_to_try.append("/".join(prefix_parts))

            # Also try with schema (for future compatibility or different archiver versions)
            prefix_parts_with_schema = []
            if base_prefix:
                prefix_parts_with_schema.append(base_prefix)
            prefix_parts_with_schema.append(database_name)
            prefix_parts_with_schema.append("public")  # Default schema
            prefix_parts_with_schema.append(table_name)
            prefixes_to_try.append("/".join(prefix_parts_with_schema))
        elif database_name:
            # Only database specified
            prefix_parts = []
            if base_prefix:
                prefix_parts.append(base_prefix)
            prefix_parts.append(database_name)
            prefixes_to_try.append("/".join(prefix_parts))
        elif base_prefix:
            prefixes_to_try.append(base_prefix)
        else:
            prefixes_to_try.append("")

        # Use the first prefix (without schema) as primary
        prefix = prefixes_to_try[0] if prefixes_to_try else ""

        self.logger.debug(
            "Listing archives",
            prefix=prefix,
            start_date=start_date,
            end_date=end_date,
        )

        try:
            # List objects with prefix(es) - try multiple patterns
            paginator = self.s3_client.client.get_paginator("list_objects_v2")
            s3_keys = []
            seen_keys = set()  # Avoid duplicates when trying multiple prefixes

            for search_prefix in prefixes_to_try:
                for page in paginator.paginate(Bucket=self.s3_config.bucket, Prefix=search_prefix):
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            key = obj["Key"]
                            # Skip if already seen (from another prefix)
                            if key in seen_keys:
                                continue
                            seen_keys.add(key)

                            # Only include .jsonl.gz files (exclude metadata files)
                            if key.endswith(".jsonl.gz"):
                                # Filter by date if specified
                                if start_date or end_date:
                                    # Extract date from key: year=YYYY/month=MM/day=DD
                                    try:
                                        parts = key.split("/")
                                        year_part = next(
                                            (p for p in parts if p.startswith("year=")), None
                                        )
                                        month_part = next(
                                            (p for p in parts if p.startswith("month=")), None
                                        )
                                        day_part = next(
                                            (p for p in parts if p.startswith("day=")), None
                                        )

                                        if year_part and month_part and day_part:
                                            year = int(year_part.split("=")[1])
                                            month = int(month_part.split("=")[1])
                                            day = int(day_part.split("=")[1])
                                            file_date = datetime(
                                                year, month, day, tzinfo=timezone.utc
                                            )

                                            if start_date:
                                                start = start_date.replace(
                                                    hour=0, minute=0, second=0, microsecond=0
                                                )
                                                if start.tzinfo is None:
                                                    start = start.replace(tzinfo=timezone.utc)
                                                if file_date < start:
                                                    continue
                                            if end_date:
                                                end = end_date.replace(
                                                    hour=23,
                                                    minute=59,
                                                    second=59,
                                                    microsecond=999999,
                                                )
                                                if end.tzinfo is None:
                                                    end = end.replace(tzinfo=timezone.utc)
                                                if file_date > end:
                                                    continue

                                            s3_keys.append(key)
                                    except (ValueError, StopIteration):
                                        # If date parsing fails, include if no date filter
                                        if not start_date and not end_date:
                                            s3_keys.append(key)
                                else:
                                    s3_keys.append(key)

            self.logger.debug("Found archives", count=len(s3_keys), prefix=prefix)
            return sorted(s3_keys)

        except Exception as e:
            raise S3Error(
                f"Failed to list archives: {e}",
                context={"prefix": prefix},
            ) from e
