"""Integration tests for Phase 4: Archive validation utility."""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from archiver.config import S3Config
from utils.checksum import ChecksumCalculator
from validate.archive_validator import ArchiveValidator


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validate_archive_integration(
    db_connection,
    s3_client,
    test_table,
    test_data,
):
    """Test archive validation with real S3 archive."""
    s3_config = S3Config(
        bucket="test-archives",
        region=os.getenv("S3_REGION", "us-east-1"),
        prefix="test/",
        endpoint=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
    )

    # Get data from database
    rows = await db_connection.fetch(
        f"SELECT id, user_id, action, metadata, created_at FROM {test_table} ORDER BY id"
    )

    # Create valid archive
    jsonl_content = "\n".join(
        json.dumps(
            {
                "id": row["id"],
                "user_id": row["user_id"],
                "action": row["action"],
                "metadata": row["metadata"],
                "created_at": (
                    row["created_at"].isoformat()
                    if isinstance(row["created_at"], datetime)
                    else str(row["created_at"])
                ),
            }
        )
        for row in rows
    )

    import gzip

    jsonl_bytes = jsonl_content.encode("utf-8")
    compressed = gzip.compress(jsonl_bytes)

    checksum_calc = ChecksumCalculator()
    checksum = checksum_calc.calculate_sha256(jsonl_bytes)  # Calculate on uncompressed data

    # Upload data file - write to temp file first
    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
        tmp_file.write(compressed)
        tmp_file_path = Path(tmp_file.name)

    try:
        s3_key = f"test_db/public/{test_table}/year=2026/month=01/day=06/valid_archive.jsonl.gz"
        s3_client.upload_file(
            file_path=tmp_file_path,
            s3_key=s3_key,
        )

        metadata = {
            "batch_info": {
                "batch_id": "valid_archive",
                "database": "test_db",
                "schema": "public",
                "table": test_table,
                "archived_at": datetime.now(timezone.utc).isoformat(),
            },
            "data_info": {
                "record_count": len(rows),
                "total_size_bytes": len(compressed),
                "compression": "gzip",
            },
            "checksums": {
                "jsonl_sha256": checksum,
            },
        }

        metadata_key = s3_key.replace(".jsonl.gz", ".metadata.json")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as meta_file:
            json.dump(metadata, meta_file, indent=2)
            meta_file_path = Path(meta_file.name)

        try:
            s3_client.upload_file(
                file_path=meta_file_path,
                s3_key=metadata_key,
            )
        finally:
            meta_file_path.unlink(missing_ok=True)
    finally:
        tmp_file_path.unlink(missing_ok=True)

    # Validate archive
    validator = ArchiveValidator(s3_config)
    result = await validator.validate_archive(s3_key, validate_record_count=True)

    assert result["valid"] is True
    assert len(result["errors"]) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validate_archive_checksum_mismatch(
    db_connection,
    s3_client,
    test_table,
    test_data,
):
    """Test validation detects checksum mismatch."""
    s3_config = S3Config(
        bucket="test-archives",
        region=os.getenv("S3_REGION", "us-east-1"),
        prefix="test/",
        endpoint=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
    )

    # Get data from database
    rows = await db_connection.fetch(
        f"SELECT id, user_id, action, metadata, created_at FROM {test_table} ORDER BY id"
    )

    jsonl_content = "\n".join(
        json.dumps(
            {
                "id": row["id"],
                "user_id": row["user_id"],
                "action": row["action"],
                "metadata": row["metadata"],
                "created_at": (
                    row["created_at"].isoformat()
                    if isinstance(row["created_at"], datetime)
                    else str(row["created_at"])
                ),
            }
        )
        for row in rows
    )

    import gzip

    compressed = gzip.compress(jsonl_content.encode("utf-8"))

    # Use wrong checksum
    wrong_checksum = "wrong_checksum_value"

    # Upload data file - write to temp file first
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
        tmp_file.write(compressed)
        tmp_file_path = Path(tmp_file.name)

    try:
        s3_key = f"test_db/public/{test_table}/year=2026/month=01/day=06/invalid_checksum.jsonl.gz"
        s3_client.upload_file(
            file_path=tmp_file_path,
            s3_key=s3_key,
        )

        metadata = {
            "batch_info": {
                "batch_id": "invalid_checksum",
                "database": "test_db",
                "schema": "public",
                "table": test_table,
                "archived_at": datetime.now(timezone.utc).isoformat(),
            },
            "data_info": {
                "record_count": len(rows),
                "total_size_bytes": len(compressed),
                "compression": "gzip",
            },
            "checksums": {
                "jsonl_sha256": wrong_checksum,
            },
        }

        metadata_key = s3_key.replace(".jsonl.gz", ".metadata.json")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as meta_file:
            json.dump(metadata, meta_file, indent=2)
            meta_file_path = Path(meta_file.name)

        try:
            s3_client.upload_file(
                file_path=meta_file_path,
                s3_key=metadata_key,
            )
        finally:
            meta_file_path.unlink(missing_ok=True)
    finally:
        tmp_file_path.unlink(missing_ok=True)

    # Validate archive - should fail - use the full key with prefix
    full_s3_key = f"test/{s3_key}"
    validator = ArchiveValidator(s3_config)
    result = await validator.validate_archive(
        full_s3_key,
        validate_checksum=True,
        validate_record_count=False,
    )

    assert result["valid"] is False
    assert any("checksum" in error.lower() for error in result["errors"])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validate_archive_record_count_mismatch(
    db_connection,
    s3_client,
    test_table,
    test_data,
):
    """Test validation detects record count mismatch."""
    s3_config = S3Config(
        bucket="test-archives",
        region=os.getenv("S3_REGION", "us-east-1"),
        prefix="test/",
        endpoint=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
    )

    # Get data from database
    all_rows = await db_connection.fetch(
        f"SELECT id, user_id, action, metadata, created_at FROM {test_table} ORDER BY id"
    )
    half_rows = all_rows[: len(all_rows) // 2]  # Only half the records

    # Create archive with fewer records than metadata claims
    jsonl_content = "\n".join(
        json.dumps(
            {
                "id": row["id"],
                "user_id": row["user_id"],
                "action": row["action"],
                "metadata": row["metadata"],
                "created_at": (
                    row["created_at"].isoformat()
                    if isinstance(row["created_at"], datetime)
                    else str(row["created_at"])
                ),
            }
        )
        for row in half_rows
    )

    import gzip

    jsonl_bytes = jsonl_content.encode("utf-8")
    compressed = gzip.compress(jsonl_bytes)

    checksum_calc = ChecksumCalculator()
    checksum = checksum_calc.calculate_sha256(jsonl_bytes)  # Calculate on uncompressed data

    # Upload data file - write to temp file first
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
        tmp_file.write(compressed)
        tmp_file_path = Path(tmp_file.name)

    try:
        s3_key = f"test_db/public/{test_table}/year=2026/month=01/day=06/count_mismatch.jsonl.gz"
        s3_client.upload_file(
            file_path=tmp_file_path,
            s3_key=s3_key,
        )

        # Metadata claims all records
        metadata = {
            "batch_info": {
                "batch_id": "count_mismatch",
                "database": "test_db",
                "schema": "public",
                "table": test_table,
                "archived_at": datetime.now(timezone.utc).isoformat(),
            },
            "data_info": {
                "record_count": len(all_rows),  # Claims all records (but only half are in file)
                "total_size_bytes": len(compressed),
                "compression": "gzip",
            },
            "checksums": {
                "jsonl_sha256": checksum,
            },
        }

        metadata_key = s3_key.replace(".jsonl.gz", ".metadata.json")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as meta_file:
            json.dump(metadata, meta_file, indent=2)
            meta_file_path = Path(meta_file.name)

        try:
            s3_client.upload_file(
                file_path=meta_file_path,
                s3_key=metadata_key,
            )
        finally:
            meta_file_path.unlink(missing_ok=True)
    finally:
        tmp_file_path.unlink(missing_ok=True)

    # Validate archive - should detect mismatch
    validator = ArchiveValidator(s3_config)
    result = await validator.validate_archive(s3_key, validate_record_count=True)

    assert result["valid"] is False
    assert any("record count" in error.lower() for error in result["errors"])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_validate_all_archives(
    db_connection,
    s3_client,
    test_table,
    test_data,
):
    """Test bulk archive validation."""
    s3_config = S3Config(
        bucket="test-archives",
        region=os.getenv("S3_REGION", "us-east-1"),
        prefix="test/",
        endpoint=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
    )

    # Get data from database
    base_rows = await db_connection.fetch(
        f"SELECT id, user_id, action, metadata, created_at FROM {test_table} ORDER BY id"
    )

    # Create multiple archives
    temp_files = []
    try:
        for i in range(3):
            jsonl_content = "\n".join(
                json.dumps(
                    {
                        "id": row["id"] + i * 1000,
                        "user_id": row["user_id"],
                        "action": row["action"],
                        "metadata": row["metadata"],
                        "created_at": (
                            row["created_at"].isoformat()
                            if isinstance(row["created_at"], datetime)
                            else str(row["created_at"])
                        ),
                    }
                )
                for row in base_rows
            )

            import gzip

            jsonl_bytes = jsonl_content.encode("utf-8")
            compressed = gzip.compress(jsonl_bytes)

            checksum_calc = ChecksumCalculator()
            checksum = checksum_calc.calculate_sha256(jsonl_bytes)  # Calculate on uncompressed data

            # Write to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
                tmp_file.write(compressed)
                tmp_file_path = Path(tmp_file.name)
                temp_files.append(tmp_file_path)

            s3_key = f"test_db/public/{test_table}/year=2026/month=01/day=06/archive_{i}.jsonl.gz"
            s3_client.upload_file(
                file_path=tmp_file_path,
                s3_key=s3_key,
            )

            metadata = {
                "batch_info": {
                    "batch_id": f"archive_{i}",
                    "database": "test_db",
                    "schema": "public",
                    "table": test_table,
                    "archived_at": datetime.now(timezone.utc).isoformat(),
                },
                "data_info": {
                    "record_count": len(base_rows),
                    "total_size_bytes": len(compressed),
                    "compression": "gzip",
                },
                "checksums": {
                    "jsonl_sha256": checksum,
                },
            }

            metadata_key = s3_key.replace(".jsonl.gz", ".metadata.json")
            with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as meta_file:
                json.dump(metadata, meta_file, indent=2)
                meta_file_path = Path(meta_file.name)
                temp_files.append(meta_file_path)

            s3_client.upload_file(
                file_path=meta_file_path,
                s3_key=metadata_key,
            )
    finally:
        # Cleanup temp files
        for tmp_file in temp_files:
            tmp_file.unlink(missing_ok=True)

    # Validate all archives
    validator = ArchiveValidator(s3_config)
    result = await validator.validate_archives(
        database_name="test_db",
        table_name=test_table,
        validate_checksum=True,
        validate_record_count=True,
    )

    assert result.total_archives == 3
    assert result.valid_archives == 3
    assert result.invalid_archives == 0
