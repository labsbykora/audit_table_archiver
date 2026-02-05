#!/usr/bin/env python3
"""
Manual restore utility for restoring archived data from S3 back to PostgreSQL.

This is a basic restore script for Phase 1 MVP. A full restore utility will be
implemented in Phase 4.

Usage:
    python scripts/manual_restore.py \
        --s3-endpoint http://localhost:9000 \
        --s3-bucket test-archives \
        --s3-key archives/test_db/audit_logs/2026/01/04/audit_logs_20260104T052021Z_batch_001.jsonl.gz \
        --db-host localhost \
        --db-port 5432 \
        --db-name test_db \
        --db-user archiver \
        --table audit_logs \
        --dry-run
"""

import argparse
import base64
import gzip
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
import boto3
from botocore.exceptions import ClientError

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def deserialize_value(value: Any, column_name: str) -> Any:
    """Deserialize a value from JSON back to Python type for PostgreSQL.

    This reverses the serialization done by the archiver.

    Args:
        value: Value from JSON
        column_name: Column name (for context)

    Returns:
        Deserialized value ready for PostgreSQL
    """
    if value is None:
        return None

    # If it's already a Python type, return as-is
    if isinstance(value, (int, float, bool)):
        return value

    # Handle strings that need conversion
    if isinstance(value, str):
        # Try to parse as datetime (ISO 8601 format)
        if "T" in value or value.endswith("Z"):
            try:
                # Remove Z suffix and parse
                dt_str = value.replace("Z", "+00:00")
                return datetime.fromisoformat(dt_str)
            except (ValueError, AttributeError):
                pass

        # Try to parse as UUID
        if column_name.endswith("_id") or "uuid" in column_name.lower():
            try:
                return UUID(value)
            except (ValueError, AttributeError):
                pass

        # Try to parse as Decimal (for numeric/decimal columns)
        if column_name in ["amount", "price", "balance"] or "decimal" in column_name.lower():
            try:
                return Decimal(value)
            except (ValueError, AttributeError):
                pass

        # Try to decode as base64 (BYTEA fields)
        if column_name.endswith("_data") or "bytea" in column_name.lower():
            try:
                return base64.b64decode(value)
            except (ValueError, Exception):
                pass

        # Return string as-is
        return value

    # Handle lists (arrays)
    if isinstance(value, list):
        return [deserialize_value(item, column_name) for item in value]

    # Handle dicts (JSONB)
    if isinstance(value, dict):
        return {k: deserialize_value(v, k) for k, v in value.items()}

    # Fallback: return as-is
    return value


async def download_and_restore(
    s3_endpoint: str,
    s3_bucket: str,
    s3_key: str,
    db_host: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_password: str,
    table_name: str,
    schema_name: str = "public",
    dry_run: bool = False,
) -> None:
    """Download archived file from S3 and restore to PostgreSQL."""
    print(f"Downloading {s3_key} from {s3_bucket}...")

    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    # Download file
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        compressed_data = response["Body"].read()
        print(f"[OK] Downloaded {len(compressed_data)} bytes")
    except ClientError as e:
        print(f"[ERROR] Error downloading file: {e}")
        sys.exit(1)

    # Decompress
    print("Decompressing...")
    try:
        jsonl_data = gzip.decompress(compressed_data)
        print(f"[OK] Decompressed to {len(jsonl_data)} bytes")
    except Exception as e:
        print(f"[ERROR] Error decompressing: {e}")
        sys.exit(1)

    # Parse JSONL
    print("Parsing JSONL...")
    records = []
    for line in jsonl_data.decode("utf-8").strip().split("\n"):
        if line.strip():
            records.append(json.loads(line))
    print(f"[OK] Parsed {len(records)} records")

    if dry_run:
        print("\n[DRY RUN] Would restore:")
        print(f"   Table: {schema_name}.{table_name}")
        print(f"   Records: {len(records)}")
        if records:
            print(f"   Sample record keys: {list(records[0].keys())[:10]}")
        return

    # Connect to database
    print(f"\nConnecting to PostgreSQL ({db_host}:{db_port}/{db_name})...")
    try:
        conn = await asyncpg.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        print("[OK] Connected")
    except Exception as e:
        print(f"[ERROR] Error connecting to database: {e}")
        sys.exit(1)

    try:
        # Get table columns (excluding metadata columns)
        metadata_columns = {"_archived_at", "_batch_id", "_source_database", "_source_table"}
        if records:
            data_columns = [col for col in records[0].keys() if col not in metadata_columns]
        else:
            print("[WARN] No records to restore")
            return

        # Build INSERT statement
        columns_str = ", ".join(data_columns)
        placeholders = ", ".join([f"${i+1}" for i in range(len(data_columns))])
        insert_query = f"""
            INSERT INTO {schema_name}.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """

        print(f"\nRestoring to {schema_name}.{table_name}...")
        print(f"   Columns: {columns_str}")

        # Restore records in batches
        batch_size = 100
        restored_count = 0
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            batch_values = []
            for record in batch:
                # Deserialize values to proper Python types
                values = [deserialize_value(record.get(col), col) for col in data_columns]
                batch_values.append(values)

            # Insert batch
            await conn.executemany(insert_query, batch_values)
            restored_count += len(batch)
            print(f"   Restored {restored_count}/{len(records)} records...", end="\r")

        print(
            f"\n[OK] Successfully restored {restored_count} records to {schema_name}.{table_name}"
        )

        # Verify
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        print(f"Total records in table: {count}")

    except Exception as e:
        print(f"\n[ERROR] Error during restore: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        await conn.close()


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Manually restore archived data from S3 to PostgreSQL"
    )
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint URL")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name")
    parser.add_argument("--s3-key", required=True, help="S3 object key")
    parser.add_argument("--db-host", default="localhost", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", required=True, help="Database name")
    parser.add_argument("--db-user", required=True, help="Database user")
    parser.add_argument("--db-password", required=True, help="Database password")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--schema", default="public", help="Schema name")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode (don't restore)")

    args = parser.parse_args()

    import asyncio

    asyncio.run(
        download_and_restore(
            s3_endpoint=args.s3_endpoint,
            s3_bucket=args.s3_bucket,
            s3_key=args.s3_key,
            db_host=args.db_host,
            db_port=args.db_port,
            db_name=args.db_name,
            db_user=args.db_user,
            db_password=args.db_password,
            table_name=args.table,
            schema_name=args.schema,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
