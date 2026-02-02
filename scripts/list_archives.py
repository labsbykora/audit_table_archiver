#!/usr/bin/env python3
"""
List archived files in S3/MinIO.

Usage:
    python scripts/list_archives.py \
        --s3-endpoint http://localhost:9000 \
        --s3-bucket test-archives
"""

import argparse
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def list_archives(s3_endpoint: str, s3_bucket: str, prefix: str = "archives/") -> None:
    """List archived files in S3."""
    print(f"Listing archives in s3://{s3_bucket}/{prefix}...")
    print()

    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    try:
        # List objects
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)

        file_count = 0
        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                size = obj["Size"]
                modified = obj["LastModified"]
                file_count += 1

                # Format size
                if size < 1024:
                    size_str = f"{size} B"
                elif size < 1024 * 1024:
                    size_str = f"{size / 1024:.2f} KB"
                else:
                    size_str = f"{size / (1024 * 1024):.2f} MB"

                print(f"  {key}")
                print(f"    Size: {size_str}, Modified: {modified}")
                print(f"    Use this exact path for restore: --s3-key \"{key}\"")
                print()

        if file_count == 0:
            print("  No files found.")
        else:
            print(f"Found {file_count} file(s)")

    except ClientError as e:
        print(f"Error listing files: {e}")
        sys.exit(1)


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="List archived files in S3/MinIO")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint URL")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", default="archives/", help="S3 prefix (default: archives/)")

    args = parser.parse_args()

    list_archives(args.s3_endpoint, args.s3_bucket, args.prefix)


if __name__ == "__main__":
    main()

