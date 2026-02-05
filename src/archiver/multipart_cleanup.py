"""Cleanup of orphaned multipart uploads in S3."""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import structlog
from botocore.exceptions import ClientError

from archiver.exceptions import S3Error
from archiver.s3_client import S3Client
from utils.logging import get_logger


class MultipartCleanup:
    """Manages cleanup of orphaned multipart uploads."""

    def __init__(
        self,
        s3_client: S3Client,
        stale_threshold_hours: int = 24,  # Consider uploads older than 24 hours as stale
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize multipart cleanup.

        Args:
            s3_client: S3 client instance
            stale_threshold_hours: Hours after which multipart uploads are considered stale
            logger: Optional logger instance
        """
        self.s3_client = s3_client
        self.stale_threshold_hours = stale_threshold_hours
        self.logger = logger or get_logger("multipart_cleanup")

    async def list_orphaned_uploads(
        self,
        prefix: Optional[str] = None,
        max_age_hours: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """List orphaned multipart uploads.

        Args:
            prefix: Optional prefix to filter uploads (e.g., database/table path)
            max_age_hours: Optional maximum age in hours (overrides stale_threshold_hours)

        Returns:
            List of orphaned upload dictionaries with 'key', 'upload_id', 'initiated' fields

        Raises:
            S3Error: If listing fails
        """
        threshold_hours = max_age_hours or self.stale_threshold_hours
        stale_threshold = datetime.now(timezone.utc) - timedelta(hours=threshold_hours)

        orphaned_uploads: list[dict[str, Any]] = []

        try:
            # Use boto3 paginator for listing multipart uploads
            paginator = self.s3_client.client.get_paginator("list_multipart_uploads")
            page_iterator = paginator.paginate(Bucket=self.s3_client.config.bucket)

            for page in page_iterator:
                uploads = page.get("Uploads", [])
                for upload in uploads:
                    # Filter by prefix if provided
                    key = upload.get("Key", "")
                    if prefix and not key.startswith(prefix):
                        continue

                    # Check if upload is stale
                    initiated_str = upload.get("Initiated", "")
                    if initiated_str:
                        try:
                            # Parse ISO format timestamp
                            if isinstance(initiated_str, str):
                                initiated = datetime.fromisoformat(
                                    initiated_str.replace("Z", "+00:00")
                                )
                            else:
                                initiated = initiated_str

                            if initiated < stale_threshold:
                                orphaned_uploads.append(
                                    {
                                        "key": key,
                                        "upload_id": upload.get("UploadId"),
                                        "initiated": initiated,
                                        "initiator": upload.get("Initiator", {}),
                                    }
                                )
                        except (ValueError, AttributeError) as e:
                            self.logger.warning(
                                "Failed to parse upload timestamp",
                                key=key,
                                timestamp=initiated_str,
                                error=str(e),
                            )

            self.logger.debug(
                "Listed orphaned multipart uploads",
                count=len(orphaned_uploads),
                prefix=prefix,
                threshold_hours=threshold_hours,
            )

            return orphaned_uploads

        except ClientError as e:
            raise S3Error(
                f"Failed to list multipart uploads: {e}",
                context={"bucket": self.s3_client.config.bucket, "prefix": prefix},
            ) from e

    async def abort_upload(self, key: str, upload_id: str) -> None:
        """Abort a single multipart upload.

        Args:
            key: S3 object key
            upload_id: Multipart upload ID

        Raises:
            S3Error: If abort fails
        """
        try:
            self.s3_client.client.abort_multipart_upload(
                Bucket=self.s3_client.config.bucket, Key=key, UploadId=upload_id
            )
            self.logger.debug(
                "Aborted multipart upload",
                key=key,
                upload_id=upload_id,
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchUpload":
                # Upload already cleaned up - this is OK
                self.logger.debug(
                    "Multipart upload not found (already cleaned up)",
                    key=key,
                    upload_id=upload_id,
                )
            else:
                raise S3Error(
                    f"Failed to abort multipart upload: {e}",
                    context={"key": key, "upload_id": upload_id},
                ) from e

    async def cleanup_orphaned_uploads(
        self,
        prefix: Optional[str] = None,
        max_age_hours: Optional[int] = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Clean up orphaned multipart uploads.

        Args:
            prefix: Optional prefix to filter uploads (e.g., database/table path)
            max_age_hours: Optional maximum age in hours (overrides stale_threshold_hours)
            dry_run: If True, only list uploads without aborting

        Returns:
            Dictionary with cleanup statistics

        Raises:
            S3Error: If cleanup fails
        """
        self.logger.debug(
            "Starting orphaned multipart upload cleanup",
            prefix=prefix,
            max_age_hours=max_age_hours,
            dry_run=dry_run,
        )

        orphaned_uploads = await self.list_orphaned_uploads(
            prefix=prefix, max_age_hours=max_age_hours
        )

        stats = {
            "total_found": len(orphaned_uploads),
            "aborted": 0,
            "failed": 0,
            "errors": [],
        }

        if dry_run:
            self.logger.info(
                "Dry run: Would abort multipart uploads",
                count=len(orphaned_uploads),
            )
            return stats

        for upload in orphaned_uploads:
            try:
                await self.abort_upload(upload["key"], upload["upload_id"])
                stats["aborted"] += 1
            except Exception as e:
                stats["failed"] += 1
                stats["errors"].append(
                    {
                        "key": upload["key"],
                        "upload_id": upload["upload_id"],
                        "error": str(e),
                    }
                )
                self.logger.error(
                    "Failed to abort multipart upload",
                    key=upload["key"],
                    upload_id=upload["upload_id"],
                    error=str(e),
                )

        self.logger.debug(
            "Completed orphaned multipart upload cleanup",
            **stats,
        )

        return stats

    async def cleanup_for_database_table(
        self,
        database_name: str,
        table_name: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Clean up orphaned multipart uploads for a specific database/table.

        Args:
            database_name: Database name
            table_name: Table name
            dry_run: If True, only list uploads without aborting

        Returns:
            Dictionary with cleanup statistics
        """
        prefix = f"{database_name}/{table_name}/"
        return await self.cleanup_orphaned_uploads(prefix=prefix, dry_run=dry_run)

