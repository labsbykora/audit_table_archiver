"""Local disk fallback for failed S3 uploads."""

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import structlog

from utils.logging import get_logger


class LocalFallback:
    """Manages local disk fallback for failed S3 uploads."""

    def __init__(
        self,
        fallback_dir: Path,
        retention_days: int = 7,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize local fallback.

        Args:
            fallback_dir: Directory to store failed uploads
            retention_days: Number of days to retain failed uploads before cleanup
            logger: Optional logger instance
        """
        self.fallback_dir = Path(fallback_dir)
        self.retention_days = retention_days
        self.logger = logger or get_logger("local_fallback")

        # Create fallback directory if it doesn't exist
        self.fallback_dir.mkdir(parents=True, exist_ok=True)

    def save_failed_upload(
        self,
        file_path: Path,
        s3_key: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Path:
        """Save a failed upload to local disk.

        Args:
            file_path: Path to the file that failed to upload
            s3_key: S3 key where the file should have been uploaded
            metadata: Optional metadata about the upload (database, table, batch, etc.)

        Returns:
            Path to the saved file in fallback directory
        """
        # Create a safe filename from S3 key
        safe_key = s3_key.replace("/", "_").replace("\\", "_")
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        fallback_filename = f"{timestamp}_{safe_key}"

        # If filename is too long, truncate it
        max_length = 255  # Common filesystem limit
        if len(fallback_filename) > max_length:
            # Keep timestamp and truncate key
            key_part = safe_key[: max_length - len(timestamp) - 1]
            fallback_filename = f"{timestamp}_{key_part}"

        fallback_path = self.fallback_dir / fallback_filename

        # Copy file to fallback directory
        shutil.copy2(file_path, fallback_path)

        # Save metadata
        metadata_file = fallback_path.with_suffix(fallback_path.suffix + ".meta.json")
        metadata_data = {
            "s3_key": s3_key,
            "original_path": str(file_path),
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "file_size": file_path.stat().st_size,
            "metadata": metadata or {},
        }

        with open(metadata_file, "w") as f:
            json.dump(metadata_data, f, indent=2)

        self.logger.warning(
            "Saved failed upload to local fallback",
            s3_key=s3_key,
            fallback_path=str(fallback_path),
            file_size=file_path.stat().st_size,
        )

        return fallback_path

    def list_failed_uploads(
        self,
        max_age_days: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """List failed uploads in fallback directory.

        Args:
            max_age_days: Optional maximum age in days (overrides retention_days)

        Returns:
            List of failed upload dictionaries with 'path', 's3_key', 'saved_at', etc.
        """
        threshold_days = max_age_days or self.retention_days
        threshold = datetime.now(timezone.utc) - timedelta(days=threshold_days)

        failed_uploads: list[dict[str, Any]] = []

        for file_path in self.fallback_dir.glob("*"):
            # Skip metadata files
            if file_path.suffix == ".json" and file_path.name.endswith(".meta.json"):
                continue

            # Skip directories
            if file_path.is_dir():
                continue

            # Try to load metadata
            metadata_file = file_path.with_suffix(file_path.suffix + ".meta.json")
            metadata_data: dict[str, Any] = {}

            if metadata_file.exists():
                try:
                    with open(metadata_file) as f:
                        metadata_data = json.load(f)
                except (json.JSONDecodeError, FileNotFoundError):
                    pass

            # Get saved_at from metadata or file mtime
            saved_at_str = metadata_data.get("saved_at")
            if saved_at_str:
                try:
                    saved_at = datetime.fromisoformat(saved_at_str.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    saved_at = datetime.fromtimestamp(
                        file_path.stat().st_mtime, tz=timezone.utc
                    )
            else:
                saved_at = datetime.fromtimestamp(
                    file_path.stat().st_mtime, tz=timezone.utc
                )

            # Filter by age
            if saved_at < threshold:
                continue

            failed_uploads.append(
                {
                    "path": file_path,
                    "s3_key": metadata_data.get("s3_key", "unknown"),
                    "saved_at": saved_at,
                    "file_size": file_path.stat().st_size,
                    "metadata": metadata_data.get("metadata", {}),
                }
            )

        # Sort by saved_at (oldest first)
        failed_uploads.sort(key=lambda x: x["saved_at"])

        return failed_uploads

    def cleanup_old_uploads(
        self,
        max_age_days: Optional[int] = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Clean up old failed uploads.

        Args:
            max_age_days: Optional maximum age in days (overrides retention_days)
            dry_run: If True, only list files without deleting

        Returns:
            Dictionary with cleanup statistics
        """
        threshold_days = max_age_days or self.retention_days
        threshold = datetime.now(timezone.utc) - timedelta(days=threshold_days)

        stats = {
            "total_found": 0,
            "deleted": 0,
            "failed": 0,
            "errors": [],
        }

        for file_path in self.fallback_dir.glob("*"):
            # Skip directories
            if file_path.is_dir():
                continue

            # Check if it's a metadata file
            is_metadata = file_path.suffix == ".json" and file_path.name.endswith(".meta.json")

            # Get corresponding file
            if is_metadata:
                # Remove .meta.json suffix to get original file
                original_file = file_path.with_suffix("").with_suffix(
                    file_path.suffix.replace(".meta.json", "")
                )
                check_file = file_path
            else:
                original_file = file_path
                check_file = file_path

            # Check file age
            try:
                file_mtime = datetime.fromtimestamp(
                    check_file.stat().st_mtime, tz=timezone.utc
                )
            except (OSError, ValueError):
                continue

            if file_mtime >= threshold:
                continue

            stats["total_found"] += 1

            if dry_run:
                continue

            # Delete file and metadata
            try:
                if original_file.exists():
                    original_file.unlink()
                if is_metadata or (original_file.with_suffix(original_file.suffix + ".meta.json").exists()):
                    metadata_file = original_file.with_suffix(
                        original_file.suffix + ".meta.json"
                    )
                    if metadata_file.exists():
                        metadata_file.unlink()
                stats["deleted"] += 1
            except Exception as e:
                stats["failed"] += 1
                stats["errors"].append(
                    {
                        "path": str(file_path),
                        "error": str(e),
                    }
                )
                self.logger.error(
                    "Failed to delete old fallback file",
                    path=str(file_path),
                    error=str(e),
                )

        self.logger.info(
            "Completed cleanup of old fallback uploads",
            **stats,
        )

        return stats

    def get_resume_info(self) -> dict[str, Any]:
        """Get information about failed uploads that can be resumed.

        Returns:
            Dictionary with resume information
        """
        failed_uploads = self.list_failed_uploads()

        return {
            "total_failed": len(failed_uploads),
            "total_size": sum(upload["file_size"] for upload in failed_uploads),
            "uploads": [
                {
                    "s3_key": upload["s3_key"],
                    "local_path": str(upload["path"]),
                    "saved_at": upload["saved_at"].isoformat(),
                    "file_size": upload["file_size"],
                }
                for upload in failed_uploads
            ],
        }

