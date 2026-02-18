"""Async upload pipeline for overlapping uploads with batch processing."""

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Optional

import structlog

from archiver.exceptions import S3Error
from archiver.s3_client import S3Client
from utils.logging import get_logger


class UploadTask:
    """Represents an async upload task."""

    def __init__(
        self,
        file_path: Path,
        s3_key: str,
        task: asyncio.Task[dict[str, Any]],
    ) -> None:
        """Initialize upload task.

        Args:
            file_path: Local file path
            s3_key: S3 object key
            task: Async task for upload
        """
        self.file_path = file_path
        self.s3_key = s3_key
        self.task = task
        self.completed = False
        self.error: Optional[Exception] = None
        self.result: Optional[dict[str, Any]] = None

    async def wait(self) -> dict[str, Any]:
        """Wait for upload to complete.

        Returns:
            Upload result dictionary

        Raises:
            S3Error: If upload failed
        """
        try:
            self.result = await self.task
            self.completed = True
            return self.result
        except Exception as e:
            self.error = e
            self.completed = True
            raise S3Error(
                f"Upload failed for {self.s3_key}: {e}",
                context={"s3_key": self.s3_key, "file_path": str(self.file_path)},
            ) from e


class UploadPipeline:
    """Manages async upload pipeline for overlapping uploads."""

    def __init__(
        self,
        s3_client: S3Client,
        max_concurrent_uploads: int = 2,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize upload pipeline.

        Args:
            s3_client: S3 client instance
            max_concurrent_uploads: Maximum concurrent uploads (default: 2)
            logger: Optional logger instance
        """
        self.s3_client = s3_client
        self.max_concurrent_uploads = max_concurrent_uploads
        self.logger = logger or get_logger("upload_pipeline")
        self._upload_queue: list[UploadTask] = []
        self._active_uploads: list[UploadTask] = []

    async def _upload_file_async(
        self, file_path: Path, s3_key: str
    ) -> dict[str, Any]:
        """Upload file asynchronously using thread pool.

        Args:
            file_path: Local file path
            s3_key: S3 object key

        Returns:
            Upload result dictionary
        """
        loop = asyncio.get_event_loop()

        # Run synchronous upload in thread pool
        def _sync_upload() -> dict[str, Any]:
            return self.s3_client.upload_file(file_path, s3_key)

        return await loop.run_in_executor(None, _sync_upload)

    async def start_upload(
        self, file_path: Path, s3_key: str
    ) -> UploadTask:
        """Start an async upload task.

        Args:
            file_path: Local file path
            s3_key: S3 object key

        Returns:
            UploadTask instance
        """
        # Create upload task
        upload_task = asyncio.create_task(
            self._upload_file_async(file_path, s3_key)
        )

        task = UploadTask(file_path, s3_key, upload_task)
        self._upload_queue.append(task)
        self._active_uploads.append(task)

        self.logger.debug(
            "Upload started",
            s3_key=s3_key,
            active_uploads=len(self._active_uploads),
        )

        # Wait if we've reached max concurrent uploads
        if len(self._active_uploads) >= self.max_concurrent_uploads:
            # Wait for oldest upload to complete
            oldest_task = self._active_uploads[0]
            try:
                await oldest_task.wait()
            except Exception as e:
                self.logger.warning(
                    "Upload failed in pipeline",
                    s3_key=oldest_task.s3_key,
                    error=str(e),
                )
            finally:
                self._active_uploads.remove(oldest_task)

        return task

    async def wait_for_upload(self, task: UploadTask) -> dict[str, Any]:
        """Wait for a specific upload to complete.

        Args:
            task: Upload task to wait for

        Returns:
            Upload result dictionary

        Raises:
            S3Error: If upload failed
        """
        return await task.wait()

    async def wait_for_all(self) -> list[dict[str, Any]]:
        """Wait for all pending uploads to complete.

        Returns:
            List of upload results

        Raises:
            S3Error: If any upload failed
        """
        results = []
        for task in self._active_uploads:
            try:
                result = await task.wait()
                results.append(result)
            except Exception as e:
                self.logger.error(
                    "Upload failed in pipeline",
                    s3_key=task.s3_key,
                    error=str(e),
                )
                raise

        self._active_uploads.clear()
        self._upload_queue.clear()
        return results

    def get_active_count(self) -> int:
        """Get number of active uploads.

        Returns:
            Number of active uploads
        """
        return len(self._active_uploads)

