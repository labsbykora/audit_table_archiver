"""S3 client for uploading archived data."""

from pathlib import Path
from typing import Any, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from structlog import BoundLogger

from archiver.config import S3Config
from archiver.exceptions import S3Error
from archiver.local_fallback import LocalFallback
from archiver.multipart_upload import MultipartUploader
from archiver.s3_rate_limiter import S3RateLimiter
from utils.circuit_breaker import CircuitBreaker
from utils.logging import get_logger
from utils.retry import RetryConfig, retry_sync


class S3Client:
    """S3 client for uploading and managing archived data."""

    def __init__(
        self,
        config: S3Config,
        logger: Optional[BoundLogger] = None,
    ) -> None:
        """Initialize S3 client.

        Args:
            config: S3 configuration
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or get_logger("s3")
        self._client: Optional[Any] = None
        self._multipart_uploader: Optional[MultipartUploader] = None
        self._local_fallback: Optional[LocalFallback] = None

        # Circuit breaker for S3 operations
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60.0,
            expected_exception=(ClientError, BotoCoreError),
            logger=self.logger,
        )

        # Retry configuration for S3 operations
        self.retry_config = RetryConfig(
            max_attempts=3,
            initial_delay=1.0,
            max_delay=30.0,
            exponential_base=2.0,
            jitter=True,
            retryable_exceptions=(ClientError, BotoCoreError),
        )

        # Rate limiter if configured
        if self.config.rate_limit_requests_per_second:
            self._rate_limiter = S3RateLimiter(
                requests_per_second=self.config.rate_limit_requests_per_second,
                logger=self.logger,
            )
        else:
            self._rate_limiter = None

        # Local fallback if configured
        if self.config.local_fallback_dir:
            from pathlib import Path

            self._local_fallback = LocalFallback(
                fallback_dir=Path(self.config.local_fallback_dir),
                retention_days=self.config.local_fallback_retention_days,
                logger=self.logger,
            )

    @property
    def client(self) -> Any:
        """Get or create S3 client."""
        if self._client is None:
            try:
                # Get credentials from config or environment
                credentials = self.config.get_credentials()

                if credentials:
                    # Use explicit credentials
                    session = boto3.Session(
                        aws_access_key_id=credentials["aws_access_key_id"],
                        aws_secret_access_key=credentials["aws_secret_access_key"],
                    )
                else:
                    # Use default credential chain (IAM role, AWS credentials file, env vars)
                    session = boto3.Session()

                s3_kwargs: dict[str, Any] = {
                    "service_name": "s3",
                    "region_name": self.config.region,
                }

                # Add endpoint URL for S3-compatible storage
                if self.config.endpoint:
                    s3_kwargs["endpoint_url"] = self.config.endpoint

                self._client = session.client(**s3_kwargs)
                self.logger.debug(
                    "S3 client initialized",
                    bucket=self.config.bucket,
                    endpoint=self.config.endpoint or "AWS S3",
                    region=self.config.region,
                )
            except Exception as e:
                raise S3Error(
                    f"Failed to create S3 client: {e}",
                    context={"bucket": self.config.bucket},
                ) from e

        return self._client

    def validate_bucket(self) -> None:
        """Validate S3 bucket exists and is accessible.

        Raises:
            S3Error: If bucket validation fails
        """
        try:
            # Check if bucket exists
            self.client.head_bucket(Bucket=self.config.bucket)
            self.logger.debug("Bucket exists and is accessible", bucket=self.config.bucket)

            # Test write permissions by uploading a small test file
            # Use a valid object name (MinIO has stricter naming rules than AWS S3)
            # Avoid leading dots and use alphanumeric characters
            import time

            timestamp = int(time.time())
            test_key = f"{self.config.prefix}test_write_permission_{timestamp}.tmp"
            # Remove leading slash if prefix already has one
            if test_key.startswith("//"):
                test_key = test_key[1:]
            test_content = b"test"
            self.client.put_object(
                Bucket=self.config.bucket,
                Key=test_key,
                Body=test_content,
            )

            # Clean up test file
            self.client.delete_object(Bucket=self.config.bucket, Key=test_key)

            self.logger.debug("Bucket write permissions validated", bucket=self.config.bucket)

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404":
                raise S3Error(
                    f"Bucket not found: {self.config.bucket}",
                    context={"bucket": self.config.bucket},
                ) from e
            elif error_code == "403":
                raise S3Error(
                    f"Access denied to bucket: {self.config.bucket}",
                    context={"bucket": self.config.bucket},
                ) from e
            else:
                raise S3Error(
                    f"Bucket validation failed: {error_code}",
                    context={"bucket": self.config.bucket, "error": str(e)},
                ) from e
        except BotoCoreError as e:
            raise S3Error(
                f"S3 client error during bucket validation: {e}",
                context={"bucket": self.config.bucket},
            ) from e

    def upload_file(
        self,
        file_path: Path,
        s3_key: str,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ) -> dict[str, Any]:
        """Upload file to S3 with retry logic.

        Args:
            file_path: Local file path
            s3_key: S3 object key
            max_retries: Maximum number of retry attempts
            retry_delay: Initial retry delay in seconds

        Returns:
            Dictionary with upload metadata (etag, size, etc.)

        Raises:
            S3Error: If upload fails after all retries
        """
        # Build full key, avoiding double slashes
        s3_key = s3_key.lstrip("/")
        if self.config.prefix:
            prefix = self.config.prefix.rstrip("/")
            # If s3_key already starts with prefix, don't add it again
            if s3_key.startswith(prefix + "/"):
                full_key = s3_key
            elif prefix:
                full_key = f"{prefix}/{s3_key}"
            else:
                full_key = s3_key
        else:
            full_key = s3_key
        file_size = file_path.stat().st_size

        self.logger.debug(
            "Starting file upload",
            bucket=self.config.bucket,
            key=full_key,
            size=file_size,
        )

        # Internal upload function with retry logic
        def _upload_internal() -> dict[str, Any]:
            """Internal upload function."""
            # Determine if we should use multipart upload
            use_multipart = file_size > (self.config.multipart_threshold_mb * 1024 * 1024)

            if use_multipart:
                result = self._upload_multipart(file_path, full_key)
            else:
                result = self._upload_simple(file_path, full_key)

            # Verify upload
            self._verify_upload(full_key, file_size, result.get("ETag", ""))

            return {
                "bucket": self.config.bucket,
                "key": full_key,
                "size": file_size,
                "etag": result.get("ETag", ""),
            }

        try:
            # Use circuit breaker to prevent cascading failures
            result = self.circuit_breaker.call(_upload_internal)

            self.logger.debug(
                "File upload successful",
                bucket=self.config.bucket,
                key=full_key,
                size=file_size,
                etag=result.get("etag", ""),
            )

            return result

        except (ClientError, BotoCoreError):
            # Retry with exponential backoff and jitter
            try:
                result = retry_sync(
                    _upload_internal,
                    config=self.retry_config,
                    logger=self.logger,
                )

                self.logger.debug(
                    "File upload successful after retry",
                    bucket=self.config.bucket,
                    key=full_key,
                    size=file_size,
                    etag=result.get("etag", ""),
                )

                return result
            except Exception as retry_error:
                # Try to save to local fallback if configured
                if self._local_fallback:
                    try:
                        fallback_path = self._local_fallback.save_failed_upload(
                            file_path=file_path,
                            s3_key=full_key,
                            metadata={
                                "bucket": self.config.bucket,
                                "error": str(retry_error),
                                "attempts": self.retry_config.max_attempts,
                            },
                        )
                        self.logger.warning(
                            "S3 upload failed, saved to local fallback",
                            bucket=self.config.bucket,
                            key=full_key,
                            fallback_path=str(fallback_path),
                            error=str(retry_error),
                        )
                    except Exception as fallback_error:
                        self.logger.error(
                            "Failed to save to local fallback",
                            bucket=self.config.bucket,
                            key=full_key,
                            fallback_error=str(fallback_error),
                        )

                raise S3Error(
                    f"File upload failed after retries: {retry_error}",
                    context={
                        "bucket": self.config.bucket,
                        "key": full_key,
                        "attempts": self.retry_config.max_attempts,
                        "fallback_saved": self._local_fallback is not None,
                    },
                ) from retry_error

    def _upload_simple(self, file_path: Path, s3_key: str) -> dict[str, Any]:
        """Upload file using simple PUT operation.

        Args:
            file_path: Local file path
            s3_key: S3 object key

        Returns:
            Upload response
        """
        extra_args: dict[str, Any] = {
            "StorageClass": self.config.storage_class,
        }

        # Add encryption (only for AWS S3, not S3-compatible storage like MinIO)
        # MinIO and other S3-compatible services may not support SSE-S3
        if self.config.encryption and self.config.encryption.lower() != "none":
            if self.config.endpoint is None:  # AWS S3
                if self.config.encryption == "SSE-S3":
                    extra_args["ServerSideEncryption"] = "AES256"
                elif self.config.encryption == "SSE-KMS":
                    extra_args["ServerSideEncryption"] = "aws:kms"
                    # Note: KMS key ID would be in config if needed
            # For S3-compatible storage (MinIO, etc.), skip encryption
            # as they may not support it or require different configuration

        with open(file_path, "rb") as f:
            return self.client.put_object(
                Bucket=self.config.bucket,
                Key=s3_key,
                Body=f,
                **extra_args,
            )

    @property
    def multipart_uploader(self) -> MultipartUploader:
        """Get or create multipart uploader."""
        if self._multipart_uploader is None:
            self._multipart_uploader = MultipartUploader(
                s3_client=self,
                logger=self.logger,
            )
        return self._multipart_uploader

    def _upload_multipart(self, file_path: Path, s3_key: str) -> dict[str, Any]:
        """Upload file using multipart upload with resume capability.

        Args:
            file_path: Local file path
            s3_key: S3 object key

        Returns:
            Dictionary with upload metadata (etag, size, etc.)

        Raises:
            S3Error: If upload fails
        """
        return self.multipart_uploader.upload_file(
            file_path=file_path,
            s3_key=s3_key,
            resume=True,
        )

    def _verify_upload(self, s3_key: str, expected_size: int, etag: str) -> None:
        """Verify uploaded file exists and size matches.

        Args:
            s3_key: S3 object key
            expected_size: Expected file size in bytes
            etag: Expected ETag

        Raises:
            S3Error: If verification fails
        """
        try:
            response = self.client.head_object(Bucket=self.config.bucket, Key=s3_key)
            actual_size = response.get("ContentLength", 0)

            if actual_size != expected_size:
                raise S3Error(
                    f"Upload verification failed: size mismatch "
                    f"(expected {expected_size}, got {actual_size})",
                    context={
                        "bucket": self.config.bucket,
                        "key": s3_key,
                        "expected_size": expected_size,
                        "actual_size": actual_size,
                    },
                )

            self.logger.debug(
                "Upload verification successful",
                bucket=self.config.bucket,
                key=s3_key,
                size=actual_size,
            )

        except ClientError as e:
            raise S3Error(
                f"Upload verification failed: {e}",
                context={"bucket": self.config.bucket, "key": s3_key},
            ) from e
        except BotoCoreError as e:
            raise S3Error(
                f"Boto3 error during upload verification: {e}",
                context={"bucket": self.config.bucket, "key": s3_key},
            ) from e

    def download_file(self, s3_key: str, local_path: Path) -> None:
        """Download file from S3.

        Args:
            s3_key: S3 object key
            local_path: Local file path to save to

        Raises:
            S3Error: If download fails
        """
        # Build full key, avoiding double slashes
        s3_key = s3_key.lstrip("/")
        if self.config.prefix:
            prefix = self.config.prefix.rstrip("/")
            if s3_key.startswith(prefix + "/"):
                full_key = s3_key
            elif prefix:
                full_key = f"{prefix}/{s3_key}"
            else:
                full_key = s3_key
        else:
            full_key = s3_key

        try:
            self.logger.debug(
                "Downloading file from S3",
                bucket=self.config.bucket,
                key=full_key,
                local_path=str(local_path),
            )

            self.client.download_file(
                Bucket=self.config.bucket,
                Key=full_key,
                Filename=str(local_path),
            )

            self.logger.debug(
                "File downloaded from S3",
                bucket=self.config.bucket,
                key=full_key,
                local_path=str(local_path),
            )

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise S3Error(
                f"Failed to download file from S3: {error_code}",
                context={
                    "bucket": self.config.bucket,
                    "key": full_key,
                    "local_path": str(local_path),
                    "error_code": error_code,
                },
            ) from e
        except BotoCoreError as e:
            raise S3Error(
                f"Boto3 error during download: {e}",
                context={
                    "bucket": self.config.bucket,
                    "key": full_key,
                    "local_path": str(local_path),
                },
            ) from e

    def get_object_bytes(self, s3_key: str) -> bytes:
        """Get object content as bytes from S3.

        Args:
            s3_key: S3 object key

        Returns:
            Object content as bytes

        Raises:
            S3Error: If download fails
        """
        # Build full key, avoiding double slashes
        s3_key = s3_key.lstrip("/")
        if self.config.prefix:
            prefix = self.config.prefix.rstrip("/")
            if s3_key.startswith(prefix + "/"):
                full_key = s3_key
            elif prefix:
                full_key = f"{prefix}/{s3_key}"
            else:
                full_key = s3_key
        else:
            full_key = s3_key

        try:
            self.logger.debug(
                "Getting object from S3",
                bucket=self.config.bucket,
                key=full_key,
            )

            response = self.client.get_object(
                Bucket=self.config.bucket,
                Key=full_key,
            )

            return response["Body"].read()

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise S3Error(
                f"Failed to get object from S3: {error_code}",
                context={
                    "bucket": self.config.bucket,
                    "key": full_key,
                    "error_code": error_code,
                },
            ) from e
        except BotoCoreError as e:
            raise S3Error(
                f"Boto3 error during get_object: {e}",
                context={
                    "bucket": self.config.bucket,
                    "key": full_key,
                },
            ) from e

    def object_exists(self, s3_key: str) -> bool:
        """Check if object exists in S3.

        Args:
            s3_key: S3 object key (may or may not include prefix)

        Returns:
            True if object exists, False otherwise
        """
        # Normalize the key - remove leading/trailing slashes and handle prefix
        s3_key = s3_key.lstrip("/")
        if self.config.prefix:
            prefix = self.config.prefix.rstrip("/")
            # If s3_key already starts with prefix, don't add it again
            if s3_key.startswith(prefix + "/"):
                full_key = s3_key
            elif prefix:
                full_key = f"{prefix}/{s3_key}"
            else:
                full_key = s3_key
        else:
            full_key = s3_key

        try:
            self.client.head_object(Bucket=self.config.bucket, Key=full_key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                return False
            raise S3Error(
                f"Error checking object existence: {e}",
                context={"bucket": self.config.bucket, "key": full_key},
            ) from e

    def list_objects(self, prefix: str, max_keys: Optional[int] = None) -> list[dict[str, Any]]:
        """List objects in S3 with given prefix.

        Args:
            prefix: S3 key prefix to search
            max_keys: Maximum number of keys to return (None for all)

        Returns:
            List of object dictionaries with 'key' and 'last_modified' fields

        Raises:
            S3Error: If listing fails
        """
        # Build full prefix, avoiding double slashes
        prefix = prefix.lstrip("/")
        if self.config.prefix:
            config_prefix = self.config.prefix.rstrip("/")
            if prefix.startswith(config_prefix + "/"):
                full_prefix = prefix
            elif config_prefix:
                full_prefix = f"{config_prefix}/{prefix}"
            else:
                full_prefix = prefix
        else:
            full_prefix = prefix

        try:
            self.logger.debug(
                "Listing S3 objects",
                bucket=self.config.bucket,
                prefix=full_prefix,
            )

            objects = []
            paginator = self.client.get_paginator("list_objects_v2")
            pagination_config = {}
            if max_keys:
                pagination_config["PageSize"] = max_keys
                pagination_config["MaxItems"] = max_keys
            page_iterator = paginator.paginate(
                Bucket=self.config.bucket,
                Prefix=full_prefix,
                PaginationConfig=pagination_config,
            )

            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects.append(
                            {
                                "key": obj["Key"],
                                "last_modified": obj["LastModified"],
                                "size": obj.get("Size", 0),
                            }
                        )

            self.logger.debug(
                "S3 objects listed",
                bucket=self.config.bucket,
                prefix=full_prefix,
                count=len(objects),
            )

            return objects

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise S3Error(
                f"Failed to list objects in S3: {error_code}",
                context={
                    "bucket": self.config.bucket,
                    "prefix": full_prefix,
                    "error_code": error_code,
                },
            ) from e
        except BotoCoreError as e:
            raise S3Error(
                f"Boto3 error during list_objects: {e}",
                context={
                    "bucket": self.config.bucket,
                    "prefix": full_prefix,
                },
            ) from e
