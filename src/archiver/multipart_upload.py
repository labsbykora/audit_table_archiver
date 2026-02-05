"""Multipart upload with resume capability for S3."""

import json
import math
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import structlog
from botocore.exceptions import ClientError

from archiver.exceptions import S3Error
from utils.logging import get_logger

if TYPE_CHECKING:
    from archiver.s3_client import S3Client


class MultipartUploadState:
    """State tracking for multipart upload."""

    def __init__(
        self,
        upload_id: str,
        key: str,
        file_path: Path,
        part_size: int,
        total_parts: int,
        uploaded_parts: list[dict[str, Any]],
        state_file: Optional[Path] = None,
    ) -> None:
        """Initialize multipart upload state.

        Args:
            upload_id: S3 multipart upload ID
            key: S3 object key
            file_path: Local file path
            part_size: Size of each part in bytes
            total_parts: Total number of parts
            uploaded_parts: List of uploaded parts with ETag and part number
            state_file: Optional path to state file for persistence
        """
        self.upload_id = upload_id
        self.key = key
        self.file_path = file_path
        self.part_size = part_size
        self.total_parts = total_parts
        self.uploaded_parts = uploaded_parts
        self.state_file = state_file

    def to_dict(self) -> dict[str, Any]:
        """Convert state to dictionary."""
        return {
            "upload_id": self.upload_id,
            "key": self.key,
            "file_path": str(self.file_path),
            "part_size": self.part_size,
            "total_parts": self.total_parts,
            "uploaded_parts": self.uploaded_parts,
        }

    @classmethod
    def from_dict(
        cls, data: dict[str, Any], state_file: Optional[Path] = None
    ) -> "MultipartUploadState":
        """Create state from dictionary."""
        return cls(
            upload_id=data["upload_id"],
            key=data["key"],
            file_path=Path(data["file_path"]),
            part_size=data["part_size"],
            total_parts=data["total_parts"],
            uploaded_parts=data["uploaded_parts"],
            state_file=state_file,
        )

    def save(self) -> None:
        """Save state to file."""
        if self.state_file:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, state_file: Path) -> Optional["MultipartUploadState"]:
        """Load state from file."""
        if not state_file.exists():
            return None
        try:
            with open(state_file) as f:
                data = json.load(f)
            return cls.from_dict(data, state_file=state_file)
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            return None

    def get_remaining_parts(self) -> list[int]:
        """Get list of part numbers that still need to be uploaded."""
        uploaded_part_numbers = {part["PartNumber"] for part in self.uploaded_parts}
        return [i for i in range(1, self.total_parts + 1) if i not in uploaded_part_numbers]


class MultipartUploader:
    """Handles multipart uploads with resume capability."""

    # Minimum part size is 5MB (except last part)
    MIN_PART_SIZE = 5 * 1024 * 1024
    # Maximum part size is 5GB
    MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
    # Default part size is 10MB
    DEFAULT_PART_SIZE = 10 * 1024 * 1024
    # Maximum number of parts is 10,000
    MAX_PARTS = 10000

    def __init__(
        self,
        s3_client: "S3Client",
        state_dir: Optional[Path] = None,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize multipart uploader.

        Args:
            s3_client: S3 client instance
            state_dir: Optional directory for storing upload state files
            logger: Optional logger instance
        """
        self.s3_client = s3_client
        self.state_dir = state_dir or Path.cwd() / ".multipart_uploads"
        self.logger = logger or get_logger("multipart_upload")

    def _calculate_part_size(self, file_size: int) -> int:
        """Calculate optimal part size for file.

        Args:
            file_size: Total file size in bytes

        Returns:
            Part size in bytes
        """
        # Start with default part size
        part_size = self.DEFAULT_PART_SIZE

        # Calculate number of parts with default size
        num_parts = math.ceil(file_size / part_size)

        # If too many parts, increase part size
        if num_parts > self.MAX_PARTS:
            part_size = math.ceil(file_size / self.MAX_PARTS)
            # Round up to nearest MB
            part_size = math.ceil(part_size / (1024 * 1024)) * (1024 * 1024)
            # Ensure it's at least MIN_PART_SIZE
            part_size = max(part_size, self.MIN_PART_SIZE)

        # Ensure it's not larger than MAX_PART_SIZE
        part_size = min(part_size, self.MAX_PART_SIZE)

        return part_size

    def _get_state_file(self, s3_key: str) -> Path:
        """Get state file path for S3 key."""
        # Create a safe filename from S3 key
        safe_key = s3_key.replace("/", "_").replace("\\", "_")
        return self.state_dir / f"{safe_key}.json"

    def _initiate_upload(
        self, s3_key: str, file_path: Path, file_size: int
    ) -> MultipartUploadState:
        """Initiate a new multipart upload.

        Args:
            s3_key: S3 object key
            file_path: Local file path
            file_size: File size in bytes

        Returns:
            Multipart upload state

        Raises:
            S3Error: If upload initiation fails
        """
        part_size = self._calculate_part_size(file_size)
        total_parts = math.ceil(file_size / part_size)

        # Prepare extra args for encryption and storage class
        extra_args: dict[str, Any] = {
            "StorageClass": self.s3_client.config.storage_class,
        }

        if self.s3_client.config.encryption and self.s3_client.config.encryption.lower() != "none":
            if self.s3_client.config.endpoint is None:  # AWS S3
                if self.s3_client.config.encryption == "SSE-S3":
                    extra_args["ServerSideEncryption"] = "AES256"
                elif self.s3_client.config.encryption == "SSE-KMS":
                    extra_args["ServerSideEncryption"] = "aws:kms"

        try:
            response = self.s3_client.client.create_multipart_upload(
                Bucket=self.s3_client.config.bucket,
                Key=s3_key,
                **extra_args,
            )
            upload_id = response["UploadId"]

            state = MultipartUploadState(
                upload_id=upload_id,
                key=s3_key,
                file_path=file_path,
                part_size=part_size,
                total_parts=total_parts,
                uploaded_parts=[],
                state_file=self._get_state_file(s3_key),
            )

            state.save()

            self.logger.info(
                "Initiated multipart upload",
                key=s3_key,
                upload_id=upload_id,
                file_size=file_size,
                part_size=part_size,
                total_parts=total_parts,
            )

            return state

        except ClientError as e:
            raise S3Error(
                f"Failed to initiate multipart upload: {e}",
                context={"bucket": self.s3_client.config.bucket, "key": s3_key},
            ) from e

    def _upload_part(
        self,
        state: MultipartUploadState,
        part_number: int,
    ) -> dict[str, Any]:
        """Upload a single part.

        Args:
            state: Multipart upload state
            part_number: Part number (1-indexed)

        Returns:
            Dictionary with ETag and part number

        Raises:
            S3Error: If part upload fails
        """
        file_size = state.file_path.stat().st_size
        start_byte = (part_number - 1) * state.part_size
        end_byte = min(start_byte + state.part_size, file_size)
        part_size = end_byte - start_byte

        try:
            with open(state.file_path, "rb") as f:
                f.seek(start_byte)
                part_data = f.read(part_size)

            response = self.s3_client.client.upload_part(
                Bucket=self.s3_client.config.bucket,
                Key=state.key,
                PartNumber=part_number,
                UploadId=state.upload_id,
                Body=part_data,
            )

            etag = response["ETag"]

            part_info = {
                "PartNumber": part_number,
                "ETag": etag,
            }

            # Update state
            state.uploaded_parts.append(part_info)
            state.save()

            self.logger.debug(
                "Uploaded part",
                key=state.key,
                upload_id=state.upload_id,
                part_number=part_number,
                part_size=part_size,
            )

            return part_info

        except ClientError as e:
            raise S3Error(
                f"Failed to upload part {part_number}: {e}",
                context={
                    "bucket": self.s3_client.config.bucket,
                    "key": state.key,
                    "upload_id": state.upload_id,
                    "part_number": part_number,
                },
            ) from e

    def _complete_upload(self, state: MultipartUploadState) -> dict[str, Any]:
        """Complete multipart upload.

        Args:
            state: Multipart upload state

        Returns:
            Completion response

        Raises:
            S3Error: If completion fails
        """
        # Sort parts by part number
        parts = sorted(state.uploaded_parts, key=lambda x: x["PartNumber"])

        try:
            response = self.s3_client.client.complete_multipart_upload(
                Bucket=self.s3_client.config.bucket,
                Key=state.key,
                UploadId=state.upload_id,
                MultipartUpload={"Parts": parts},
            )

            # Clean up state file
            if state.state_file and state.state_file.exists():
                state.state_file.unlink()

            self.logger.info(
                "Completed multipart upload",
                key=state.key,
                upload_id=state.upload_id,
                total_parts=len(parts),
            )

            return response

        except ClientError as e:
            raise S3Error(
                f"Failed to complete multipart upload: {e}",
                context={
                    "bucket": self.s3_client.config.bucket,
                    "key": state.key,
                    "upload_id": state.upload_id,
                },
            ) from e

    def _abort_upload(self, state: MultipartUploadState) -> None:
        """Abort multipart upload.

        Args:
            state: Multipart upload state

        Raises:
            S3Error: If abort fails
        """
        try:
            self.s3_client.client.abort_multipart_upload(
                Bucket=self.s3_client.config.bucket,
                Key=state.key,
                UploadId=state.upload_id,
            )

            # Clean up state file
            if state.state_file and state.state_file.exists():
                state.state_file.unlink()

            self.logger.info(
                "Aborted multipart upload",
                key=state.key,
                upload_id=state.upload_id,
            )

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code != "NoSuchUpload":
                raise S3Error(
                    f"Failed to abort multipart upload: {e}",
                    context={
                        "bucket": self.s3_client.config.bucket,
                        "key": state.key,
                        "upload_id": state.upload_id,
                    },
                ) from e

    def upload_file(
        self,
        file_path: Path,
        s3_key: str,
        resume: bool = True,
    ) -> dict[str, Any]:
        """Upload file using multipart upload with resume capability.

        Args:
            file_path: Local file path
            s3_key: S3 object key
            resume: If True, attempt to resume from existing state

        Returns:
            Dictionary with upload metadata (etag, size, etc.)

        Raises:
            S3Error: If upload fails
        """
        file_size = file_path.stat().st_size
        state_file = self._get_state_file(s3_key)

        # Try to resume from existing state
        state: Optional[MultipartUploadState] = None
        if resume and state_file.exists():
            state = MultipartUploadState.load(state_file)
            if state:
                # Verify file hasn't changed
                if state.file_path != file_path or state.file_path.stat().st_size != file_size:
                    self.logger.warning(
                        "State file exists but file has changed, starting new upload",
                        key=s3_key,
                        old_file=str(state.file_path),
                        new_file=str(file_path),
                    )
                    state = None
                else:
                    self.logger.info(
                        "Resuming multipart upload",
                        key=s3_key,
                        upload_id=state.upload_id,
                        uploaded_parts=len(state.uploaded_parts),
                        total_parts=state.total_parts,
                    )

        # Initiate new upload if no state
        if state is None:
            state = self._initiate_upload(s3_key, file_path, file_size)

        try:
            # Upload remaining parts
            remaining_parts = state.get_remaining_parts()
            for part_number in remaining_parts:
                self._upload_part(state, part_number)

            # Complete upload
            response = self._complete_upload(state)

            return {
                "bucket": self.s3_client.config.bucket,
                "key": s3_key,
                "size": file_size,
                "etag": response.get("ETag", ""),
            }

        except Exception as e:
            # Save state before raising error
            state.save()
            raise S3Error(
                f"Multipart upload failed: {e}",
                context={
                    "bucket": self.s3_client.config.bucket,
                    "key": s3_key,
                    "upload_id": state.upload_id,
                    "uploaded_parts": len(state.uploaded_parts),
                    "total_parts": state.total_parts,
                },
            ) from e
