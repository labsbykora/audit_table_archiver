"""Checkpoint management for resuming interrupted archival runs."""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import structlog

from archiver.exceptions import ArchiverError
from archiver.s3_client import S3Client
from utils.logging import get_logger


class CheckpointError(ArchiverError):
    """Raised when checkpoint operations fail."""

    pass


class Checkpoint:
    """Represents a checkpoint for resuming archival."""

    def __init__(
        self,
        database_name: str,
        table_name: str,
        schema_name: str,
        batch_number: int,
        last_timestamp: Optional[datetime],
        last_primary_key: Optional[Any],
        records_archived: int,
        batches_processed: int,
        checkpoint_time: datetime,
        batch_id: Optional[str] = None,
    ) -> None:
        """Initialize checkpoint.

        Args:
            database_name: Database name
            table_name: Table name
            schema_name: Schema name
            batch_number: Last completed batch number
            last_timestamp: Last archived timestamp
            last_primary_key: Last archived primary key
            records_archived: Total records archived so far
            batches_processed: Total batches processed so far
            checkpoint_time: When checkpoint was created
            batch_id: Last batch ID (optional)
        """
        self.database_name = database_name
        self.table_name = table_name
        self.schema_name = schema_name
        self.batch_number = batch_number
        self.last_timestamp = last_timestamp
        self.last_primary_key = last_primary_key
        self.records_archived = records_archived
        self.batches_processed = batches_processed
        self.checkpoint_time = checkpoint_time
        self.batch_id = batch_id

    def to_dict(self) -> dict[str, Any]:
        """Convert checkpoint to dictionary.

        Returns:
            Dictionary representation of checkpoint
        """
        return {
            "version": "1.0",  # Checkpoint format version
            "database": self.database_name,
            "table": self.table_name,
            "schema": self.schema_name,
            "batch_number": self.batch_number,
            "last_timestamp": (self.last_timestamp.isoformat() if self.last_timestamp else None),
            "last_primary_key": (
                str(self.last_primary_key) if self.last_primary_key is not None else None
            ),
            "records_archived": self.records_archived,
            "batches_processed": self.batches_processed,
            "checkpoint_time": self.checkpoint_time.isoformat(),
            "batch_id": self.batch_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Checkpoint":
        """Create checkpoint from dictionary.

        Args:
            data: Dictionary containing checkpoint data

        Returns:
            Checkpoint object

        Raises:
            CheckpointError: If data is invalid
        """
        try:
            last_timestamp = None
            if data.get("last_timestamp"):
                last_timestamp = datetime.fromisoformat(
                    data["last_timestamp"].replace("Z", "+00:00")
                )

            # Convert last_primary_key back to original type if possible
            last_primary_key = data.get("last_primary_key")
            if last_primary_key is not None:
                # Try to convert back to int if it looks like an integer
                try:
                    if isinstance(last_primary_key, str) and last_primary_key.isdigit():
                        last_primary_key = int(last_primary_key)
                except (ValueError, AttributeError):
                    pass  # Keep as string if conversion fails

            checkpoint_time = datetime.fromisoformat(data["checkpoint_time"].replace("Z", "+00:00"))

            return cls(
                database_name=data["database"],
                table_name=data["table"],
                schema_name=data["schema"],
                batch_number=data["batch_number"],
                last_timestamp=last_timestamp,
                last_primary_key=last_primary_key,
                records_archived=data["records_archived"],
                batches_processed=data["batches_processed"],
                checkpoint_time=checkpoint_time,
                batch_id=data.get("batch_id"),
            )
        except (KeyError, ValueError, TypeError) as e:
            raise CheckpointError(
                f"Invalid checkpoint data: {e}",
                context={"data": data},
            ) from e


class CheckpointManager:
    """Manages checkpoints for resuming interrupted archival runs."""

    def __init__(
        self,
        storage_type: str = "s3",  # "s3" or "local"
        checkpoint_interval: int = 10,  # Save checkpoint every N batches
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize checkpoint manager.

        Args:
            storage_type: Storage type for checkpoints ("s3" or "local")
            checkpoint_interval: Save checkpoint every N batches (default: 10)
            logger: Optional logger instance
        """
        if storage_type not in ("s3", "local"):
            raise ValueError(f"Invalid storage_type: {storage_type}. Must be 's3' or 'local'")

        self.storage_type = storage_type
        self.checkpoint_interval = checkpoint_interval
        self.logger = logger or get_logger("checkpoint_manager")

    def should_save_checkpoint(self, batch_number: int) -> bool:
        """Check if checkpoint should be saved for this batch number.

        Args:
            batch_number: Current batch number

        Returns:
            True if checkpoint should be saved
        """
        return batch_number % self.checkpoint_interval == 0

    async def save_checkpoint(
        self,
        checkpoint: Checkpoint,
        s3_client: Optional[S3Client] = None,
        local_path: Optional[Path] = None,
    ) -> None:
        """Save checkpoint to storage.

        Args:
            checkpoint: Checkpoint object to save
            s3_client: S3 client (required if storage_type is "s3")
            local_path: Local file path (required if storage_type is "local")

        Raises:
            CheckpointError: If checkpoint save fails
        """
        if self.storage_type == "s3":
            if s3_client is None:
                raise ValueError("s3_client is required for S3 storage type")
            await self._save_checkpoint_to_s3(checkpoint, s3_client)
        else:  # local
            if local_path is None:
                raise ValueError("local_path is required for local storage type")
            await self._save_checkpoint_to_local(checkpoint, local_path)

    async def load_checkpoint(
        self,
        database_name: str,
        table_name: str,
        s3_client: Optional[S3Client] = None,
        local_path: Optional[Path] = None,
    ) -> Optional[Checkpoint]:
        """Load checkpoint from storage.

        Args:
            database_name: Database name
            table_name: Table name
            s3_client: S3 client (required if storage_type is "s3")
            local_path: Local file path (required if storage_type is "local")

        Returns:
            Checkpoint object or None if not found

        Raises:
            CheckpointError: If checkpoint load fails
        """
        if self.storage_type == "s3":
            if s3_client is None:
                raise ValueError("s3_client is required for S3 storage type")
            return await self._load_checkpoint_from_s3(database_name, table_name, s3_client)
        else:  # local
            if local_path is None:
                raise ValueError("local_path is required for local storage type")
            return await self._load_checkpoint_from_local(database_name, table_name, local_path)

    async def delete_checkpoint(
        self,
        database_name: str,
        table_name: str,
        s3_client: Optional[S3Client] = None,
        local_path: Optional[Path] = None,
    ) -> None:
        """Delete checkpoint from storage (after successful completion).

        Args:
            database_name: Database name
            table_name: Table name
            s3_client: S3 client (required if storage_type is "s3")
            local_path: Local file path (required if storage_type is "local")

        Raises:
            CheckpointError: If checkpoint deletion fails
        """
        if self.storage_type == "s3":
            if s3_client is None:
                raise ValueError("s3_client is required for S3 storage type")
            await self._delete_checkpoint_from_s3(database_name, table_name, s3_client)
        else:  # local
            if local_path is None:
                raise ValueError("local_path is required for local storage type")
            await self._delete_checkpoint_from_local(database_name, table_name, local_path)

    async def _save_checkpoint_to_s3(self, checkpoint: Checkpoint, s3_client: S3Client) -> None:
        """Save checkpoint to S3.

        Args:
            checkpoint: Checkpoint object
            s3_client: S3 client
        """
        checkpoint_key = f"{checkpoint.database_name}/{checkpoint.table_name}/.checkpoint.json"

        checkpoint_json = json.dumps(checkpoint.to_dict(), indent=2, default=str)

        # Write to temporary file and upload
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json", encoding="utf-8"
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
            tmp_file.write(checkpoint_json)

        try:
            s3_client.upload_file(tmp_path, checkpoint_key)
            self.logger.debug(
                "Checkpoint saved to S3",
                database=checkpoint.database_name,
                table=checkpoint.table_name,
                batch_number=checkpoint.batch_number,
                key=checkpoint_key,
            )
        finally:
            try:
                tmp_path.unlink()
            except Exception as e:
                self.logger.warning(
                    "Failed to delete temporary checkpoint file",
                    path=str(tmp_path),
                    error=str(e),
                )

    async def _load_checkpoint_from_s3(
        self, database_name: str, table_name: str, s3_client: S3Client
    ) -> Optional[Checkpoint]:
        """Load checkpoint from S3.

        Args:
            database_name: Database name
            table_name: Table name
            s3_client: S3 client

        Returns:
            Checkpoint object or None if not found

        Raises:
            CheckpointError: If checkpoint load fails
        """
        checkpoint_key = f"{database_name}/{table_name}/.checkpoint.json"

        try:
            checkpoint_data = s3_client.get_object_bytes(checkpoint_key)
            checkpoint_json = checkpoint_data.decode("utf-8")
            checkpoint_dict = json.loads(checkpoint_json)

            checkpoint = Checkpoint.from_dict(checkpoint_dict)

            self.logger.info(
                "Checkpoint loaded from S3",
                database=database_name,
                table=table_name,
                batch_number=checkpoint.batch_number,
                key=checkpoint_key,
            )

            return checkpoint

        except Exception as e:
            # Checkpoint not found or invalid - this is OK for first run
            self.logger.debug(
                "Checkpoint not found in S3 (first run or not yet created)",
                database=database_name,
                table=table_name,
                error=str(e),
            )
            return None

    async def _delete_checkpoint_from_s3(
        self, database_name: str, table_name: str, s3_client: S3Client
    ) -> None:
        """Delete checkpoint from S3.

        Args:
            database_name: Database name
            table_name: Table name
            s3_client: S3 client
        """
        from botocore.exceptions import ClientError

        checkpoint_key = f"{database_name}/{table_name}/.checkpoint.json"

        try:
            # Use S3Client's client property
            s3_client.client.delete_object(Bucket=s3_client.config.bucket, Key=checkpoint_key)
            self.logger.info(
                "Checkpoint deleted from S3",
                database=database_name,
                table=table_name,
                key=checkpoint_key,
            )
        except ClientError as e:
            # Ignore if checkpoint doesn't exist
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code != "NoSuchKey":
                raise CheckpointError(
                    f"Failed to delete checkpoint from S3: {e}",
                    context={"key": checkpoint_key},
                ) from e

    async def _save_checkpoint_to_local(self, checkpoint: Checkpoint, local_path: Path) -> None:
        """Save checkpoint to local file.

        Args:
            checkpoint: Checkpoint object
            local_path: Directory path for checkpoint files
        """
        checkpoint_file = (
            local_path / f"{checkpoint.database_name}_{checkpoint.table_name}.checkpoint.json"
        )

        # Ensure directory exists
        local_path.mkdir(parents=True, exist_ok=True)

        checkpoint_json = json.dumps(checkpoint.to_dict(), indent=2, default=str)
        checkpoint_file.write_text(checkpoint_json, encoding="utf-8")

        self.logger.debug(
            "Checkpoint saved to local file",
            database=checkpoint.database_name,
            table=checkpoint.table_name,
            batch_number=checkpoint.batch_number,
            file=str(checkpoint_file),
        )

    async def _load_checkpoint_from_local(
        self, database_name: str, table_name: str, local_path: Path
    ) -> Optional[Checkpoint]:
        """Load checkpoint from local file.

        Args:
            database_name: Database name
            table_name: Table name
            local_path: Directory path for checkpoint files

        Returns:
            Checkpoint object or None if not found

        Raises:
            CheckpointError: If checkpoint load fails
        """
        checkpoint_file = local_path / f"{database_name}_{table_name}.checkpoint.json"

        if not checkpoint_file.exists():
            self.logger.debug(
                "Checkpoint file not found (first run or not yet created)",
                database=database_name,
                table=table_name,
                file=str(checkpoint_file),
            )
            return None

        try:
            checkpoint_json = checkpoint_file.read_text(encoding="utf-8")
            checkpoint_dict = json.loads(checkpoint_json)
            checkpoint = Checkpoint.from_dict(checkpoint_dict)

            self.logger.info(
                "Checkpoint loaded from local file",
                database=database_name,
                table=table_name,
                batch_number=checkpoint.batch_number,
                file=str(checkpoint_file),
            )

            return checkpoint

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise CheckpointError(
                f"Failed to load checkpoint from local file: {e}",
                context={"file": str(checkpoint_file)},
            ) from e

    async def _delete_checkpoint_from_local(
        self, database_name: str, table_name: str, local_path: Path
    ) -> None:
        """Delete checkpoint from local file.

        Args:
            database_name: Database name
            table_name: Table name
            local_path: Directory path for checkpoint files
        """
        checkpoint_file = local_path / f"{database_name}_{table_name}.checkpoint.json"

        try:
            if checkpoint_file.exists():
                checkpoint_file.unlink()
                self.logger.info(
                    "Checkpoint deleted from local file",
                    database=database_name,
                    table=table_name,
                    file=str(checkpoint_file),
                )
        except Exception as e:
            raise CheckpointError(
                f"Failed to delete checkpoint from local file: {e}",
                context={"file": str(checkpoint_file)},
            ) from e
