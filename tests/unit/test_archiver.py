"""Unit tests for archiver module."""

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from archiver.archiver import Archiver
from archiver.config import (
    ArchiverConfig,
    DatabaseConfig,
    DefaultsConfig,
    MonitoringConfig,
    S3Config,
    TableConfig,
)
from archiver.exceptions import DatabaseError


@pytest.fixture
def archiver_config() -> ArchiverConfig:
    """Create test archiver configuration."""
    os.environ.setdefault("TEST_DB_PASSWORD", "test_password")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")

    return ArchiverConfig(
        version="1.0",
        defaults=DefaultsConfig(
            batch_size=1000,
            retention_days=90,
        ),
        s3=S3Config(
            bucket="test-bucket",
            prefix="archives/",
            endpoint="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        ),
        databases=[
            DatabaseConfig(
                name="test_db",
                host="localhost",
                port=5432,
                user="test_user",
                password_env="TEST_DB_PASSWORD",
                tables=[
                    TableConfig(
                        schema_name="public",
                        name="audit_logs",
                        timestamp_column="created_at",
                        primary_key="id",
                    )
                ],
            )
        ],
    )


@pytest.fixture
def archiver(archiver_config: ArchiverConfig) -> Archiver:
    """Create test archiver instance."""
    # Disable metrics to avoid registry conflicts
    if archiver_config.monitoring is None:
        archiver_config.monitoring = MonitoringConfig(metrics_enabled=False)
    else:
        archiver_config.monitoring.metrics_enabled = False
    return Archiver(archiver_config, dry_run=False)


@pytest.mark.asyncio
async def test_archiver_init(archiver_config: ArchiverConfig) -> None:
    """Test archiver initialization."""
    # Disable metrics to avoid registry conflicts
    if archiver_config.monitoring is None:
        archiver_config.monitoring = MonitoringConfig(metrics_enabled=False)
    else:
        archiver_config.monitoring.metrics_enabled = False
    archiver = Archiver(archiver_config, dry_run=True)

    assert archiver.config == archiver_config
    assert archiver.dry_run is True
    assert archiver.serializer is not None
    assert archiver.compressor is not None
    assert archiver.verifier is not None
    assert archiver.checksum_calculator is not None
    assert archiver.metadata_generator is not None
    assert archiver.manifest_generator is not None
    assert archiver.sample_verifier is not None
    assert archiver.schema_detector is not None
    assert archiver.schema_drift_detector is not None
    assert archiver.watermark_manager is not None
    assert archiver.lock_manager is not None
    assert archiver.checkpoint_manager is not None


@pytest.mark.asyncio
async def test_archiver_archive_sequential(archiver: Archiver) -> None:
    """Test sequential database archival."""
    # Mock database manager
    mock_db_manager = MagicMock()
    mock_db_manager.connect = AsyncMock()
    mock_db_manager.disconnect = AsyncMock()
    mock_db_manager.health_check = AsyncMock(return_value=True)

    # Mock S3 client
    mock_s3_client = MagicMock()
    mock_s3_client.validate_bucket = AsyncMock()

    # Mock batch processor
    mock_batch_processor = MagicMock()
    mock_batch_processor.count_eligible_records = AsyncMock(return_value=0)

    with patch("archiver.archiver.DatabaseManager", return_value=mock_db_manager):
        with patch("archiver.archiver.S3Client", return_value=mock_s3_client):
            with patch("archiver.archiver.BatchProcessor", return_value=mock_batch_processor):
                stats = await archiver.archive()

    assert stats["databases_processed"] >= 0
    assert "start_time" in stats
    assert "end_time" in stats
    assert "database_stats" in stats


@pytest.mark.asyncio
async def test_archiver_archive_parallel(archiver_config: ArchiverConfig) -> None:
    """Test parallel database archival."""
    archiver_config.defaults.parallel_databases = True
    archiver_config.defaults.max_parallel_databases = 2
    # Disable metrics to avoid registry conflicts
    if archiver_config.monitoring is None:
        archiver_config.monitoring = MonitoringConfig(metrics_enabled=False)
    else:
        archiver_config.monitoring.metrics_enabled = False

    archiver = Archiver(archiver_config, dry_run=False)

    # Mock database manager
    mock_db_manager = MagicMock()
    mock_db_manager.connect = AsyncMock()
    mock_db_manager.disconnect = AsyncMock()
    mock_db_manager.health_check = AsyncMock(return_value=True)

    # Mock S3 client
    mock_s3_client = MagicMock()
    mock_s3_client.validate_bucket = AsyncMock()

    # Mock batch processor
    mock_batch_processor = MagicMock()
    mock_batch_processor.count_eligible_records = AsyncMock(return_value=0)

    with patch("archiver.archiver.DatabaseManager", return_value=mock_db_manager):
        with patch("archiver.archiver.S3Client", return_value=mock_s3_client):
            with patch("archiver.archiver.BatchProcessor", return_value=mock_batch_processor):
                stats = await archiver.archive()

    assert stats["databases_processed"] >= 0
    assert "database_stats" in stats


@pytest.mark.asyncio
async def test_archiver_archive_database_failure(archiver: Archiver) -> None:
    """Test archival with database failure."""
    # Mock database manager to raise error
    mock_db_manager = MagicMock()
    mock_db_manager.connect = AsyncMock(side_effect=DatabaseError("Connection failed"))

    # Mock S3 client
    mock_s3_client = MagicMock()
    mock_s3_client.validate_bucket = AsyncMock()

    with patch("archiver.archiver.DatabaseManager", return_value=mock_db_manager):
        with patch("archiver.archiver.S3Client", return_value=mock_s3_client):
            stats = await archiver.archive()

    assert stats["databases_failed"] == 1
    assert stats["databases_processed"] == 0
    assert len(stats["database_stats"]) == 1
    assert stats["database_stats"][0]["success"] is False
    assert "error" in stats["database_stats"][0]


@pytest.mark.asyncio
async def test_archiver_generate_batch_id(archiver: Archiver) -> None:
    """Test batch ID generation."""
    batch_id = archiver._generate_batch_id("test_db", "test_table", 1)

    assert isinstance(batch_id, str)
    assert len(batch_id) == 16  # SHA-256 hash truncated to 16 chars
    # Batch ID is a hash, so it won't contain the original strings directly
    # But same inputs should produce same hash
    batch_id2 = archiver._generate_batch_id("test_db", "test_table", 1)
    assert batch_id == batch_id2


@pytest.mark.asyncio
async def test_archiver_load_previous_schema(archiver: Archiver) -> None:
    """Test loading previous schema from S3."""
    # Mock S3 client
    mock_s3_client = MagicMock()

    # Test with no previous schema
    mock_s3_client.list_objects = MagicMock(return_value=[])
    schema = await archiver._load_previous_schema(mock_s3_client, "test_db", "test_table")
    assert schema is None

    # Test with previous schema
    import json

    mock_metadata = {
        "version": "1.0",
        "batch_info": {"batch_number": 1},
        "table_schema": {
            "columns": [{"name": "id", "data_type": "bigint"}],
            "primary_key": {"columns": ["id"]},
        },
    }
    metadata_json = json.dumps(mock_metadata)
    metadata_bytes = metadata_json.encode("utf-8")

    mock_s3_client.list_objects = MagicMock(
        return_value=[
            {
                "key": "test_db/test_table/year=2024/month=01/day=01/batch_1.metadata.json",
                "last_modified": datetime.now(timezone.utc),
            }
        ]
    )
    mock_s3_client.get_object_bytes = MagicMock(return_value=metadata_bytes)

    # Mock metadata_from_json
    with patch.object(
        archiver.metadata_generator, "metadata_from_json", return_value=mock_metadata
    ):
        schema = await archiver._load_previous_schema(mock_s3_client, "test_db", "test_table")
        assert schema is not None
        assert "columns" in schema
        assert "primary_key" in schema
