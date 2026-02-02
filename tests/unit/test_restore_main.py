"""Unit tests for restore CLI entry point."""

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from archiver.config import ArchiverConfig, DatabaseConfig, S3Config, TableConfig
from restore.main import main


@pytest.fixture
def mock_config_file(tmp_path: Path) -> Path:
    """Create a temporary config file."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
version: "2.0"
defaults:
  batch_size: 1000
  retention_days: 90
s3:
  bucket: "test-bucket"
  prefix: "archives/"
  endpoint: "http://localhost:9000"
  access_key_id: "minioadmin"
  secret_access_key: "minioadmin"
databases:
  - name: "test_db"
    host: "localhost"
    port: 5432
    user: "test_user"
    password_env: "TEST_DB_PASSWORD"
    tables:
      - schema: "public"
        name: "audit_logs"
        timestamp_column: "created_at"
        primary_key: "id"
"""
    )
    return config_file


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


def test_restore_main_missing_config(runner: CliRunner) -> None:
    """Test restore with missing config."""
    # Click requires --config, so this will fail at Click level
    result = runner.invoke(main, ["--s3-key", "test/key.jsonl.gz"])
    
    # Click will show error about missing required option
    assert result.exit_code != 0


def test_restore_main_missing_s3_key(runner: CliRunner, mock_config_file: Path) -> None:
    """Test restore with missing S3 key."""
    result = runner.invoke(main, ["--config", str(mock_config_file)])
    
    # Should exit with error code (JSON logging may not show in output)
    assert result.exit_code == 1


def test_restore_main_success(runner: CliRunner, mock_config_file: Path) -> None:
    """Test successful restore execution."""
    with patch("restore.main.S3ArchiveReader") as mock_reader_class, \
         patch("restore.main.DatabaseManager") as mock_db_class, \
         patch("restore.main.RestoreEngine") as mock_engine_class:
        
        # Mock archive file
        mock_archive = MagicMock()
        mock_archive.record_count = 100
        mock_archive.database_name = "test_db"
        mock_archive.table_name = "audit_logs"
        mock_archive.parse_records.return_value = [{"id": 1, "data": "test"}]
        
        # Mock S3 reader
        mock_reader = MagicMock()
        mock_reader.read_archive = AsyncMock(return_value=mock_archive)
        mock_reader_class.return_value = mock_reader
        
        # Mock database manager
        mock_db = MagicMock()
        mock_db.connect = AsyncMock()
        mock_db.disconnect = AsyncMock()
        mock_db_class.return_value = mock_db
        
        # Mock restore engine
        mock_engine = MagicMock()
        mock_engine.restore_archive = AsyncMock(return_value={
            "records_restored": 100,
            "records_processed": 100,
            "records_failed": 0,
        })
        mock_engine_class.return_value = mock_engine
        
        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--s3-key",
                "archives/test_db/public/audit_logs/year=2026/month=01/day=04/file.jsonl.gz",
            ],
        )
        
        assert result.exit_code == 0
        mock_reader.read_archive.assert_called_once()
        mock_engine.restore_archive.assert_called_once()


def test_restore_main_dry_run(runner: CliRunner, mock_config_file: Path) -> None:
    """Test restore with dry run."""
    with patch("restore.main.S3ArchiveReader") as mock_reader_class:
        mock_archive = MagicMock()
        mock_archive.record_count = 100
        mock_archive.database_name = "test_db"
        mock_archive.table_name = "audit_logs"
        mock_archive.parse_records.return_value = [{"id": 1, "data": "test"}]
        
        mock_reader = MagicMock()
        mock_reader.read_archive = AsyncMock(return_value=mock_archive)
        mock_reader_class.return_value = mock_reader
        
        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--s3-key",
                "archives/test_db/public/audit_logs/year=2026/month=01/day=04/file.jsonl.gz",
                "--dry-run",
            ],
        )
        
        assert result.exit_code == 0
        mock_reader.read_archive.assert_called_once()


def test_restore_main_with_conflict_strategy(runner: CliRunner, mock_config_file: Path) -> None:
    """Test restore with conflict strategy."""
    with patch("restore.main.S3ArchiveReader") as mock_reader_class, \
         patch("restore.main.DatabaseManager") as mock_db_class, \
         patch("restore.main.RestoreEngine") as mock_engine_class:
        
        mock_archive = MagicMock()
        mock_archive.record_count = 100
        mock_archive.database_name = "test_db"
        mock_archive.table_name = "audit_logs"
        
        mock_reader = MagicMock()
        mock_reader.read_archive = AsyncMock(return_value=mock_archive)
        mock_reader_class.return_value = mock_reader
        
        mock_db = MagicMock()
        mock_db.connect = AsyncMock()
        mock_db.disconnect = AsyncMock()
        mock_db_class.return_value = mock_db
        
        mock_engine = MagicMock()
        mock_engine.restore_archive = AsyncMock(return_value={"records_restored": 100})
        mock_engine_class.return_value = mock_engine
        
        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--s3-key",
                "archives/test_db/public/audit_logs/year=2026/month=01/day=04/file.jsonl.gz",
                "--conflict-strategy",
                "overwrite",
            ],
        )
        
        assert result.exit_code == 0
        call_args = mock_engine.restore_archive.call_args
        assert call_args[1]["conflict_strategy"] == "overwrite"


def test_restore_main_with_schema_migration(runner: CliRunner, mock_config_file: Path) -> None:
    """Test restore with schema migration strategy."""
    with patch("restore.main.S3ArchiveReader") as mock_reader_class, \
         patch("restore.main.DatabaseManager") as mock_db_class, \
         patch("restore.main.RestoreEngine") as mock_engine_class:
        
        mock_archive = MagicMock()
        mock_archive.record_count = 100
        mock_archive.database_name = "test_db"
        mock_archive.table_name = "audit_logs"
        
        mock_reader = MagicMock()
        mock_reader.read_archive = AsyncMock(return_value=mock_archive)
        mock_reader_class.return_value = mock_reader
        
        mock_db = MagicMock()
        mock_db.connect = AsyncMock()
        mock_db.disconnect = AsyncMock()
        mock_db_class.return_value = mock_db
        
        mock_engine = MagicMock()
        mock_engine.restore_archive = AsyncMock(return_value={"records_restored": 100})
        mock_engine_class.return_value = mock_engine
        
        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--s3-key",
                "archives/test_db/public/audit_logs/year=2026/month=01/day=04/file.jsonl.gz",
                "--schema-migration-strategy",
                "transform",
            ],
        )
        
        assert result.exit_code == 0
        call_args = mock_engine.restore_archive.call_args
        assert call_args[1]["schema_migration_strategy"] == "transform"


def test_restore_main_database_not_found(runner: CliRunner, mock_config_file: Path) -> None:
    """Test restore with database not in config."""
    with patch("restore.main.S3ArchiveReader") as mock_reader_class:
        mock_archive = MagicMock()
        mock_archive.record_count = 100
        mock_archive.database_name = "nonexistent_db"
        mock_archive.table_name = "audit_logs"
        
        mock_reader = MagicMock()
        mock_reader.read_archive = AsyncMock(return_value=mock_archive)
        mock_reader_class.return_value = mock_reader
        
        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--s3-key",
                "archives/nonexistent_db/public/audit_logs/year=2026/month=01/day=04/file.jsonl.gz",
            ],
        )
        
        # Should exit with error code (JSON logging may not show in output)
        assert result.exit_code == 1


def test_restore_main_extract_db_table_from_key(runner: CliRunner, mock_config_file: Path) -> None:
    """Test restore extracts database and table from S3 key."""
    with patch("restore.main.S3ArchiveReader") as mock_reader_class, \
         patch("restore.main.DatabaseManager") as mock_db_class, \
         patch("restore.main.RestoreEngine") as mock_engine_class:
        
        mock_archive = MagicMock()
        mock_archive.record_count = 100
        mock_archive.database_name = "test_db"
        mock_archive.table_name = "audit_logs"
        
        mock_reader = MagicMock()
        mock_reader.read_archive = AsyncMock(return_value=mock_archive)
        mock_reader_class.return_value = mock_reader
        
        mock_db = MagicMock()
        mock_db.connect = AsyncMock()
        mock_db.disconnect = AsyncMock()
        mock_db_class.return_value = mock_db
        
        mock_engine = MagicMock()
        mock_engine.restore_archive = AsyncMock(return_value={"records_restored": 100})
        mock_engine_class.return_value = mock_engine
        
        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--s3-key",
                "archives/test_db/public/audit_logs/year=2026/month=01/day=04/file.jsonl.gz",
            ],
        )
        
        assert result.exit_code == 0


def test_restore_main_with_options(runner: CliRunner, mock_config_file: Path) -> None:
    """Test restore with various options."""
    with patch("restore.main.S3ArchiveReader") as mock_reader_class, \
         patch("restore.main.DatabaseManager") as mock_db_class, \
         patch("restore.main.RestoreEngine") as mock_engine_class:
        
        mock_archive = MagicMock()
        mock_archive.record_count = 100
        mock_archive.database_name = "test_db"
        mock_archive.table_name = "audit_logs"
        
        mock_reader = MagicMock()
        mock_reader.read_archive = AsyncMock(return_value=mock_archive)
        mock_reader_class.return_value = mock_reader
        
        mock_db = MagicMock()
        mock_db.connect = AsyncMock()
        mock_db.disconnect = AsyncMock()
        mock_db_class.return_value = mock_db
        
        mock_engine = MagicMock()
        mock_engine.restore_archive = AsyncMock(return_value={"records_restored": 100})
        mock_engine_class.return_value = mock_engine
        
        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--s3-key",
                "archives/test_db/public/audit_logs/year=2026/month=01/day=04/file.jsonl.gz",
                "--database",
                "test_db",
                "--table",
                "audit_logs",
                "--schema",
                "public",
                "--batch-size",
                "5000",
                "--drop-indexes",
                "--commit-frequency",
                "10",
                "--no-validate-checksum",
                "--no-detect-conflicts",
            ],
        )
        
        assert result.exit_code == 0
        call_args = mock_engine.restore_archive.call_args
        assert call_args[1]["batch_size"] == 5000
        assert call_args[1]["drop_indexes"] is True
        assert call_args[1]["commit_frequency"] == 10
        assert call_args[1]["detect_conflicts"] is False

