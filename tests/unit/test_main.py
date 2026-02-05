"""Unit tests for main CLI entry point."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from archiver.exceptions import ConfigurationError
from archiver.main import main


@pytest.fixture
def mock_config_file(tmp_path: Path) -> Path:
    """Create a temporary config file."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
version: "1.0"
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


def test_main_success(runner: CliRunner, mock_config_file: Path) -> None:
    """Test successful main execution."""
    with patch("archiver.archiver.Archiver") as mock_archiver_class:
        mock_archiver = MagicMock()
        # archive() is async, so use AsyncMock
        mock_archiver.archive = AsyncMock(
            return_value={
                "databases_processed": 1,
                "tables_processed": 1,
                "records_archived": 100,
            }
        )
        mock_archiver_class.return_value = mock_archiver

        result = runner.invoke(main, ["--config", str(mock_config_file), "--dry-run"])

        assert result.exit_code == 0
        mock_archiver_class.assert_called_once()
        mock_archiver.archive.assert_called_once()


def test_main_config_not_found(runner: CliRunner) -> None:
    """Test main with non-existent config file."""
    result = runner.invoke(main, ["--config", "/nonexistent/config.yaml"])

    assert result.exit_code != 0
    assert "does not exist" in result.output or "No such file" in result.output


def test_main_config_error(runner: CliRunner, mock_config_file: Path) -> None:
    """Test main with configuration error."""
    with patch("archiver.main.load_config") as mock_load_config:
        mock_load_config.side_effect = ConfigurationError(
            "Invalid configuration", correlation_id="test-123"
        )

        result = runner.invoke(main, ["--config", str(mock_config_file)])

        assert result.exit_code == 1


def test_main_archival_failure(runner: CliRunner, mock_config_file: Path) -> None:
    """Test main when archival fails."""
    with patch("archiver.archiver.Archiver") as mock_archiver_class:
        mock_archiver = MagicMock()
        mock_archiver.archive = AsyncMock(side_effect=Exception("Archival failed"))
        mock_archiver_class.return_value = mock_archiver

        result = runner.invoke(main, ["--config", str(mock_config_file)])

        assert result.exit_code == 1


def test_main_database_filter(runner: CliRunner, mock_config_file: Path) -> None:
    """Test main with database filter."""
    with patch("archiver.archiver.Archiver") as mock_archiver_class:
        mock_archiver = MagicMock()
        mock_archiver.archive = AsyncMock(return_value={"databases_processed": 1})
        mock_archiver_class.return_value = mock_archiver

        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--database",
                "test_db",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        mock_archiver_class.assert_called_once()


def test_main_table_filter(runner: CliRunner, mock_config_file: Path) -> None:
    """Test main with table filter."""
    with patch("archiver.archiver.Archiver") as mock_archiver_class:
        mock_archiver = MagicMock()
        mock_archiver.archive = AsyncMock(return_value={"tables_processed": 1})
        mock_archiver_class.return_value = mock_archiver

        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--database",
                "test_db",
                "--table",
                "audit_logs",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0


def test_main_no_databases_after_filter(runner: CliRunner, mock_config_file: Path) -> None:
    """Test main when filtering results in no databases."""
    result = runner.invoke(
        main,
        [
            "--config",
            str(mock_config_file),
            "--database",
            "nonexistent_db",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0  # Should exit gracefully with warning


def test_main_verbose_mode(runner: CliRunner, mock_config_file: Path) -> None:
    """Test main with verbose flag."""
    with patch("archiver.archiver.Archiver") as mock_archiver_class:
        mock_archiver = MagicMock()
        mock_archiver.archive = AsyncMock(return_value={})
        mock_archiver_class.return_value = mock_archiver

        result = runner.invoke(
            main,
            ["--config", str(mock_config_file), "--verbose", "--dry-run"],
        )

        assert result.exit_code == 0


def test_main_log_level(runner: CliRunner, mock_config_file: Path) -> None:
    """Test main with custom log level."""
    with patch("archiver.archiver.Archiver") as mock_archiver_class:
        mock_archiver = MagicMock()
        mock_archiver.archive = AsyncMock(return_value={})
        mock_archiver_class.return_value = mock_archiver

        result = runner.invoke(
            main,
            [
                "--config",
                str(mock_config_file),
                "--log-level",
                "DEBUG",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0


def test_main_unexpected_error(runner: CliRunner, mock_config_file: Path) -> None:
    """Test main with unexpected error."""
    with patch("archiver.main.load_config") as mock_load_config:
        mock_load_config.side_effect = RuntimeError("Unexpected error")

        result = runner.invoke(main, ["--config", str(mock_config_file)])

        assert result.exit_code == 1
