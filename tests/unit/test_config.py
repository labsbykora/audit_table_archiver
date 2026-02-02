"""Unit tests for configuration module."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from archiver.config import (
    ArchiverConfig,
    DatabaseConfig,
    S3Config,
    TableConfig,
    load_config,
)
from archiver.exceptions import ConfigurationError


def test_s3_config_validation() -> None:
    """Test S3 configuration validation."""
    config = S3Config(
        bucket="test-bucket",
        region="us-east-1",
    )
    assert config.bucket == "test-bucket"
    assert config.region == "us-east-1"
    assert config.storage_class == "STANDARD_IA"  # default


def test_table_config_validation() -> None:
    """Test table configuration validation."""
    config = TableConfig(
        name="test_table",
        timestamp_column="created_at",
        primary_key="id",
    )
    assert config.name == "test_table"
    assert config.schema_name == "public"  # default
    assert config.retention_days is None


def test_database_config_validation() -> None:
    """Test database configuration validation."""
    os.environ["TEST_DB_PASSWORD"] = "test_password"

    config = DatabaseConfig(
        name="test_db",
        host="localhost",
        user="test_user",
        password_env="TEST_DB_PASSWORD",
        tables=[
            TableConfig(
                name="test_table",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )
    assert config.name == "test_db"
    assert config.port == 5432  # default
    assert len(config.tables) == 1


def test_database_config_missing_password_env() -> None:
    """Test database config fails when password env var missing."""
    from archiver.config import TableConfig
    
    config = DatabaseConfig(
        name="test_db",
        host="localhost",
        user="test_user",
        password_env="NONEXISTENT_ENV_VAR",
        tables=[
            TableConfig(
                schema_name="public",
                name="test_table",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )
    
    # Password validation happens when get_password() is called, not during initialization
    with pytest.raises(ValueError, match="Environment variable.*not set"):
        config.get_password()


def test_load_config_from_file() -> None:
    """Test loading configuration from YAML file."""
    os.environ["TEST_DB_PASSWORD"] = "test_password"

    config_data = {
        "version": "2.0",
        "s3": {
            "bucket": "test-bucket",
            "region": "us-east-1",
        },
        "defaults": {
            "retention_days": 90,
            "batch_size": 10000,
        },
        "databases": [
            {
                "name": "test_db",
                "host": "localhost",
                "user": "test_user",
                "password_env": "TEST_DB_PASSWORD",
                "tables": [
                    {
                        "name": "test_table",
                        "timestamp_column": "created_at",
                        "primary_key": "id",
                    }
                ],
            }
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(config_data, f)
        config_path = Path(f.name)

    try:
        config = load_config(config_path)
        assert config.version == "2.0"
        assert config.s3.bucket == "test-bucket"
        assert len(config.databases) == 1
        assert len(config.databases[0].tables) == 1
        # Check defaults applied
        assert config.databases[0].tables[0].retention_days == 90
    finally:
        config_path.unlink()


def test_load_config_env_var_substitution() -> None:
    """Test environment variable substitution in config."""
    os.environ["S3_BUCKET"] = "my-bucket"
    os.environ["TEST_DB_PASSWORD"] = "test_password"

    config_data = {
        "version": "2.0",
        "s3": {
            "bucket": "${S3_BUCKET}",
            "region": "us-east-1",
        },
        "databases": [
            {
                "name": "test_db",
                "host": "localhost",
                "user": "test_user",
                "password_env": "TEST_DB_PASSWORD",
                "tables": [
                    {
                        "name": "test_table",
                        "timestamp_column": "created_at",
                        "primary_key": "id",
                    }
                ],
            }
        ],
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(config_data, f)
        config_path = Path(f.name)

    try:
        config = load_config(config_path)
        assert config.s3.bucket == "my-bucket"
    finally:
        config_path.unlink()


def test_load_config_invalid_file() -> None:
    """Test loading non-existent config file raises error."""
    with pytest.raises(ValueError, match="Configuration file not found"):
        load_config(Path("/nonexistent/config.yaml"))


def test_load_config_invalid_yaml() -> None:
    """Test loading invalid YAML raises error."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("invalid: yaml: content: [")
        config_path = Path(f.name)

    try:
        with pytest.raises(ValueError, match="Invalid YAML"):
            load_config(config_path)
    finally:
        config_path.unlink()

