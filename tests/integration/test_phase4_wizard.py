"""Integration tests for Phase 4: Configuration wizard."""

import os

import pytest

from archiver.config import ArchiverConfig
from wizard.config_wizard import ConfigWizard


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wizard_detect_tables_integration(
    db_connection,
):
    """Test wizard table detection with real database."""
    os.environ["TEST_DB_PASSWORD"] = "archiver_password"

    wizard = ConfigWizard()

    # Test table detection
    tables = await wizard.detect_tables(
        host="localhost",
        port=5432,
        database="test_db",
        user="archiver",
        password="archiver_password",
        schema="public",
    )

    # Should detect at least one table (the test table created by fixture)
    assert len(tables) > 0

    # Verify table structure
    table = tables[0]
    assert "name" in table
    assert "schema" in table
    assert "suggested_timestamp" in table or table.get("timestamp_columns") is not None
    assert "primary_key" in table or table.get("id_columns") is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wizard_estimate_record_count_integration(
    db_connection,
    test_table,
    test_data,
):
    """Test wizard record count estimation with real database."""
    # test_data is already inserted by fixture, no need to insert again

    os.environ["TEST_DB_PASSWORD"] = "archiver_password"

    wizard = ConfigWizard()

    # Estimate record count
    estimates = await wizard.estimate_record_count(
        host="localhost",
        port=5432,
        database="test_db",
        user="archiver",
        password="archiver_password",
        schema="public",
        table=test_table,
        timestamp_column="created_at",
        retention_days=90,
    )

    assert "total_records" in estimates
    assert estimates["total_records"] >= len(test_data)
    assert "eligible_records" in estimates
    assert "age_days" in estimates


@pytest.mark.integration
def test_wizard_generate_config():
    """Test wizard configuration generation."""
    wizard = ConfigWizard()

    databases = [
        {
            "name": "test_db",
            "host": "localhost",
            "port": 5432,
            "user": "archiver",
            "password_env": "TEST_DB_PASSWORD",
            "tables": [
                {
                    "name": "test_table",
                    "schema_name": "public",
                    "timestamp_column": "created_at",
                    "primary_key": "id",
                    "retention_days": 90,
                }
            ],
        }
    ]

    s3_config = {
        "bucket": "test-bucket",
        "region": "us-east-1",
        "prefix": "archives/",
    }

    defaults = {
        "retention_days": 90,
        "batch_size": 1000,
    }

    config = wizard.generate_config(databases, s3_config, defaults)

    assert isinstance(config, ArchiverConfig)
    assert len(config.databases) == 1
    assert config.databases[0].name == "test_db"
    assert len(config.databases[0].tables) == 1
    assert config.s3.bucket == "test-bucket"


@pytest.mark.integration
def test_wizard_suggest_batch_size():
    """Test batch size suggestions."""
    wizard = ConfigWizard()

    # Small dataset
    assert wizard.suggest_batch_size(500) == 100

    # Medium dataset
    assert wizard.suggest_batch_size(5000) == 500

    # Large dataset
    assert wizard.suggest_batch_size(50000) == 1000

    # Very large dataset
    assert wizard.suggest_batch_size(1000000) == 10000

