"""Unit tests for configuration wizard module."""

from unittest.mock import AsyncMock, patch

import pytest

from archiver.config import ArchiverConfig
from archiver.exceptions import DatabaseError
from wizard.config_wizard import ConfigWizard


class TestConfigWizard:
    """Tests for ConfigWizard class."""

    @pytest.fixture
    def wizard(self):
        """Create ConfigWizard fixture."""
        return ConfigWizard()

    @pytest.mark.asyncio
    async def test_detect_tables_success(self, wizard):
        """Test successful table detection."""
        # Create a proper async context manager mock
        mock_conn = AsyncMock()

        # Mock fetch result - asyncpg Record objects support dict-like access
        class MockRecord:
            def __init__(self, data):
                self._data = data
            def __getitem__(self, key):
                return self._data[key]

        mock_row = MockRecord({
            "table_name": "audit_logs",
            "timestamp_columns": ["created_at", "updated_at"],
            "id_columns": ["id"],
        })
        mock_conn.fetch = AsyncMock(return_value=[mock_row])

        # Mock fetchrow for primary key query
        mock_pk_row = MockRecord({"column_name": "id"})
        mock_conn.fetchrow = AsyncMock(return_value=mock_pk_row)
        mock_conn.close = AsyncMock()

        with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect:
            mock_connect.return_value = mock_conn

            tables = await wizard.detect_tables(
                host="localhost",
                port=5432,
                database="testdb",
                user="testuser",
                password="testpass",
            )

            assert len(tables) == 1
            assert tables[0]["name"] == "audit_logs"
            assert tables[0]["suggested_timestamp"] == "created_at"
            assert tables[0]["primary_key"] == "id"

    @pytest.mark.asyncio
    async def test_detect_tables_database_error(self, wizard):
        """Test table detection with database error."""
        with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")

            with pytest.raises(DatabaseError):
                await wizard.detect_tables(
                    host="localhost",
                    port=5432,
                    database="testdb",
                    user="testuser",
                    password="testpass",
                )

    @pytest.mark.asyncio
    async def test_estimate_record_count(self, wizard):
        """Test record count estimation."""
        from datetime import datetime, timezone

        mock_conn = AsyncMock()
        # Setup fetchval to return different values for different queries
        call_count = 0
        async def fetchval_side_effect(query, *args):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return 10000  # total_count
            elif call_count == 2:
                return 5000  # eligible_count
            elif call_count == 3:
                return datetime(2025, 1, 1, tzinfo=timezone.utc)  # oldest
            elif call_count == 4:
                return datetime(2026, 1, 1, tzinfo=timezone.utc)  # newest
            elif call_count == 5:
                return 365.0  # age_days
            return None

        mock_conn.fetchval = AsyncMock(side_effect=fetchval_side_effect)
        mock_conn.close = AsyncMock()

        with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect:
            mock_connect.return_value = mock_conn

            estimates = await wizard.estimate_record_count(
                host="localhost",
                port=5432,
                database="testdb",
                user="testuser",
                password="testpass",
                schema="public",
                table="audit_logs",
                timestamp_column="created_at",
                retention_days=90,
            )

            assert estimates["total_records"] == 10000
            assert estimates["eligible_records"] == 5000
            assert estimates["age_days"] == 365

    def test_generate_config(self, wizard):
        """Test configuration generation."""
        databases = [
            {
                "name": "testdb",
                "host": "localhost",
                "port": 5432,
                "user": "testuser",
                "password_env": "DB_PASSWORD",
                "tables": [
                    {
                        "name": "audit_logs",
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
        assert config.databases[0].name == "testdb"
        assert len(config.databases[0].tables) == 1
        assert config.databases[0].tables[0].name == "audit_logs"
        assert config.s3.bucket == "test-bucket"

    def test_suggest_batch_size_small(self, wizard):
        """Test batch size suggestion for small datasets."""
        batch_size = wizard.suggest_batch_size(500)

        assert batch_size == 100

    def test_suggest_batch_size_medium(self, wizard):
        """Test batch size suggestion for medium datasets."""
        batch_size = wizard.suggest_batch_size(5000)

        assert batch_size == 500

    def test_suggest_batch_size_large(self, wizard):
        """Test batch size suggestion for large datasets."""
        batch_size = wizard.suggest_batch_size(50000)

        assert batch_size == 1000

    def test_suggest_batch_size_very_large(self, wizard):
        """Test batch size suggestion for very large datasets."""
        batch_size = wizard.suggest_batch_size(1000000)

        assert batch_size == 10000

