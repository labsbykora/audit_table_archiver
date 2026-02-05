"""Unit tests for conflict resolver module."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from archiver.database import DatabaseManager
from restore.conflict_resolver import ConflictDetector, ConflictReport, ConflictResolver


class TestConflictReport:
    """Tests for ConflictReport class."""

    def test_init(self):
        """Test ConflictReport initialization."""
        report = ConflictReport(
            conflicts=[],
            total_conflicts=0,
            conflict_types={},
        )

        assert report.total_conflicts == 0
        assert report.has_conflicts is False
        assert len(report.conflicts) == 0

    def test_add_conflict(self):
        """Test adding a conflict."""
        conflicts = []
        conflict_types = {}
        conflicts.append({"pk": 123, "type": "primary_key_violation"})
        conflict_types["primary_key_violation"] = 1

        report = ConflictReport(
            conflicts=conflicts,
            total_conflicts=1,
            conflict_types=conflict_types,
        )

        assert report.total_conflicts == 1
        assert report.has_conflicts is True
        assert report.conflict_types["primary_key_violation"] == 1

    def test_to_string(self):
        """Test generating conflict report string."""
        conflicts = [
            {"pk": 123, "type": "primary_key_exists", "description": "PK 123 exists"},
            {"pk": 456, "type": "primary_key_exists", "description": "PK 456 exists"},
        ]
        conflict_types = {"primary_key_exists": 2}

        report = ConflictReport(
            conflicts=conflicts,
            total_conflicts=2,
            conflict_types=conflict_types,
        )

        report_str = report.to_string()

        assert "Conflict Report" in report_str
        assert "2 conflict(s)" in report_str
        assert "primary_key_exists: 2" in report_str


class TestConflictDetector:
    """Tests for ConflictDetector class."""

    @pytest.fixture
    def detector(self):
        """Create ConflictDetector fixture."""
        return ConflictDetector()

    @pytest.fixture
    def primary_key(self):
        """Create primary key fixture."""
        return "id"  # String, not dict

    @pytest.fixture
    def records(self):
        """Create records fixture."""
        return [
            {"id": 1, "data": "test1"},
            {"id": 2, "data": "test2"},
            {"id": 3, "data": "test3"},
        ]

    @pytest.mark.asyncio
    async def test_detect_conflicts_no_primary_key(self, detector):
        """Test conflict detection with no primary key."""
        db_manager = MagicMock(spec=DatabaseManager)

        report = await detector.detect_conflicts(
            records=[{"id": 1}],
            primary_key="",  # Empty string instead of dict
            schema="public",
            table="test_table",
            db_manager=db_manager,
        )

        assert report.has_conflicts is False
        assert db_manager.fetch.call_count == 0

    @pytest.mark.asyncio
    async def test_detect_conflicts_no_conflicts(self, detector, primary_key, records):
        """Test conflict detection when no conflicts exist."""
        db_manager = MagicMock(spec=DatabaseManager)
        db_manager.fetch = AsyncMock(return_value=[])  # No existing records

        report = await detector.detect_conflicts(
            records=records,
            primary_key=primary_key,
            schema="public",
            table="test_table",
            db_manager=db_manager,
        )

        assert report.has_conflicts is False
        assert report.total_conflicts == 0

    @pytest.mark.asyncio
    async def test_detect_conflicts_with_conflicts(self, detector, primary_key, records):
        """Test conflict detection when conflicts exist."""
        db_manager = MagicMock(spec=DatabaseManager)

        # Return existing records with IDs 1 and 2
        # The fetch returns rows that can be indexed by primary_key
        # The code does: existing_pk_set = {row[primary_key] for row in existing_pks if primary_key in row}
        # So we need rows that support both __getitem__ and __contains__
        class MockRow(dict):
            def __init__(self, pk_value):
                super().__init__()
                self[primary_key] = pk_value  # Store as int to match record values

        mock_row1 = MockRow(1)
        mock_row2 = MockRow(2)
        db_manager.fetch = AsyncMock(return_value=[mock_row1, mock_row2])

        report = await detector.detect_conflicts(
            records=records,
            primary_key=primary_key,
            schema="public",
            table="test_table",
            db_manager=db_manager,
        )

        assert report.has_conflicts is True
        assert report.total_conflicts == 2
        # Check conflicts list contains the conflicting PKs
        conflict_pk_values = [
            c.get("primary_key_value") for c in report.conflicts if "primary_key_value" in c
        ]
        assert 1 in conflict_pk_values
        assert 2 in conflict_pk_values

    @pytest.mark.asyncio
    async def test_detect_conflicts_empty_records(self, detector, primary_key):
        """Test conflict detection with empty records."""
        db_manager = MagicMock(spec=DatabaseManager)

        report = await detector.detect_conflicts(
            records=[],
            primary_key=primary_key,
            schema="public",
            table="test_table",
            db_manager=db_manager,
        )

        assert report.has_conflicts is False
        assert report.total_conflicts == 0


class TestConflictResolver:
    """Tests for ConflictResolver class."""

    @pytest.fixture
    def resolver_skip(self):
        """Create ConflictResolver with skip strategy."""
        return ConflictResolver(strategy="skip")

    @pytest.fixture
    def resolver_fail(self):
        """Create ConflictResolver with fail strategy."""
        return ConflictResolver(strategy="fail")

    def test_validate_strategy_no_conflicts(self, resolver_fail):
        """Test strategy validation with no conflicts."""
        report = ConflictReport(conflicts=[], total_conflicts=0, conflict_types={})

        # Should not raise
        resolver_fail.validate_strategy(report)

    def test_validate_strategy_fail_with_conflicts(self, resolver_fail):
        """Test fail strategy validation with conflicts."""
        from archiver.exceptions import ArchiverError

        report = ConflictReport(
            conflicts=[{"pk": 123, "type": "primary_key_exists"}],
            total_conflicts=1,
            conflict_types={"primary_key_exists": 1},
        )

        with pytest.raises(ArchiverError):
            resolver_fail.validate_strategy(report)

    def test_filter_conflicting_records_skip(self, resolver_skip):
        """Test filtering conflicting records with skip strategy."""
        report = ConflictReport(
            conflicts=[
                {"primary_key_value": 1, "type": "primary_key_exists"},
                {"primary_key_value": 2, "type": "primary_key_exists"},
            ],
            total_conflicts=2,
            conflict_types={"primary_key_exists": 2},
        )

        records = [
            {"id": 1, "data": "test1"},
            {"id": 2, "data": "test2"},
            {"id": 3, "data": "test3"},
        ]

        primary_key = "id"  # String, not dict

        filtered = resolver_skip.filter_conflicting_records(records, report, primary_key)

        assert len(filtered) == 1
        assert filtered[0]["id"] == 3

    def test_filter_conflicting_records_no_conflicts(self, resolver_skip):
        """Test filtering when no conflicts exist."""
        report = ConflictReport(conflicts=[], total_conflicts=0, conflict_types={})

        records = [
            {"id": 1, "data": "test1"},
            {"id": 2, "data": "test2"},
        ]

        primary_key = "id"  # String, not dict

        filtered = resolver_skip.filter_conflicting_records(records, report, primary_key)

        assert len(filtered) == 2
        assert filtered == records

    def test_filter_conflicting_records_missing_pk_column(self, resolver_skip):
        """Test filtering when PK column is missing from records."""
        report = ConflictReport(
            conflicts=[{"primary_key_value": 1, "type": "primary_key_exists"}],
            total_conflicts=1,
            conflict_types={"primary_key_exists": 1},
        )

        records = [
            {"data": "test1"},  # Missing id
            {"id": 2, "data": "test2"},
        ]

        primary_key = "id"  # String, not dict

        filtered = resolver_skip.filter_conflicting_records(records, report, primary_key)

        # Record without id should be kept
        assert len(filtered) == 2
