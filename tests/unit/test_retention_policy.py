"""Unit tests for retention policy enforcement."""

import pytest

from archiver.config import ComplianceConfig, TableConfig
from archiver.exceptions import ConfigurationError
from archiver.retention_policy import RetentionPolicyEnforcer


class TestRetentionPolicyEnforcer:
    """Tests for RetentionPolicyEnforcer class."""

    def test_init_no_config(self):
        """Test initialization without compliance config."""
        enforcer = RetentionPolicyEnforcer()
        assert enforcer.compliance_config is None

    def test_init_with_config(self):
        """Test initialization with compliance config."""
        compliance_config = ComplianceConfig(
            min_retention_days=7,
            max_retention_days=2555,
        )
        enforcer = RetentionPolicyEnforcer(compliance_config=compliance_config)
        assert enforcer.compliance_config == compliance_config

    def test_validate_retention_no_config(self):
        """Test that validation passes when no compliance config is set."""
        enforcer = RetentionPolicyEnforcer()
        table_config = TableConfig(
            name="test_table",
            timestamp_column="created_at",
            primary_key="id",
            retention_days=90,
        )
        
        # Should not raise exception
        enforcer.validate_retention(table_config)

    def test_validate_retention_valid_range(self):
        """Test validation with retention days within valid range."""
        compliance_config = ComplianceConfig(
            min_retention_days=7,
            max_retention_days=2555,
        )
        enforcer = RetentionPolicyEnforcer(compliance_config=compliance_config)
        
        table_config = TableConfig(
            name="test_table",
            timestamp_column="created_at",
            primary_key="id",
            retention_days=90,
        )
        
        # Should not raise exception
        enforcer.validate_retention(table_config)

    def test_validate_retention_below_minimum(self):
        """Test validation fails when retention days is below minimum."""
        compliance_config = ComplianceConfig(
            min_retention_days=7,
            max_retention_days=2555,
        )
        enforcer = RetentionPolicyEnforcer(compliance_config=compliance_config)
        
        table_config = TableConfig(
            name="test_table",
            timestamp_column="created_at",
            primary_key="id",
            retention_days=5,  # Below minimum
        )
        
        with pytest.raises(ConfigurationError) as exc_info:
            enforcer.validate_retention(table_config)
        
        assert "below minimum" in str(exc_info.value).lower()

    def test_validate_retention_above_maximum(self):
        """Test validation fails when retention days exceeds maximum."""
        compliance_config = ComplianceConfig(
            min_retention_days=7,
            max_retention_days=2555,
        )
        enforcer = RetentionPolicyEnforcer(compliance_config=compliance_config)
        
        table_config = TableConfig(
            name="test_table",
            timestamp_column="created_at",
            primary_key="id",
            retention_days=10000,  # Above maximum
        )
        
        with pytest.raises(ConfigurationError) as exc_info:
            enforcer.validate_retention(table_config)
        
        assert "exceeds maximum" in str(exc_info.value).lower()

    def test_validate_retention_missing_retention_days(self):
        """Test validation fails when retention_days is None."""
        compliance_config = ComplianceConfig(
            min_retention_days=7,
            max_retention_days=2555,
        )
        enforcer = RetentionPolicyEnforcer(compliance_config=compliance_config)
        
        table_config = TableConfig(
            name="test_table",
            timestamp_column="created_at",
            primary_key="id",
            retention_days=None,  # Not set
        )
        
        with pytest.raises(ConfigurationError) as exc_info:
            enforcer.validate_retention(table_config)
        
        assert "must be specified" in str(exc_info.value).lower()

    def test_validate_retention_with_classification_match(self):
        """Test validation with classification-specific retention that matches."""
        compliance_config = ComplianceConfig(
            min_retention_days=7,
            max_retention_days=2555,
            data_classifications={"PII": 2555},
        )
        enforcer = RetentionPolicyEnforcer(compliance_config=compliance_config)
        
        table_config = TableConfig(
            name="test_table",
            timestamp_column="created_at",
            primary_key="id",
            retention_days=2555,  # Matches PII requirement
        )
        
        # Should not raise exception (but may log warning if classification doesn't match)
        enforcer.validate_retention(table_config, classification="PII")

    def test_validate_retention_with_classification_mismatch(self):
        """Test validation with classification-specific retention that doesn't match."""
        compliance_config = ComplianceConfig(
            min_retention_days=7,
            max_retention_days=2555,
            data_classifications={"PII": 2555},
        )
        enforcer = RetentionPolicyEnforcer(compliance_config=compliance_config)
        
        table_config = TableConfig(
            name="test_table",
            timestamp_column="created_at",
            primary_key="id",
            retention_days=365,  # Doesn't match PII requirement (2555)
        )
        
        # Should not raise exception, but logs warning
        # The validation still passes because 365 is within min/max range
        enforcer.validate_retention(table_config, classification="PII")

    def test_validate_retention_at_minimum_boundary(self):
        """Test validation at minimum boundary."""
        compliance_config = ComplianceConfig(
            min_retention_days=7,
            max_retention_days=2555,
        )
        enforcer = RetentionPolicyEnforcer(compliance_config=compliance_config)
        
        table_config = TableConfig(
            name="test_table",
            timestamp_column="created_at",
            primary_key="id",
            retention_days=7,  # Exactly at minimum
        )
        
        # Should not raise exception
        enforcer.validate_retention(table_config)

    def test_validate_retention_at_maximum_boundary(self):
        """Test validation at maximum boundary."""
        compliance_config = ComplianceConfig(
            min_retention_days=7,
            max_retention_days=2555,
        )
        enforcer = RetentionPolicyEnforcer(compliance_config=compliance_config)
        
        table_config = TableConfig(
            name="test_table",
            timestamp_column="created_at",
            primary_key="id",
            retention_days=2555,  # Exactly at maximum
        )
        
        # Should not raise exception
        enforcer.validate_retention(table_config)

