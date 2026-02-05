"""Retention policy enforcement for compliance."""

from typing import Optional

import structlog

from archiver.config import ComplianceConfig, TableConfig
from archiver.exceptions import ConfigurationError
from utils.logging import get_logger


class RetentionPolicyEnforcer:
    """Enforces retention policy compliance."""

    def __init__(
        self,
        compliance_config: Optional[ComplianceConfig] = None,
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize retention policy enforcer.

        Args:
            compliance_config: Compliance configuration
            logger: Optional logger instance
        """
        self.compliance_config = compliance_config
        self.logger = logger or get_logger("retention_policy")

    def validate_retention(
        self,
        table_config: TableConfig,
        classification: Optional[str] = None,
    ) -> None:
        """Validate retention period against compliance rules.

        Args:
            table_config: Table configuration
            classification: Optional data classification (PII, INTERNAL, etc.)

        Raises:
            ConfigurationError: If retention policy is violated
        """
        if not self.compliance_config:
            # No compliance config, skip validation
            return

        retention_days = table_config.retention_days
        if retention_days is None:
            raise ConfigurationError(
                "Retention days must be specified for compliance validation",
                context={"table": table_config.name},
            )

        # Check minimum retention
        min_retention = self.compliance_config.min_retention_days
        if retention_days < min_retention:
            raise ConfigurationError(
                f"Retention period ({retention_days} days) is below minimum "
                f"({min_retention} days) required by compliance policy",
                context={
                    "table": table_config.name,
                    "retention_days": retention_days,
                    "min_retention_days": min_retention,
                },
            )

        # Check maximum retention
        max_retention = self.compliance_config.max_retention_days
        if retention_days > max_retention:
            raise ConfigurationError(
                f"Retention period ({retention_days} days) exceeds maximum "
                f"({max_retention} days) allowed by compliance policy",
                context={
                    "table": table_config.name,
                    "retention_days": retention_days,
                    "max_retention_days": max_retention,
                },
            )

        # Check classification-specific retention if provided
        if classification and self.compliance_config.data_classifications:
            classification_retention = self.compliance_config.data_classifications.get(
                classification
            )
            if classification_retention is not None:
                if retention_days != classification_retention:
                    self.logger.warning(
                        "Table retention does not match classification requirement",
                        table=table_config.name,
                        classification=classification,
                        table_retention=retention_days,
                        required_retention=classification_retention,
                    )

        self.logger.debug(
            "Retention policy validated",
            table=table_config.name,
            retention_days=retention_days,
            min_retention=min_retention,
            max_retention=max_retention,
        )
