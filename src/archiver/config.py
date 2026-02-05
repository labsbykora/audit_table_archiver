"""Configuration management using YAML and Pydantic."""

import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


def _substitute_env_vars(value: str) -> str:
    """Substitute environment variables in string.

    Supports ${VAR} and ${VAR:-default} syntax.

    Args:
        value: String potentially containing env var references

    Returns:
        String with environment variables substituted
    """
    # Pattern: ${VAR} or ${VAR:-default}
    pattern = r"\$\{([^}:]+)(?::-([^}]*))?\}"

    def replacer(match: re.Match[str]) -> str:
        var_name = match.group(1)
        default = match.group(2) if match.group(2) is not None else None
        env_value = os.getenv(var_name)
        if env_value is not None:
            return env_value
        if default is not None:
            return default
        raise ValueError(f"Environment variable {var_name} not set and no default provided")

    return re.sub(pattern, replacer, value)


def _substitute_env_in_dict(data: Any) -> Any:
    """Recursively substitute environment variables in dictionary.

    Args:
        data: Dictionary or nested structure

    Returns:
        Dictionary with environment variables substituted
    """
    if isinstance(data, dict):
        return {key: _substitute_env_in_dict(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_substitute_env_in_dict(item) for item in data]
    elif isinstance(data, str):
        return _substitute_env_vars(data)
    return data


class S3Config(BaseModel):
    """S3 configuration."""

    endpoint: Optional[str] = Field(
        default=None,
        description="S3 endpoint URL (null for AWS S3, or custom endpoint for S3-compatible)",
    )
    bucket: str = Field(description="S3 bucket name")
    prefix: str = Field(default="", description="S3 key prefix")
    region: str = Field(default="us-east-1", description="AWS region")
    storage_class: str = Field(
        default="STANDARD_IA",
        description="S3 storage class (STANDARD, STANDARD_IA, GLACIER_IR, etc.)",
    )
    encryption: str = Field(
        default="SSE-S3",
        description="Encryption method (SSE-S3, SSE-KMS, SSE-C, none). Use 'none' for S3-compatible storage like MinIO",
    )
    multipart_threshold_mb: int = Field(
        default=10,
        description="File size threshold (MB) for multipart upload",
        ge=1,
    )
    rate_limit_requests_per_second: Optional[float] = Field(
        default=None,
        description="Rate limit for S3 API calls (requests per second). If None, no rate limiting is applied.",
        gt=0,
    )
    local_fallback_dir: Optional[str] = Field(
        default=None,
        description="Directory for local disk fallback on S3 upload failures. If None, fallback is disabled.",
    )
    local_fallback_retention_days: int = Field(
        default=7,
        description="Number of days to retain failed uploads in local fallback directory",
        ge=1,
    )
    aws_access_key_id: Optional[str] = Field(
        default=None,
        alias="access_key_id",
        description="AWS access key ID (development only - use AWS_ACCESS_KEY_ID env var in production)",
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None,
        alias="secret_access_key",
        description="AWS secret access key (development only - use AWS_SECRET_ACCESS_KEY env var in production)",
    )

    model_config = {"populate_by_name": True}

    def get_credentials(self) -> Optional[dict[str, str]]:
        """Get AWS credentials from config file or environment variables.

        Returns:
            Dictionary with 'aws_access_key_id' and 'aws_secret_access_key', or None
            if credentials should be obtained from standard AWS locations (IAM role, etc.)

        Raises:
            ValueError: If credentials are partially specified
        """
        config_has_key = self.aws_access_key_id is not None
        config_has_secret = self.aws_secret_access_key is not None

        env_key = os.getenv("AWS_ACCESS_KEY_ID")
        env_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

        # If both are in config file, use them (with warning)
        if config_has_key and config_has_secret:
            import warnings

            warnings.warn(
                "Using AWS credentials from config file. "
                "This is not recommended for production. "
                "Use AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables instead.",
                UserWarning,
                stacklevel=2,
            )
            return {
                "aws_access_key_id": self.aws_access_key_id,
                "aws_secret_access_key": self.aws_secret_access_key,
            }

        # If one is in config but not the other, error
        if config_has_key or config_has_secret:
            raise ValueError(
                "Both aws_access_key_id and aws_secret_access_key must be provided together, "
                "or use environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)"
            )

        # If both are in environment variables, use them
        if env_key and env_secret:
            return {
                "aws_access_key_id": env_key,
                "aws_secret_access_key": env_secret,
            }

        # If neither, return None to let boto3 use default credential chain (IAM role, etc.)
        return None


class TableConfig(BaseModel):
    """Table configuration."""

    model_config = {"populate_by_name": True}

    name: str = Field(description="Table name")
    schema_name: str = Field(default="public", description="Schema name", alias="schema")
    timestamp_column: str = Field(description="Timestamp column name for age-based filtering")
    primary_key: str = Field(description="Primary key column name")
    retention_days: Optional[int] = Field(
        default=None,
        description="Retention period in days (overrides global default)",
        gt=0,
        lt=36500,  # 100 years max
    )
    batch_size: Optional[int] = Field(
        default=None,
        description="Batch size (overrides global default)",
        gt=0,
    )
    critical: bool = Field(
        default=False,
        description="Critical flag (enables additional safety checks)",
    )


class DatabaseConfig(BaseModel):
    """Database configuration."""

    name: str = Field(description="Database name")
    host: str = Field(description="Database host")
    port: int = Field(default=5432, description="Database port", gt=0, lt=65536)
    user: str = Field(description="Database user")
    password_env: Optional[str] = Field(
        default=None,
        description="Environment variable name containing database password (preferred)",
    )
    password: Optional[str] = Field(
        default=None,
        description="Database password (development only - use password_env in production)",
    )
    read_replica: Optional[str] = Field(
        default=None,
        description="Read replica host (optional)",
    )
    connection_pool_size: Optional[int] = Field(
        default=None,
        description="Connection pool size for this database (overrides global default)",
        gt=0,
        le=50,
    )
    tables: list[TableConfig] = Field(description="List of tables to archive", min_length=1)

    @model_validator(mode="after")
    def validate_password_source(self) -> "DatabaseConfig":
        """Validate that exactly one password source is provided."""
        if not self.password_env and not self.password:
            raise ValueError(
                "Either 'password_env' or 'password' must be provided. "
                "Use 'password_env' for production (recommended) or 'password' for development only."
            )
        if self.password_env and self.password:
            raise ValueError(
                "Cannot specify both 'password_env' and 'password'. "
                "Use 'password_env' for production (recommended) or 'password' for development only."
            )
        return self

    def get_password(self) -> str:
        """Get password from environment variable or config file.

        Returns:
            Database password

        Raises:
            ValueError: If password cannot be retrieved
        """
        if self.password_env:
            password = os.getenv(self.password_env)
            if not password:
                raise ValueError(f"Environment variable {self.password_env} not set")
            return password
        elif self.password:
            # Log warning when using password from config file
            import warnings

            warnings.warn(
                f"Using password from config file for database '{self.name}'. "
                f"This is not recommended for production. Use 'password_env' instead.",
                UserWarning,
                stacklevel=2,
            )
            return self.password
        else:
            raise ValueError("No password source configured")


class MonitoringConfig(BaseModel):
    """Monitoring and metrics configuration."""

    metrics_enabled: bool = Field(
        default=True,
        description="Enable Prometheus metrics",
    )
    metrics_port: int = Field(
        default=8000,
        description="Port for Prometheus metrics endpoint",
        gt=0,
        lt=65536,
    )
    progress_enabled: bool = Field(
        default=True,
        description="Enable real-time progress tracking",
    )
    progress_update_interval: float = Field(
        default=5.0,
        description="Progress update interval in seconds",
        gt=0,
    )
    quiet_mode: bool = Field(
        default=False,
        description="Quiet mode (suppress progress output for cron)",
    )
    health_check_enabled: bool = Field(
        default=True,
        description="Enable health check endpoint",
    )
    health_check_port: int = Field(
        default=8001,
        description="Port for health check endpoint",
        gt=0,
        lt=65536,
    )


class EmailConfig(BaseModel):
    """Email notification configuration."""

    enabled: bool = Field(default=False, description="Enable email notifications")
    smtp_host: str = Field(default="localhost", description="SMTP server hostname")
    smtp_port: int = Field(default=587, description="SMTP server port", gt=0, lt=65536)
    smtp_user: Optional[str] = Field(default=None, description="SMTP username")
    smtp_password_env: Optional[str] = Field(
        default=None,
        description="Environment variable name for SMTP password",
    )
    from_email: str = Field(
        default="archiver@example.com",
        description="Sender email address",
    )
    to_emails: list[str] = Field(
        default_factory=list,
        description="List of recipient email addresses",
    )
    use_tls: bool = Field(default=True, description="Use TLS encryption")


class SlackConfig(BaseModel):
    """Slack notification configuration."""

    enabled: bool = Field(default=False, description="Enable Slack notifications")
    webhook_url_env: str = Field(
        default="SLACK_WEBHOOK_URL",
        description="Environment variable name for Slack webhook URL",
    )
    channel: Optional[str] = Field(
        default=None,
        description="Slack channel to post to (optional, can be set in webhook)",
    )
    username: str = Field(
        default="Audit Archiver",
        description="Bot username",
    )


class TeamsConfig(BaseModel):
    """Microsoft Teams notification configuration."""

    enabled: bool = Field(default=False, description="Enable Teams notifications")
    webhook_url_env: str = Field(
        default="TEAMS_WEBHOOK_URL",
        description="Environment variable name for Teams webhook URL",
    )


class NotificationConfig(BaseModel):
    """Notification configuration."""

    enabled: bool = Field(
        default=False,
        description="Enable notifications",
    )
    email: EmailConfig = Field(
        default_factory=EmailConfig,
        description="Email notification configuration",
    )
    slack: SlackConfig = Field(
        default_factory=SlackConfig,
        description="Slack notification configuration",
    )
    teams: TeamsConfig = Field(
        default_factory=TeamsConfig,
        description="Teams notification configuration",
    )
    send_on_success: bool = Field(
        default=True,
        description="Send notification on successful archival",
    )
    send_on_failure: bool = Field(
        default=True,
        description="Send notification on archival failure",
    )
    send_on_start: bool = Field(
        default=False,
        description="Send notification when archival starts",
    )
    send_on_threshold_violation: bool = Field(
        default=True,
        description="Send notification on threshold violations",
    )
    digest_mode: bool = Field(
        default=False,
        description="Enable digest mode (daily summary instead of individual notifications)",
    )
    digest_hour: int = Field(
        default=9,
        description="Hour (UTC) to send daily digest",
        ge=0,
        le=23,
    )
    rate_limit_hours: float = Field(
        default=4.0,
        description="Minimum hours between notifications of the same type (alert fatigue prevention)",
        gt=0,
    )
    quiet_hours_start: Optional[int] = Field(
        default=None,
        description="Start hour (UTC) for quiet hours (no notifications)",
        ge=0,
        le=23,
    )
    quiet_hours_end: Optional[int] = Field(
        default=None,
        description="End hour (UTC) for quiet hours (no notifications)",
        ge=0,
        le=23,
    )


class DefaultsConfig(BaseModel):
    """Global default configuration."""

    retention_days: int = Field(default=90, description="Default retention period in days", gt=0)
    batch_size: int = Field(default=10000, description="Default batch size", gt=0)
    sleep_between_batches: int = Field(
        default=2,
        description="Sleep duration (seconds) between batches",
        ge=0,
    )
    vacuum_after: bool = Field(default=True, description="Run VACUUM after archival")
    vacuum_strategy: str = Field(
        default="standard",
        description="Vacuum strategy (none, analyze, standard, full)",
    )
    parallel_databases: bool = Field(
        default=False,
        description="Enable parallel database processing (default: sequential)",
    )
    max_parallel_databases: int = Field(
        default=3,
        description="Maximum number of databases to process in parallel (if parallel_databases=True)",
        gt=0,
        le=10,
    )
    connection_pool_size: int = Field(
        default=5,
        description="Default connection pool size per database",
        gt=0,
        le=50,
    )
    compression_level: int = Field(
        default=6,
        description="Gzip compression level (1=fastest, 9=best compression)",
        ge=1,
        le=9,
    )
    fail_on_schema_drift: bool = Field(
        default=False,
        description="Fail archival if schema drift is detected between batches",
    )
    lock_type: str = Field(
        default="postgresql",
        description="Distributed lock type (postgresql, redis, file)",
    )
    watermark_storage_type: str = Field(
        default="s3",
        description="Storage backend for watermarks (s3, database, both)",
    )
    checkpoint_storage_type: str = Field(
        default="s3",
        description="Storage backend for checkpoints (s3, local)",
    )
    checkpoint_interval: int = Field(
        default=10,
        description="Save checkpoint every N batches",
        ge=1,
    )
    audit_trail_storage_type: str = Field(
        default="s3",
        description="Storage backend for audit trail (s3, database)",
    )


class LegalHoldConfig(BaseModel):
    """Legal hold configuration."""

    enabled: bool = Field(
        default=True,
        description="Enable legal hold checking",
    )
    check_table: Optional[str] = Field(
        default=None,
        description="Database table containing legal holds (format: schema.table or table)",
    )
    check_database: Optional[str] = Field(
        default=None,
        description="Database name to check for legal holds (if different from target database)",
    )
    api_endpoint: Optional[str] = Field(
        default=None,
        description="API endpoint for legal hold checking (optional)",
    )
    api_timeout: int = Field(
        default=5,
        description="API request timeout in seconds",
        gt=0,
        le=60,
    )


class ComplianceConfig(BaseModel):
    """Compliance configuration."""

    min_retention_days: int = Field(
        default=7,
        description="Minimum retention period in days (prevents archiving too early)",
        gt=0,
    )
    max_retention_days: int = Field(
        default=2555,
        description="Maximum retention period in days (7 years default)",
        gt=0,
        lt=36500,  # 100 years max
    )
    enforce_encryption: bool = Field(
        default=False,
        description="Enforce encryption for sensitive tables",
    )
    data_classifications: Optional[dict[str, int]] = Field(
        default=None,
        description="Retention days by data classification (e.g., {'PII': 2555, 'INTERNAL': 365})",
    )


class RestoreWatermarkConfig(BaseModel):
    """Restore watermark configuration."""

    enabled: bool = Field(
        default=True,
        description="Enable restore watermark tracking (skip already-restored archives)",
    )
    storage_type: str = Field(
        default="s3",
        description="Storage type for restore watermarks ('s3', 'database', or 'both')",
    )
    update_after_each_archive: bool = Field(
        default=True,
        description="Update watermark after each archive is restored (vs only at end)",
    )

    @field_validator("storage_type")
    @classmethod
    def validate_storage_type(cls, v: str) -> str:
        """Validate storage type."""
        if v not in ("s3", "database", "both"):
            raise ValueError("storage_type must be 's3', 'database', or 'both'")
        return v


class ArchiverConfig(BaseModel):
    """Root configuration model."""

    version: str = Field(description="Configuration version")
    s3: S3Config = Field(description="S3 configuration")
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig, description="Global defaults")
    databases: list[DatabaseConfig] = Field(
        description="List of databases to archive",
        min_length=1,
    )
    legal_holds: Optional[LegalHoldConfig] = Field(
        default=None,
        description="Legal hold configuration",
    )
    compliance: Optional[ComplianceConfig] = Field(
        default=None,
        description="Compliance configuration",
    )
    monitoring: Optional[MonitoringConfig] = Field(
        default=None,
        description="Monitoring and metrics configuration",
    )
    notifications: Optional[NotificationConfig] = Field(
        default=None,
        description="Notification configuration",
    )
    restore_watermark: Optional[RestoreWatermarkConfig] = Field(
        default_factory=RestoreWatermarkConfig,
        description="Restore watermark configuration",
    )

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        """Validate configuration version."""
        if v not in ["1.0", "2.0"]:
            raise ValueError(f"Unsupported configuration version: {v}")
        return v

    @model_validator(mode="after")
    def apply_defaults(self) -> "ArchiverConfig":
        """Apply global defaults to tables that don't have overrides."""
        for db in self.databases:
            for table in db.tables:
                if table.retention_days is None:
                    table.retention_days = self.defaults.retention_days
                if table.batch_size is None:
                    table.batch_size = self.defaults.batch_size
        return self


def load_config(config_path: Path) -> ArchiverConfig:
    """Load and validate configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Validated configuration object

    Raises:
        ConfigurationError: If configuration is invalid
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)

        if not raw_config:
            raise ValueError("Configuration file is empty")

        # Substitute environment variables
        config_data = _substitute_env_in_dict(raw_config)

        # Parse and validate with Pydantic
        config = ArchiverConfig.model_validate(config_data)

        return config

    except FileNotFoundError:
        raise ValueError(f"Configuration file not found: {config_path}") from None
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in configuration file: {e}") from e
    except Exception as e:
        raise ValueError(f"Configuration validation failed: {e}") from e
