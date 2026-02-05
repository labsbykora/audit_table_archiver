"""S3 storage cost estimation."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Optional

import structlog

from utils.logging import get_logger


class StorageClass(StrEnum):
    """S3 storage class options."""

    STANDARD = "STANDARD"
    STANDARD_IA = "STANDARD_IA"  # Infrequent Access
    ONEZONE_IA = "ONEZONE_IA"
    INTELLIGENT_TIERING = "INTELLIGENT_TIERING"
    GLACIER = "GLACIER"
    DEEP_ARCHIVE = "DEEP_ARCHIVE"


@dataclass
class CostEstimate:
    """S3 storage cost estimate."""

    total_size_gb: float
    compressed_size_gb: float
    compression_ratio: float
    monthly_storage_cost: float
    monthly_retrieval_cost: float
    monthly_total_cost: float
    annual_storage_cost: float
    annual_retrieval_cost: float
    annual_total_cost: float
    storage_class: str
    region: str

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "total_size_gb": round(self.total_size_gb, 2),
            "compressed_size_gb": round(self.compressed_size_gb, 2),
            "compression_ratio": round(self.compression_ratio, 2),
            "monthly_storage_cost": round(self.monthly_storage_cost, 2),
            "monthly_retrieval_cost": round(self.monthly_retrieval_cost, 2),
            "monthly_total_cost": round(self.monthly_total_cost, 2),
            "annual_storage_cost": round(self.annual_storage_cost, 2),
            "annual_retrieval_cost": round(self.annual_retrieval_cost, 2),
            "annual_total_cost": round(self.annual_total_cost, 2),
            "storage_class": self.storage_class,
            "region": self.region,
        }

    def to_string(self) -> str:
        """Generate human-readable report."""
        lines = ["S3 Storage Cost Estimate", "=" * 70]
        lines.append(f"\nStorage Class: {self.storage_class}")
        lines.append(f"Region: {self.region}")
        lines.append("\nData Sizes:")
        lines.append(f"  Uncompressed: {self.total_size_gb:,.2f} GB")
        lines.append(f"  Compressed: {self.compressed_size_gb:,.2f} GB")
        lines.append(f"  Compression Ratio: {self.compression_ratio:.2f}x")
        lines.append("\nMonthly Costs:")
        lines.append(f"  Storage: ${self.monthly_storage_cost:,.2f}")
        lines.append(f"  Retrieval: ${self.monthly_retrieval_cost:,.2f}")
        lines.append(f"  Total: ${self.monthly_total_cost:,.2f}")
        lines.append("\nAnnual Costs:")
        lines.append(f"  Storage: ${self.annual_storage_cost:,.2f}")
        lines.append(f"  Retrieval: ${self.annual_retrieval_cost:,.2f}")
        lines.append(f"  Total: ${self.annual_total_cost:,.2f}")
        return "\n".join(lines)


class CostEstimator:
    """Estimates S3 storage costs based on data size and storage class."""

    # S3 pricing per GB per month (as of 2024, approximate)
    # These are base prices; actual prices vary by region
    STORAGE_PRICES = {
        StorageClass.STANDARD: 0.023,  # $0.023 per GB/month
        StorageClass.STANDARD_IA: 0.0125,  # $0.0125 per GB/month
        StorageClass.ONEZONE_IA: 0.01,  # $0.01 per GB/month
        StorageClass.INTELLIGENT_TIERING: 0.023,  # Same as Standard, with monitoring fee
        StorageClass.GLACIER: 0.0036,  # $0.0036 per GB/month
        StorageClass.DEEP_ARCHIVE: 0.00099,  # $0.00099 per GB/month
    }

    # Retrieval costs per GB (varies by storage class)
    RETRIEVAL_PRICES = {
        StorageClass.STANDARD: 0.0,  # No retrieval cost
        StorageClass.STANDARD_IA: 0.01,  # $0.01 per GB retrieved
        StorageClass.ONEZONE_IA: 0.01,  # $0.01 per GB retrieved
        StorageClass.INTELLIGENT_TIERING: 0.0,  # No retrieval cost for frequent access
        StorageClass.GLACIER: 0.02,  # $0.02 per GB retrieved (expedited)
        StorageClass.DEEP_ARCHIVE: 0.02,  # $0.02 per GB retrieved (expedited)
    }

    # Region multipliers (approximate, based on common regions)
    REGION_MULTIPLIERS = {
        "us-east-1": 1.0,  # Base (cheapest)
        "us-east-2": 1.0,
        "us-west-1": 1.05,
        "us-west-2": 1.0,
        "eu-west-1": 1.05,
        "eu-central-1": 1.05,
        "ap-southeast-1": 1.1,
        "ap-southeast-2": 1.1,
        "ap-northeast-1": 1.15,
    }

    def __init__(
        self,
        default_compression_ratio: float = 0.3,
        default_retrieval_percentage: float = 0.05,  # 5% of data retrieved per month
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize cost estimator.

        Args:
            default_compression_ratio: Default compression ratio (0.3 = 70% compression)
            default_retrieval_percentage: Default percentage of data retrieved per month
            logger: Optional logger instance
        """
        self.default_compression_ratio = default_compression_ratio
        self.default_retrieval_percentage = default_retrieval_percentage
        self.logger = logger or get_logger("cost_estimator")

    def estimate_cost(
        self,
        uncompressed_size_gb: float,
        storage_class: StorageClass = StorageClass.STANDARD_IA,
        region: str = "us-east-1",
        compression_ratio: Optional[float] = None,
        retrieval_percentage: Optional[float] = None,
    ) -> CostEstimate:
        """Estimate S3 storage costs.

        Args:
            uncompressed_size_gb: Uncompressed data size in GB
            storage_class: S3 storage class
            region: AWS region
            compression_ratio: Compression ratio (if None, uses default)
            retrieval_percentage: Percentage of data retrieved per month (if None, uses default)

        Returns:
            CostEstimate object
        """
        compression_ratio = compression_ratio or self.default_compression_ratio
        retrieval_percentage = retrieval_percentage or self.default_retrieval_percentage

        # Calculate compressed size
        compressed_size_gb = uncompressed_size_gb * compression_ratio

        # Get base prices
        base_storage_price = self.STORAGE_PRICES.get(storage_class, 0.023)
        base_retrieval_price = self.RETRIEVAL_PRICES.get(storage_class, 0.0)

        # Apply region multiplier
        region_multiplier = self.REGION_MULTIPLIERS.get(region.lower(), 1.0)
        storage_price = base_storage_price * region_multiplier
        retrieval_price = base_retrieval_price * region_multiplier

        # Calculate monthly costs
        monthly_storage_cost = compressed_size_gb * storage_price
        monthly_retrieval_cost = compressed_size_gb * retrieval_percentage * retrieval_price
        monthly_total_cost = monthly_storage_cost + monthly_retrieval_cost

        # Calculate annual costs
        annual_storage_cost = monthly_storage_cost * 12
        annual_retrieval_cost = monthly_retrieval_cost * 12
        annual_total_cost = monthly_total_cost * 12

        self.logger.debug(
            "Cost estimate calculated",
            uncompressed_gb=uncompressed_size_gb,
            compressed_gb=compressed_size_gb,
            storage_class=storage_class.value,
            region=region,
            monthly_cost=monthly_total_cost,
        )

        return CostEstimate(
            total_size_gb=uncompressed_size_gb,
            compressed_size_gb=compressed_size_gb,
            compression_ratio=1.0 / compression_ratio if compression_ratio > 0 else 0,
            monthly_storage_cost=monthly_storage_cost,
            monthly_retrieval_cost=monthly_retrieval_cost,
            monthly_total_cost=monthly_total_cost,
            annual_storage_cost=annual_storage_cost,
            annual_retrieval_cost=annual_retrieval_cost,
            annual_total_cost=annual_total_cost,
            storage_class=storage_class.value,
            region=region,
        )

    def estimate_from_records(
        self,
        record_count: int,
        avg_record_size_bytes: float,
        storage_class: StorageClass = StorageClass.STANDARD_IA,
        region: str = "us-east-1",
        compression_ratio: Optional[float] = None,
        retrieval_percentage: Optional[float] = None,
    ) -> CostEstimate:
        """Estimate costs from record count and average size.

        Args:
            record_count: Number of records
            avg_record_size_bytes: Average record size in bytes
            storage_class: S3 storage class
            region: AWS region
            compression_ratio: Compression ratio (if None, uses default)
            retrieval_percentage: Percentage of data retrieved per month (if None, uses default)

        Returns:
            CostEstimate object
        """
        # Calculate uncompressed size
        total_bytes = record_count * avg_record_size_bytes
        uncompressed_size_gb = total_bytes / (1024**3)

        return self.estimate_cost(
            uncompressed_size_gb=uncompressed_size_gb,
            storage_class=storage_class,
            region=region,
            compression_ratio=compression_ratio,
            retrieval_percentage=retrieval_percentage,
        )

    def compare_storage_classes(
        self,
        uncompressed_size_gb: float,
        region: str = "us-east-1",
        compression_ratio: Optional[float] = None,
        retrieval_percentage: Optional[float] = None,
    ) -> dict[str, CostEstimate]:
        """Compare costs across different storage classes.

        Args:
            uncompressed_size_gb: Uncompressed data size in GB
            region: AWS region
            compression_ratio: Compression ratio (if None, uses default)
            retrieval_percentage: Percentage of data retrieved per month (if None, uses default)

        Returns:
            Dictionary mapping storage class names to CostEstimate objects
        """
        comparisons = {}
        for storage_class in StorageClass:
            estimate = self.estimate_cost(
                uncompressed_size_gb=uncompressed_size_gb,
                storage_class=storage_class,
                region=region,
                compression_ratio=compression_ratio,
                retrieval_percentage=retrieval_percentage,
            )
            comparisons[storage_class.value] = estimate

        return comparisons
