"""Unit tests for cost estimation module."""

import pytest
from datetime import datetime

from cost.cost_estimator import CostEstimator, CostEstimate, StorageClass


class TestCostEstimate:
    """Tests for CostEstimate dataclass."""

    def test_to_dict(self):
        """Test converting CostEstimate to dictionary."""
        estimate = CostEstimate(
            total_size_gb=100.0,
            compressed_size_gb=30.0,
            compression_ratio=3.33,
            monthly_storage_cost=0.375,
            monthly_retrieval_cost=0.015,
            monthly_total_cost=0.39,
            annual_storage_cost=4.5,
            annual_retrieval_cost=0.18,
            annual_total_cost=4.68,
            storage_class="STANDARD_IA",
            region="us-east-1",
        )

        result = estimate.to_dict()

        assert result["total_size_gb"] == 100.0
        assert result["compressed_size_gb"] == 30.0
        assert result["compression_ratio"] == 3.33
        assert result["storage_class"] == "STANDARD_IA"
        assert result["region"] == "us-east-1"

    def test_to_string(self):
        """Test converting CostEstimate to string."""
        estimate = CostEstimate(
            total_size_gb=100.0,
            compressed_size_gb=30.0,
            compression_ratio=3.33,
            monthly_storage_cost=0.375,
            monthly_retrieval_cost=0.015,
            monthly_total_cost=0.39,
            annual_storage_cost=4.5,
            annual_retrieval_cost=0.18,
            annual_total_cost=4.68,
            storage_class="STANDARD_IA",
            region="us-east-1",
        )

        result = estimate.to_string()

        assert "S3 Storage Cost Estimate" in result
        assert "STANDARD_IA" in result
        assert "us-east-1" in result
        assert "100.00" in result
        assert "30.00" in result


class TestCostEstimator:
    """Tests for CostEstimator class."""

    def test_init_default(self):
        """Test CostEstimator initialization with defaults."""
        estimator = CostEstimator()

        assert estimator.default_compression_ratio == 0.3
        assert estimator.default_retrieval_percentage == 0.05

    def test_init_custom(self):
        """Test CostEstimator initialization with custom values."""
        estimator = CostEstimator(
            default_compression_ratio=0.2,
            default_retrieval_percentage=0.1,
        )

        assert estimator.default_compression_ratio == 0.2
        assert estimator.default_retrieval_percentage == 0.1

    def test_estimate_cost_standard_ia(self):
        """Test cost estimation for STANDARD_IA storage class."""
        estimator = CostEstimator()
        estimate = estimator.estimate_cost(
            uncompressed_size_gb=100.0,
            storage_class=StorageClass.STANDARD_IA,
            region="us-east-1",
        )

        assert estimate.total_size_gb == 100.0
        assert estimate.compressed_size_gb == 30.0  # 100 * 0.3
        assert estimate.storage_class == "STANDARD_IA"
        assert estimate.region == "us-east-1"
        assert estimate.monthly_storage_cost > 0
        assert estimate.annual_total_cost > 0

    def test_estimate_cost_glacier(self):
        """Test cost estimation for GLACIER storage class."""
        estimator = CostEstimator()
        estimate = estimator.estimate_cost(
            uncompressed_size_gb=100.0,
            storage_class=StorageClass.GLACIER,
            region="us-east-1",
        )

        assert estimate.storage_class == "GLACIER"
        # Glacier should be cheaper than STANDARD_IA
        assert estimate.monthly_storage_cost < 0.375

    def test_estimate_cost_deep_archive(self):
        """Test cost estimation for DEEP_ARCHIVE storage class."""
        estimator = CostEstimator()
        estimate = estimator.estimate_cost(
            uncompressed_size_gb=100.0,
            storage_class=StorageClass.DEEP_ARCHIVE,
            region="us-east-1",
        )

        assert estimate.storage_class == "DEEP_ARCHIVE"
        # Deep Archive should be cheapest
        assert estimate.monthly_storage_cost < 0.1

    def test_estimate_cost_custom_compression(self):
        """Test cost estimation with custom compression ratio."""
        estimator = CostEstimator()
        estimate = estimator.estimate_cost(
            uncompressed_size_gb=100.0,
            storage_class=StorageClass.STANDARD_IA,
            compression_ratio=0.2,  # 80% compression
        )

        assert estimate.compressed_size_gb == 20.0  # 100 * 0.2
        assert estimate.compression_ratio == 5.0  # 1 / 0.2

    def test_estimate_cost_custom_retrieval(self):
        """Test cost estimation with custom retrieval percentage."""
        estimator = CostEstimator()
        estimate = estimator.estimate_cost(
            uncompressed_size_gb=100.0,
            storage_class=StorageClass.STANDARD_IA,
            retrieval_percentage=0.1,  # 10% retrieved
        )

        assert estimate.monthly_retrieval_cost > 0

    def test_estimate_cost_region_multiplier(self):
        """Test cost estimation with region multiplier."""
        estimator = CostEstimator()
        estimate_us = estimator.estimate_cost(
            uncompressed_size_gb=100.0,
            storage_class=StorageClass.STANDARD_IA,
            region="us-east-1",
        )
        estimate_eu = estimator.estimate_cost(
            uncompressed_size_gb=100.0,
            storage_class=StorageClass.STANDARD_IA,
            region="eu-west-1",
        )

        # EU should be slightly more expensive
        assert estimate_eu.monthly_storage_cost >= estimate_us.monthly_storage_cost

    def test_estimate_from_records(self):
        """Test cost estimation from record count."""
        estimator = CostEstimator()
        estimate = estimator.estimate_from_records(
            record_count=1000000,
            avg_record_size_bytes=1024,  # 1 KB per record
            storage_class=StorageClass.STANDARD_IA,
        )

        # 1M records * 1KB = 1GB
        assert estimate.total_size_gb == pytest.approx(1.0, rel=0.1)
        assert estimate.storage_class == "STANDARD_IA"

    def test_compare_storage_classes(self):
        """Test comparing costs across storage classes."""
        estimator = CostEstimator()
        comparisons = estimator.compare_storage_classes(
            uncompressed_size_gb=100.0,
            region="us-east-1",
        )

        assert "STANDARD" in comparisons
        assert "STANDARD_IA" in comparisons
        assert "GLACIER" in comparisons
        assert "DEEP_ARCHIVE" in comparisons

        # Verify costs are ordered (DEEP_ARCHIVE should be cheapest)
        deep_archive_cost = comparisons["DEEP_ARCHIVE"].annual_total_cost
        standard_cost = comparisons["STANDARD"].annual_total_cost
        assert deep_archive_cost < standard_cost

