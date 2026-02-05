"""Integration tests for Phase 4: Cost estimation."""

import pytest

from cost.cost_estimator import CostEstimator, StorageClass


@pytest.mark.integration
def test_cost_estimation_integration():
    """Test cost estimation with various scenarios."""
    estimator = CostEstimator()

    # Test standard storage class
    estimate = estimator.estimate_cost(
        uncompressed_size_gb=100.0,
        storage_class=StorageClass.STANDARD,
        region="us-east-1",
    )

    assert estimate.total_size_gb == 100.0
    assert estimate.compressed_size_gb == 30.0  # 70% compression
    assert estimate.storage_class == "STANDARD"
    assert estimate.region == "us-east-1"
    assert estimate.monthly_storage_cost > 0
    assert estimate.annual_total_cost > 0


@pytest.mark.integration
def test_cost_estimation_storage_class_comparison():
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

    # Verify DEEP_ARCHIVE is cheapest
    deep_archive_cost = comparisons["DEEP_ARCHIVE"].annual_total_cost
    standard_cost = comparisons["STANDARD"].annual_total_cost
    assert deep_archive_cost < standard_cost


@pytest.mark.integration
def test_cost_estimation_from_records():
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


@pytest.mark.integration
def test_cost_estimation_custom_compression():
    """Test cost estimation with custom compression ratio."""
    estimator = CostEstimator()

    estimate = estimator.estimate_cost(
        uncompressed_size_gb=100.0,
        storage_class=StorageClass.STANDARD_IA,
        compression_ratio=0.2,  # 80% compression
    )

    assert estimate.compressed_size_gb == 20.0  # 100 * 0.2
    assert estimate.compression_ratio == 5.0  # 1 / 0.2


@pytest.mark.integration
def test_cost_estimation_region_variation():
    """Test cost estimation with different regions."""
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
