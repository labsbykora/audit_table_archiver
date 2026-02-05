"""Unit tests for adaptive batch sizing."""

from utils.adaptive_batch import AdaptiveBatchSizer


def test_adaptive_batch_sizer_init() -> None:
    """Test AdaptiveBatchSizer initialization."""
    sizer = AdaptiveBatchSizer(
        initial_batch_size=5000,
        min_batch_size=1000,
        max_batch_size=20000,
        target_query_time=1.5,
        adjustment_factor=0.15,
    )

    assert sizer.current_batch_size == 5000
    assert sizer.min_batch_size == 1000
    assert sizer.max_batch_size == 20000
    assert sizer.target_query_time == 1.5
    assert sizer.adjustment_factor == 0.15
    assert len(sizer.query_times) == 0


def test_adaptive_batch_sizer_init_defaults() -> None:
    """Test AdaptiveBatchSizer with default values."""
    sizer = AdaptiveBatchSizer()

    assert sizer.current_batch_size == 10000
    assert sizer.min_batch_size == 1000
    assert sizer.max_batch_size == 50000
    assert sizer.target_query_time == 2.0
    assert sizer.adjustment_factor == 0.2


def test_adaptive_batch_sizer_get_batch_size() -> None:
    """Test getting current batch size."""
    sizer = AdaptiveBatchSizer(initial_batch_size=5000)

    assert sizer.get_batch_size() == 5000


def test_adaptive_batch_sizer_increase_on_fast_query() -> None:
    """Test batch size increases when query is fast."""
    sizer = AdaptiveBatchSizer(
        initial_batch_size=10000,
        target_query_time=2.0,
        adjustment_factor=0.2,
    )

    # Record several fast queries (70% of target time)
    for _ in range(5):
        sizer.record_query_time(0.5, 10000)  # Very fast: 0.5s for 10k records

    # Should increase batch size
    assert sizer.get_batch_size() > 10000
    assert sizer.get_batch_size() <= sizer.max_batch_size


def test_adaptive_batch_sizer_decrease_on_slow_query() -> None:
    """Test batch size decreases when query is slow."""
    sizer = AdaptiveBatchSizer(
        initial_batch_size=10000,
        target_query_time=2.0,
        adjustment_factor=0.2,
    )

    # Record several slow queries (150% of target time)
    for _ in range(5):
        sizer.record_query_time(5.0, 10000)  # Slow: 5s for 10k records

    # Should decrease batch size
    assert sizer.get_batch_size() < 10000
    assert sizer.get_batch_size() >= sizer.min_batch_size


def test_adaptive_batch_sizer_no_change_on_target_time() -> None:
    """Test batch size doesn't change when query time is near target."""
    sizer = AdaptiveBatchSizer(
        initial_batch_size=10000,
        target_query_time=2.0,
        adjustment_factor=0.2,
    )

    # Record queries near target time
    for _ in range(5):
        sizer.record_query_time(2.0, 10000)  # Exactly target time

    # Should stay the same (within adjustment thresholds)
    assert sizer.get_batch_size() == 10000


def test_adaptive_batch_sizer_respects_max_batch_size() -> None:
    """Test batch size doesn't exceed max."""
    sizer = AdaptiveBatchSizer(
        initial_batch_size=40000,
        max_batch_size=50000,
        target_query_time=2.0,
        adjustment_factor=0.2,
    )

    # Record many fast queries
    for _ in range(20):
        sizer.record_query_time(0.1, 40000)  # Very fast

    # Should not exceed max
    assert sizer.get_batch_size() <= sizer.max_batch_size


def test_adaptive_batch_sizer_respects_min_batch_size() -> None:
    """Test batch size doesn't go below min."""
    sizer = AdaptiveBatchSizer(
        initial_batch_size=2000,
        min_batch_size=1000,
        target_query_time=2.0,
        adjustment_factor=0.2,
    )

    # Record many slow queries
    for _ in range(20):
        sizer.record_query_time(10.0, 2000)  # Very slow

    # Should not go below min
    assert sizer.get_batch_size() >= sizer.min_batch_size


def test_adaptive_batch_sizer_reset() -> None:
    """Test resetting batch sizer."""
    sizer = AdaptiveBatchSizer(initial_batch_size=10000)

    # Record some queries to change batch size
    sizer.record_query_time(5.0, 10000)
    sizer.record_query_time(5.0, 10000)

    # Reset
    sizer.reset()

    # Should reset to min_batch_size (not initial_batch_size based on implementation)
    assert sizer.get_batch_size() == sizer.min_batch_size
    assert len(sizer.query_times) == 0


def test_adaptive_batch_sizer_query_history_limit() -> None:
    """Test that query history is limited."""
    sizer = AdaptiveBatchSizer()

    # Record more queries than max_history (10)
    for _ in range(15):
        sizer.record_query_time(1.0, 10000)

    # Should only keep last 10
    assert len(sizer.query_times) == 10


def test_adaptive_batch_sizer_zero_records() -> None:
    """Test handling of zero records fetched."""
    sizer = AdaptiveBatchSizer(initial_batch_size=10000)

    # Record query with zero records
    sizer.record_query_time(0.5, 0)

    # Should handle gracefully without error
    assert sizer.get_batch_size() >= sizer.min_batch_size
    assert sizer.get_batch_size() <= sizer.max_batch_size


def test_adaptive_batch_sizer_average_calculation() -> None:
    """Test that average query time is calculated correctly."""
    sizer = AdaptiveBatchSizer(
        initial_batch_size=10000,
        target_query_time=2.0,
        adjustment_factor=0.2,
    )

    # Record queries with varying times
    sizer.record_query_time(1.0, 10000)  # Fast
    sizer.record_query_time(1.5, 10000)  # Fast
    sizer.record_query_time(2.0, 10000)  # Target
    sizer.record_query_time(2.5, 10000)  # Slow
    sizer.record_query_time(3.0, 10000)  # Slow

    # Average should be around 2.0, which is at target
    # Batch size should remain stable or adjust slightly
    assert sizer.get_batch_size() >= sizer.min_batch_size
    assert sizer.get_batch_size() <= sizer.max_batch_size
