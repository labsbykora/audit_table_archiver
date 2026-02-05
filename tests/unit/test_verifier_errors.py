"""Unit tests for verifier error paths."""

import pytest

from archiver.exceptions import VerificationError
from archiver.verifier import Verifier


@pytest.fixture
def verifier() -> Verifier:
    """Create a Verifier instance."""
    return Verifier()


def test_verify_counts_mismatch(verifier: Verifier) -> None:
    """Test verify_counts with count mismatch."""
    with pytest.raises(VerificationError, match="Count mismatch"):
        verifier.verify_counts(db_count=100, memory_count=99, s3_count=100)


def test_verify_counts_all_match(verifier: Verifier) -> None:
    """Test verify_counts with all counts matching."""
    # Should not raise
    verifier.verify_counts(db_count=100, memory_count=100, s3_count=100)


def test_verify_counts_zero(verifier: Verifier) -> None:
    """Test verify_counts with zero counts."""
    # Should not raise
    verifier.verify_counts(db_count=0, memory_count=0, s3_count=0)


def test_verify_primary_keys_mismatch(verifier: Verifier) -> None:
    """Test verify_primary_keys with mismatch."""
    db_pks = [1, 2, 3, 4, 5]
    expected_pks = [1, 2, 3, 6, 7]  # Different

    with pytest.raises(VerificationError, match="Primary key mismatch"):
        verifier.verify_primary_keys(db_pks, expected_pks)


def test_verify_primary_keys_match(verifier: Verifier) -> None:
    """Test verify_primary_keys with matching keys."""
    db_pks = [1, 2, 3, 4, 5]
    expected_pks = [1, 2, 3, 4, 5]

    # Should not raise
    verifier.verify_primary_keys(db_pks, expected_pks)


def test_verify_primary_keys_empty(verifier: Verifier) -> None:
    """Test verify_primary_keys with empty lists."""
    # Should not raise
    verifier.verify_primary_keys([], [])


def test_verify_primary_keys_different_order(verifier: Verifier) -> None:
    """Test verify_primary_keys with different order (should still match)."""
    db_pks = [1, 2, 3, 4, 5]
    expected_pks = [5, 4, 3, 2, 1]  # Reversed

    # Should not raise (order doesn't matter, sets are compared)
    verifier.verify_primary_keys(db_pks, expected_pks)


def test_verify_primary_keys_duplicates(verifier: Verifier) -> None:
    """Test verify_primary_keys with duplicates."""
    db_pks = [1, 2, 2, 3]  # Duplicate 2
    expected_pks = [1, 2, 3]  # No duplicate

    # Duplicates are handled by converting to sets, so these should match
    # (both become {1, 2, 3})
    verifier.verify_primary_keys(db_pks, expected_pks)
