"""Unit tests for verifier module."""

import pytest

from archiver.exceptions import VerificationError
from archiver.verifier import Verifier


def test_verify_counts_match() -> None:
    """Test count verification when all counts match."""
    verifier = Verifier()
    verifier.verify_counts(db_count=100, memory_count=100, s3_count=100)


def test_verify_counts_db_memory_mismatch() -> None:
    """Test count verification fails when DB and memory counts don't match."""
    verifier = Verifier()
    with pytest.raises(VerificationError, match="DB count.*!= Memory count"):
        verifier.verify_counts(db_count=100, memory_count=99, s3_count=100)


def test_verify_counts_memory_s3_mismatch() -> None:
    """Test count verification fails when memory and S3 counts don't match."""
    verifier = Verifier()
    with pytest.raises(VerificationError, match="Memory count.*!= S3 count"):
        verifier.verify_counts(db_count=100, memory_count=100, s3_count=99)


def test_verify_counts_db_s3_mismatch() -> None:
    """Test count verification fails when DB and S3 counts don't match."""
    verifier = Verifier()
    # When DB and Memory match but S3 differs, it fails on Memory vs S3 check first
    with pytest.raises(VerificationError, match="Memory count.*!= S3 count"):
        verifier.verify_counts(db_count=100, memory_count=100, s3_count=99)


def test_verify_counts_with_context() -> None:
    """Test count verification with context."""
    verifier = Verifier()
    context = {"database": "test_db", "table": "test_table"}
    verifier.verify_counts(db_count=50, memory_count=50, s3_count=50, context=context)


def test_verify_primary_keys_match() -> None:
    """Test primary key verification when keys match."""
    verifier = Verifier()
    fetched_pks = [1, 2, 3, 4, 5]
    delete_pks = [1, 2, 3, 4, 5]

    verifier.verify_primary_keys(fetched_pks, delete_pks)


def test_verify_primary_keys_order_independent() -> None:
    """Test primary key verification is order-independent."""
    verifier = Verifier()
    fetched_pks = [1, 2, 3, 4, 5]
    delete_pks = [5, 4, 3, 2, 1]  # Different order

    verifier.verify_primary_keys(fetched_pks, delete_pks)


def test_verify_primary_keys_mismatch() -> None:
    """Test primary key verification fails when keys don't match."""
    verifier = Verifier()
    fetched_pks = [1, 2, 3, 4, 5]
    delete_pks = [1, 2, 3, 4]  # Missing one

    with pytest.raises(VerificationError, match="Primary key mismatch"):
        verifier.verify_primary_keys(fetched_pks, delete_pks)


def test_verify_primary_keys_extra() -> None:
    """Test primary key verification fails when extra keys in delete."""
    verifier = Verifier()
    fetched_pks = [1, 2, 3]
    delete_pks = [1, 2, 3, 4, 5]  # Extra keys

    with pytest.raises(VerificationError, match="Primary key mismatch"):
        verifier.verify_primary_keys(fetched_pks, delete_pks)


def test_verify_primary_keys_empty() -> None:
    """Test primary key verification with empty lists."""
    verifier = Verifier()
    verifier.verify_primary_keys([], [])


def test_verify_primary_keys_with_context() -> None:
    """Test primary key verification with context."""
    verifier = Verifier()
    context = {"batch": 1}
    verifier.verify_primary_keys([1, 2, 3], [1, 2, 3], context=context)
