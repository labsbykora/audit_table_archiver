"""Unit tests for checksum module."""

import hashlib

import pytest

from utils.checksum import ChecksumCalculator


def test_calculate_sha256() -> None:
    """Test SHA-256 checksum calculation."""
    calculator = ChecksumCalculator()

    data = b"test data"
    checksum = calculator.calculate_sha256(data)

    # Verify it's a valid SHA-256 hex string (64 characters)
    assert len(checksum) == 64
    assert all(c in "0123456789abcdef" for c in checksum)

    # Verify it matches expected hash
    expected = hashlib.sha256(data).hexdigest()
    assert checksum == expected


def test_calculate_sha256_different_inputs() -> None:
    """Test that different inputs produce different checksums."""
    calculator = ChecksumCalculator()

    checksum1 = calculator.calculate_sha256(b"data1")
    checksum2 = calculator.calculate_sha256(b"data2")

    assert checksum1 != checksum2


def test_verify_checksum_match() -> None:
    """Test checksum verification with matching checksum."""
    calculator = ChecksumCalculator()

    data = b"test data"
    checksum = calculator.calculate_sha256(data)

    result = calculator.verify_checksum(data, checksum)
    assert result is True


def test_verify_checksum_mismatch() -> None:
    """Test checksum verification with mismatched checksum."""
    calculator = ChecksumCalculator()

    data = b"test data"
    wrong_checksum = "a" * 64  # Wrong checksum

    result = calculator.verify_checksum(data, wrong_checksum)
    assert result is False


def test_verify_checksum_case_insensitive() -> None:
    """Test checksum verification is case-insensitive."""
    calculator = ChecksumCalculator()

    data = b"test data"
    checksum = calculator.calculate_sha256(data)

    # Should work with uppercase
    result = calculator.verify_checksum(data, checksum.upper())
    assert result is True


def test_verify_checksum_or_raise_match() -> None:
    """Test verify_checksum_or_raise with matching checksum."""
    calculator = ChecksumCalculator()

    data = b"test data"
    checksum = calculator.calculate_sha256(data)

    # Should not raise
    calculator.verify_checksum_or_raise(data, checksum)


def test_verify_checksum_or_raise_mismatch() -> None:
    """Test verify_checksum_or_raise with mismatched checksum."""
    calculator = ChecksumCalculator()

    data = b"test data"
    wrong_checksum = "a" * 64  # Wrong checksum

    with pytest.raises(ValueError, match="Checksum mismatch"):
        calculator.verify_checksum_or_raise(data, wrong_checksum)


def test_verify_checksum_empty_data() -> None:
    """Test checksum verification with empty data."""
    calculator = ChecksumCalculator()

    data = b""
    checksum = calculator.calculate_sha256(data)

    result = calculator.verify_checksum(data, checksum)
    assert result is True
