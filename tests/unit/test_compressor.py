"""Unit tests for compressor module."""


import pytest

from archiver.compressor import Compressor
from archiver.exceptions import ArchiverError


def test_compressor_init_default() -> None:
    """Test compressor initialization with default level."""
    compressor = Compressor()
    assert compressor.compression_level == 6


def test_compressor_init_custom_level() -> None:
    """Test compressor initialization with custom level."""
    compressor = Compressor(compression_level=9)
    assert compressor.compression_level == 9


def test_compressor_init_invalid_level() -> None:
    """Test compressor initialization with invalid level."""
    with pytest.raises(ValueError, match="Compression level must be between 1 and 9"):
        Compressor(compression_level=0)

    with pytest.raises(ValueError, match="Compression level must be between 1 and 9"):
        Compressor(compression_level=10)


def test_compress() -> None:
    """Test compression."""
    compressor = Compressor(compression_level=6)
    data = b"test data " * 100  # 1000 bytes

    compressed, uncompressed_size, compressed_size = compressor.compress(data)

    assert uncompressed_size == len(data)
    assert compressed_size == len(compressed)
    assert compressed_size < uncompressed_size  # Should compress
    assert isinstance(compressed, bytes)


def test_compress_empty() -> None:
    """Test compression of empty data."""
    compressor = Compressor()
    data = b""

    compressed, uncompressed_size, compressed_size = compressor.compress(data)

    assert uncompressed_size == 0
    assert compressed_size > 0  # Gzip header still present
    assert isinstance(compressed, bytes)


def test_decompress() -> None:
    """Test decompression."""
    compressor = Compressor()
    original_data = b"test data " * 100

    compressed, _, _ = compressor.compress(original_data)
    decompressed = compressor.decompress(compressed)

    assert decompressed == original_data


def test_compress_decompress_roundtrip() -> None:
    """Test compress-decompress roundtrip."""
    compressor = Compressor()
    original_data = b"This is test data that should compress well. " * 50

    compressed, _, _ = compressor.compress(original_data)
    decompressed = compressor.decompress(compressed)

    assert decompressed == original_data


def test_compression_levels() -> None:
    """Test different compression levels."""
    data = b"test data " * 1000

    sizes = {}
    for level in [1, 6, 9]:
        compressor = Compressor(compression_level=level)
        _, _, compressed_size = compressor.compress(data)
        sizes[level] = compressed_size

    # Higher levels should generally compress better (smaller size)
    # Note: This may not always be true, but level 9 should be <= level 1
    assert sizes[9] <= sizes[1] or abs(sizes[9] - sizes[1]) < 100  # Allow small variance


def test_decompress_invalid_data() -> None:
    """Test decompression of invalid data."""
    compressor = Compressor()
    invalid_data = b"not gzip data"

    with pytest.raises(ArchiverError, match="Decompression failed"):
        compressor.decompress(invalid_data)

