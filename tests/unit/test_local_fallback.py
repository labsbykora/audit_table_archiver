"""Unit tests for local disk fallback."""

import json
import shutil
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from archiver.local_fallback import LocalFallback


class TestLocalFallback:
    """Tests for LocalFallback."""

    @pytest.fixture
    def fallback_dir(self):
        """Create temporary fallback directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def fallback(self, fallback_dir):
        """Create LocalFallback instance."""
        return LocalFallback(
            fallback_dir=fallback_dir,
            retention_days=7,
        )

    def test_init(self, fallback_dir):
        """Test initialization."""
        fallback = LocalFallback(fallback_dir=fallback_dir)
        assert fallback.fallback_dir == fallback_dir
        assert fallback_dir.exists()

    def test_save_failed_upload(self, fallback, fallback_dir):
        """Test saving failed upload."""
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            f.write(b"test data")
            file_path = Path(f.name)

        try:
            fallback_path = fallback.save_failed_upload(
                file_path=file_path,
                s3_key="test/database/table/batch.jsonl.gz",
                metadata={"database": "test_db", "table": "test_table"},
            )

            assert fallback_path.exists()
            assert fallback_path.stat().st_size == file_path.stat().st_size

            # Check metadata file
            metadata_file = fallback_path.with_suffix(fallback_path.suffix + ".meta.json")
            assert metadata_file.exists()

            with open(metadata_file) as mf:
                metadata = json.load(mf)
                assert metadata["s3_key"] == "test/database/table/batch.jsonl.gz"
                assert metadata["metadata"]["database"] == "test_db"
        finally:
            file_path.unlink()

    def test_list_failed_uploads(self, fallback, fallback_dir):
        """Test listing failed uploads."""
        # Create a test file
        test_file = fallback_dir / "20240101_120000_test.jsonl.gz"
        test_file.write_bytes(b"test data")

        # Create metadata
        metadata_file = test_file.with_suffix(test_file.suffix + ".meta.json")
        with open(metadata_file, "w") as f:
            json.dump({
                "s3_key": "test/key",
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "file_size": 9,
            }, f)

        uploads = fallback.list_failed_uploads()
        assert len(uploads) == 1
        assert uploads[0]["s3_key"] == "test/key"

    def test_list_failed_uploads_filtered_by_age(self, fallback, fallback_dir):
        """Test listing uploads filtered by age."""
        # Create old file
        old_file = fallback_dir / "old_file.jsonl.gz"
        old_file.write_bytes(b"test data")
        old_file.touch()  # Update mtime to now

        # Create metadata with old timestamp
        metadata_file = old_file.with_suffix(old_file.suffix + ".meta.json")
        old_time = datetime.now(timezone.utc) - timedelta(days=10)
        with open(metadata_file, "w") as f:
            json.dump({
                "s3_key": "old/key",
                "saved_at": old_time.isoformat(),
                "file_size": 9,
            }, f)

        # Should not appear in list (older than 7 days default retention)
        uploads = fallback.list_failed_uploads()
        assert len(uploads) == 0

    def test_cleanup_old_uploads(self, fallback, fallback_dir):
        """Test cleaning up old uploads."""
        # Create old file
        old_file = fallback_dir / "old_file.jsonl.gz"
        old_file.write_bytes(b"test data")
        # Set mtime to 10 days ago
        old_time = (datetime.now(timezone.utc) - timedelta(days=10)).timestamp()
        old_file.touch()
        import os
        os.utime(old_file, (old_time, old_time))

        metadata_file = old_file.with_suffix(old_file.suffix + ".meta.json")
        metadata_file.write_bytes(b'{"s3_key": "old/key"}')

        stats = fallback.cleanup_old_uploads(dry_run=False)
        assert stats["deleted"] > 0
        assert not old_file.exists()
        assert not metadata_file.exists()

    def test_cleanup_old_uploads_dry_run(self, fallback, fallback_dir):
        """Test cleanup dry run."""
        # Create old file
        old_file = fallback_dir / "old_file.jsonl.gz"
        old_file.write_bytes(b"test data")
        old_time = (datetime.now(timezone.utc) - timedelta(days=10)).timestamp()
        old_file.touch()
        import os
        os.utime(old_file, (old_time, old_time))

        stats = fallback.cleanup_old_uploads(dry_run=True)
        assert stats["total_found"] > 0
        assert stats["deleted"] == 0  # Dry run doesn't delete
        assert old_file.exists()  # File still exists

    def test_get_resume_info(self, fallback, fallback_dir):
        """Test getting resume information."""
        # Create a test file
        test_file = fallback_dir / "test.jsonl.gz"
        test_file.write_bytes(b"test data" * 100)

        metadata_file = test_file.with_suffix(test_file.suffix + ".meta.json")
        with open(metadata_file, "w") as f:
            json.dump({
                "s3_key": "test/key",
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "file_size": 900,
            }, f)

        info = fallback.get_resume_info()
        assert info["total_failed"] == 1
        assert info["total_size"] == 900
        assert len(info["uploads"]) == 1
        assert info["uploads"][0]["s3_key"] == "test/key"

