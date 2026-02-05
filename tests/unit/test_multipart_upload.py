"""Unit tests for multipart upload."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from archiver.multipart_upload import MultipartUploader, MultipartUploadState


class TestMultipartUploadState:
    """Tests for MultipartUploadState."""

    def test_to_dict(self):
        """Test state to dictionary conversion."""
        state = MultipartUploadState(
            upload_id="test-upload-id",
            key="test-key",
            file_path=Path("/tmp/test.txt"),
            part_size=1024 * 1024,
            total_parts=5,
            uploaded_parts=[{"PartNumber": 1, "ETag": "etag1"}],
        )
        data = state.to_dict()
        assert data["upload_id"] == "test-upload-id"
        assert data["key"] == "test-key"
        assert data["total_parts"] == 5
        assert len(data["uploaded_parts"]) == 1

    def test_from_dict(self):
        """Test state from dictionary creation."""
        data = {
            "upload_id": "test-upload-id",
            "key": "test-key",
            "file_path": "/tmp/test.txt",
            "part_size": 1024 * 1024,
            "total_parts": 5,
            "uploaded_parts": [{"PartNumber": 1, "ETag": "etag1"}],
        }
        state = MultipartUploadState.from_dict(data)
        assert state.upload_id == "test-upload-id"
        assert state.key == "test-key"
        assert state.total_parts == 5

    def test_save_and_load(self):
        """Test saving and loading state from file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = Path(tmpdir) / "state.json"
            state = MultipartUploadState(
                upload_id="test-upload-id",
                key="test-key",
                file_path=Path("/tmp/test.txt"),
                part_size=1024 * 1024,
                total_parts=5,
                uploaded_parts=[],
                state_file=state_file,
            )
            state.save()
            assert state_file.exists()

            loaded = MultipartUploadState.load(state_file)
            assert loaded is not None
            assert loaded.upload_id == "test-upload-id"

    def test_get_remaining_parts(self):
        """Test getting remaining parts."""
        state = MultipartUploadState(
            upload_id="test-upload-id",
            key="test-key",
            file_path=Path("/tmp/test.txt"),
            part_size=1024 * 1024,
            total_parts=5,
            uploaded_parts=[
                {"PartNumber": 1, "ETag": "etag1"},
                {"PartNumber": 3, "ETag": "etag3"},
            ],
        )
        remaining = state.get_remaining_parts()
        assert set(remaining) == {2, 4, 5}


class TestMultipartUploader:
    """Tests for MultipartUploader."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        client = MagicMock()
        client.config = MagicMock()
        client.config.bucket = "test-bucket"
        client.config.storage_class = "STANDARD_IA"
        client.config.encryption = "SSE-S3"
        client.config.endpoint = None
        return client

    @pytest.fixture
    def uploader(self, mock_s3_client):
        """Create MultipartUploader instance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            uploader = MultipartUploader(
                s3_client=mock_s3_client,
                state_dir=Path(tmpdir),
            )
            yield uploader

    def test_calculate_part_size_small_file(self, uploader):
        """Test part size calculation for small file."""
        part_size = uploader._calculate_part_size(5 * 1024 * 1024)  # 5MB
        assert part_size == uploader.DEFAULT_PART_SIZE

    def test_calculate_part_size_large_file(self, uploader):
        """Test part size calculation for very large file."""
        # File that would require >10,000 parts with default size
        file_size = 100 * 1024 * 1024 * 1024  # 100GB
        part_size = uploader._calculate_part_size(file_size)
        assert part_size >= uploader.MIN_PART_SIZE
        assert part_size <= uploader.MAX_PART_SIZE

    def test_initiate_upload(self, uploader, mock_s3_client):
        """Test initiating multipart upload."""
        mock_s3_client.client.create_multipart_upload.return_value = {"UploadId": "test-upload-id"}

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data")
            file_path = Path(f.name)

        try:
            state = uploader._initiate_upload("test-key", file_path, file_path.stat().st_size)
            assert state.upload_id == "test-upload-id"
            assert state.total_parts == 1
            assert state.state_file.exists()
        finally:
            file_path.unlink()

    def test_upload_part(self, uploader, mock_s3_client):
        """Test uploading a single part."""
        mock_s3_client.client.upload_part.return_value = {"ETag": "test-etag"}

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data" * 1000)  # ~9KB
            file_path = Path(f.name)

        try:
            state = MultipartUploadState(
                upload_id="test-upload-id",
                key="test-key",
                file_path=file_path,
                part_size=1024 * 1024,
                total_parts=1,
                uploaded_parts=[],
                state_file=Path(uploader.state_dir) / "test.json",
            )

            part_info = uploader._upload_part(state, 1)
            assert part_info["PartNumber"] == 1
            assert part_info["ETag"] == "test-etag"
            assert len(state.uploaded_parts) == 1
        finally:
            file_path.unlink()

    def test_complete_upload(self, uploader, mock_s3_client):
        """Test completing multipart upload."""
        mock_s3_client.client.complete_multipart_upload.return_value = {"ETag": "final-etag"}

        state = MultipartUploadState(
            upload_id="test-upload-id",
            key="test-key",
            file_path=Path("/tmp/test.txt"),
            part_size=1024 * 1024,
            total_parts=2,
            uploaded_parts=[
                {"PartNumber": 1, "ETag": "etag1"},
                {"PartNumber": 2, "ETag": "etag2"},
            ],
            state_file=Path(uploader.state_dir) / "test.json",
        )
        state.state_file.parent.mkdir(parents=True, exist_ok=True)
        state.save()

        response = uploader._complete_upload(state)
        assert response["ETag"] == "final-etag"
        assert not state.state_file.exists()  # Should be cleaned up

    def test_abort_upload(self, uploader, mock_s3_client):
        """Test aborting multipart upload."""
        state = MultipartUploadState(
            upload_id="test-upload-id",
            key="test-key",
            file_path=Path("/tmp/test.txt"),
            part_size=1024 * 1024,
            total_parts=1,
            uploaded_parts=[],
            state_file=Path(uploader.state_dir) / "test.json",
        )
        state.state_file.parent.mkdir(parents=True, exist_ok=True)
        state.save()

        uploader._abort_upload(state)
        mock_s3_client.client.abort_multipart_upload.assert_called_once()
        assert not state.state_file.exists()  # Should be cleaned up
