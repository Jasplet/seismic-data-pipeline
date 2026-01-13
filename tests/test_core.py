import asyncio
import io
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime
from obspy.core.trace import Stats

from pipeline.config import PipelineConfig
from pipeline.core import DataPipeline


class TestDataPipeline:
    station_ips = {"TEST": "62.58.106.115"}
    config = PipelineConfig(data_dir=Path("/path/to/data"))

    def test_pipeline_initialization(self):
        pipeline = DataPipeline(self.station_ips, self.config)
        assert pipeline.station_ips == self.station_ips
        assert pipeline.config == self.config

    @pytest.fixture
    def mock_mseed_data(self):
        """Generate synthetic miniSEED data for testing."""
        stats = Stats()
        stats.network = "XX"
        stats.station = "TEST"
        stats.location = "00"
        stats.channel = "HHZ"
        stats.starttime = UTCDateTime("2026-01-01T00:00:00")
        stats.sampling_rate = 100.0
        stats.npts = 60 * 100  # 1 minute of data

        # Synthetic waveform
        t = np.arange(0, stats.npts) / stats.sampling_rate
        data = np.sin(2 * np.pi * 1.0 * t) + 0.1 * np.random.randn(stats.npts)

        trace = Trace(data=data, header=stats)
        stream = Stream(traces=[trace])
        # Convert to miniSEED bytes
        mseed_buffer = io.BytesIO()
        stream.write(mseed_buffer, format="MSEED")
        return mseed_buffer.getvalue()

    @pytest.mark.asyncio
    async def test_make_async_request_mocked(self, mock_mseed_data, tmp_path):
        """Test the _make_async_request method with mocked aiohttp response."""
        pipeline = DataPipeline(self.station_ips, self.config)

        # Mock response
        mock_response = AsyncMock()
        mock_response.read = AsyncMock(return_value=mock_mseed_data)
        mock_response.status = 200  # HTTP OK
        mock_response.raise_for_status = MagicMock()

        # Make session.get return an async context manager yielding mock_response
        mock_session = MagicMock()
        cm = AsyncMock()  # context manager object
        cm.__aenter__.return_value = mock_response
        cm.__aexit__.return_value = False
        mock_session.get.return_value = cm

        request_url = (
            "http://giveme.data/data?"
            + "channel=XX.TEST.00.HHZ&from=1704067200&to=1704067260"
        )
        outfile = tmp_path / "TEST_async.mseed"
        semaphore = asyncio.Semaphore(3)

        await pipeline._make_async_request(
            request_url, outfile, mock_session, semaphore
        )
        # Test fucntions were called and file was written
        mock_session.get.assert_called_with(request_url)
        mock_response.raise_for_status.assert_called()
        mock_response.read.assert_called_once()
        assert outfile.exists()
        assert outfile.read_bytes() == mock_mseed_data

        # Clean up
        outfile.unlink()

    @pytest.mark.asyncio
    async def test_make_async_request_empty_response(self, tmp_path, caplog):
        """Test handling of empty response in _make_async_request."""
        pipeline = DataPipeline(self.station_ips, self.config)

        # Mock response
        mock_response = AsyncMock()
        mock_response.read = AsyncMock(return_value=b"")  # Empty response
        mock_response.status = 200  # HTTP OK
        mock_response.raise_for_status = MagicMock()

        # Make session.get return an async context manager yielding mock_response
        mock_session = MagicMock()
        cm = AsyncMock()  # context manager object
        cm.__aenter__.return_value = mock_response
        cm.__aexit__.return_value = False
        mock_session.get.return_value = cm

        request_url = (
            "http://giveme.data/data?"
            + "channel=XX.TEST.00.HHZ&from=1704067200&to=1704067260"
        )
        outfile = tmp_path / "TEST_async_empty.mseed"
        semaphore = asyncio.Semaphore(3)

        await pipeline._make_async_request(
            request_url, outfile, mock_session, semaphore
        )
        # Test fucntions were called and no file was written
        mock_session.get.assert_called_with(request_url)
        mock_response.raise_for_status.assert_called()
        mock_response.read.assert_called_once()
        assert "Request is empty!" in caplog.text
        assert not outfile.exists()

    @pytest.mark.asyncio
    async def test_make_async_request_http_errors(
        self, mock_mseed_data, tmp_path, caplog
    ):
        """Test handling of HTTP errors in _make_async_request."""
        pipeline = DataPipeline(self.station_ips, self.config)

        for err_code in [400, 404, 500]:
            # Mock response
            mock_response = AsyncMock()
            mock_response.read = AsyncMock(return_value=mock_mseed_data)
            mock_response.status = err_code  # HTTP error code
            mock_response.raise_for_status = MagicMock(
                side_effect=aiohttp.ClientResponseError(
                    request_info=MagicMock(),
                    history=(),
                    status=err_code,
                    message=f"Error: {err_code}",
                )
            )

            # Make session.get return an async context manager yielding mock_response
            mock_session = MagicMock()
            cm = AsyncMock()  # context manager object
            cm.__aenter__.return_value = mock_response
            cm.__aexit__.return_value = False
            mock_session.get.return_value = cm

            request_url = (
                "http://giveme.data/data?"
                + "channel=XX.TEST.00.HHZ&from=1704067200&to=1704067260"
            )
            outfile = tmp_path / f"TEST_async_err_{err_code}.mseed"
            semaphore = asyncio.Semaphore(3)

            await pipeline._make_async_request(
                request_url, outfile, mock_session, semaphore
            )
            # Test errror were logged and no file was written
            assert "Client error" in caplog.text
            mock_response.raise_for_status.assert_called()
            assert str(err_code) in caplog.text
            assert request_url in caplog.text
            assert mock_response.read.call_count == 0  # No data read on error

            # Ensure no file was created
            assert not outfile.exists()

    @pytest.mark.asyncio
    async def test_make_async_request_network_error(self, tmp_path, caplog):
        """Test handling of network errors (connection failures)."""
        pipeline = DataPipeline(self.station_ips, self.config)

        # Mock connection error
        mock_session = MagicMock()
        mock_session.get.side_effect = aiohttp.ClientConnectionError(
            "Connection failed"
        )

        request_url = "http://giveme.data/data?channel=XX.TEST.00.HHZ&from=1704067200&to=1704067260"
        outfile = tmp_path / "TEST_network.mseed"
        semaphore = asyncio.Semaphore(3)

        await pipeline._make_async_request(
            request_url, outfile, mock_session, semaphore
        )

        assert "Connection error" in caplog.text
        assert not outfile.exists()

    @pytest.mark.asyncio
    async def test_make_async_unexpected_errors(
        self, mock_mseed_data, tmp_path, caplog
    ):
        """Test handling of HTTP errors in _make_async_request."""
        pipeline = DataPipeline(self.station_ips, self.config)

        mock_response = AsyncMock()
        mock_response.read = AsyncMock()

        # Make session.get return an async context manager yielding mock_response
        mock_session = MagicMock()
        cm = AsyncMock()  # context manager object
        cm.__aenter__.return_value = mock_response
        cm.__aexit__.return_value = False
        mock_session.get.return_value = Exception("Unexpected error")

        request_url = (
            "http://giveme.data/data?"
            + "channel=XX.TEST.00.HHZ&from=1704067200&to=1704067260"
        )
        outfile = tmp_path / "TEST_async_unexpected_err.mseed"
        semaphore = asyncio.Semaphore(3)

        await pipeline._make_async_request(
            request_url, outfile, mock_session, semaphore
        )
        # Test errror were logged and no file was written
        assert "Unexpected error" in caplog.text
        # Ensure no file was created
        assert not outfile.exists()

    @pytest.mark.asyncio
    async def test_malformed_url_handling(self, tmp_path, caplog):
        """Test handling of malformed URLs."""
        pipeline = DataPipeline(self.station_ips, self.config)

        mock_session = MagicMock()
        mock_session.get.side_effect = ValueError("Invalid URL")

        request_url = "not-a-valid-url"
        outfile = tmp_path / "TEST_malformed.mseed"
        semaphore = asyncio.Semaphore(3)

        await pipeline._make_async_request(
            request_url, outfile, mock_session, semaphore
        )

        assert "Unexpected error" in caplog.text
        assert not outfile.exists()

    @pytest.mark.asyncio
    async def test_file_write_permission_error(self, mock_mseed_data, tmp_path, caplog):
        """Test handling of file write permission errors."""
        pipeline = DataPipeline(self.station_ips, self.config)

        mock_response = MagicMock()
        mock_response.read = AsyncMock(return_value=mock_mseed_data)
        mock_response.status = 200
        mock_response.raise_for_status = MagicMock()

        mock_session = MagicMock()
        cm = AsyncMock()
        cm.__aenter__.return_value = mock_response
        cm.__aexit__.return_value = False
        mock_session.get.return_value = cm

        # Create a read-only directory
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)  # Read-only

        request_url = "http://giveme.data/data?channel=XX.TEST.00.HHZ&from=1704067200&to=1704067260"
        outfile = readonly_dir / "TEST.mseed"
        semaphore = asyncio.Semaphore(3)

        await pipeline._make_async_request(
            request_url, outfile, mock_session, semaphore
        )

        assert "Unexpected error" in caplog.text or "Permission" in caplog.text

        # Cleanup
        readonly_dir.chmod(0o755)
