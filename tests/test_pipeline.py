import asyncio
import io
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime
from obspy.core.trace import Stats

from pipeline.aync_requests import DataPipeline
from pipeline.config import PipelineConfig


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
        print(stream)
        # Convert to miniSEED bytes
        mseed_buffer = io.BytesIO()
        stream.write(mseed_buffer, format="MSEED")
        return mseed_buffer.getvalue()

    @pytest.mark.asyncio
    async def test_make_async_request_mocked(self):
        """Test the _make_async_request method with mocked aiohttp response."""
        pipeline = DataPipeline(self.station_ips, self.config)

        # Mock response
        mock_response = AsyncMock()
        mock_response.read.return_value = AsyncMock(return_value=self.mock_mseed_data)
        mock_response.status = 200  # HTTP OK
        mock_response.raise_for_status = AsyncMock()

        # Make Mock aiohttp ClientSession.get to return the mock response
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)

        request_url = (
            "http://giveme.data/data?"
            + "channel=XX.TEST.00.HHZ&from=1704067200&to=1704067260"
        )
        outfile = Path("./test_data/TEST_async.mseed")
        semaphore = asyncio.Semaphore(3)

        await pipeline._make_async_request(
            request_url, outfile, mock_session, semaphore
        )
        # Test fucntions were called and file was written
        mock_session.get.assert_called_with(request_url)
        # mock_response.raise_for_status.assert_called()
        mock_response.read.assert_called_once()
        assert outfile.exists()
        assert outfile.read_bytes() == self.mock_mseed_data

        # Clean up
        outfile.unlink()

    # @pytest.mark.asyncio
    # async def test_make_async_request_http_errors(self):
    #     """Test handling of HTTP errors in _make_async_request."""
    #     pipeline = DataPipeline(self.station_ips, self.config)

    #     for err_code in [400, 404, 500]:
    #         # Mock response
    #         mock_response = AsyncMock()
    #         mock_response.raise_for_status = AsyncMock(
    #             side_effect=aiohttp.ClientResponseError(
    #                 status=err_code, request_info=MagicMock(), history=()
    #             )
    #         )

    #         mock_session = AsyncMock()
    #         mock_session.get = MagicMock(return_value=mock_response)
    #         request_url = (
    #             "http://giveme.data/data?"
    #             + "channel=XX.TEST.00.HHZ&from=1704067200&to=1704067260"
    #         )
    #         outfile = Path("./test_data/TEST_async_err.mseed")
    #         semaphore = asyncio.Semaphore(3)

    #         with pytest.raises(aiohttp.ClientResponseError) as excinfo:
    #             await pipeline._make_async_request(
    #                 request_url, outfile, mock_session, semaphore
    #             )
    #             assert excinfo.value.status == err_code
    #         # Ensure no file was created
    #         assert not outfile.exists()


#
