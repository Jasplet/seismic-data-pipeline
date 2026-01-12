from pathlib import Path

import pytest_asyncio

from pipeline.aync_requests import DataPipeline
from pipeline.config import PipelineConfig


class TestDataPipeline:
    station_ips = {"TEST": "62.58.106.115"}
    config = PipelineConfig(data_dir=Path("/path/to/data"))

    def test_pipeline_initialization(self):
        pipeline = DataPipeline(self.station_ips, self.config)
        assert pipeline.station_ips == self.station_ips
        assert pipeline.config == self.config


@pytest_asyncio.fixture
def test_make_async_request():
    pass  # To be implemented
