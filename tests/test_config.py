from datetime import timedelta
from pathlib import Path

import obspy
import pytest

from pipeline.config import PipelineConfig, RequestParams


class TestRequestParams:
    def test_from_user_inputs_all_params(self):
        user_inputs = {
            "networks": ["XX", "YY"],
            "stations": ["STA1", "STA2"],
            "locations": ["00", "10"],
            "channels": ["BHZ", "EHN"],
            "start": [obspy.UTCDateTime("2024-01-01T00:00:00")],
            "end": [obspy.UTCDateTime("2024-01-02T00:00:00")],
        }
        config = RequestParams.from_user_inputs(**user_inputs)

        assert config.networks == ["XX", "YY"]
        assert config.stations == ["STA1", "STA2"]
        assert config.locations == ["00", "10"]
        assert config.channels == ["BHZ", "EHN"]
        assert config.start == [obspy.UTCDateTime("2024-01-01T00:00:00")]
        assert config.end == [obspy.UTCDateTime("2024-01-02T00:00:00")]
        assert hasattr(config, "all_request_params")

    def test_from_user_inputs_missing_params(self):
        user_inputs = {
            "networks": ["XX"],
            "stations": ["STA1"],
            # Missing locations
            "channels": ["BHZ"],
            "start": [obspy.UTCDateTime("2024-01-01T00:00:00")],
            "end": [obspy.UTCDateTime("2024-01-02T00:00:00")],
        }
        try:
            RequestParams.from_user_inputs(**user_inputs)
        except ValueError as e:
            assert "Missing required parameters: location(s)" in str(e)
        else:
            assert False, "ValueError not raised for missing parameters"

    def test_from_user_inputs_single_param_names(self):
        user_inputs = {
            "network": ["XX"],
            "station": ["STA1"],
            "location": ["00"],
            "channel": ["BHZ"],
            "start": [obspy.UTCDateTime("2024-01-01T00:00:00")],
            "end": [obspy.UTCDateTime("2024-01-02T00:00:00")],
        }
        config = RequestParams.from_user_inputs(**user_inputs)

        assert config.networks == ["XX"]
        assert config.stations == ["STA1"]
        assert config.locations == ["00"]
        assert config.channels == ["BHZ"]
        assert config.start == [obspy.UTCDateTime("2024-01-01T00:00:00")]
        assert config.end == [obspy.UTCDateTime("2024-01-02T00:00:00")]
        assert hasattr(config, "all_request_params")


class TestPipelineConfig:
    def test_pipeline_config_initialization(self):
        data_dir = Path("/path/to/data")
        config = PipelineConfig(data_dir=data_dir)

        assert config.data_dir == data_dir
        assert config.chunksize_hours == timedelta(hours=1)
        assert config.buffer_seconds == timedelta(hours=1)

    def test_pipeline_catches_too_many_async_requests(self):
        with pytest.raises(ValueError) as excinfo:
            PipelineConfig(data_dir=Path("/path/to/data"), n_async_requests=20)
        assert "Max number of async requests supported by the sensors is 3" in str(
            excinfo.value
        )
