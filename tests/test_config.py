# Create a temporary pickle file with bulk requests
import pickle
import tempfile
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
            "start": obspy.UTCDateTime("2024-01-01T00:00:00"),
            "end": obspy.UTCDateTime("2024-01-02T00:00:00"),
        }
        config = RequestParams.from_user_inputs(**user_inputs)

        assert config.requests_to_make[0] == (
            "XX",
            "STA1",
            "00",
            "BHZ",
            obspy.UTCDateTime("2024-01-01T00:00:00"),
            obspy.UTCDateTime("2024-01-02T00:00:00"),
        )
        assert config.requests_to_make[-1] == (
            "YY",
            "STA2",
            "10",
            "EHN",
            obspy.UTCDateTime("2024-01-01T00:00:00"),
            obspy.UTCDateTime("2024-01-02T00:00:00"),
        )
        assert len(config) == 16  # 2 networks * 2 stations * 2 locations * 2 channels

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
            "start": obspy.UTCDateTime("2024-01-01T00:00:00"),
            "end": obspy.UTCDateTime("2024-01-02T00:00:00"),
        }
        config = RequestParams.from_user_inputs(**user_inputs)

        assert config.requests_to_make[0] == (
            "XX",
            "STA1",
            "00",
            "BHZ",
            obspy.UTCDateTime("2024-01-01T00:00:00"),
            obspy.UTCDateTime("2024-01-02T00:00:00"),
        )
        assert len(config) == 1

    def test_from_time_windows(self):
        params = {
            "networks": ["XX", "YY"],
            "stations": ["STA1"],
            "locations": ["00"],
            "channels": ["BHZ"],
            "time_windows": [
                (
                    obspy.UTCDateTime("2024-01-01T00:00:00"),
                    obspy.UTCDateTime("2024-01-01T12:00:00"),
                ),
                (
                    obspy.UTCDateTime("2024-03-02T00:00:00"),
                    obspy.UTCDateTime("2024-03-02T12:00:00"),
                ),
            ],
        }
        config = RequestParams.from_time_windows(**params)

        assert config.requests_to_make[0] == (
            "XX",
            "STA1",
            "00",
            "BHZ",
            obspy.UTCDateTime("2024-01-01T00:00:00"),
            obspy.UTCDateTime("2024-01-01T12:00:00"),
        )
        assert config.requests_to_make[-1] == (
            "YY",
            "STA1",
            "00",
            "BHZ",
            obspy.UTCDateTime("2024-03-02T00:00:00"),
            obspy.UTCDateTime("2024-03-02T12:00:00"),
        )
        assert len(config) == 4  # 2 networks * 1 station * 1 location * 1 channel

    def test_bulk_requests_empty(self):
        with pytest.raises(ValueError) as excinfo:
            config = RequestParams.from_bulk_inputs(bulk_requests=[])

    def test_bulk_requests_valid_tuple_list(self):
        bulk_requests = [
            (
                "XX",
                "STA1",
                "00",
                "BHZ",
                obspy.UTCDateTime("2024-01-01"),
                obspy.UTCDateTime("2024-01-02"),
            ),
            (
                "YY",
                "STA2",
                "10",
                "EHN",
                obspy.UTCDateTime("2024-02-01"),
                obspy.UTCDateTime("2024-02-02"),
            ),
        ]
        config = RequestParams.from_bulk_inputs(bulk_requests=bulk_requests)

        assert config.requests_to_make[0] == bulk_requests[0]
        assert config.requests_to_make[1] == bulk_requests[1]
        assert len(config) == 2

    def test_bulk_requests_valid_pickle(self):
        bulk_requests = [
            (
                "XX",
                "STA1",
                "00",
                "BHZ",
                obspy.UTCDateTime("2024-01-01"),
                obspy.UTCDateTime("2024-01-02"),
            ),
            (
                "YY",
                "STA2",
                "10",
                "EHN",
                obspy.UTCDateTime("2024-02-01"),
                obspy.UTCDateTime("2024-02-02"),
            ),
        ]

        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            pickle.dump(bulk_requests, tmpfile)
            tmpfile_path = tmpfile.name

        config = RequestParams.from_bulk_inputs(bulk_requests=tmpfile_path)

        assert config.requests_to_make[0] == bulk_requests[0]
        assert config.requests_to_make[1] == bulk_requests[1]
        assert len(config) == 2


class TestPipelineConfig:
    def test_pipeline_config_initialization(self):
        data_dir = Path("/path/to/data")
        config = PipelineConfig(data_dir=data_dir)

        assert config.data_dir == data_dir
        assert config.chunksize_hours == timedelta(hours=1)
        assert config.buffer_seconds == timedelta(seconds=150)

    def test_pipeline_catches_too_many_async_requests(self):
        with pytest.raises(ValueError) as excinfo:
            PipelineConfig(data_dir=Path("/path/to/data"), n_async_requests=20)
        assert "Max number of async requests supported by the sensors is 3" in str(
            excinfo.value
        )
