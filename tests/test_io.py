import datetime
import json
from pathlib import Path

import pytest
import yaml
from obspy import UTCDateTime

from pipeline.io import (
    _create_pipeline_config,
    _create_request_params,
    _load_config_file,
    _load_station_ips,
)


def test_load_station_ips_from_dict():
    station_ip_config = {
        "station_ips": {
            "STA1": "192.168.1.1",
            "STA2": "192.168.0.2",
        }
    }
    station_ips = _load_station_ips(station_ip_config)
    assert station_ips == station_ip_config["station_ips"]


def test_load_station_ips_from_file(tmp_path):
    station_ip_data = {
        "STA1": "192.168.1.1",
        "STA2": "192.168.0.2",
    }
    ip_file = tmp_path / "station_ips.json"
    with open(ip_file, "w") as f:
        json.dump(station_ip_data, f)
    station_ip_config = {"station_ips_file": str(ip_file)}
    station_ips = _load_station_ips(station_ip_config)
    assert station_ips == station_ip_data


def test_load_station_ips_no_input():
    station_ip_config = {}
    with pytest.raises(ValueError) as excinfo:
        _load_station_ips(station_ip_config)
    assert "No station IPs provided" in str(excinfo.value)


def test_load_config_file():
    """Test reading of the YML config file"""
    test_file = "example_scripts/dummy_config.yml"
    with open(test_file, "r") as f:
        expected_config = yaml.safe_load(f)
    config = _load_config_file(test_file)
    assert config == expected_config


def test_load_config_file_not_found():
    """Test error when config file not found"""
    test_file = "non_existent_config.yml"
    with pytest.raises(FileNotFoundError) as excinfo:
        _ = _load_config_file(test_file)
    assert "Config file" in str(excinfo.value)


def test_load_config_file_invalid_yaml(tmp_path):
    """Test error when config file is invalid YAML"""
    test_file = tmp_path / "invalid_config.yml"
    with open(test_file, "w") as f:
        f.write("This is not valid YAML: [unclosed_list\n")
    with pytest.raises(ValueError) as excinfo:
        _ = _load_config_file(test_file)
    assert "Error parsing YAML config file" in str(excinfo.value)


def test_load_config_file_empty(tmp_path):
    """Test error when config file is empty"""
    test_file = tmp_path / "empty_config.yml"
    with open(test_file, "w") as f:
        f.write("")
    with pytest.raises(ValueError) as excinfo:
        _ = _load_config_file(test_file)
    assert "Config file is empty." in str(excinfo.value)


def test_create_pipeline_config():
    pipeline_config_yml = {
        "data_dir": "/path/to/data",
        "chunksize_hours": 2,
        "buffer_seconds": 300,
    }
    pipeline_config = _create_pipeline_config(pipeline_config_yml)
    assert pipeline_config.data_dir == "/path/to/data"
    assert pipeline_config.chunksize_hours == datetime.timedelta(hours=2)
    assert pipeline_config.buffer_seconds == datetime.timedelta(seconds=300)


def test_create_pipeline_config_defaults():
    pipeline_config_yml = {}
    pipeline_config = _create_pipeline_config(pipeline_config_yml)
    assert pipeline_config.data_dir == pytest.approx(Path.cwd())
    assert pipeline_config.chunksize_hours == datetime.timedelta(
        hours=1
    )  # default 1 hour
    assert pipeline_config.buffer_seconds == datetime.timedelta(
        seconds=150
    )  # default 150 seconds


def test_create_request_params_from_file(tmp_path):
    bulk_requests = [
        (
            "XX",
            "STA1",
            "00",
            "BHZ",
            UTCDateTime(2024, 1, 1),
            UTCDateTime(2024, 1, 2),
        ),
        (
            "YY",
            "STA2",
            "10",
            "EHN",
            UTCDateTime(2024, 2, 1),
            UTCDateTime(2024, 2, 2),
        ),
    ]
    request_file = tmp_path / "requests.pkl"
    with open(request_file, "wb") as f:
        import pickle

        pickle.dump(bulk_requests, f)

    output_request_params = _create_request_params(
        {"request_param_file": str(request_file)}
    )
    assert len(output_request_params) == len(bulk_requests)
    assert output_request_params.requests_to_make[0] == bulk_requests[0]
    assert output_request_params.requests_to_make[1] == bulk_requests[1]


def test_create_request_params_from_yml_seed_params():
    seed_params = {
        "networks": ["XX", "XX"],
        "stations": ["STA1", "STA6"],
        "locations": ["00"],
        "channels": ["HHZ"],
        "start": UTCDateTime(2024, 1, 1),
        "end": UTCDateTime(2024, 1, 2),
    }
    output_request_params = _create_request_params(seed_params)
    assert len(output_request_params) == 4
    assert output_request_params.requests_to_make[0] == (
        "XX",
        "STA1",
        "00",
        "HHZ",
        UTCDateTime(2024, 1, 1),
        UTCDateTime(2024, 1, 2),
    )
    assert output_request_params.requests_to_make[1] == (
        "YY",
        "STA6",
        "10",
        "EHN",
        UTCDateTime(2024, 1, 1),
        UTCDateTime(2024, 1, 2),
    )


def test_create_request_params_missing_fields():
    seed_params = [
        {
            "network": "XX",
            "station": "STA1",
            # Missing location
            "channel": "BHZ",
            "starttime": datetime.datetime(2024, 1, 1),
            "endtime": datetime.datetime(2024, 1, 2),
        }
    ]
    with pytest.raises(ValueError):
        _ = _create_request_params({"seed_params": seed_params})


def test_create_request_params_no_file():
    with pytest.raises(FileNotFoundError) as excinfo:
        _ = _create_request_params({"request_param_file": "non_existent_file.pkl"})
    assert "Request parameters file non_existent_file.pkl does not exist." in str(
        excinfo.value
    )
