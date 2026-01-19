import datetime
import json
from pathlib import Path

import pytest
import yaml

from pipeline.io import _create_pipeline_config, _load_config_file, _load_station_ips


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


def test_create_pipeline_config():
    pipeline_config_yml = {
        "data_dir": "/path/to/data",
        "chunksize_hours": 2,
        "buffer_seconds": 300,
    }
    pipeline_config = _create_pipeline_config(pipeline_config_yml)
    assert pipeline_config.data_dir == Path("/path/to/data")
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
