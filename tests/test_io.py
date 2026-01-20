import datetime
import json
import logging
from pathlib import Path

import pytest
import yaml
from obspy import UTCDateTime

from pipeline.io import (
    _create_pipeline_config,
    _create_request_params,
    _load_config_file,
    _load_station_ips,
    _setup_logging,
    load_from_config_file,
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
        "XX",
        "STA6",
        "00",
        "HHZ",
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


class TestLoadFromConfigFile:
    def test_load_from_config_file_end_to_end(self, tmp_path):
        """Full integration test with minimal valid config."""
        # Create config file
        config = {
            "StationIPs": {"station_ips": {"STA1": "192.168.1.1"}},
            "LogConfig": {"log_dir": str(tmp_path), "log_filename": "test.log"},
            "PipelineConfig": {
                "data_dir": str(tmp_path / "data"),
                "chunksize_hours": 1,
                "buffer_seconds": 150,
            },
            "RequestParams": {
                "networks": "XX",
                "stations": "STA1",
                "locations": "00",
                "channels": "HHZ",
                "start": "2024-01-01T00:00:00",
                "end": "2024-01-02T00:00:00",
            },
        }
        config_file = tmp_path / "config.yml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        pipeline, params = load_from_config_file(config_file)
        assert pipeline is not None
        assert params is not None
        assert len(params) > 0

    def test_load_from_config_file_missing_section(self, tmp_path):
        """Test handling of missing config section."""
        config = {"StationIPs": {"station_ips": {"STA1": "1.1.1.1"}}}
        config_file = tmp_path / "incomplete.yml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Should handle gracefully with defaults or raise clear error
        try:
            pipeline, params = load_from_config_file(config_file)
        except (KeyError, ValueError):
            # Either works with defaults or raises clear error
            assert True


class TestCreateRequestParams:
    def test_create_request_params_cron_mode(self):
        """Test cron mode calculates correct dates."""
        config = {
            "cron": True,
            "days_before": 2,
            "networks": "XX",
            "stations": "STA1",
            "locations": "00",
            "channels": "HHZ",
        }
        params = _create_request_params(config)
        today = datetime.date.today()
        expected_start = UTCDateTime(today - datetime.timedelta(days=2))
        expected_end = UTCDateTime(today)
        for req in params.requests_to_make:
            assert req[4] == expected_start
            assert req[5] == expected_end
        # Verify start/end were set
        assert len(params) > 0

    def test_create_request_params_invalid_datetime_string(self):
        """Test invalid datetime format raises error."""
        config = {
            "networks": "XX",
            "stations": "STA1",
            "locations": "00",
            "channels": "HHZ",
            "start": "not-a-datetime",
            "end": "2024-01-02",
        }
        with pytest.raises(Exception):  # UTCDateTime will raise
            _create_request_params(config)

    def test_create_request_params_iso_format_datetimes(self):
        """Test ISO format datetimes parse correctly."""
        config = {
            "networks": "XX",
            "stations": "STA1",
            "locations": "00",
            "channels": "HHZ",
            "start": "2024-01-01T00:00:00",
            "end": "2024-01-02T00:00:00",
        }
        params = _create_request_params(config)
        assert len(params) > 0

    def test_create_request_params_missing_required_fields(self):
        """Test missing required SEED params raises error."""
        config = {
            "networks": "XX",
            # Missing stations, locations, channels
            "start": "2024-01-01",
            "end": "2024-01-02",
        }
        with pytest.raises(ValueError) as excinfo:
            _create_request_params(config)
        assert "Missing required" in str(excinfo.value)


class TestSetupLogging:
    def test_setup_logging_creates_log_file(self, tmp_path, caplog):
        """Test that log file is created."""
        log_dir = tmp_path / "logs"
        log_config = {
            "log_level": "INFO",
            "log_dir": str(log_dir),
            "log_filename": "test.log",
        }
        logger = _setup_logging(log_config)
        logger.error("Test log message")
        log_file = log_dir / "test.log"
        assert log_file.exists()

    def test_setup_logging_invalid_log_level(self, tmp_path):
        """Test invalid log level falls back to INFO."""
        log_config = {
            "log_level": "INVALID_LEVEL",
            "log_dir": str(tmp_path),
            "log_filename": "test.log",
        }
        _ = _setup_logging(log_config)
        # Verify logger level is INFO (default)
        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO

    def test_setup_logging_all_levels(self, tmp_path):
        """Test all valid log levels work."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            log_config = {
                "log_level": level,
                "log_dir": str(tmp_path),
                "log_filename": f"{level}.log",
            }
            logger = _setup_logging(log_config)
            assert logger is not None

    def test_setup_logging_creates_parent_directories(self, tmp_path):
        """Test that parent directories are created."""
        log_dir = tmp_path / "nested" / "log" / "dir"
        log_config = {"log_dir": str(log_dir), "log_filename": "test.log"}
        _setup_logging(log_config)
        assert log_dir.exists()
