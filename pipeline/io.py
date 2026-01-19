import datetime
import json
import logging
import timeit
from pathlib import Path

import yaml
from obspy import UTCDateTime

from pipeline.config import PipelineConfig, RequestParams
from pipeline.core import DataPipeline


def read_from_config_file(config_file: str | Path):
    """
    Reads requests config from a YML file.

    Expected format:
    StationIPs:
        # Either provide station_ip_file or station_ips directly
        station_ip_file: /path/to/station_ips.json
        station_ips:
            STA1: "192.168.1.1"
            STA2: "192.168.1.2"
    LogConfig:
        log_level: INFO
        log_dir: /path/to/logdir
        log_filename: my_log.log
    PipelineConfig:
        data_dir: /path/to/datadir
        chunksize_hours: 1
        buffer_seconds: 150
    RequestParams:
        start: 2023-01-01T00:00:00
        end: 2023-01-02T00:00:00

    """
    print("Reading Config file and setting up DataPipeline...")
    with open(config_file, "r") as f:
        all_config = yaml.safe_load(f)
        pipe_config_yml = all_config.get("PipelineConfig", {})
        req_config_yml = all_config.get("RequestParams", {})
        log_config_yml = all_config.get("LogConfig", {})

    if not log_config_yml:
        log_level = "INFO"
        log_dir = Path.cwd()
        log_filename = "data_pipeline.log"
    else:
        log_level = log_config_yml.get("log_level", "INFO")
        log_dir = Path(log_config_yml.get("log_dir", Path.cwd()))
        log_filename = log_config_yml.get("log_filename", "data_pipeline.log")
    # Set up logging
    if not log_dir.exists():
        print(f"Log directory {log_dir} does not exist, creating it.")
        log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_filename
    logging.basicConfig(
        filename=log_path, level=getattr(logging, log_level.upper(), logging.INFO)
    )

    logging.info("Log setup complete, starting pipeline configuration.")
    # Set PipelineConfig parameters

    data_dir = pipe_config_yml.get("data_dir", Path.cwd())
    chunksize_hours = datetime.timedelta(
        hours=pipe_config_yml.get("chunksize_hours", 1)
    )
    buffer_seconds = datetime.timedelta(
        seconds=pipe_config_yml.get("buffer_seconds", 150)
    )
    pipeline_config = PipelineConfig(
        data_dir=Path(data_dir),
        chunksize_hours=chunksize_hours,
        buffer_seconds=buffer_seconds,
    )
    # Load station IPs
    logging.info("Loading station IPs.")
    if "station_ips_file" in all_config.get("StationIPs"):
        logging.info("Loading station IPs from file.")
        print(
            f"Read station IPs from file {all_config['StationIPs']['station_ips_file']}"
        )
        station_ip_file = all_config["StationIPs"]["station_ips_file"]
        with open(station_ip_file, "r") as f:
            station_ips = json.load(f)
    else:
        logging.info("Loading station IPs from config.")
        station_ips = all_config.get("StationIPs", {}).get("station_ips", {})

    ConfiguredPipeline = DataPipeline(station_ips=station_ips, config=pipeline_config)
    logging.info("DataPipeline initialized.")
    # Set RequestParams parameters
    if "request_param_file" in req_config_yml:
        print(
            f"Reading request parameters from file {req_config_yml['request_param_file']}"
        )
        logging.info(
            f"Loading request parameters from file {req_config_yml['request_param_file']}"
        )
        request_param_file = req_config_yml["request_param_file"]
        ParamsForRequest = RequestParams.from_bulk_inputs(request_param_file)
    else:
        logging.info("Loading request parameters from config file.")
        expected_seed_params = [
            "networks",
            "stations",
            "locations",
            "channels",
            "start",
            "end",
        ]
        if not all(param in req_config_yml for param in expected_seed_params):
            missing = [
                param for param in expected_seed_params if param not in req_config_yml
            ]
            raise ValueError(
                f"Missing required RequestParams fields in config file: {missing}"
            )
        networks = req_config_yml.get("networks")
        stations = req_config_yml.get("stations")
        locations = req_config_yml.get("locations")
        channels = req_config_yml.get("channels")
        start = req_config_yml.get("start")
        end = req_config_yml.get("end")
        ParamsForRequest = RequestParams.from_date_range(
            networks=networks,
            stations=stations,
            locations=locations,
            channels=channels,
            starttime=UTCDateTime(start),
            endtime=UTCDateTime(end),
        )

    logging.info("Starting data download.")
    print(
        f"Starting data download. There are {len(ParamsForRequest)} requests to make."
    )
    ConfiguredPipeline.get_data(ParamsForRequest)
    logging.info("Data download complete.")
    print("Data download complete.")


if __name__ == "__main__":
    # Example usage
    script_start = timeit.default_timer()
    config_file = "path/to/config.yml"
    pipeline_config, request_config, run_config = read_from_config_file(config_file)
    script_end = timeit.default_timer()
    runtime = script_end - script_start
    print(f"Runtime was {runtime:4.2f} seconds, or {runtime / 60:4.2f} minutes.")
    logging.info(f"Runtime was {runtime:4.2f} seconds, or {runtime / 60:4.2f} minutes.")
