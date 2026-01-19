import datetime
import json
import logging
import timeit
from pathlib import Path

import yaml
from obspy import UTCDateTime

from pipeline.config import PipelineConfig, RequestParams
from pipeline.core import DataPipeline


def _load_config_file(config_file: str | Path):
    """
    Reads requests config from a YML file.
    """
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file {config_file} does not exist.")

    try:
        with open(config_path, "r") as f:
            all_config = yaml.safe_load(f)
            if all_config is None:
                raise ValueError("Config file is empty.")
            return all_config
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML config file: {e}")


def _setup_logging(log_config: dict):
    """
    Sets up logging based on provided configuration.
    """
    log_level = log_config.get("log_level", "INFO")
    log_dir = Path(log_config.get("log_dir", Path.cwd()))
    log_filename = log_config.get("log_filename", "data_pipeline.log")

    if not log_dir.exists():
        print(f"Log directory {log_dir} does not exist, creating it.")
        log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / log_filename
    # Remove any existing handlers to avoid duplicate logs
    logger = logging.getLogger(__name__)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.propagate = False
    logging.info("Log setup complete.")
    return logger


def _load_station_ips(station_ip_config: dict):
    """
    Load station IPs from file or dictionary.

    :param station_ip_config: Description
    :type station_ip_config: dict
    """
    logger = logging.getLogger(__name__)
    # Load station IPs
    if "station_ips_file" in station_ip_config:
        logger.info("Loading station IPs from file.")
        print(f"Read station IPs from file {station_ip_config['station_ips_file']}")
        station_ip_file = Path(station_ip_config["station_ips_file"])
        if not station_ip_file.exists():
            raise FileNotFoundError(
                f"Station IPs file {station_ip_file} does not exist."
            )
        try:
            with open(station_ip_file, "r") as f:
                ips = json.load(f)
            logger.info("Station IPs loaded from file.")
            return ips
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON station IPs file: {e}")

    logger.info("Loading station IPs from config.")
    ips = station_ip_config.get("station_ips", {})
    if not ips:
        raise ValueError("No station IPs provided in config.")

    logger.info("Station IPs loaded from config.")
    return ips


def _create_pipeline_config(pipeline_config_yml: dict):
    """
    Create PipelineConfig object from YML config.

    :param pipeline_config_yml: Description
    :type pipeline_config_yml: dict
    """
    data_dir = pipeline_config_yml.get("data_dir", Path.cwd())

    kwargs = {"data_dir": data_dir}
    if "chunksize_hours" in pipeline_config_yml:
        kwargs["chunksize_hours"] = datetime.timedelta(
            hours=pipeline_config_yml["chunksize_hours"]
        )

    if "buffer_seconds" in pipeline_config_yml:
        kwargs["buffer_seconds"] = datetime.timedelta(
            seconds=pipeline_config_yml["buffer_seconds"]
        )

    pipeline_config = PipelineConfig(**kwargs)
    logging.info("PipelineConfig created from config.")
    return pipeline_config


def _create_request_params(request_config_yml: dict):
    """
    Create RequestParams object from YML config.

    :param request_params_yml: Description
    :type request_config_yml: dict
    """
    if "request_param_file" in request_config_yml:
        request_file = Path(request_config_yml["request_param_file"])
        if not request_file.exists():
            raise FileNotFoundError(
                f"Request parameters file {request_file} does not exist."
            )
        print(
            f"Reading request parameters from file {request_config_yml['request_param_file']}"
        )
        logging.info(
            f"Loading request parameters from file {request_config_yml['request_param_file']}"
        )
        request_param_file = request_config_yml["request_param_file"]
        return RequestParams.from_bulk_requests(request_param_file)

    # Otherwise load from YML
    logging.info("Creating RequestParams from SEED params in config.")

    # Create start / end times for cron jobs if needed
    if request_config_yml.get("cron", False):
        days_before = request_config_yml.get("days_before", 2)
        date_today = datetime.date.today()
        end = UTCDateTime(date_today)
        start = UTCDateTime(date_today - datetime.timedelta(days=days_before))
        logging.info(
            f"Cron mode active. Setting starttime to {start} and endtime to {end}."
        )
        request_config_yml["start"] = start
        request_config_yml["end"] = end

    expected_seed_params = [
        "networks",
        "stations",
        "locations",
        "channels",
        "start",
        "end",
    ]
    if not all(param in request_config_yml for param in expected_seed_params):
        missing = [
            param for param in expected_seed_params if param not in request_config_yml
        ]
        raise ValueError(
            f"Missing required RequestParams fields in config file: {missing}"
        )

    ParamsForRequest = RequestParams.from_date_range(
        networks=request_config_yml["networks"],
        stations=request_config_yml["stations"],
        locations=request_config_yml["locations"],
        channels=request_config_yml["channels"],
        starttime=UTCDateTime(request_config_yml["start"]),
        endtime=UTCDateTime(request_config_yml["end"]),
    )
    return ParamsForRequest


def load_from_config_file(config_file: str | Path):
    """
    Reads requests config from a YML file.

    Parameters:
    ----------
    config_file : str | Path
        Path to YAML config file

    Returns:
    -------
    DataPipeline
        Configured pipeline ready to call get_data()

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
        # Either provide request_param_file or the parameters directly
        request_param_file: /path/to/request_params.json
        networks:
            - OX
        stations:
            - STA1
            - STA2
        locations:
            - 00
        channels:
            - HHZ
        start: 2023-01-01T00:00:00
        end: 2023-01-02T00:00:00
        # For a cron job, you might want to set start and end dynamically based on current date
        cron: True
        days_before: 2
    """
    print("Reading Config file and setting up DataPipeline...")
    # Read config file
    all_config = _load_config_file(config_file)

    # Setup logging
    log_config_yml = all_config.get("LogConfig", {})
    logger = _setup_logging(log_config_yml)

    # Load station IPs
    logger.info("Loading station IPs from config.")
    station_ips = _load_station_ips(all_config.get("StationIPs", {}))

    # Set PipelineConfig parameters
    logger.info("Creating PipelineConfig from config.")
    pipeline_config = _create_pipeline_config(all_config.get("PipelineConfig", {}))

    # Initialize DataPipeline object
    logger.info("Initializing DataPipeline object.")
    ConfiguredPipeline = DataPipeline(station_ips=station_ips, config=pipeline_config)

    # Create RequestParams object
    logger.info("Creating RequestParams from config.")
    ParamsForRequest = _create_request_params(all_config.get("RequestParams", {}))

    logger.info(f"Setup complete: {len(ParamsForRequest)} requests to make")
    print(f"Setup complete: {len(ParamsForRequest)} requests to make")

    return ConfiguredPipeline, ParamsForRequest


if __name__ == "__main__":
    # Example usage
    script_start = timeit.default_timer()
    config_file = "path/to/config.yml"

    try:
        data_pipeline, request_params = load_from_config_file(config_file)
        # Now get the data
        logging.info("Starting data download")
        print("Starting data download...")
        data_pipeline.get_data(request_params)
        logging.info("Data download complete")
        print("Data download complete!")
    except Exception as e:
        logging.error(f"Error running data pipeline: {e}")
        print(f"Error running data pipeline: {e}")
    finally:
        script_end = timeit.default_timer()
        runtime = script_end - script_start
        msg = f"Runtime was {runtime:4.2f} seconds, or {runtime / 60:4.2f} minutes."
        msg += f" or {runtime / 3600:4.2f} hours."
        print(msg)
        logging.info(msg)
