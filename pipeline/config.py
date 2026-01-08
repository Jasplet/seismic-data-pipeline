from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

from obspy import UTCDateTime


@dataclass
class RequestParams:
    """
    Data class to hold parameters for forming Certimus HTTP API requests.
    These correspond to SEED parameters and time windows.
    Parameters:
    ----------
    sensor_ip : str
        IP address of sensor. Includes port no if any port forwarding needed
    network : str
        SEED Network code
    station : str
        SEED Station code
    location : str
        Location code
    channel : str
        SEED Channel code
    starttime : UTCDateTime
        Start time of request
    endtime : UTCDateTime
        End time of request
    timeout : int
        Timeout for HTTP requests in seconds
    """

    sensor_ip: str
    network: str
    station: str
    location: str
    channel: str
    starttime: UTCDateTime
    endtime: UTCDateTime
    timeout: int = field(default=10)  # seconds


@dataclass
class PipelineConfig:
    """
    Data class to hold overall pipeline configuration parameters.
    Parameters:
    ----------
    data_dir : str
        Directory to store downloaded data
    chunksize_hours : int
        Size of time chunks to split requests into (in hours)
    buffer_seconds : int
        Buffer time to add to each end of request (in seconds)
    n_async_requests : int
        Number of asynchronous requests to make simultaneously per sensor
    """

    data_dir: Path = field(default=Path.cwd())
    chunksize_hours: timedelta = field(default=timedelta(hours=1))
    buffer_seconds: timedelta = field(default=timedelta(hours=1))
    # Max number of async requests is hard limited to three
    n_async_requests: int = field(default=3)

    def __post_init__(self):
        if self.n_async_requests > 3:
            raise ValueError(
                "Max number of async requests supported by the sensors is 3"
            )
