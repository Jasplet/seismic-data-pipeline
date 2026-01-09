import itertools
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
    network : list[str]
        SEED Network code
    station : list[str]
        SEED Station code
    location : list[str]
        Location code
    channel : list[str]
        SEED Channel code
    start : list[UTCDateTime]
        Start time of request
    end : list[UTCDateTime]
        End time of request
    timeout : int
        Timeout for HTTP requests in seconds
    """

    networks: list[str]
    stations: list[str]
    locations: list[str]
    channels: list[str]
    start: UTCDateTime
    end: UTCDateTime
    timeout: int = field(default=10)  # seconds

    def make_request_param_list(self):
        """
        Creates an iterator of all possible combinations of the request
        parameters.

        Returns:
        -------
        iterator of tuples
            Each tuple is of the form
            (network, station, location, channel, start, end)
        """
        return itertools.product(
            self.networks,
            self.stations,
            self.locations,
            self.channels,
            self.start,
            self.end,
        )


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
