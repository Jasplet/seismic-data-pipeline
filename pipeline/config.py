import itertools
import pickle
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

from obspy import UTCDateTime


class RequestParams:
    """
    Data class to hold parameters for forming Certimus HTTP API requests.
    These correspond to SEED parameters and time windows.
    Parameters:
    ----------
    networks : list[str]
        SEED Network code
    stations : list[str]
        SEED Station code
    locations : list[str]
        Location code
    channels : list[str]
        SEED Channel code
    start : list[UTCDateTime]
        Start time of request
    end : list[UTCDateTime]
        End time of request
    timeout : int
        Timeout for HTTP requests in seconds
    """

    def __init__(
        self,
        networks: list[str] | str,
        stations: list[str] | str,
        locations: list[str] | str,
        channels: list[str] | str,
        start: list[UTCDateTime] | UTCDateTime,
        end: list[UTCDateTime] | UTCDateTime,
        timeout: int = 10,
    ):  # seconds
        self.networks = networks
        self.stations = stations
        self.locations = locations
        self.channels = channels
        self.start = start
        self.end = end
        self.timeout = timeout

        # Convert all parameters to lists if they aren't already
        if not isinstance(self.networks, list):
            self.networks = [self.networks]

        if not isinstance(self.stations, list):
            self.stations = [self.stations]

        if not isinstance(self.locations, list):
            self.locations = [self.locations]

        if not isinstance(self.channels, list):
            self.channels = [self.channels]

        if not isinstance(self.start, list):
            if isinstance(self.start, UTCDateTime):
                self.start = [self.start]
            else:
                raise TypeError(
                    f"start must be a UTCDateTime object or list of UTCDateTime objects, got {type(self.start)}"
                )
        if not isinstance(self.end, list):
            if isinstance(self.end, UTCDateTime):
                self.end = [self.end]
            else:
                raise TypeError(
                    f"end must be a UTCDateTime object or list of UTCDateTime objects, got {type(self.end)}"
                )

        self.all_request_params = itertools.product(
            self.networks,
            self.stations,
            self.locations,
            self.channels,
            self.start,
            self.end,
        )

    @classmethod
    def from_user_inputs(cls, **kwargs):
        """
        Class method to create RequestParams object from user inputs.

        Parameters:
        ----------
        kwargs : dict
            Dictionary of user inputs with expected keys:
            networks, stations, locations, channels, start, end
        """
        networks = kwargs.get("networks", kwargs.get("network"))
        stations = kwargs.get("stations", kwargs.get("station"))
        locations = kwargs.get("locations", kwargs.get("location"))
        channels = kwargs.get("channels", kwargs.get("channel"))
        start = kwargs.get("start")
        end = kwargs.get("end")
        timeout = kwargs.get("timeout", 10)

        missing_params = [
            name
            for name, value in {
                "network(s)": networks,
                "station(s)": stations,
                "location(s)": locations,
                "channel(s)": channels,
                "start": start,
                "end": end,
            }.items()
            if value is None
        ]
        if missing_params:
            raise ValueError(
                f"Missing required parameters: {', '.join(missing_params)}"
            )
        return cls(
            networks=networks,  # type: ignore
            stations=stations,  # type: ignore
            locations=locations,  # type: ignore
            channels=channels,  # type: ignore
            start=start,  # type: ignore
            end=end,  # type: ignore
            timeout=timeout,  # type: ignore
        )

    def from_bulk_inputs(self, bulk_requests):
        """
        Initializes RequestParams from tuple containing all request parameters.
        Intended use if for bulk, discontinuous requests, such as for gapfilling.

        Example of what request_params should look like...
        ```
        request_params = [('OX','NYM2','00','HHN',
                        UTCDateTime(2024, 10, 1, 0, 0, 0),
                        UTCDateTime(2024, 10,2, 0, 0, 0)),
                        ('OX','NYM2','00','HHE',
                        UTCDateTime(2024, 4, 28, 0, 0, 0),
                        UTCDateTime(2024, 4, 28, 0, 0, 0)),
                        ('OX','NYM3','00','HHN',
                        UTCDateTime(2023, 12, 1, 0, 0, 0),
                        UTCDateTime(2023, 12,2, 0, 0, 0)),
                        ('OX','NYM4','00','HHZ',
                        UTCDateTime(2024, 10, 1, 0, 0, 0),
                        UTCDateTime(2024, 10,2, 0, 0, 0))]
        ```
        Parameters:
        ----------
        bulk_requests: Description
        """
        if Path(bulk_requests).is_file():
            with open(bulk_requests, "rb") as f:
                self.all_request_params = pickle.load(f)


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
    buffer_seconds: timedelta = field(default=timedelta(seconds=150))
    # Max number of async requests is hard limited to three
    n_async_requests: int = field(default=3)

    def __post_init__(self):
        if self.n_async_requests > 3:
            raise ValueError(
                "Max number of async requests supported by the sensors is 3"
            )
