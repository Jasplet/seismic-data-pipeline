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
        requests_to_make: list[
            tuple[str, str, str, str, UTCDateTime, UTCDateTime]
        ] = [],
        timeout: int = 10,
    ):  # seconds
        self.requests_to_make = requests_to_make
        self.timeout = timeout
        self._validate()

    def _validate(self):
        if not self.requests_to_make:
            raise ValueError("requests_to_make is empty")
        for req in self.requests_to_make:
            if len(req) != 6:
                raise ValueError(
                    "Each request tuple must have 6 elements: "
                    "(network, station, location, channel, starttime, endtime)"
                )
            if not isinstance(req[4], UTCDateTime) or not isinstance(
                req[5], UTCDateTime
            ):
                raise TypeError("Start and end times must be of type UTCDateTime")
            if req[4] >= req[5]:
                raise ValueError("Start time must be before end time in each request")

    def __iter__(self):
        """Make the class iterable over the requests to make."""
        return iter(self.requests_to_make)

    def __len__(self):
        """Return the number of requests to make."""
        return len(self.requests_to_make)

    @classmethod
    def from_date_range(
        cls,
        networks: list[str] | str,
        stations: list[str] | str,
        locations: list[str] | str,
        channels: list[str] | str,
        starttime: UTCDateTime,
        endtime: UTCDateTime,
        timeout: int = 10,
    ):
        """
        Create RequestParams for a continuous date range.

        Parameters:
        ----------
        networks : list[str] | str
            SEED Network code(s)
        stations : list[str] | str
            SEED Station code(s)
        locations : list[str] | str
            Location code(s)
        channels : list[str] | str
            SEED Channel code(s)
        starttime : UTCDateTime
            Start time of request
        endtime : UTCDateTime
            End time of request
        timeout : int
            Timeout for HTTP requests in seconds
        """
        if endtime <= starttime:
            raise ValueError("endtime must be greater than starttime")

        if not isinstance(networks, list):
            networks = [networks]
        if not isinstance(stations, list):
            stations = [stations]
        if not isinstance(locations, list):
            locations = [locations]
        if not isinstance(channels, list):
            channels = [channels]

        reqs = itertools.product(
            networks, stations, locations, channels, [starttime], [endtime]
        )

        return cls(
            requests_to_make=list(reqs),
            timeout=timeout,
        )

    @classmethod
    def for_time_windows(
        cls,
        networks: list[str] | str,
        stations: list[str] | str,
        locations: list[str] | str,
        channels: list[str] | str,
        time_windows: list[tuple[UTCDateTime, UTCDateTime]],
        timeout: int = 10,
    ):
        """
        Initializes RequestParams from tuple containing time windows.

        Parameters:
        ----------
        networks : list[str] | str
            SEED Network code(s)
        stations : list[str] | str
            SEED Station code(s)
        locations : list[str] | str
            Location code(s)
        channels : list[str] | str
            SEED Channel code(s)
        time_windows : list[tuple[UTCDateTime, UTCDateTime]]
            List of (start, end) time tuples
        timeout : int
            Timeout for HTTP requests in seconds

        Returns:
        -------
        RequestParams

        """
        # Normalize to lists
        if not isinstance(networks, list):
            networks = [networks]
        if not isinstance(stations, list):
            stations = [stations]
        if not isinstance(locations, list):
            locations = [locations]
        if not isinstance(channels, list):
            channels = [channels]

        if not time_windows:
            raise ValueError("time_windows are empty")

        reqs = [
            (net, sta, loc, cha, start, end)
            for net, sta, loc, cha, (start, end) in itertools.product(
                networks, stations, locations, channels, time_windows
            )
        ]

        return cls(
            requests_to_make=reqs,
            timeout=timeout,
        )

    @classmethod
    def from_bulk_inputs(cls, bulk_requests, timeout: int = 10):
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
                reqs = pickle.load(f)
        elif not bulk_requests:
            raise ValueError("bulk_requests is empty")
        else:
            reqs = bulk_requests

        return cls(requests_to_make=reqs, timeout=timeout)

    @classmethod
    def from_user_inputs(cls, **kwargs):
        """
        Flexible constructor from keyword arguments.
        Routes to appropriate constructor based on provided arguments.

        Parameters:
        ----------
        **kwargs : dict
            Accepts:
            - network(s), station(s), location(s), channel(s) with:
              - time_windows: list of (start, end) tuples
              - OR start/starttime and end/endtime: single time window
            - bulk_requests: list of complete request tuples

        Examples:
        --------
        >>> # Single time window
        >>> params = RequestParams.from_user_inputs(
        ...     network="XX",
        ...     stations=["STA1", "STA2"],
        ...     location="00",
        ...     channel="HHZ",
        ...     start=UTCDateTime("2026-01-01"),
        ...     end=UTCDateTime("2026-01-02"),
        ... )

        >>> # Multiple time windows
        >>> params = RequestParams.from_user_inputs(
        ...     network="XX",
        ...     station="TEST",
        ...     location="00",
        ...     channel="HHZ",
        ...     time_windows=[
        ...         (UTCDateTime("2026-01-01"), UTCDateTime("2026-01-02")),
        ...         (UTCDateTime("2026-01-10"), UTCDateTime("2026-01-11")),
        ...     ]
        ... )

        >>> # Bulk requests
        >>> params = RequestParams.from_user_inputs(
        ...     bulk_requests=[
        ...         ("XX", "STA1", "00", "HHZ", UTCDateTime("2026-01-01"), UTCDateTime("2026-01-02")),
        ...     ]
        ... )
        """
        # Check for bulk_requests first
        bulk_requests = kwargs.get("bulk_requests")
        if bulk_requests:
            return cls.from_bulk_requests(bulk_requests)

        # Extract SEED parameters (singular or plural)
        networks = kwargs.get("networks", kwargs.get("network"))
        stations = kwargs.get("stations", kwargs.get("station"))
        locations = kwargs.get("locations", kwargs.get("location"))
        channels = kwargs.get("channels", kwargs.get("channel"))

        # Check for time_windows
        time_windows = kwargs.get("time_windows")
        if time_windows:
            if any(x is None for x in [networks, stations, locations, channels]):
                raise ValueError(
                    "Must provide network(s), station(s), location(s), and channel(s)"
                )
            return cls.from_time_windows(
                networks=networks,
                stations=stations,
                locations=locations,
                channels=channels,
                time_windows=time_windows,
            )

        # Otherwise use start/end for date range
        start = kwargs.get("start", kwargs.get("starttime"))
        end = kwargs.get("end", kwargs.get("endtime"))

        missing_params = []
        for name, value in {
            "network(s)": networks,
            "station(s)": stations,
            "location(s)": locations,
            "channel(s)": channels,
            "start/starttime": start,
            "end/endtime": end,
        }.items():
            if value is None:
                missing_params.append(name)

        if missing_params:
            raise ValueError(
                f"Missing required parameters: {', '.join(missing_params)}"
            )

        return cls.from_date_range(
            networks=networks,  # type: ignore
            stations=stations,  # type: ignore
            locations=locations,  # type: ignore
            channels=channels,  # type: ignore
            starttime=start,  # type: ignore
            endtime=end,  #  type: ignore
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
    buffer_seconds: timedelta = field(default=timedelta(seconds=150))
    # Max number of async requests is hard limited to three
    n_async_requests: int = field(default=3)

    def __post_init__(self):
        if self.n_async_requests > 3:
            raise ValueError(
                "Max number of async requests supported by the sensors is 3"
            )
