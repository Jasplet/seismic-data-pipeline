import asyncio
import datetime
import logging

import aiohttp
import obspy

# Imports from across module
from pipeline.config import PipelineConfig, RequestParams
from pipeline.urls import group_urls_by_station, make_urls


class DataPipeline:
    def __init__(self, station_ips, config: PipelineConfig = PipelineConfig()):
        """
        Initializes DataPipeline with station IPs and configuration.§

        Parameters:
        ----------
        station_ips : dict
            Dictionary of station codes and their corresponding IP addresses.
        config : PipelineConfig
            Configuration parameters for the data pipeline.
            If not provided, defaults to PipelineConfig().
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.config = config
        self.station_ips = station_ips
        self._validate_ips()
        # Only configure if is no higher level logging handler
        # Adds a handler only if none exist (avoid duplicate logs)
        # If a user configures ther own logging, this will not override it
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.propagate = False  # Don't propagate to root logger

    def _validate_ips(self):
        """
        Validates that station IPs are in correct format.

        Raises ValueError if any IP is invalid.
        """

        for station, ip in self.station_ips.items():
            # check if IP is non-empty string
            if not isinstance(ip, str) or len(ip) == 0:
                raise ValueError(f"Invalid IP address for station {station}: {ip}")
            if ":" in ip:
                # check if port is valid integer
                ip_part, port_part = ip.split(":")
                if not port_part.isdigit():
                    raise ValueError(
                        f"Invalid port in IP address for station {station}: {ip}"
                    )
                ip_sections = ip_part.split(".")
                for section in ip_sections:
                    if not (0 <= int(section) <= 255):
                        raise ValueError(
                            f"Invalid IP address for station {station}: {ip}"
                        )
            if ip.count(".") != 3:
                raise ValueError(
                    f"Invalid IP address format for station {station}: {ip}"
                )
        if self.logger:
            self.logger.info(f"Validated IP address for station {station}: {ip}")

    def get_data(self, SEED_params: RequestParams):
        """
        Main function to get data asynchronously from multiple Certimus
        instruments.

        Parameters:
        ----------
        Params : RequestParams
            Data class holding parameters for forming Certimus HTTP API requests.
        """

        self.logger.info("Starting asynchronous data retrieval")
        asyncio.run(self._get_data(SEED_params))

    async def _get_data(
        self,
        SEED_params: RequestParams,
    ):
        # Make all urls to query.
        self.logger.info("Forming request URLs")
        urls, outfiles = self._make_urls(SEED_params)
        self.logger.info(f"There are {len(urls)} requests to make")
        requests_by_ip = self._group_by_stations(urls, outfiles)
        # Use semaphores to limit simultaneous requests per sensor
        semaphores = {
            sensor_ip: asyncio.Semaphore(self.config.n_async_requests)
            for sensor_ip in requests_by_ip
        }

        async with aiohttp.ClientSession() as async_client_session:
            tasks = []
            # Limit the number of simultaneous requests
            # Adjust based on seismometer capacity
            for sensor_ip, reqs in requests_by_ip.items():
                self.logger.info(f"Making requests to sensor at {sensor_ip}")
                semaphore = semaphores[sensor_ip]
                for request_url, outfile in reqs:
                    self.logger.info(f"Calling _make_async_request for {request_url}")
                    task = asyncio.create_task(
                        self._make_async_request(
                            request_url, outfile, async_client_session, semaphore
                        )
                    )
                    tasks.append(task)
            await asyncio.gather(*tasks)

    async def _make_async_request(self, request_url, outfile, session, semaphore):
        """
        Function to actually make the HTTP GET request from the Certimus

        Gets one file, which corresponds to the request_url and writes
        it out as miniSEED
        to the specified outfile

        Parameters:
        ----------
        request_url : str
            The formed request url in the form:
            http://{sensor_ip}/data?channel={net_code}.{stat_code}.{loc_code}.{channel}&from={startUNIX}&to={endUNIX}
        outfile : str
            Filename to write the miniSEED data to, generated in make_urls
        session : aiohttp.ClientSession
            The aiohttp client session to use for making requests
        semaphore : asyncio.Semaphore
            Semaphore to limit simultaneous requests per sensor
        """

        async with semaphore:
            try:
                async with session.get(request_url) as resp:
                    self.logger.info(f"Request at {datetime.datetime.now()}")
                    # Print start and end times in a human readable format
                    st = obspy.UTCDateTime(
                        float(request_url.split("=")[-2].strip("&to"))
                    )
                    ed = obspy.UTCDateTime(float(request_url.split("=")[-1]))
                    self.logger.info(f"Requesting {st} to {ed}")
                    # Raise HTTP error for 4xx/5xx errors
                    resp.raise_for_status()

                    # Read binary data from the response
                    data = await resp.read()
                    if len(data) == 0:
                        self.logger.error(
                            "Request is empty!" + "Won’t write a zero byte file."
                        )
                        return
                    # Now write data
                    with open(outfile, "wb") as f:
                        f.write(data)
                    self.logger.info(f"Successfully wrote data to {outfile}")

            except aiohttp.ClientResponseError as e:
                self.logger.error(f"Client error for {request_url}: {e}")
                # Additional handling could go here, like retry logic

            except aiohttp.ClientConnectionError as e:
                self.logger.error(f"Connection error for {request_url}: {e}")

            except Exception as e:
                self.logger.error(f"Unexpected error for {request_url}: {e}")
            return

    def _group_by_stations(self, urls, outfiles):
        """
        Groups urls and outfiles by station IP address, using utility function.

        :param self: Description
        :param urls: Description
        :param outfiles: Description
        """
        return group_urls_by_station(urls, outfiles)

    def _make_urls(self, request_params):
        """
        Calls utility function to make urls for chunked requests.

        :param self: Description
        :param request_params: Description
        """
        return make_urls(
            self.station_ips,
            request_params,
            data_dir=self.config.data_dir,
            chunksize=self.config.chunksize_hours,
            buffer=self.config.buffer_seconds,
        )
