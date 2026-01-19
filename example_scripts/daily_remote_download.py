# Author: J Asplet, U of Oxford, 20/11/2023

# Python script to remotely query data from NYMAR array stations
# We are using Wget as implemented in the requests library
# This script is designed to be run as a cron job to send daily requests to
# remotely installed Certimus/Minimus to get data
# Data is requested in hourly chunks and then recombined into
# a day length miniSEED file
#
# Some editing of this script could make it request minute chunks
# (for a whole day) or make hourly / minutely requests for data

import datetime
import json
import logging
import timeit
from pathlib import Path

from obspy import UTCDateTime

from pipeline.config import PipelineConfig, RequestParams
from pipeline.core import DataPipeline

log = logging.getLogger(__name__)
logdir = Path("/home/joseph/logs")
# test is logdir exists, if not then set to cwd
if logdir.exists():
    print(f"Logs written to {logdir}")
else:
    logdir = Path.cwd()
    print("Logs written to cwd")

if __name__ == "__main__":
    today = datetime.datetime.today()
    script_start = timeit.default_timer()
    logging.basicConfig(
        filename=f"{logdir}/remote_download_" + f"{today.strftime('%Y_%m_%d')}.log",
        level=logging.INFO,
    )
    log.info(f"Starting download. Time is {datetime.datetime.now()}")

    # ========== Start of variables to set ==========

    # Create pipeline configuration object
    # The config is quite simple. The key variable is the data_dir
    # which sets where data will be written to.
    # You can also set the chunksize_hours variable to change
    # the length of each data request.
    # The buffer_seconds variable adds a small buffer to the start
    # and end of each request to help ensure no data is missed
    # due to timing issues.
    # If a config object is not provided to the DataPipeline object
    # then a default config is used using the current working directory as
    # data_dir and the same chunk and buffer settings as shown here.

    request_config = PipelineConfig(
        data_dir=Path(
            "/path/to/datadir"
        ),  # directory to write data to, change to /your/path/to/datadir
        chunksize_hours=datetime.timedelta(hours=1),  # length of data to request
        buffer_seconds=datetime.timedelta(
            seconds=150
        ),  # buffer to add to the start/end of each request
    )

    # Load IP addresses.
    # Here I have stored them in a JSON file to keep them off GitHub.
    # JSON should be in the format:
    # {
    #   "STA1": "192.168.1.1",
    #   "STA2": "192.168.1.2"
    # }
    with open("/path/to/instrument_ips.json", "r") as w:
        ips_dict = json.load(w)
    # Alternatively you can hardcode the IPs here as a dictionary
    # ips_dict = {
    #     "STA1": "192.168.1.1",
    #     "STA2": "192.168.1.2"
    # }

    # Initialize DataPipeline object
    data_fetcher = DataPipeline(station_ips=ips_dict, config=request_config)

    # Set a number of days to backfill from
    # (i.e., how many previous days to request)
    backfill_span = datetime.timedelta(days=2)
    # SEED parameters to request
    # Check the request parameters match the settings on your Certimus/Minimus
    # These can be seen on the Certimus/Minimus status page (https://{your-ip-here})
    start = UTCDateTime(datetime.date.today() - backfill_span)
    end = UTCDateTime(datetime.date.today())
    log.info(f"Query start time: {start}")
    log.info(f"Query end time: {end}")

    Params_for_request = RequestParams.from_date_range(
        networks=["OX"],  # Network code
        stations=["STA1", "STA2"],  # Station codes
        locations=["00"],  # Location code
        channels=["HHZ", "HHN", "HHE"],  # Channel codes
        # Time span to get data for. Edit these start/end objects
        # to customise the timespan to get data for.
        starttime=start,
        endtime=end,
    )

    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(
        f"Runtime is {runtime:4.2f} seconds,"
        + f"or {runtime / 60:4.2f} minutes,"
        + f" or {runtime / 3600:4.2f} hours"
    )
