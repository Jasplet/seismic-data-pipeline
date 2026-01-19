# Author: J Asplet, U of Oxford, 20/11/2023

# Python script to remotely query data from NYMAR array stations
# We are using Wget as implemented in the requests library
# This script is designed to download all available data
# From an installed Certimus/Minimus for a given period of time.

# If the instruments are networked then you can downalod data from mutliple
# Instruments by specifiying a dictionary of IP addresses.
# The example here uses a dictionary of IPs for intruments deployed
# for the North York Moors Array (NYMAR) and is read in from a json file.

# Data is requested in hourly chunks and then recombined into
#  a day length miniSEED file

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
## ADD YOUR LOG DIRECTORY HERE
logdir = Path("/path/to/custom/logdir")
# test is logdir exists, if not then set to cwd
if logdir.exists():
    print(f"Logs written to {logdir}")
else:
    logdir = Path.cwd()
    print(f"Logs written to cwd - {logdir}")

if __name__ == "__main__":
    script_start = timeit.default_timer()
    now = datetime.datetime.now()
    logname = f"data_download_{now.strftime('%Y%m%dT%H%M%S')}.log"
    logging.basicConfig(filename=f"{logdir}/{logname}", level=logging.INFO)
    log.info("Starting download.")

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

    # SEED parameters to request
    # Check the request parameters match the settings on your Certimus/Minimus
    # These can be seen on the Certimus/Minimus status page (https://{your-ip-here})
    ParamsForRequest = RequestParams.from_date_range(
        networks=["OX"],  # Network code
        stations=["STA1", "STA2"],  # Station codes
        locations=["00"],  # Location code
        channels=["HHZ", "HHN", "HHE"],  # Channel codes
        # Time span to get data for. Edit these start/end objects
        # to customise the timespan to get data for.
        starttime=UTCDateTime(2026, 1, 10, 0, 0, 0),
        endtime=UTCDateTime(2026, 1, 11, 0, 0, 0),
    )

    # =========== End of variables to set ===========

    # Get the data

    data_fetcher.get_data(ParamsForRequest)
    log.info("Finished download.")
    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(
        f"Runtime is {runtime:4.2f} seconds,"
        + f"or {runtime / 60:4.2f} minutes,"
        + f" or {runtime / 3600:4.2f} hours"
    )
