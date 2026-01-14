# Author: J Asplet, U of Oxford, 20/11/2023

# Python script to remotely query data from NYMAR array stations
# We are using Wget as implemented in the requests library
# This script is designed to download data for specific channel
# or network codes. For a bulk download from all insturments
# in a network or array, use download_data.py
# In this script each tuple (net,sta,loc,chan, start, end) must be provided.
# This script is intended to be used for trying to fillin gaps in your dataset.

# If the instruments are networked then you can downalod data from mutliple
# Instruments by specifiying a dictionary of IP addresses.
# The example here uses a dictionary of IPs
# for intruments deployed for the North York Moors Array (NYMAR)
# and is read in from a json file.

# Data is requested in hourly chunks and then recombined
# into a day length miniSEED file

# Some editing of this script could make it request minute chunks
# (for a whole day) or make hourly / minutely requests for data

import datetime
import json
import logging
import timeit
from pathlib import Path

from pipeline.config import PipelineConfig, RequestParams
from pipeline.core import DataPipeline

log = logging.getLogger(__name__)
logdir = Path("/home/joseph/logs")
# test is logdir exists, if not then set to cwd
if logdir.exists():
    print(f"Logs written to {logdir}")
else:
    logdir = Path.cwd()
    print(f"Logs written to cwd {logdir}")

if __name__ == "__main__":
    script_start = timeit.default_timer()
    logging.basicConfig(filename=f"{logdir}/nymar_backfill.log", level=logging.INFO)
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

    data_fetcher = DataPipeline(station_ips=ips_dict, config=request_config)

    # Set up request parameters here. This is an example only. You may want
    # To use this script as an exmample to build your own code which finds
    # gaps that need filling and then sends the requests.
    bulk_requests = "/path/to/bulk_requests.pkl"
    requests = RequestParams.from_bulk_inputs(bulk_requests)
    # Example of what bulk_requests should look like
    # RequestParams.from_bulk_inputs will also take a
    # premade list of tuples as shown below.
    # request_params = [('OX','NYM2','00','HHN',
    #                   UTCDateTime(2024, 10, 1, 0, 0, 0),
    #                   UTCDateTime(2024, 10,2, 0, 0, 0)),
    #                   ('OX','NYM2','00','HHE',
    #                   UTCDateTime(2024, 4, 28, 0, 0, 0),
    #                   UTCDateTime(2024, 4, 28, 0, 0, 0)),
    #                   ('OX','NYM3','00','HHN',
    #                   UTCDateTime(2023, 12, 1, 0, 0, 0),
    #                   UTCDateTime(2023, 12,2, 0, 0, 0)),
    #                   ('OX','NYM4','00','HHZ',
    #                   UTCDateTime(2024, 10, 1, 0, 0, 0),
    #                   UTCDateTime(2024, 10,2, 0, 0, 0))]
    # requests = RequestParams.from_bulk_inputs(request_params)
    # ========== End of variables to set ==========
    # Get the data
    data_fetcher.get_data(requests)
    log.info(f"Download finished. Time is {datetime.datetime.now()}")
    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(
        f"Runtime is {runtime:4.2f} seconds, "
        + f"or {runtime / 60:4.2f} minutes, or {runtime / 3600:4.2f} hours"
    )
