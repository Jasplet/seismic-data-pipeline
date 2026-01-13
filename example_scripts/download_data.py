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

    # ========== Start of variable to set ==========

    request_config = PipelineConfig(
        data_dir=Path(
            "/path/to/datadir"
        ),  # directory to write data to, change to /your/path/to/datadir
        chunksize_hours=datetime.timedelta(hours=1),  # length of data to request
        buffer_seconds=datetime.timedelta(
            seconds=150
        ),  # buffer to add to the start/end of each request
    )

    # Provide IP addresses.
    # Here I have stored them in a JSON file to keep them off GitHub.
    # JSON should be in the format:
    # {
    #   "STA1": "192.168.1.1",
    #   "STA2": "192.168.1.2"
    # }
    with open("/path/to/instrument_ips.json", "r") as w:
        ips_dict = json.load(w)
    # with open('/home/joseph/nymar_zerotier_ips.json', 'r') as w:
    #     ips_dict = json.load(w)

    starttime = UTCDateTime(2024, 9, 30, 0, 0, 0)
    endtime = UTCDateTime(2024, 10, 1, 0, 0, 0)
    # SEED parameters to request
    # Check the request parameters match the settings on your Certimus/Minimus
    # These can be seen on the status/setup page of the instrument
    Params_for_request = RequestParams(
        networks=["OX"],  # Network code
        stations=["STA1", "STA2"],  # Station codes
        locations=["00"],  # Location code
        channels=["HHZ", "HHN", "HHE"],  # Channel codes
        start=starttime,
        end=endtime,
    )

    # Time span to get data for. Edit these start/end objects
    # to customise the timespan to get data for.
    log.info(f"Query start time: {starttime.strftime('%Y-%m-%dT%H:%M:%S')}")
    log.info(f"Query end time: {endtime.strftime('%Y-%m-%dT%H:%M:%S')}")
    # SET TO CORRECT CODE. should be '00' for veloctity data
    # will be somehing different for voltage,
    # check Certimus/Minimus status page (https://{your-ip-here})

    # ========== End of variables to set ==========

    data_fetcher = DataPipeline(station_ips=ips_dict, config=request_config)
    data_fetcher.get_data(Params_for_request)
    log.info("Finished download.")
    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(
        f"Runtime is {runtime:4.2f} seconds,"
        + f"or {runtime / 60:4.2f} minutes,"
        + f" or {runtime / 3600:4.2f} hours"
    )
