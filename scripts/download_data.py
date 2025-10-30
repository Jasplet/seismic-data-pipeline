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

import asyncio
import datetime
import json
import logging
import itertools
from pathlib import Path
import timeit

from obspy import UTCDateTime

from data_pipeline import get_data

log = logging.getLogger(__name__)
logdir = Path("/home/joseph/logs")
# test is logdir exists, if not then set to cwd
if logdir.exists():
    print(f"Logs written to {logdir}")
else:
    logdir = Path.cwd()
    print(f"Logs written to cwd - {logdir}")

if __name__ == "__main__":
    script_start = timeit.default_timer()
    logging.basicConfig(filename=f"{logdir}/nymar_backfill.log", level=logging.INFO)
    log.info(f"Starting download. Time is {datetime.datetime.now()}")

    # ========== Start of variable to set ==========
    # directory to write data to
    # change to /your/path/to/datadir
    data_dir = Path("/Users/eart0593/Projects/Agile/NYMAR/" + "data_dump/")
    # Provide IP addresses. Here I have stored them in a JSON file to keep
    # them off GitHub.
    with open("/Users/eart0593/Projects/Agile/NYMAR/nymar_zerotier_ips.json", "r") as w:
        ips_dict = json.load(w)
    # with open('/home/joseph/nymar_zerotier_ips.json', 'r') as w:
    #     ips_dict = json.load(w)

    with open(
        "/Users/eart0593/Projects/Agile/NYMAR/nymar_request_params.json", "r"
    ) as param_file:
        params = json.load(param_file)

    # Seedlink Parameters
    networks = params["networks"]
    stations = params["stations"]
    stations = ["NYM1"]
    channels = params["channels"]
    locations = params["locations"]

    # Time span to get data for. Edit these start/end objects
    # to customise the timespan to get data for.
    start = [UTCDateTime(2024, 9, 30, 0, 0, 0)]
    end = [UTCDateTime(2024, 10, 1, 0, 0, 0)]
    log.info(f"Query start time: {start}")
    log.info(f"Query end time: {end}")
    # SET TO CORRECT CODE. should be '00' for veloctity data
    # will be somehing different for voltage,
    # check Certimus/Minimus status page (https://{your-ip-here})
    locations = ["00"]
    # flatten seedlink parameters into an iterator of
    # tuples of all possible combinations.

    # params should be form (net, stat, loc, channel, start, end)
    # here start/end are the start and end time of all data to request
    # ========== End of variables to set ==========

    request_params = itertools.product(
        networks, stations, locations, channels, start, end
    )
    # call get_data
    asyncio.run(get_data(request_params, station_ips=ips_dict, data_dir=data_dir))

    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(
        f"Runtime is {runtime:4.2f} seconds,"
        + f"or {runtime/60:4.2f} minutes,"
        + f" or {runtime/3600:4.2f} hours"
    )
