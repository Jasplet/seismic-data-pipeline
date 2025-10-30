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

import asyncio
import datetime
import json
import logging
from pathlib import Path
import pickle
import timeit

from data_pipeline import get_data

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

    # ---------- Start of variable to set ----------
    # directory to write data to
    data_dir = Path("/Volumes/NYMAR_Y1/" + "NYM1_gap_filling")
    # change to /your/path/to/datadir
    # data_dir = Path.cwd()
    # Provide IP addresses. Here I have stored them in a JSON file to keep
    # them off GitHub.
    with open("/Users/eart0593/Projects/Agile/NYMAR/nymar_zerotier_ips.json", "r") as w:
        ips_dict = json.load(w)

    # Set up request parameters here. This is an example only. You may want
    # To use this script as an exmample to build your own code which finds
    # gaps that need filling and then sends the requests.
    # gapfile = '/Users/eart0593/Projects/Agile/NYMAR/July_Oct_missing_files.pkl'
    gapfile = (
        "/Users/eart0593/Projects/Agile/NYMAR/data-gaps/NYM1_missing_files_30_10_25.pkl"
    )
    with open(gapfile, "rb") as f:
        request_params = pickle.load(f)

    # Example of what request_params should look like...

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
    # ----------- End of variables to set ----------

    asyncio.run(get_data(request_params, station_ips=ips_dict, data_dir=data_dir))

    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(
        f"Runtime is {runtime:4.2f} seconds, "
        + f"or {runtime/60:4.2f} minutes, or {runtime/3600:4.2f} hours"
    )
