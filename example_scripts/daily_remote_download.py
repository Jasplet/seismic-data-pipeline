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
import itertools
import json
import logging
import timeit
from pathlib import Path

from obspy import UTCDateTime

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

    # ----------- Start of variable to set ----------
    # directory to write data to
    # change to the path you want to store data

    data_dir = Path("/your/path/here")
    # Provide IP addresses. Here I have stored them in a JSON file to keep
    # them off GitHub.
    with open("/path/to/your/station_ips.json", "r") as w:
        ips_dict = json.load(w)

    # Seedlink Parameters
    networks = ["OX"]
    stations = ["STA1", "STA2"]
    channels = ["HHZ", "HHN", "HHE"]
    locations = ["00"]

    # Set number of days to downlaod (for preliminary gapfilling)
    backfill_span = datetime.timedelta(days=2)
    #  backfill_span = datetime.timedelta(hours=2)

    # SET TO CORRECT CODE. should be '00' for veloctity data
    # will be somehing different for voltage
    # check status page (https://{your-ip-here})

    # set start / end date.
    # try to get previous 2 days of data (current day will not be available)
    # Here we want to iterate over the preding days
    # so truncate the today datetime object
    start = [UTCDateTime(today.year, today.month, today.day, 0, 0, 0) - backfill_span]
    end = [UTCDateTime(today.year, today.month, today.day, 0, 0, 0)]
    log.info(f"Query start time: {start}")
    log.info(f"Query end time: {end}")
    # ---------- End of variables to set ----------

    request_params = itertools.product(
        networks, stations, locations, channels, start, end
    )
    # call get_data
    # asyncio.run(get_data(request_params, station_ips=ips_dict, data_dir=data_dir))

    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(
        f"Runtime is {runtime:4.2f} seconds,"
        + f"or {runtime / 60:4.2f} minutes,"
        + f" or {runtime / 3600:4.2f} hours"
    )
