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

from pathlib import Path
from obspy import UTCDateTime
import timeit
import datetime
import json
import logging
import itertools

from data_pipeline import chunked_data_query, gather_chunks

log = logging.getLogger(__name__)
logdir = Path('/home/joseph/logs')
# test is logdir exists, if not then set to cwd
if logdir.exists():
    print(f'Logs written to {logdir}')
else:
    logdir = Path.cwd()
    print(f'Logs written to cwd - {logdir}')


if __name__ == '__main__':
    script_start = timeit.default_timer()
    logging.basicConfig(filename=f'{logdir}/nymar_backfill.log',
                        level=logging.INFO)
    log.info(f'Starting download. Time is {datetime.datetime.now()}')

    # ========== Start of variable to set ==========
    # directory to write data to
    # change to /your/path/to/datadir
    data_dir = Path('/home/joseph/data')
    # Provide IP addresses. Here I have stored them in a JSON file to keep
    # them off GitHub.
    with open('/home/joseph/nymar_zerotier_ips.json', 'r') as w:
        ips_dict = json.load(w)

    # Seedlink Parameters
    network = ["OX"]
    station_list = ['NYM1', 'NYM2', 'NYM3', 'NYM4',
                    'NYM5', 'NYM6', 'NYM7', 'NYM8']
    channels = ["HHZ",  "HHN", "HHE"]

    # Time span to get data for. Edit these start/end objects
    # to customise the timespan to get data for.
    start = UTCDateTime(2024, 10, 25, 0, 0, 0)
    end = UTCDateTime(2024, 10, 27, 0, 0, 0)

    # SET TO CORRECT CODE. should be '00' for veloctity data
    # will be somehing different for voltage,
    # check Certimus/Minimus status page (https://{your-ip-here})
    location = ["00"]
    # flatten seedlink parameters into an iterator of
    # tuples of all possible combinations.
    request_params = itertools.product(network, station_list,
                                       location, channels)

    log.info(f'Query start time: {start}')
    log.info(f'Query end time: {end}')
    # ========== End of variables to set ==========

    for params in request_params:
        # params should be form (net, stat, loc, channel)
        log.info(f'Request data for {params}')
        station_ip = ips_dict[params[1]]

        chunked_data_query(station_ip, network=params[0], station=params[1],
                           location=params[2], channel=params[3],
                           starttime=start, endtime=end,
                           data_dir=data_dir,
                           chunksize=datetime.timedelta(hours=1),
                           buffer=datetime.timedelta(seconds=150))
        gather_chunks(network=params[0], station=params[1],
                      location=params[2], channel=params[3],
                      starttime=start, endtime=end,
                      data_dir=data_dir,
                      gather_size=datetime.timedelta(days=1))

    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(f'Runtime is {runtime:4.2f} seconds,' +
             f'or {runtime/60:4.2f} minutes,' +
             f' or {runtime/3600:4.2f} hours')
