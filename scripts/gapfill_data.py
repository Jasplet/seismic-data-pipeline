# Author: J Asplet, U of Oxford, 20/11/2023

## Python script to remotely query data from NYMAR array stations
## We are using Wget as implemented in the requests library
## This script is designed to download data for specific channel
## or network codes. For a bulk download from all insturments in a 
## network or array, use download_data.py
## In this script each tuple (net,sta,loc,chan, start, end)
## must be provided. 
## This script is intended to be used for trying to fillin gaps in 
## your dataset. 

## If the instruments are networked then you can downalod data from mutliple
## Instruments by specifiying a dictionary of IP addresses. 
## The example here uses a dictionary of IPs for intruments deployed for the North
## York Moors Array (NYMAR) and is read in from a json file.

## Data is requested in hourly chunks and then recombined into a day length miniSEED file

## Some editing of this script could make it request minute chunks (for a whole day) or
## make hourly / minutely requests for data 

from pathlib import Path
from obspy import UTCDateTime
import timeit
import datetime
import json
import logging
import pickle

from data_pipeline import chunked_data_query, gather_chunks

log = logging.getLogger(__name__)
logdir = Path('/home/joseph/logs')
# test is logdir exists, if not then set to cwd
if logdir.exists():
    print(f'Logs written to {logdir}')
else:
    logdir = Path.cwd()
    print(f'Logs written to cwd')


if __name__ == '__main__':
    script_start = timeit.default_timer()
    logging.basicConfig(filename=f'{logdir}/nymar_backfill.log', level=logging.INFO)
    log.info(f'Starting download. Time is {datetime.datetime.now()}')

    # ---------- Start of variable to set ----------
    # directory to write data to
    # data_dir = Path('/home/joseph/data') # change to /your/path/to/datadir
    data_dir = Path.cwd()
    # Provide IP addresses. Here I have stored them in a JSON file to keep
    # them off GitHub.
    with open('/home/joseph/nymar_zerotier_ips.json','r') as w:
        ips_dict = json.load(w)

    # Set up request parameters here. This is an example only. You may want
    # To use this script as an exmample to build your own code which finds
    # gaps that need filling and then sends the requests.
    gapfile = '/home/eart0593/NYMAR/raw_data/July_Oct_missing_files.pkl'
    with open(gapfile, 'rb') as f:
        request_params = pickle.load(f)

    # request_params = [('OX','NYM2','00','HHN',
    #                   UTCDateTime(2024, 10, 1, 0, 0, 0),
    #                   UTCDateTime(2024,10,2, 0, 0, 0)),
    #                   ('OX','NYM2','00','HHE',
    #                   UTCDateTime(2024, 4, 28, 0, 0, 0),
    #                   UTCDateTime(2024,4, 28, 0, 0, 0)),
    #                   ('OX','NYM3','00','HHN',
    #                   UTCDateTime(2023, 12, 1, 0, 0, 0),
    #                   UTCDateTime(2023,12,2, 0, 0, 0)),
    #                   ('OX','NYM4','00','HHZ',
    #                   UTCDateTime(2024, 10, 1, 0, 0, 0),
    #                   UTCDateTime(2024,10,2, 0, 0, 0))]
    # ----------- End of variables to set ----------

    for params in request_params:
        # params should be form (net, stat, loc, channel)
        log.info(f'Request data for {params}')
        station_ip = ips_dict[params[1]]

        chunked_data_query(station_ip, network=params[0], station=params[1],
                           location=params[2], channel=params[3],
                           starttime=params[4], endtime=params[5],
                           data_dir=data_dir,
                           chunksize=datetime.timedelta(hours=1),
                           buffer=datetime.timedelta(seconds=150))
        gather_chunks(network=params[0], station=params[1],
                      location=params[2], channel=params[3],
                      starttime=params[4], endtime=params[5],
                      data_dir=data_dir, gather_size=datetime.timedelta(days=1)
                      )

    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(f'Runtime is {runtime:4.2f} seconds, or {runtime/60:4.2f} minutes, or {runtime/3600:4.2f} hours')
