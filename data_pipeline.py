# data_pipepline.py 
# Author: J Asplet - U Oxford
# Date: 23/10/2024
# A set of functional tools to allow a user to make data scraping scripts
# To get data from Guralp Certimus (and Certmius-like) instruments


from pathlib import Path
from obspy import UTCDateTime
import obspy
import timeit
import datetime
import requests
import logging
log = logging.getLogger(__name__)

# functions

def form_request(sensor_ip, network, station, location, channel, starttime, endtime):
    '''
    Form the request url 

    Parameters:
    ----------
    sensor_ip : str
        IP address of sensor. Includes port no if any port forwarding needed
    network : str
        Network code
    station : str
        Station code
    location : str
        Location code
    channel : str
        Channel code
    starttime : obspy.UTCDateTime
        Start time of request
    endtime : obspy.UTCDataTime
        End time of request
    '''
    if starttime > endtime:
        raise ValueError('Start of request if before the end!')

    seed_params = f'{network}.{station}.{location}.{channel}'
    request = f'http://{sensor_ip}/data?channel={seed_params}&from={starttime.timestamp}&to={endtime.timestamp}'

    return request

def iterate_chunks(start, end, chunksize):

    chunk_start = start
    while chunk_start < end:
        yield chunk_start
        chunk_start += chunksize

def chunked_data_query(sensor_ip, network, station, location,
                       channel, starttime, endtime, data_dir='',  chunksize=datetime.timedelta(hours=1),
                       buffer=datetime.timedelta(seconds=150)):
    '''
    Make chunked requests. Suitable for larger (or regular) data downloads

    Default chunk size is 1 hour

    Parameters:
    ----------
    sensor_ip : str
        IP address of sensor. Includes port no if any port forwarding needed
    network : str
        Network code
    station : str
        Station code
    location : str
        Location code
    channel : str
        Channel code
    starttime : obspy.UTCDateTime
        Start time of request
    endtime : obspy.UTCDataTime
        End time of request
    data_dir : str,
        Directory to write data to 
    chunksize : datetime.timedelta
        Size of chunked request
    '''
    #If data dir is empty then use current directory
    if data_dir == '':
        data_dir = Path.cwd()

    for chunk_start in iterate_chunks(starttime, endtime, chunksize):
        # Add 150 seconds buffer on either side
        query_start = chunk_start - buffer
        query_end = chunk_start + buffer
        year = chunk_start.year
        month = chunk_start.month
        day = chunk_start.day
        hour = chunk_start.hour
        mins = chunk_start.minute
        sec = chunk_start.second

        ddir = Path(f'{data_dir}/{year}/{month:02d}/{day:02d}')
        seed_params = f'{network}.{station}.{location}.{channel}'
        outfile = Path(ddir, f"{seed_params}.{year}{month:02d}{day:02d}T{hour:02d}{mins:02d}{sec:02d}_.mseed")
        if outfile.is_file():
            log.info(f'Data chunk {outfile} exists')
            continue
        else:
            request_url = form_request(sensor_ip, network, station, location,
                                       channel, starttime, endtime)
            make_request(request_url, outfile)

        # Iterate otherwise we will have an infinite loop!

def make_request(request_url, outfile):
    '''
    Function to actually make the HTTP GET request from the Certimus

    Gets one file, which corresponds to the request_url and writes it out as miniSEED
    to the specified outfile

    Parameters:
    ----------
    request_url : str
        The formed request url in the form:
        http://{sensor_ip}/data?channel={net_code}.{stat_code}.{loc_code}.{channel}&from={startUNIX}&to={endUNIX}
    outfile : str
        Filename (including full path) to write out to
    '''
    try:
        r = requests.get(request_url, stream=True)
        log.info(f'Request elapsed time {r.elapsed}')
        #raise HTTP error for 4xx/5xx errors
        r.raise_for_status()
        # Check if we get data 
        if int(r.headers.get('Content-Length', 0)) == 0:
            log.error('Request is empty! Won’t write a zero byte file.')
            return
        # Now write data
        with open(outfile, "wb") as f:
            f.write(r.content)
    except requests.exceptions.RequestException as e:
        log.error('GET requst failed with error {e}')
    
    return