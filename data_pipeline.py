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

def form_request(sensor_ip, network, station, location, channel, starttime, endtime)
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
    
    seed_params = f'{network}.{station}.{location}.{channel}'
    request = f'http://{sensor_ip}/data?channel={seed_params}&from={starttime.timestamp}&to={endtime.timestamp}'

    return request

def chunked_data_query(request, query_date):

    hour_shift = datetime.timedelta(hours=1)
    end = query_date + datetime.timedelta(days=1)
    chunk_start = query_date 
    chunk_end = query_date + hour_shift
    while chunk_start < end:
        query_start = chunk_start - 150
        query_end = chunk_end + 150
        year = chunk_start.year
        month = chunk_start.month
        day = chunk_start.day
        hour = chunk_start.hour

        ddir = Path(f'{path_cwd}/{year}/{month:02d}/{day:02d}')
        outfile = Path(ddir, f"{request}.{year}{month:02d}{day:02d}T{hour:02d}0000_tmp.mseed")
        if outfile.is_file():
            log.info(f'Data chunk {outfile} exists')
        else:
            try:
                make_request(station_ip, request, query_start, query_end)
            except:
                log.error(f'Unable to request hour {hour}')

        chunk_start += day_shift
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
        log.error('GET rqeust failed with error {e}')
    
    return