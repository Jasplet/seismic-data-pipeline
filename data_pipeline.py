# data_pipepline.py 
# Author: J Asplet - U Oxford
# Date: 23/10/2024
# A set of functional tools to allow a user to make data scraping scripts
# To get data from Guralp Certimus (and Certmius-like) instruments


from pathlib import Path
import glob
import obspy
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
        outfile = ddir / f"{seed_params}.{year}{month:02d}{day:02d}T{hour:02d}{mins:02d}{sec:02d}.mseed"
        if outfile.is_file():
            log.info(f'Data chunk {outfile} exists')
            continue
        else:
            request_url = form_request(sensor_ip, network, station, location,
                                       channel, query_start, query_end)
            make_request(request_url, outfile)

    return

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
        log.error(f'GET requst failed with error {e}')
    
    return

def gather_chunks(network, station, location, channel,
                  starttime, endtime, data_dir, gather_size=datetime.timedelta(days=1),
                  file_format='MSEED'):
    '''
    Function to gather all chunks of data pulled from server and gather then into
    larger files

    Parameters:
    ----------
    starttime : obspy.UTCDateTime
        Start time of period to gather
    endtime : obspy.UTCDateTime
        End time of period to gather
    gather_size : datetime.timedelta
        Time period of gathers. Default is one day (i.e, all data in a day will be gathered)
    '''
    if data_dir == '':
        data_dir = Path.cwd()

    for gather_start in iterate_chunks(starttime, endtime, gather_size):
        year = gather_start.year
        month = gather_start.month
        day = gather_start.day
        hour = gather_start.hour
        mins = gather_start.minute
        sec = gather_start.second
        ddir = Path(f'{data_dir}/{year}/{month:02d}/{day:02d}')
        seed_params = f'{network}.{station}.{location}.{channel}'
        if gather_size.days == 1:
            # Want to read all files in that day
            files = ddir / f"{seed_params}.{year}{month:02d}{day:02d}T*.mseed"
        elif gather_size.second == 3600:
            # Hour gather 
            files = ddir / f"{seed_params}.{year}{month:02d}{day:02d}T{hour:02d}*.mseed"
        elif gather_size.second == 60:
            # Minute gather 
            files = ddir / f"{seed_params}.{year}{month:02d}{day:02d}T{hour:02d}{mins:02d}*.mseed"
        else:
            raise ValueError(f'Gather {gather_size} not supported. Must be day, hour, or minute.')
        
        gathered_st = obspy.read(files)
        # Merge traces with no gap filling (this is the default beavhiour of st.merge())
        gathered_st.merge(method=0, fill_value=None)
        log.info(f'Merge complete for files on {gather_start}, gather size {gather_size}')
        # Now clean up the chunked_files and write out our shiny new one!
        for f in glob.glob(files):
            path_f = Path(f)
            path_f.unlink(missing_ok=True)
        # Write out. Convention here is that file names describe seed codes and the START time of the file.
        # I will try to support writing as all file types support by obspy.
        # N.B for SAC files this will default to small-endian files and will need byte-swapping
        # for use with MacSAC 
        format_ext = file_format.lower()
        outfile = ddir / f"{seed_params}.{year}{month:02d}{day:02d}T{hour:02d}{mins:02d}{sec:02d}.{format_ext}"
        gathered_st.write(outfile, format=file_format)
