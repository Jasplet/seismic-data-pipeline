# data_plumbing.py
# Author: J Asplet - U Oxford
# Date: 23/10/2024. Refactored 16/1/2025

# A set of functional tools to allow a user to make data scraping scripts
# To get data from Guralp Certimus (and Certmius-like) instruments
# Refactoring to generalsie this to include FDSNWS services
# The intended use it the BGS EIDA node which runs FDNWS v 1.1.2
# As of 16/1/2025
import asyncio
import aiohttp
import datetime
import logging
from pathlib import Path

import obspy

from .core_utils import iterate_chunks, form_request

log = logging.getLogger(__name__)

SUPPORTED_FDSNWS = {'BGS-EIDA': 'https://eida.bgs.ac.uk/fdsnws/dataselect/1'}
REQUEST_INTERVAL = 5
# Note for self/interested readers

# The HTTP requests arre not the same for
# FDSNWS and for Certimus-like insturments
# the queries are strucutred differently


def make_urls_instrument(ip_dict,
                         request_params,
                         data_dir='',
                         chunksize=datetime.timedelta(hours=1),
                         buffer=datetime.timedelta(seconds=150)):
    '''
    Makes urls for chunked requests.
    Suitable for larger (or regular) data downloads

    Default chunk size is 1 hour

    Parameters:
    ----------
    ip_dict : dict
        Dictionary of IP addresses of sensors.
        Includes port number if any port forwarding needed
    request_params : list
        List of tuples (net, stat, loc, channel, start, end)
    data_dir : str,
        Directory to write data to
    chunksize : datetime.timedelta
        Size of chunked request
    '''
#   If data dir is empty then use current directory
    if data_dir == '':
        data_dir = Path.cwd()
    urls = []
    outfiles = []

    for params in request_params:
        if len(params) != 6:
            log.error(f'Malformed params {params}')
            raise ValueError('Too few parameters in params')
        network = params[0]
        station = params[1]
        location = params[2]
        channel = params[3]
        start = params[4]
        end = params[5]
        sensor_ip = ip_dict[station]
        if start > end:
            raise ValueError('Start after End!')
        if not isinstance(start,
                          obspy.UTCDateTime
                          ) or not isinstance(end, obspy.UTCDateTime):
            raise TypeError("Start and end times must be of type UTCDateTime.")

        for chunk_start in iterate_chunks(params[4], params[5], chunksize):
            # Add 150 seconds buffer on either side
            query_start = chunk_start - buffer
            query_end = chunk_start + chunksize + buffer
            year = chunk_start.year
            month = chunk_start.month
            day = chunk_start.day
            hour = chunk_start.hour
            mins = chunk_start.minute
            sec = chunk_start.second

            ddir = Path(f'{data_dir}/{year}/{month:02d}/{day:02d}')
            ddir.mkdir(exist_ok=True, parents=True)
            seed_params = f'{network}.{station}.{location}.{channel}'
            date = f'{year}{month:02d}{day:02d}'
            time = f'{hour:02d}{mins:02d}{sec:02d}'
            timestamp = f'{date}T{time}'
            outfile = ddir / f"{seed_params}.{timestamp}.mseed"
            if outfile.is_file():
                log.info(f'Data chunk {outfile} exists')
                continue
            else:
                request_url = form_request(sensor_ip,
                                           network,
                                           station,
                                           location,
                                           channel,
                                           query_start,
                                           query_end
                                           )
                urls.append(request_url)
                outfiles.append(outfile)

    return urls, outfiles


def make_urls_fdsnws(base_url,
                     request_params,
                     data_dir,
                     chunksize=datetime.timedelta(hours=24),
                     buffer=datetime.timedelta(seconds=150)):
    '''
    Makes urls for chunked requests.
    Suitable for larger (or regular) data downloads


    Parameters:
    ----------
    ip_dict : dict
        Dictionary of IP addresses of sensors.
        Includes port number if any port forwarding needed
    request_params : list
        List of tuples (net, stat, loc, channel, start, end)
    data_dir : str,
        Directory to write data to
    chunksize : datetime.timedelta
        Size of chunked request
    '''

    urls = []
    outfiles = []

    for params in request_params:
        if len(params) != 6:
            log.error(f'Malformed params {params}')
            raise ValueError('Too few parameters in params')
        network = params[0]
        station = params[1]
        location = params[2]
        channel = params[3]
        start = params[4]
        end = params[5]
        if start > end:
            raise ValueError('Start after End!')
        if not isinstance(start,
                          obspy.UTCDateTime
                          ) or not isinstance(end, obspy.UTCDateTime):
            raise TypeError("Start and end times must be of type UTCDateTime.")

        for chunk_start in iterate_chunks(params[4], params[5], chunksize):
            # Add 150 seconds buffer on either side
            query_start = chunk_start - buffer
            query_end = chunk_start + chunksize + buffer
            year = chunk_start.year
            month = chunk_start.month
            day = chunk_start.day
            hour = chunk_start.hour
            mins = chunk_start.minute
            sec = chunk_start.second

            ddir = Path(f'{data_dir}/{year}/{month:02d}/{day:02d}')
            ddir.mkdir(exist_ok=True, parents=True)
            seed_params = f'{network}.{station}.{location}.{channel}'
            date = f'{year}{month:02d}{day:02d}'
            time = f'{hour:02d}{mins:02d}{sec:02d}'
            timestamp = f'{date}T{time}'
            outfile = ddir / f"{seed_params}.{timestamp}.mseed"
            if outfile.is_file():
                log.info(f'Data chunk {outfile} exists')
                continue
            else:
                seed_params1 = f'network={network}&station={station}'
                seed_params2 = f'&location={location}&channel={channel}'
                time1 = f'starttime={query_start.strftime("%y-%m-%dT%H:%M:%S")}'
                time2 = f'endtime={query_end.strftime("%y-%m-%dT%H:%M:%S")}'
                query = f'{seed_params1}{seed_params2}{time1}{time2}'
# FDSNWS Dataselect query take the form
#  http://service.iris.edu/fdsnws/dataselect/1/query?network=IU&station=COLA&
# starttime=2012-01-01T00:00:00&endtime=2012-01-01T12:00:00
                request_url = f'{base_url}/query?{query}&nodata=404'
                urls.append(request_url)
                outfiles.append(outfile)

    return urls, outfiles

# Core asynchonrous functions. Using these is
# better (i.e., faster) that making synchronous
# requests.


def get_data(request_params,
             data_dir=Path.cwd(),
             chunksize=datetime.timedelta(hours=1),
             buffer=datetime.timedelta(seconds=120),
             is_fdsnws=False,
             fdsnws_name='BGS-EIDA',
             ips_dict=None,
             **kwargs):

    if is_fdsnws:
        if fdsnws_name in SUPPORTED_FDSNWS.keys():
            log.info(f'Make request to {SUPPORTED_FDSNWS["fdsnws_name"]}')
            get_data_fdsnws()
        else:
            raise ValueError(f'{fdsnws_name} not supported!')
#   Assume request is certiums-like if False
    elif ips_dict is not None:
#   Hooray we have IPs
        log.info(f'Make requests to provided IPs for instruments')
        if 'n_async_requests' in kwargs:
            log.warning('Changing number of request per station')
            n_async = kwargs['n_async_requests']
        else:
            n_async = 3

        get_data_from_instruments(request_params,
                                  ips_dict,
                                  data_dir,
                                  chunksize,
                                  buffer,
                                  n_async_requests=n_async)
    else:
        raise ValueError('You need to provide IPs for the instruments!')


async def get_data_from_instruments(request_params,
                                    station_ips,
                                    data_dir,
                                    chunksize,
                                    buffer,
                                    n_async_requests):
    '''
    Requests waveform data from Certimus-like instruments. These
    are done asynchronously to improve performence. For a Certimus-like
    instrument the MAX number of requests it can handle concurrently is 3.

    This function also uses Semaphores to send async requests to each sensor
    concurrently (i.e., we send 3 requests to each station).

    '''
    # Make all urls to query.
    urls, outfiles = make_urls_instrument(station_ips,
                                          request_params,
                                          data_dir,
                                          chunksize,
                                          buffer)

    log.info(f'There are {len(urls)} requests to make')
    requests_by_ip = {}
    for url, outfile in zip(urls, outfiles):
        sensor_ip = url.split("/")[2]
        if sensor_ip not in requests_by_ip:
            requests_by_ip[sensor_ip] = []
        requests_by_ip[sensor_ip].append((url, outfile))
    # Set up asyncio's HTTP client session
    semaphores = {sensor_ip: asyncio.Semaphore(n_async_requests)
                  for sensor_ip in requests_by_ip}  # Adjust as needed
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Limit the number of simultaneous requests
        # Adjust based on seismometer capacity
        for sensor_ip, reqs in requests_by_ip.items():
            semaphore = semaphores[sensor_ip]
            for request_url, outfile in reqs:
                task = asyncio.create_task(make_async_request(session,
                                           semaphore,
                                           request_url,
                                           outfile)
                                           )
                tasks.append(task)
        await asyncio.gather(*tasks)


async def get_data_fdsnws(request_params,
                          base_url,
                          chunksize=datetime.timedelta(hours=24),
                          buffer=datetime.timedelta(seconds=60),
                          n_async_requests=50,
                          data_dir=None,
                          return_ppsd=False):
    '''
    Function to make HTTP requests to a server running
    FDSN WebServices
    '''
    if data_dir is None:
        if return_ppsd:

            log.info('Data will returned as PPSD')
        else:
            raise ValueError('No data directory to write files to!!')

    urls, outfiles = make_urls_fdsnws(base_url,
                                      request_params,
                                      data_dir,
                                      chunksize=chunksize,
                                      buffer=buffer)

    log.info(f'There are {len(urls)} requests to make')

    # Set up asyncio's HTTP client session
    semaphore = asyncio.Semaphore(n_async_requests)
    await asyncio.sleep(REQUEST_INTERVAL)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for request_url, outfile in zip(urls, outfiles):
            if return_ppsd:
                task = asyncio.create_task(make_async_ppsd(session,
                                                           semaphore,
                                                           request_url,
                                                           outfile
                                                           )
                                )
            else:
                task = asyncio.create_task(make_async_request(session,
                                                              semaphore,
                                                              request_url,
                                                              outfile
                                                              )
                                          )
            tasks.append(task)
        await asyncio.gather(*tasks)

# To make this run asynchronously we need to make a list of the individual
# request URLS


async def make_async_request(session, semaphore, request_url, outfile):
    '''
    Function to actually make the HTTP GET request from the Certimus

    Gets one file, which corresponds to the request_url and writes
    it out as miniSEED
    to the specified outfile

    Parameters:
    ----------
    request_url : str
        The formed request url in the form:
        http://{sensor_ip}/data?channel={net_code}.{stat_code}.{loc_code}.{channel}&from={startUNIX}&to={endUNIX}
    '''
    async with semaphore:
        try:
            async with session.get(request_url) as resp:
                print(f'Request at {datetime.datetime.now()}')
                print(request_url)
                # Raise HTTP error for 4xx/5xx errors
                resp.raise_for_status()

                # Read binary data from the response
                data = await resp.read()
                if len(data) == 0:
                    log.error('Request is empty!' +
                              'Won’t write a zero byte file.')
                    return
                # Now write data
                with open(outfile, "wb") as f:
                    f.write(data)
                log.info(f'Successfully wrote data to {outfile}')

        except aiohttp.ClientResponseError as e:
            log.error(f'Client error for {request_url}: {e}')
            # Additional handling could go here, like retry logic
        except Exception as e:
            log.error(f'Unexpected error for {request_url}: {e}')
        return

async def make_async_ppsd(session, semaphore, request_url, outfile):
    '''
    Functions basically the same as make async reuqest, BUT 
    isntead of writing out a miniseed file we read the file 
    into memory and then calculate a ppsd

    Why? Because for UK Network PPSDs i don't want to aggregate
    all the UK data. 

    Parameters:
    ----------
    request_url : str
        The formed request url in the form:
        http://{sensor_ip}/data?channel={net_code}.{stat_code}.{loc_code}.{channel}&from={startUNIX}&to={endUNIX}
    '''
    async with semaphore:
        try:
            async with session.get(request_url) as resp:
                print(f'Request at {datetime.datetime.now()}')
                print(request_url)
                # Raise HTTP error for 4xx/5xx errors
                resp.raise_for_status()

                # Read binary data from the response
                data = await resp.read()
                if len(data) == 0:
                    log.error('Request is empty!' +
                              'Won’t write a zero byte file.')
                    return
                


        except aiohttp.ClientResponseError as e:
            log.error(f'Client error for {request_url}: {e}')
            # Additional handling could go here, like retry logic
        except Exception as e:
            log.error(f'Unexpected error for {request_url}: {e}')
        return


# core synchronous functions
# These functions are deprecated but i will
# leave them here for users that may want to use them
# these functions are still tested
