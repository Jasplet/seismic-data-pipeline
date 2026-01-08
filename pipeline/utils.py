# data_pipeline.py
# Author: J Asplet - U Oxford
# Date: 23/10/2024
# A set of functional tools to allow a user to make data scraping scripts
# To get data from Guralp Certimus (and Certmius-like) instruments

import datetime
import logging
from pathlib import Path

import obspy

log = logging.getLogger(__name__)

# Utility functions


def iterate_chunks(start, end, chunksize):
    """
    Function that makes an interator between two dates (start, end)
    in intervals of <chunksize>.

    Parameters:
    ----------
    start : UTCDateTime
        start time
    end : obspy.UTCDateTime
        end time
    chunksize : datetime.timedelta
        timespan of chunks to split timespan into and iterate over
    """
    chunk_start = start
    while chunk_start < end:
        yield chunk_start
        chunk_start += chunksize


def form_request(
    sensor_ip: str,
    network: str,
    station: str,
    location: str,
    channel: str,
    starttime: obspy.UTCDateTime,
    endtime: obspy.UTCDateTime,
) -> str:
    """
    Creates a request URL for the Certimus HTTP API for given
    SEED parameters and a start/end time.

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
    """

    if starttime > endtime:
        raise ValueError("Start of request if before the end!")

    seed_params = f"{network}.{station}.{location}.{channel}"
    timeselect = f"from={starttime.timestamp}&to={endtime.timestamp}"
    request = f"http://{sensor_ip}/data?channel={seed_params}&{timeselect}"

    return request


def group_urls_by_station(urls, outfiles):
    """
    Groups urls and outfiles by station IP address.

    Parameters:
    ----------
    urls : list
        List of request URLs
    outfiles : list
        List of output file paths

    Returns:
    -------
    dict
        Dictionary with station IPs as keys and list of (url, outfile) tuples as values
    """
    requests_by_ip = {}
    for url, outfile in zip(urls, outfiles):
        sensor_ip = url.split("/")[2].split(":")[0]  # Extract IP from URL
        if sensor_ip not in requests_by_ip:
            requests_by_ip[sensor_ip] = []
        requests_by_ip[sensor_ip].append((url, outfile))
    return requests_by_ip


def make_urls(
    ip_dict,
    request_params,
    data_dir="",
    chunksize=datetime.timedelta(hours=1),
    buffer=datetime.timedelta(seconds=150),
):
    """
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
    """
    #   If data dir is empty then use current directory
    if data_dir == "":
        data_dir = Path.cwd()
    urls = []
    outfiles = []

    for params in request_params:
        if len(params) != 6:
            log.error(f"Malformed params {params}")
            raise ValueError("Too few parameters in params")
        network = params[0]
        station = params[1]
        location = params[2]
        channel = params[3]
        start = params[4]
        end = params[5]
        sensor_ip = ip_dict[station]
        if start > end:
            raise ValueError("Start after End!")
        if not isinstance(start, obspy.UTCDateTime) or not isinstance(
            end, obspy.UTCDateTime
        ):
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

            ddir = Path(f"{data_dir}/{year}/{month:02d}/{day:02d}")
            ddir.mkdir(exist_ok=True, parents=True)
            seed_params = f"{network}.{station}.{location}.{channel}"
            date = f"{year}{month:02d}{day:02d}"
            time = f"{hour:02d}{mins:02d}{sec:02d}"
            timestamp = f"{date}T{time}"
            outfile = ddir / f"{seed_params}.{timestamp}.mseed"
            if outfile.is_file():
                log.info(f"Data chunk {outfile} exists")
                continue
            else:
                request_url = form_request(
                    sensor_ip,
                    network,
                    station,
                    location,
                    channel,
                    query_start,
                    query_end,
                )
                urls.append(request_url)
                outfiles.append(outfile)

    return urls, outfiles
