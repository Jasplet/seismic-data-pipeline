# data_pipeline.py
# Author: J Asplet - U Oxford
# Date: 23/10/2024
# A set of functional tools to allow a user to make data scraping scripts
# To get data from Guralp Certimus (and Certmius-like) instruments

import logging

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
