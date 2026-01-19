# data_pipeline.py
# Author: J Asplet - U Oxford
# Date: 23/10/2024
# A set of functional tools to allow a user to make data scraping scripts
# To get data from Guralp Certimus (and Certmius-like) instruments

import logging
from pathlib import Path

import obspy

log = logging.getLogger(__name__)


# Utility functions
def find_data_gaps(RequestParams, PipelineConfig, outfile=None):
    """
    Find gaps in (expected) continuous passive seismic data stored in
    miniSEED format in a given directory structure.

    Currenly assumes an internal "Oxford" directory/ filenaming convention of
    {data_dir}/{year}/{month}/{day}/{network}.{station}.{location}.{channel}.{timestamp}.mseed

    Note thats this function assumes that the chunksize is constant throughout
    the dataset and uses this to define gaps.

    """
    if not PipelineConfig.data_dir.exists():
        raise FileNotFoundError(f"Data directory {PipelineConfig.data_dir} not found!")

    data_gaps = []
    # Iterate over days
    for h in iterate_chunks(
        RequestParams.start, RequestParams.end, PipelineConfig.chunksize
    ):
        year = h.year
        month = h.month
        day = h.day
        hour = h.hour
        mins = h.minute
        secs = h.second
        ddir = Path(f"{PipelineConfig.data_dir}/{year}/{month:02d}/{day:02d}")
        timestamp = f"{year}{month:02d}{day:02d}T{hour:02d}{mins:02d}{secs:02d}"

        for params in PipelineConfig.expected_file_params:
            seedparams = f"{params[0]}.{params[1]}.{params[2]}.{params[3]}"
            fname = f"{seedparams}.{timestamp}.mseed"
            f = ddir / fname
            if f.is_file():
                # could add check that miniseed file is as we expect
                st = obspy.read(f)
                gaps = st.get_gaps(min_gap=1)
                if len(gaps) > 0:
                    log.info(f"{f} has {len(gaps)} data gaps")
                    gap_params = (
                        params[0],
                        params[1],
                        params[2],
                        params[3],
                        h,
                        h + PipelineConfig.chunksize,
                    )
                    data_gaps.append(gap_params)
                else:
                    continue
            else:
                log.info(f"{f} is missing")
                gap_params = (
                    params[0],
                    params[1],
                    params[2],
                    params[3],
                    h,
                    h + PipelineConfig.chunksize,
                )
                data_gaps.append(gap_params)
    return data_gaps


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
