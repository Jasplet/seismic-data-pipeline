# Author: J Asplet, U of Oxford, 20/11/2023

## Python script to remotely query data from NYMAR array stations
## We are using Wget as implemented in the requests library 

from pathlib import Path
from obspy import UTCDateTime
import timeit
import datetime
import requests
import logging
log = logging.getLogger(__name__)

# functions

def hour_by_hour_query(request, query_date):

    hour_shift = datetime.timedelta(hours=1)
    end = query_date + datetime.timedelta(days=1)
    chunk_start = query_date 
    chunk_end = query_date + hour_shift
    while chunk_start < end:
        query_start = chunk_start - 150
        query_end = chunk_end + 150
        startUNIX = query_start.timestamp
        endUNIX = query_end.timestamp
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
                timer_start = timeit.default_timer()
                #time.sleep(5) # for testing directory creation
            
                r = requests.get(f"http://{station_ip}:8080/data?channel={request}&from={startUNIX}&to={endUNIX}", f"{outfile}")
                with open(outfile, "wb") as f:
                    f.write(r.content)
            
                timer_end = timeit.default_timer()
                runtime = timer_end - timer_start
                log.info(f'Request took {runtime:4.2f} seconds')
            except:
                log.error(f'Unable to request hour {hour}')

        chunk_start += day_shift
        # Iterate otherwise we will have an infinite loop!

# __main__ 

if __name__ == '__main__':
    script_start = timeit.default_timer()
    logging.basicConfig(filename='/home/joseph/logs/nymar_backfill.log', level=logging.INFO)
    log.info(f'Starting download. Time is {datetime.datetime.now()}')

    nym_zt_ips = {'NYM1':'172.24.59.19', 'NYM2':'172.24.239.162',
                'NYM3':'172.24.40.146', 'NYM4':'172.24.77.181',
                'NYM5':'172.24.43.200', 'NYM6':'172.24.150.216',
                'NYM7':'172.24.194.185', 'NYM8':'172.24.3.251' }

    ######### Start of variable to set #############

    network = "OX"
    station_list = ['NYM2','NYM3','NYM5','NYM6','NYM7','NYM8']
    #station_list = ['NYM2']
    channels = ["HHZ",  "HHN", "HHE"] 
    #SET TO CORRECT CODE. should be '00' for veloctity data
    # will be somehing different for voltage, check status page (https://{your-ip-here})
    location = "00" 
    # try to get previous 2 days of data (current day will not be available)
    start = UTCDateTime(2024, 10, 13, 0, 0, 0)
    end = UTCDateTime(2024, 10, 16, 0, 0, 0)
    log.info(f'Query start time: {start}')
    log.info(f'Query end time: {end}')
    # some test start/ends that are 'safe' for testing the directory 
    # creation and file handling. uncomment if needed.
    # also dont forget to comment OUT the wget command as this data
    # does not (or should not) exist, reducing the risk we accidentally remove data files
    # start = UTCDateTime("3023-01-01T00:00:00")
    # end = UTCDateTime("3023-01-02T00:00:00")
    ########### End of variables to set ###########

    for station in station_list:
        log.info(f'Request data for {station}')
        day_shift = datetime.timedelta(days=1)
        chunk_start = start
        chunk_end = start + day_shift
        station_ip = nym_zt_ips[station]
        # path_cwd = Path('/Volumes/NYMAR_DATA/NYMAR/data_dump') / station
        path_cwd = Path('/home/joseph/data')
        # keep requesting hour chunks of data until 
        # query start time reaches the end time
        while chunk_start < end:
            chunk_end = chunk_start + day_shift
            # add a 2.5 minute buffer either side of date query to reduce gap risk
            query_start = chunk_start - 150
            query_end = chunk_end + 150
            startUNIX = query_start.timestamp
            endUNIX = query_end.timestamp
            year = chunk_start.year
            month = chunk_start.month
            day = chunk_start.day
            hour = chunk_start.hour
            ddir = Path(f'{path_cwd}/{year}/{month:02d}/{day:02d}')
            ddir.mkdir(exist_ok=True, parents=True)
            for channel in channels:
                request = f"{network}.{station}.{location}.{channel}"
                # Make filename to wirte out to
                outfile = Path(ddir, f"{request}.{year}{month:02d}{day:02d}T{hour:02d}0000.mseed")
                #Test if we have already downloaded this chunk
                if outfile.is_file():
                    log.info(f'Data chunk {outfile} exists')
                else:
                    try:
                        timer_start = timeit.default_timer()
                        r = requests.get(f"http://{station_ip}:8080/data?channel={request}&from={startUNIX}&to={endUNIX}", f"{outfile}")
                        timer_end = timeit.default_timer()
                        runtime = timer_end - timer_start
                        log.info(f'Request took {runtime:4.2f} seconds')
                    except Exception as e:
                        log.exception(f'Handling Exception {e}. Try hourly chunks')
                        try:
                           hour_by_hour_query(request, chunk_start)
                        except:
                            log.error(f'Exception {e} could not be handled, move to next day')

                        chunk_start +=  day_shift
                        continue

                # Iterate otherwise we will have an infinite loop!
            chunk_start += day_shift
            
    # Get dataless (not really neccesary but its nice)

    script_end = timeit.default_timer()
    runtime = script_end - script_start

    log.info(f'Runtime is {runtime:4.2f} seconds, or {runtime/60:4.2f} minutes, or {runtime/3600:4.2f} hours')
