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

script_start = timeit.default_timer()
log.info(f'Starting download. Time is {datetime.now()}')

nym_zt_ips = {'NYM1':'172.24.59.19', 'NYM2':'172.24.239.162',
              'NYM3':'172.24.40.146', 'NYM4':'172.24.77.181',
              'NYM5':'172.24.43.200', 'NYM6':'172.24.150.216',
              'NYM7':'172.24.194.185', 'NYM8':'172.24.3.251' }


######### Start of variable to set #############

network = "OX"
station_list = ['NYM1','NYM2','NYM3','NYM4','NYM5','NYM6','NYM7','NYM8']
#add channel code ME4  for voltage
# channels MUST be a list, even it just has one item
# e.g. ['ME4']
channels = ["HHZ",  "HHN", "HHE"] 
#SET TO CORRECT CODE. should be '00' for veloctity data
# will be somehing different for voltage, check status page (https://{your-ip-here})
location = "00" 
# set start / end data. 
#for NYMAR earliest avaialable date is 2023-09-29T00:00:00
# data has been collected (from the field) up to 2024-03-20T00:00:00
start = UTCDateTime("2023-11-02T00:00:00")
end = UTCDateTime("2023-11-03T00:00:00")
# some test start/ends that are 'safe' for testing the directory 
# creation and file handling. uncomment if needed.
# also dont forget to comment OUT the wget command as this data
# does not (or should not) exist, reducing the risk we accidentally remove data files
# start = UTCDateTime("3023-01-01T00:00:00")
# end = UTCDateTime("3023-01-02T00:00:00")
########### End of variables to set ###########


for station in station_list:
    day_shift = datetime.timedelta(days=1)
    chunk_start = start
    chunk_end = start + day_shift
    station_ip = nym_zt_ips[station]
    # path_cwd = Path('/Volumes/NYMAR_DATA/NYMAR/data_dump') / station
    path_cwd = Path('/Users/eart0593/Projects/Agile/NYMAR/data/test/2023/11/02')
    # keep requesting hour chunks of data until 
    # query start time reaches the end time
    while chunk_start < end:
        chunk_end = chunk_start + day_shift
        # add a 2.5 minute buffer either side of date query to reduce gap risk
        query_start = chunk_start - 150
        query_end = chunk_end + 150
        startUNIX = query_start.timestamp
        endUNIX = query_end.timestamp
        # We use the 'startUNIX'&'endUNIX' to pull the
        # data from the Certimus
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
                print(f'Data chunk {outfile} exists')
                print('Continue to next . . .')
            else:
                print(f"\n http://{station_ip}:8080/data?channel={request}&from={startUNIX}&to={endUNIX}")
                print(outfile)
                timer_start = timeit.default_timer()
                #time.sleep(5) # for testing directory creation
                r = requests.get(f"http://{station_ip}:8080/data?channel={request}&from={startUNIX}&to={endUNIX}", f"{outfile}")
                with open(outfile, "wb") as f:
                    f.write(r.content)
                timer_end = timeit.default_timer()
                runtime = timer_end - timer_start
                print(f'\n Request for hour chunk from {request} at {chunk_start} complete')
                print(f'Request took {runtime:4.2f} seconds')
            # Iterate otherwise we will have an infinite loop!
        chunk_start += day_shift
        
# Get dataless (not really neccesary but its nice)

script_end = timeit.default_timer()
runtime = script_end - script_start
print(f'Done download for {network}.{station}')
print(f'Runtime is {runtime:4.2f} seconds')
print(f'Or {runtime/60:4.2f} minutes')
print(f'Or {runtime/3600:4.2f} hours')