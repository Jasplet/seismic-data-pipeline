# ----- Script to find gaps in downloaded data

# Here "Gaps" as assumed to be missing day files
# It is still possible that there may be gaps within
# the files - check your logs, these will have been logged

from obspy import UTCDateTime
import itertools
import pickle
from pathlib import Path
from datetime import timedelta
from data_pipeline import iterate_chunks

# Seedlink Parameters
network = ["OX"]
station_list = ['NYM1', 'NYM2', 'NYM3', 'NYM4',
                'NYM5', 'NYM6', 'NYM7', 'NYM8']
channels = ["HHZ",  "HHN", "HHE"]
location = ["00"]

expected_file_params = itertools.product(network, station_list,
                                         location, channels)


start = UTCDateTime(2024, 7, 1)
end = UTCDateTime(2024, 10, 31)

dpath = Path('/home/eart0593/NYMAR/raw_data')
print(f'Assuming data is in: {dpath}')

outfile = 'July_Oct_missing_files.pkl'

data_gaps = []

chunksize = timedelta(days=1)

# Iterate over days
for h in iterate_chunks(start, end, chunksize):
    year = h.year
    month = h.month
    day = h.day
    hour = h.hour
    ddir = Path(f'{dpath}/{year}/{month:02d}/{day:02d}')
    timestamp = f'{year}{month:02d}{day:02d}T*'

    for params in expected_file_params:
        print(h)
        seedparams = f'{params[0]}.{params[1]}.{params[2]}.{params[3]}'
        fname = f'{seedparams}.{timestamp}.mseed'
        f = ddir / fname
        if f.is_file():
            # could add check that miniseed file is as we expect
            continue
        else:
            gap_params = (params[0], params[1], params[2],
                          params[3], h, h + timedelta(chunksize))
            data_gaps.append(gap_params)
    print(h)

with open(f'{dpath}/{outfile}', 'wb') as f:
    pickle.dump(data_gaps, f)
