# ----- Script to find gaps in downloaded data

# Here "Gaps" as assumed to be missing day files
# It is still possible that there may be gaps within
# the files - check your logs, these will have been logged
import itertools
import pickle
from datetime import timedelta
from pathlib import Path

import obspy
from obspy import UTCDateTime

from pipeline.utils import iterate_chunks

# Seedlink Parameters
network = ["OX"]
station_list = ["NYM1", "NYM2", "NYM3", "NYM4", "NYM5", "NYM6", "NYM7", "NYM8"]
station_list = ["NYM1"]
channels = ["HHZ", "HHN", "HHE"]
location = ["00"]

expected_file_params = [
    p for p in itertools.product(network, station_list, location, channels)
]


start = UTCDateTime(2024, 11, 1)
end = UTCDateTime(2025, 1, 1)

dpath = Path("/Volumes/NYMAR_DATA/NYMAR/data_dump/NYM1")
print(f"Assuming data is in: {dpath}")

outfile = "NYM1_missing_files_Nov_1st_thru_Dec31st_2024.pkl"

data_gaps = []

chunksize = timedelta(days=1)

# Iterate over days
for h in iterate_chunks(start, end, chunksize):
    year = h.year
    month = h.month
    day = h.day
    hour = h.hour
    ddir = Path(f"{dpath}/{year}/{month:02d}/{day:02d}")
    timestamp = f"{year}{month:02d}{day:02d}T{hour:02d}0000"

    for params in expected_file_params:
        seedparams = f"{params[0]}.{params[1]}.{params[2]}.{params[3]}"
        fname = f"{seedparams}.{timestamp}.mseed"
        f = ddir / fname
        if f.is_file():
            # could add check that miniseed file is as we expect
            st = obspy.read(f)
            gaps = st.get_gaps(min_gap=1)
            if len(gaps) > 0:
                print(f"{f} has {len(gaps)} data gaps")
                gap_params = (
                    params[0],
                    params[1],
                    params[2],
                    params[3],
                    h,
                    h + chunksize,
                )
                data_gaps.append(gap_params)
            else:
                continue
        else:
            print(f"{f} is missing")
            gap_params = (params[0], params[1], params[2], params[3], h, h + chunksize)
            data_gaps.append(gap_params)

print(f"There are {len(data_gaps)} files missing or with gaps!")
print(f"From {start} to {end}")

with open(f"{dpath}/{outfile}", "wb") as f:
    pickle.dump(data_gaps, f)
