# ----- Script to find gaps in downloaded data

# Here "Gaps" as assumed to be missing day files
# It is still possible that there may be gaps within
# the files - check your logs, these will have been logged
import pickle
from datetime import timedelta
from pathlib import Path

from obspy import UTCDateTime

from pipeline.config import PipelineConfig, RequestParams
from pipeline.utils import find_data_gaps

# Seedlink Parameters
network = ["OX"]
stations = ["NYM1", "NYM2", "NYM3", "NYM4", "NYM5", "NYM6", "NYM7", "NYM8"]
channels = ["HHZ", "HHN", "HHE"]
location = ["00"]

start = UTCDateTime(2024, 11, 1)
end = UTCDateTime(2025, 1, 1)

Config = PipelineConfig(
    data_dir=Path("/path/to/top/level/data_dir"),  # change this to your data dir
    chunksize_hours=timedelta(days=1),
)

GapParams = RequestParams.from_date_range(
    networks=network,
    stations=stations,
    locations=location,
    channels=channels,
    starttime=start,
    endtime=end,
)

print(f"Assuming data is in: {Config.data_dir}")

outfile = "your_outfile_name.pkl"

data_gaps = find_data_gaps(GapParams, Config, outfile=outfile)

print(f"There are {len(data_gaps)} files missing or with gaps!")
print(f"From {start} to {end}")

# In this case we will write out the data gaps, but you could
# also initialise a DataPipeline object and use the get_data method
# to fill the gaps straight away.

with open(f"{Config.data_dir}/{outfile}", "wb") as f:
    pickle.dump(data_gaps, f)

print(f"Data gaps written to {Config.data_dir}/{outfile}")
