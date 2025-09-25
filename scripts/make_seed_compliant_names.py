#! make_seed_compliant_names.py

# ----- Script to rename NYMAR data files to be SEED/FDSN compliant -----

# The primary job is to change the network code from "OX" to "3N"
# Also merge files to be day files.
import obspy
import time
from obspy import UTCDateTime
import itertools
from multiprocessing import Pool
from pathlib import Path
from datetime import timedelta
from data_pipeline import iterate_chunks

path = Path("/data/eart0593/NYMAR/raw_data/")
# local dev path
# path = Path("/Users/eart0593/Projects/Agile/NYMAR/data_dump/")

path_out = Path("/data/eart0593/NYMAR/seed_compliant/archive/")
# local dev path
# path_out = Path("/Users/eart0593/Projects/Agile/NYMAR/archive/")
path_out.mkdir(parents=True, exist_ok=True)

fdsn_network = ["3N"]
# Seedlink Parameters
internal_network = ["OX"]

station_list = ["NYM1", "NYM2", "NYM3", "NYM4", "NYM5", "NYM6", "NYM7", "NYM8"]
channels = ["HHZ", "HHN", "HHE"]
location = ["00"]


def rename_to_seed_compliant(single_date, net, sta, loc, chan):
    year = single_date.year
    day = single_date.julday
    curr_dstamp = single_date.strftime("%Y%m%d")
    path_to_data = path / f"{year}/{single_date.month:02d}/{single_date.day:02d}"
    print(f"Processing data for {single_date} in {path_to_data}")
    if not path_to_data.exists():
        print(f"Path {path_to_data} does not exist, skipping...")
        return
    expected_file = f"{net}.{sta}.{loc}.{chan}.{curr_dstamp}T*.mseed"
    files = list(path_to_data.rglob(f"*{expected_file}"))
    if len(files) == 0:
        print(f"Missing file: {expected_file}")
        return

    # Read in all files found (should be one per hour)
    st = obspy.Stream()
    for f in files:
        st += obspy.read(f)
    # Merge to fill gaps with zeros
    if len(st) > 1:
        print(
            f"Merging {len(st)} files for {sta} {chan} on {single_date}. Gaps filled with zeros"
        )
        st.merge(method=1, fill_value=0)
    # Change network code to fdsn one
    for tr in st:
        tr.stats.network = fdsn_network[0]
    # Rename to FDSN compliant name
    new_name = f"{fdsn_network[0]}.{sta}.00.{chan}.{year}.{day:03d}.mseed"
    out_dir = path_out / f"{year}/{fdsn_network[0]}/{sta}/{chan}/"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / new_name
    st.write(out_file, format="MSEED")
    print(f"Wrote {out_file}")


if __name__ == "__main__":
    start_time = time.time()
    start = UTCDateTime(2023, 9, 29)
    end = UTCDateTime(2023, 10, 1)

    dates = list(iterate_chunks(start, end, timedelta(days=1)))
    params = itertools.product(
        dates, internal_network, station_list, location, channels
    )
    # Loop over each day and each station/channel/location/network

    nproc = 1

    with Pool(nproc) as p:
        p.starmap(rename_to_seed_compliant, params)

    end_time = time.time()
    print(f"Processing took {end_time - start_time} seconds")
    print(f"Or {(end_time - start_time)/60.0} minutes")
# Example call to script:
