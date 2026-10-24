import logging
import timeit
from pathlib import Path

from pipeline.io import load_from_config_file

log = logging.getLogger(__name__)
logdir = Path("/home/joseph/logs")
# test is logdir exists, if not then set to cwd
if logdir.exists():
    print(f"Logs written to {logdir}")
else:
    logdir = Path.cwd()
    print("Logs written to cwd")

if __name__ == "__main__":
    script_start = timeit.default_timer()
    config_file = "path/to/config.yml"

    data_pipeline, request_params = load_from_config_file(config_file)
    # Now get the data
    log.info("Starting data download")
    print("Starting data download...")
    data_pipeline.get_data(request_params)
    log.info("Data download complete")
    print("Data download complete!")

    # Finally block will always run to log runtime even
    # if an exception occurs.
    script_end = timeit.default_timer()
    runtime = script_end - script_start
    msg = f"Runtime was {runtime:4.2f} seconds, or {runtime / 60:4.2f} minutes."
    msg += f" or {runtime / 3600:4.2f} hours."
    print(msg)
    log.info(msg)
