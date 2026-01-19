import logging
import timeit

from pipeline.io import load_from_config_file

if __name__ == "__main__":
    script_start = timeit.default_timer()
    config_file = "path/to/config.yml"

    try:
        data_pipeline, request_params = load_from_config_file(config_file)
        # Now get the data
        logging.info("Starting data download")
        print("Starting data download...")
        data_pipeline.get_data(request_params)
        logging.info("Data download complete")
        print("Data download complete!")
    except Exception as e:
        logging.error(f"Error running data pipeline: {e}")
        print(f"Error running data pipeline: {e}")
    finally:
        # Finally block will always run to log runtime even
        # if an exception occurs.
        script_end = timeit.default_timer()
        runtime = script_end - script_start
        msg = f"Runtime was {runtime:4.2f} seconds, or {runtime / 60:4.2f} minutes."
        msg += f" or {runtime / 3600:4.2f} hours."
        print(msg)
        logging.info(msg)
