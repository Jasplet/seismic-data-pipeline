# nymar-data-pipeline
Code to run on digital ocean droplet to stage nymar data

To install code currently you need to use `pip -e install seismic-data-pipeline` after cloning

Example use case scripts can be found in `scripts`. N.B these are not generalised and require you to configure your target instrument 

Examples cases:
 - `daily_remote_download.py`. A script intended to run on a crontab for a daily data request
 - `download_data.py`. Downloads a batch of data between a given start/end dates
 - `gapfill_data.py`. More precise download requests for filling in pesky gaps. 
