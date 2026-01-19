# seismic-data-pipeline
Code to download miniseed data from Certimus/Minimus type seismometers. 
Initially developed for a project in Oxford, but has now hopefully been generalised for wider use. 

To install code currently you need to use `pip -e install seismic-data-pipeline` after cloning

Example use case scripts can be found in `scripts`. N.B these are not generalised and require you to configure your target instrument 

Examples cases:
 - `daily_remote_download.py`. A script intended to run on a crontab for a daily data request
 - `download_data.py`. Downloads a batch of data between a given start/end dates
 - `gapfill_data.py`. More precise download requests for filling in pesky gaps. 


## To-Dos

2. Improve packaging / gathering of files.
3. Fold in EIDA/FDSN requests.
4. Async requests for regular downloads
    a. Reorgansie gapfill script
    b. add tests for async functions
    c. make daily download use async functions
