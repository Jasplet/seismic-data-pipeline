import asyncio
from pathlib import Path
import datetime
import json
import logging
import pickle
import aiohttp  # For async HTTP requests
from data_pipeline import make_async_request, make_asnyc_urls

log = logging.getLogger(__name__)
logdir = Path('/home/joseph/logs')

# Check if log directory exists, if not set to current working directory
if logdir.exists():
    print(f'Logs written to {logdir}')
else:
    logdir = Path.cwd()
    print('Logs written to cwd')


async def main():
    script_start = datetime.datetime.now()
    logging.basicConfig(filename=f'{logdir}/nymar_backfill.log',
                        level=logging.INFO)
    log.info(f'Starting download. Time is {script_start}')

    # Set data directory and IP addresses
    data_dir = Path('/Users/eart0593/Projects/Agile/NYMAR/data_dump/gap_filling') # change to /your/path/to/datadir
    with open('/Users/eart0593/Projects/Agile/NYMAR/nymar_zerotier_ips.json',
              'r') as w:
        ips_dict = json.load(w)

    # Load request parameters
    gapfile = '/Users/eart0593/Projects/Agile/NYMAR/July_Oct_missing_files.pkl'
    with open(gapfile, 'rb') as f:
        in_params = pickle.load(f)
    request_params = [params for params in in_params if params[1] not in ['NYM1','NYM4']]
    urls, outfiles = make_asnyc_urls(ips_dict, request_params,
                                     data_dir,
                                     chunksize=datetime.timedelta(hours=1),
                                     buffer=datetime.timedelta(seconds=120))          
    requests_by_ip = {}
    for url, outfile in zip(urls, outfiles):
        sensor_ip = url.split("/")[2]
        if sensor_ip not in requests_by_ip:
            requests_by_ip[sensor_ip] = []
        requests_by_ip[sensor_ip].append((url, outfile))
    # Set up asyncio's HTTP client session
    semaphores = {sensor_ip: asyncio.Semaphore(2)
                  for sensor_ip in requests_by_ip}  # Adjust as needed
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Limit the number of simultaneous requests

        # Adjust based on seismometer capacity
        for sensor_ip, requests in requests_by_ip.items():
            semaphore = semaphores[sensor_ip]
            for request_url, outfile in requests:
                task = asyncio.create_task(make_async_request(session,
                                           semaphore,
                                           request_url,
                                           outfile)
                                           )
                tasks.append(task)
        await asyncio.gather(*tasks)

    script_end = datetime.datetime.now()
    runtime = (script_end - script_start).total_seconds()
    log.info(f'Runtime is {runtime:.2f} seconds, or {runtime / 60:.2f} minutes, or {runtime / 3600:.2f} hours')


if __name__ == '__main__':
    asyncio.run(main())
