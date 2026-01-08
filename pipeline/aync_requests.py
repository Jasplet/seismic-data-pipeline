import asyncio
import datetime
from pathlib import Path

import aiohttp
import obspy

# Core asynchonrous functions. Using these is
# better (i.e., faster) that making synchronous
# requests.


async def get_data(
    request_params,
    station_ips,
    data_dir=Path.cwd(),
    chunksize=datetime.timedelta(hours=1),
    buffer=datetime.timedelta(seconds=120),
    n_async_requests=3,
):
    # Make all urls to query.
    urls, outfiles = make_urls(station_ips, request_params, data_dir, chunksize, buffer)

    log.info(f"There are {len(urls)} requests to make")
    requests_by_ip = {}
    for url, outfile in zip(urls, outfiles):
        sensor_ip = url.split("/")[2]
        if sensor_ip not in requests_by_ip:
            requests_by_ip[sensor_ip] = []
        requests_by_ip[sensor_ip].append((url, outfile))
    # Set up asyncio's HTTP client session
    semaphores = {
        sensor_ip: asyncio.Semaphore(n_async_requests) for sensor_ip in requests_by_ip
    }  # Adjust as needed
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Limit the number of simultaneous requests
        # Adjust based on seismometer capacity
        for sensor_ip, reqs in requests_by_ip.items():
            semaphore = semaphores[sensor_ip]
            for request_url, outfile in reqs:
                task = asyncio.create_task(
                    make_async_request(session, semaphore, request_url, outfile)
                )
                tasks.append(task)
        await asyncio.gather(*tasks)


async def make_async_request(session, semaphore, request_url, outfile):
    """
    Function to actually make the HTTP GET request from the Certimus

    Gets one file, which corresponds to the request_url and writes
    it out as miniSEED
    to the specified outfile

    Parameters:
    ----------
    request_url : str
        The formed request url in the form:
        http://{sensor_ip}/data?channel={net_code}.{stat_code}.{loc_code}.{channel}&from={startUNIX}&to={endUNIX}
    """
    async with semaphore:
        try:
            async with session.get(request_url) as resp:
                print(f"Request at {datetime.datetime.now()}")
                # Print start and end times in a human readable format
                st = obspy.UTCDateTime(float(request_url.split("=")[-2].strip("&to")))
                ed = obspy.UTCDateTime(float(request_url.split("=")[-1]))
                print(f"Requesting {st} to {ed}")
                print(request_url)
                # Raise HTTP error for 4xx/5xx errors
                resp.raise_for_status()

                # Read binary data from the response
                data = await resp.read()
                if len(data) == 0:
                    log.error("Request is empty!" + "Won’t write a zero byte file.")
                    return
                # Now write data
                with open(outfile, "wb") as f:
                    f.write(data)
                log.info(f"Successfully wrote data to {outfile}")

        except aiohttp.ClientResponseError as e:
            log.error(f"Client error for {request_url}: {e}")
            # Additional handling could go here, like retry logic
        except Exception as e:
            log.error(f"Unexpected error for {request_url}: {e}")
        return
