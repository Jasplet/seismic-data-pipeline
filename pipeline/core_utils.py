# Some core utility functions

def iterate_chunks(start, end, chunksize):
    '''
    Function that makes an interator between two dates (start, end)
    in intervals of <chunksize>.

    Parameters:
    ----------
    start : UTCDateTime
        start time
    end : obspy.UTCDateTime
        end time
    chunksize : datetime.timedelta
        timespan of chunks to split timespan into and iterate over
    '''
    chunk_start = start
    while chunk_start < end:
        yield chunk_start
        chunk_start += chunksize


def form_request(sensor_ip,
                 network,
                 station,
                 location,
                 channel,
                 starttime,
                 endtime):
    '''
    Form the request url

    Parameters:
    ----------
    sensor_ip : str
        IP address of sensor. Includes port no if any port forwarding needed
    network : str
        Network code
    station : str
        Station code
    location : str
        Location code
    channel : str
        Channel code
    starttime : obspy.UTCDateTime
        Start time of request
    endtime : obspy.UTCDataTime
        End time of request
    '''

    if starttime > endtime:
        raise ValueError('Start of request if before the end!')

    seed_params = f'{network}.{station}.{location}.{channel}'
    timeselect = f'from={starttime.timestamp}&to={endtime.timestamp}'
    request = f'http://{sensor_ip}/data?channel={seed_params}&{timeselect}'

    return request
