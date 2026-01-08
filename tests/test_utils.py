import datetime
import unittest

import pytest
from obspy import UTCDateTime

import pipeline.utils as utils  # Assuming this is saved as data_pipeline.py


@pytest.mark.parametrize(
    "start,end,chunksize,expected_chunks",
    [
        (
            UTCDateTime("2024-01-01T00:00:00"),
            UTCDateTime("2024-01-01T03:00:00"),
            datetime.timedelta(hours=1),
            [
                UTCDateTime("2024-01-01T00:00:00"),
                UTCDateTime("2024-01-01T01:00:00"),
                UTCDateTime("2024-01-01T02:00:00"),
            ],
        ),
        (
            UTCDateTime("2024-01-01T12:00:00"),
            UTCDateTime("2024-01-01T15:30:00"),
            datetime.timedelta(minutes=30),
            [
                UTCDateTime("2024-01-01T12:00:00"),
                UTCDateTime("2024-01-01T12:30:00"),
                UTCDateTime("2024-01-01T13:00:00"),
                UTCDateTime("2024-01-01T13:30:00"),
                UTCDateTime("2024-01-01T14:00:00"),
                UTCDateTime("2024-01-01T14:30:00"),
                UTCDateTime("2024-01-01T15:00:00"),
            ],
        ),
        (
            UTCDateTime("2024-01-01T12:00:00"),
            UTCDateTime("2024-01-01T12:00:40"),
            datetime.timedelta(seconds=10),
            [
                UTCDateTime("2024-01-01T12:00:00"),
                UTCDateTime("2024-01-01T12:00:10"),
                UTCDateTime("2024-01-01T12:00:20"),
                UTCDateTime("2024-01-01T12:00:30"),
                UTCDateTime("2024-01-01T12:00:40"),
            ],
        ),
    ],
)
def test_iterate_chunks(start, end, chunksize, expected_chunks):
    start = UTCDateTime("2024-01-01T00:00:00")
    end = UTCDateTime("2024-01-01T03:00:00")
    chunksize = datetime.timedelta(hours=1)

    chunks = list(utils.iterate_chunks(start, end, chunksize))
    expected_chunks = [
        UTCDateTime("2024-01-01T00:00:00"),
        UTCDateTime("2024-01-01T01:00:00"),
        UTCDateTime("2024-01-01T02:00:00"),
    ]

    assert chunks == expected_chunks


@pytest.mark.parametrize(
    "sensor_ip,network,station,location,channel,starttime,endtime",
    [
        (
            "192.168.0.0",
            "XX",
            "TEST",
            "00",
            "XXZ",
            UTCDateTime("2024-01-01T01:00:00"),
            UTCDateTime("2024-01-01T02:00:00"),
        )
    ],
)
def test_form_request(
    sensor_ip, network, station, location, channel, starttime, endtime
):
    expected_url = "".join(
        [
            f"http://{sensor_ip}/data?"
            + f"channel={network}.{station}.{location}.{channel}&"
            + f"from={starttime.timestamp}&to={endtime.timestamp}"
        ]
    )
    request_url = utils.form_request(
        sensor_ip,
        network,
        station,
        location,
        channel,
        starttime,
        endtime,
    )
    assert request_url == expected_url


def test_form_request_start_after_end():
    sensor_ip = "192.168.0.0"
    network = "XX"
    station = "TEST"
    location = "00"
    channel = "XXZ"
    starttime = UTCDateTime("2024-01-01T03:00:00")
    endtime = UTCDateTime("2024-01-01T02:00:00")
    with pytest.raises(ValueError, match="Start of request if before the end!"):
        utils.form_request(
            sensor_ip,
            network,
            station,
            location,
            channel,
            starttime,
            endtime,
        )


if __name__ == "__main__":
    unittest.main()
