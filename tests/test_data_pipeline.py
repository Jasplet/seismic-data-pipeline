import unittest
from unittest.mock import patch
from pathlib import Path
import datetime
from obspy import UTCDateTime
import pipeline.data_plumbing as data_plumbing


class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        # Set up variables used across tests
        self.sensor_ip = "192.168.1.1:8080"
        self.network = "TS"
        self.station = "TEST"
        self.location = "00"
        self.channel = "BHZ"
        self.starttime = UTCDateTime("2024-10-01T00:00:00")
        self.endtime = UTCDateTime("2024-10-01T02:00:00")
        self.ip_dict = {"TEST": self.sensor_ip}

    def test_make_urls_instrument(self):
        request_params = [
                         (self.network,
                          self.station,
                          self.location,
                          self.channel,
                          self.starttime,
                          self.endtime)
                          ]

        with patch.object(Path, 'mkdir') as mock_mkdir:
            mock_mkdir.return_value = None  # Mock mkdir to do nothing

            chunksize = datetime.timedelta(hours=1)
            buffer = datetime.timedelta(seconds=150)
            data_dir = 'test/'
            urls, outfiles = data_plumbing.make_urls_instrument(self.ip_dict,
                                                                request_params,
                                                                data_dir,
                                                                chunksize,
                                                                buffer)
            # Check that the number of URLs matches
            # the expected number of chunks
            assert len(urls) == 2
            assert len(outfiles) == 2
            # Verify URLs are formatted correctly
            assert urls[0].startswith("http://192.168.1.1")
            # Timestamp is included
            assert f'{(self.starttime - buffer).timestamp}' in urls[0]
            # Verify outfile paths
            assert str(outfiles[0]).startswith(data_dir)
            assert outfiles[0].suffix == ".mseed"

# TO-DO: add tests for async functions


if __name__ == '__main__':

    unittest.main()
