import unittest
from unittest.mock import patch
from pathlib import Path
import datetime
import pytest
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

    def test_form_request(self):
        """Tests form_request function"""

        time_req = f'{self.starttime.timestamp}&to={self.endtime.timestamp}'
        seed = f'{self.network}.{self.station}.{self.location}.{self.channel}'
        ex_url = f'http://{self.sensor_ip}/data?channel={seed}&from={time_req}'
        url = data_plumbing.form_request(self.sensor_ip,
                                         self.network,
                                         self.station,
                                         self.location,
                                         self.channel,
                                         self.starttime,
                                         self.endtime)
        self.assertEqual(url, ex_url)

        # Test ValueError on invalid time range
        with self.assertRaises(ValueError):
            data_plumbing.form_request(self.sensor_ip,
                                       self.network,
                                       self.station,
                                       self.location,
                                       self.channel,
                                       self.endtime,
                                       self.starttime)

    def test_make_urls(self):
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
            urls, outfiles = data_plumbing.make_urls(self.ip_dict,
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

    @patch("data_pipeline.log")
    def test_make_urls_param_errors(self, mock_log):
        # faulty_ip_dict = {"ST01": "192.168.1.1"}
        data_dir = "/mocked_dir"
        # Case 1: Missing fields in request_params tuple
        malformed_request_params_1 = [
            (self.network,
             self.station,
             self.location,
             self.channel, UTCDateTime(2023, 1, 1, 0, 0))  # Missing end time
        ]
        # Case 2: Invalid date range (end date before start date)
        malformed_request_params_2 = [
            (self.network,
             self.station,
             self.location,
             self.channel,
             UTCDateTime(2023, 1, 1, 2, 0),
             UTCDateTime(2023, 1, 1, 0, 0))
        ]
        # Case 3: Non-UTCDateTime types in start or end time
        malformed_request_params_3 = [
            (self.network,
             self.station,
             self.location,
             self.channel,
             "not-a-date",
             datetime.datetime(2023, 1, 1, 2, 0))
        ]
        with patch.object(Path, 'mkdir') as mock_mkdir:
            mock_mkdir.return_value = None  # Mock mkdir to do nothing
            # Expecting a ValueError for the missing end date
            with pytest.raises(ValueError):
                data_plumbing.make_urls(self.ip_dict,
                                        malformed_request_params_1,
                                        data_dir)
            # Test error is logged
            mock_log.error.assert_called_once()
            # Expecting ValueError or similar for invalid date range
            with pytest.raises(ValueError):
                data_plumbing.make_urls(self.ip_dict,
                                        malformed_request_params_2,
                                        data_dir)
            mock_log.error.assert_called_once()
            # Expecting TypeError or ValueError for incorrect date format
            with pytest.raises(TypeError):
                data_plumbing.make_urls(self.ip_dict,
                                        malformed_request_params_3,
                                        data_dir)
            mock_log.error.assert_called_once()

    def test_iterate_chunks(self):
        """Test iterate_chunks yields correct time intervals."""
        chunks = list(data_plumbing.iterate_chunks(self.starttime,
                                                   self.endtime,
                                                   datetime.timedelta(
                                                       minutes=60)))
        self.assertEqual(len(chunks), 2)
        # 2 chunks expected: 00:00-01:00, 01:00-02:00
        self.assertEqual(chunks[0], self.starttime)
        self.assertEqual(chunks[1], self.starttime + datetime.timedelta(
                         minutes=60))


if __name__ == '__main__':

    unittest.main()
