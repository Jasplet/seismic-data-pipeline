import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
import requests
import datetime
import pytest
from obspy import UTCDateTime
import pipeline.data_plumbing as data_plumbing  # Assuming this is saved as data_pipeline.py


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

    # Mock Path.mkdir so no directories are created
    @patch("pathlib.Path.mkdir")
    @patch("data_pipeline.form_request")
    @patch("data_pipeline.make_request")
    def test_chunked_data_query(self,
                                mock_make_request,
                                mock_form_request,
                                mock_mkdir):
        """Test chunked_data_query forms and makes requests in chunks."""
        mock_form_request.side_effect = lambda *args, **kwargs: "mock_url"

        data_plumbing.chunked_data_query(
            self.sensor_ip, self.network, self.station, self.location,
            self.channel, self.starttime, self.endtime, data_dir="test_data"
        )
        # Expect 2 chunks to be processed
        self.assertEqual(mock_form_request.call_count, 2)
        self.assertEqual(mock_make_request.call_count, 2)
        # Confirm mkdir was called to ensure the directory
        # structure would have been created
        mock_mkdir.assert_called()

    @patch("requests.get")
    def test_make_request(self, mock_get):
        """Test make_request handles responses correctly."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'some_binary_data'
        mock_response.elapsed = datetime.timedelta(seconds=1)
        mock_get.return_value = mock_response

        with patch("builtins.open", unittest.mock.mock_open()) as mock_file:
            data_plumbing.make_request("mock_url", "mock_outfile.mseed")
            mock_file.assert_called_once_with("mock_outfile.mseed", "wb")
            mock_file().write.assert_called_once_with(b'some_binary_data')

    @patch("data_pipeline.log")
    @patch("requests.get")
    def test_make_request_fails(self, mock_get, mock_log):
        """Test make_request fails correctly."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.content = b'some data'  # No data so should raise error
        mock_response.elapsed = datetime.timedelta(seconds=1)
        mock_get.return_value = mock_response

        # Test that an HTTPError is raised as logged
        with self.assertRaises(requests.exceptions.HTTPError):
            data_plumbing.make_request("mock_url", "mock_outfile.mseed")
        # change status code back to 200 and test that an empty file is logged
        # and that make_request continues instead
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b''
        mock_get.return_value = mock_response
        with patch("builtins.open", unittest.mock.mock_open()):
            data_plumbing.make_request("mock_url", "mock_outfile.mseed")
            expected_call = "Request is empty! Won’t write a zero byte file."
            mock_log.error.assert_any_call(expected_call)

    @patch("obspy.Stream.get_gaps")
    @patch("obspy.read")
    @patch("glob.glob")
    @patch("pathlib.Path.unlink")
    @patch("data_pipeline.log")
    def test_gather_chunks(self, mock_log, mock_unlink, mock_glob,
                           mock_obspy_read, mock_get_gaps):
        """Test gather_chunks reads and merges files correctly."""
        mock_obspy_read.return_value = MagicMock()
        mock_obspy_read.return_value.merge = MagicMock()
        mock_obspy_read.return_value.get_gaps = MagicMock()
        mock_glob.return_value = [Path(f"file_{i}.mseed") for i in range(3)]
        mock_get_gaps.return_value = []

        data_plumbing.gather_chunks(self.network,
                                    self.station,
                                    self.location,
                                    self.channel,
                                    self.starttime,
                                    self.endtime,
                                    data_dir="test_data",
                                    gather_size=datetime.timedelta(days=1)
                                    )

        # Verify that obspy.read was called with the correct file pattern
        mock_obspy_read.assert_called_once()
        # Check that merge and cleanup were called
        mock_obspy_read.return_value.merge.assert_called_once()
        self.assertEqual(mock_unlink.call_count, 3)
        # Each file should be unlinked

        # Verify that logging was called with expected messages
        mock_log.info.assert_called_once()
        mock_log.warning.assert_not_called()
        mock_log.error.assert_not_called()

    @patch("obspy.read")
    @patch("pathlib.Path.glob")
    @patch("pathlib.Path.unlink")
    @patch("data_pipeline.log")
    def test_gather_chunks_warning(self,
                                   mock_log,
                                   mock_unlink,
                                   mock_glob,
                                   mock_obspy_read):
        """
        Test gather_chunks reads and merges files correctly.
        """
        mock_obspy_read.return_value = MagicMock()
        mock_obspy_read.return_value.merge = MagicMock()
        mock_obspy_read.return_value.get_gaps = MagicMock()
        mock_obspy_read.return_value.get_gaps.return_value = ['some', 'gaps']
        mock_glob.return_value = [Path(f"file_{i}.mseed") for i in range(3)]
        with patch('builtins.open', unittest.mock.mock_open()):
            data_plumbing.gather_chunks(
                self.network, self.station, self.location, self.channel,
                self.starttime, self.endtime, data_dir="test_data",
                gather_size=datetime.timedelta(days=1)
            )

        mock_log.warning.assert_called_once()


if __name__ == '__main__':

    unittest.main()
