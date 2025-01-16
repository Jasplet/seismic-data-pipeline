
import unittest
from unittest.mock import patch, MagicMock
import requests
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


if __name__ == '__main__':

    unittest.main()
