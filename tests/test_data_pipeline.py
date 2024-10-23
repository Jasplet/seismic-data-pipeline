import unittest
import pytest
from data_pipeline import form_request
from obspy import UTCDateTime

class TestFormRequest(unittest.TestCase):
    def test_form_request_valid(self):
    # Example input data
        sensor_ip = "192.168.1.1:8080"
        network = "XX"
        station = "ABC"
        location = "00"
        channel = "BHZ"
        starttime = UTCDateTime("2024-10-01T00:00:00")
        endtime = UTCDateTime("2024-10-01T01:00:00")

        # Expected URL based on the inputs
        expected_url = f"http://{sensor_ip}/data?channel={network}.{station}.{location}.{channel}&from={starttime.timestamp}&to={endtime.timestamp}"

        # Call the function
        actual_url = form_request(sensor_ip, network, station, location, channel, starttime, endtime)

        # Assertion to check if the URLs match
        self.assertEqual(actual_url, expected_url)
    
    def test_form_request_invalid_time(self):
    # Example input data
        sensor_ip = "192.168.1.1:8080"
        network = "XX"
        station = "ABC"
        location = "00"
        channel = "BHZ"
        starttime = UTCDateTime("2024-11-01T00:00:00")
        endtime = UTCDateTime("2024-10-01T01:00:00")

        with pytest.raises(ValueError):
            form_request(sensor_ip, network, station, location,
                         channel, starttime, endtime)


if __name__ == '__main__':
    unittest.main()