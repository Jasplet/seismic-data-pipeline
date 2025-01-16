import unittest
import datetime
from obspy import UTCDateTime
import pipeline.core_utils as core_utils


class TestCoreUtils(unittest.TestCase):

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
        url = core_utils.form_request(self.sensor_ip,
                                      self.network,
                                      self.station,
                                      self.location,
                                      self.channel,
                                      self.starttime,
                                      self.endtime)
        self.assertEqual(url, ex_url)

        # Test ValueError on invalid time range
        with self.assertRaises(ValueError):
            core_utils.form_request(self.sensor_ip,
                                    self.network,
                                    self.station,
                                    self.location,
                                    self.channel,
                                    self.endtime,
                                    self.starttime)

    def test_iterate_chunks(self):
        """Test iterate_chunks yields correct time intervals."""
        chunks = list(core_utils.iterate_chunks(self.starttime,
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
