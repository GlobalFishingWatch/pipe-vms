

import unittest
from datetime import datetime

from vms_ingestion.normalization.transforms.calculate_msgid import (
    get_message_id, get_raw_message_id)


class TestCalculateMsgId(unittest.TestCase):
    def test_raw_message_id(self):
        result = get_raw_message_id(source="vms_some_fleet",
                                    shipname="AUSTRAL TRAVELER",
                                    timestamp=datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
                                    lat=-52.546,
                                    lon=-71.947,
                                    callsign="ABC123")
        self.assertEqual(
            result,
            "vms_some_fleet|2020-01-01 20:23:01+00:00|-52.546000|-71.947000|AUSTRAL TRAVELER|ABC123")

    def test_message_id(self):

        result = get_message_id(source="vms_some_fleet",
                                shipname="AUSTRAL TRAVELER",
                                timestamp=datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
                                lat=-52.546,
                                lon=-71.947,
                                callsign="ABC123")

        self.assertEqual(result, 'a9d4b20de1a2e1da3a5f617b8a71ae2d')


if __name__ == '__main__':
    unittest.main()
