import unittest
from datetime import datetime

from vms_ingestion.normalization.transforms.calculate_msgid import (
    get_message_id,
    get_raw_message_id,
)


class TestCalculateMsgId(unittest.TestCase):
    def test_raw_message_id_mandatory_fields(self):
        result = get_raw_message_id(
            timestamp=datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
            lat=-52.546,
            lon=-71.947,
            ssvid="CHL|s:AUSTRAL TRAVELER|c:ABC123",
            fleet=None,
        )
        self.assertEqual(result, "CHL|s:AUSTRAL TRAVELER|c:ABC123|2020-01-01 20:23:01+00:00|-52.546000|-71.947000")

    def test_raw_message_id_optional_fields(self):
        result = get_raw_message_id(
            timestamp=datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
            lat=-52.546,
            lon=-71.947,
            ssvid="CHL|s:AUSTRAL TRAVELER|c:ABC123",
            fleet="some_fleet",
            speed=9,
            course=145,
        )
        self.assertEqual(
            result,
            "CHL|s:AUSTRAL TRAVELER|c:ABC123|some_fleet|2020-01-01 20:23:01+00:00"
            + "|-52.546000|-71.947000|9.000000|145.000000",
        )

    def test_message_id_mandatory_fields(self):

        result = get_message_id(
            timestamp=datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
            lat=-52.546,
            lon=-71.947,
            ssvid="CHL|s:AUSTRAL TRAVELER|c:ABC123",
            fleet="some_fleet",
        )

        self.assertEqual(result, "824be9dba1e80b77edf2994eb1000e2e")

    def test_message_id_differs_using_optional_fields(self):

        result = get_message_id(
            timestamp=datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
            lat=-52.546,
            lon=-71.947,
            ssvid="CHL|s:AUSTRAL TRAVELER|c:ABC123",
            fleet="some_fleet",
            speed=9,
            course=145,
        )
        self.assertNotEqual(result, "824be9dba1e80b77edf2994eb1000e2e")
        self.assertEqual(result, "b0f1737088144c3d889276f8aff78ac9")


if __name__ == "__main__":
    unittest.main()
