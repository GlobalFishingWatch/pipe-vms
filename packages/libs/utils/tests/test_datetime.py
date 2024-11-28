import unittest
from datetime import datetime, timezone

from utils.datetime import parse_yyyy_mm_dd_param


class TestUtilsDatetime(unittest.TestCase):

    def test_parse_yyyy_mm_dd_param(self):
        value = "2023-01-01"
        result = parse_yyyy_mm_dd_param(value)
        self.assertEqual(result, datetime(2023, 1, 1, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
