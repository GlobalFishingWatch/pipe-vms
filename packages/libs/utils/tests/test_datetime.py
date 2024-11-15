import unittest
from datetime import datetime, timezone

from utils.datetime import parse_yyyy_mm_dd_param, prev_month_from_YYYYMMDD


class TestUtilsDatetime(unittest.TestCase):
    def test_prev_month_from_YYYYMMDD(self):
        result = prev_month_from_YYYYMMDD("20220101")

        self.assertEqual(result, "20211201")

    def test_prev_month_from_YYYYMMDD_with_invalid_input(self):
        with self.assertRaises(Exception):
            prev_month_from_YYYYMMDD("wrong_date")

    def test_parse_yyyy_mm_dd_param(self):
        value = "2023-01-01"
        result = parse_yyyy_mm_dd_param(value)
        self.assertEqual(result, datetime(2023, 1, 1, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
