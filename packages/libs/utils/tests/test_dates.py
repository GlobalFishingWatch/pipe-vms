import unittest

from utils.dates import prev_month_from_YYYYMMDD


class TestDates(unittest.TestCase):
    def test_prev_month_from_YYYYMMDD(self):
        result = prev_month_from_YYYYMMDD("20220101")

        self.assertEqual(result, "20211201")

    def test_prev_month_from_YYYYMMDD_with_invalid_input(self):
        with self.assertRaises(Exception):
            prev_month_from_YYYYMMDD("wrong_date")


if __name__ == "__main__":
    unittest.main()
