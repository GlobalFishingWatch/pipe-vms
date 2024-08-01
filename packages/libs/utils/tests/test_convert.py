import unittest

from utils.convert import to_float


class TestConvert(unittest.TestCase):
    def test_none(self):
        result = to_float(None)
        self.assertEqual(result, None)

    def test_valid(self):
        result = to_float("9.8")
        self.assertEqual(result, 9.8)

    def test_prev_month_from_YYYYMMDD_with_invalid_input(self):
        with self.assertRaises(ValueError):
            to_float("wrong_float")


if __name__ == "__main__":
    unittest.main()
