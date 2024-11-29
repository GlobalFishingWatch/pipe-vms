import argparse
import unittest
from datetime import datetime

from utils.validators import check_if_a_key_exists_in_a_dic, check_if_it_is_a_valid_YYYYMMDD, is_valid_mmsi


class TestValidators(unittest.TestCase):
    def test_check_if_a_key_exists_in_a_dic(self):
        dictionary = {"key1": "value1", "key2": "value2"}
        keys = ["key1", "key2"]
        check_if_a_key_exists_in_a_dic(keys, dictionary)

    def test_check_if_a_key_exists_in_a_dic_with_missing_key(self):
        dictionary = {"key1": "value1", "key2": "value2"}
        keys = ["key1", "key3"]
        with self.assertRaises(SystemExit):
            check_if_a_key_exists_in_a_dic(keys, dictionary)

    def test_check_if_it_is_a_valid_YYYYMMDD(self):
        result = check_if_it_is_a_valid_YYYYMMDD("20220101")

        self.assertEqual(result, "20220101")

    def test_check_if_it_is_a_valid_YYYYMMDD_with_invalid_input(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            check_if_it_is_a_valid_YYYYMMDD("wrong_date")

    def test_is_valid_mmsi(self):
        self.assertFalse(is_valid_mmsi(datetime.now()), "mmsi should be a string or a number, not a datetime")
        self.assertFalse(is_valid_mmsi(True), "mmsi should be a string or a number, not a boolean")
        self.assertFalse(is_valid_mmsi(""), "mmsi should be a number between 200000000 and 799999999")
        self.assertFalse(is_valid_mmsi("abcdefghi"), "mmsi should be a number between 200000000 and 799999999")

        self.assertFalse(is_valid_mmsi("199999999"), "mmsi should be a number between 200000000 and 799999999")
        self.assertFalse(is_valid_mmsi(199999999), "mmsi should be a number between 200000000 and 799999999")
        self.assertFalse(is_valid_mmsi("800000000"), "mmsi should be a number between 200000000 and 799999999")
        self.assertFalse(is_valid_mmsi(800000000), "mmsi should be a number between 200000000 and 799999999")

        self.assertTrue(is_valid_mmsi("200000000"), "mmsi should be a number between 200000000 and 799999999")
        self.assertTrue(is_valid_mmsi(200000000), "mmsi should be a number between 200000000 and 799999999")
        self.assertTrue(is_valid_mmsi("799999999"), "mmsi should be a number between 200000000 and 799999999")
        self.assertTrue(is_valid_mmsi(799999999), "mmsi should be a number between 200000000 and 799999999")
        self.assertTrue(is_valid_mmsi("200000000"), "mmsi should be a number between 200000000 and 799999999")


if __name__ == "__main__":
    unittest.main()
