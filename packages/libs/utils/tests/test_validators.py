import unittest
import argparse
from utils.validators import check_if_a_key_exists_in_a_dic, check_if_it_is_a_valid_YYYYMMDD


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


if __name__ == '__main__':
    unittest.main()
