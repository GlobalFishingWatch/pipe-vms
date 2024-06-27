import unittest

from utils.parsers import parse_json_from_string, parse_yaml_from_string


class TestParsers(unittest.TestCase):
    def test_parse_yaml_from_string(self):
        result = parse_yaml_from_string("key: value")

        self.assertEqual(result, {"key": "value"})

    def test_parse_yaml_from_string_with_invalid_input(self):
        with self.assertRaises(Exception):
            parse_yaml_from_string("incorrect yaml")

    def test_parse_json_from_string(self):
        result = parse_json_from_string('{"key": "value"}')

        self.assertEqual(result, {"key": "value"})

    def test_parse_json_from_string_with_invalid_input(self):
        with self.assertRaises(Exception):
            parse_json_from_string("incorrect json")


if __name__ == "__main__":
    unittest.main()
