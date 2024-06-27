import unittest
from utils.decoders import decode_base64_string


class TestDecoders(unittest.TestCase):
    def test_decode_base64_string(self):
        result = decode_base64_string(b"SGVsbG8gd29ybGQ=")

        self.assertEqual(result, b"Hello world")

    def test_decode_base64_string_with_invalid_input(self):
        with self.assertRaises(Exception):
            decode_base64_string("wrong_input")


if __name__ == "__main__":
    unittest.main()
