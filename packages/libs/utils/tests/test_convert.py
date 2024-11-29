import unittest

from utils.convert import mmsi_to_iso3166_alpha3, to_float


class TestConvert(unittest.TestCase):
    def test_none(self):
        result = to_float(None)
        self.assertEqual(result, None)

    def test_valid(self):
        result = to_float("9.8")
        self.assertEqual(result, 9.8)

    def test_with_invalid_input(self):
        with self.assertRaises(ValueError):
            to_float("wrong_float")

    def test_mmsi_to_iso3166_alpha3_valid(self):
        # Assuming mmsi_mid_codes contains {'369': [None, 'USA', None, None]}
        result = mmsi_to_iso3166_alpha3('369456789')
        self.assertEqual(result, 'USA')

    def test_mmsi_to_iso3166_alpha3_invalid_mmsi(self):
        result = mmsi_to_iso3166_alpha3('invalid_mmsi')
        self.assertIsNone(result)

    def test_mmsi_to_iso3166_alpha3_not_in_mid_codes(self):
        # Assuming mmsi_mid_codes does not contain '999'
        result = mmsi_to_iso3166_alpha3('999456789')
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
