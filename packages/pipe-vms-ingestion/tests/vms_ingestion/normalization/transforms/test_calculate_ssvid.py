import unittest

import pytest
from vms_ingestion.normalization.transforms.calculate_ssvid import decode_ssvid, encode_ssvid


@pytest.mark.parametrize(
    "decoded,encoded",
    [
        ({"country": "arg", "internal_id": "30832745"}, "arg|i:30832745"),
        ({"country": "arg", "shipname": "SANTA MARIA DEL MAR"}, "arg|s:SANTA MARIA DEL MAR"),
    ],
)
class TestCalculateSSVID:

    def test_encode_ssvid(self, decoded, encoded):
        assert encode_ssvid(**decoded) == encoded

    def test_decode_ssvid(self, encoded, decoded):
        expected = {
            "country": None,
            "internal_id": None,
            "shipname": None,
            "callsign": None,
            "licence": None,
            **decoded,
        }
        result = decode_ssvid(encoded)
        unittest.TestCase().assertDictEqual(result, expected)
