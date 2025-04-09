import unittest
from datetime import datetime
from unittest.mock import patch

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from common.transforms.map_naf_to_position import MapNAFToPosition, map_naf_to_position

FAKE_TIME = datetime(2020, 2, 3, 17, 5, 55)


class TestMapNAFToPosition(unittest.TestCase):

    @patch('common.transforms.map_naf_to_position.now', side_effect=lambda tz: FAKE_TIME)
    def test_map_naf_to_position_valid(self, mock_now):
        input_data = {
            "message": {
                "RC": "CALLSIGN",
                "CO": "123.4",
                "XR": "EXTERNAL_ID",
                "FS": "FLAG",
                "IR": "INTERNAL_ID",
                "LT": "12.3456",
                "LG": "65.4321",
                "NA": "SHIPNAME",
                "SP": "10.5",
                "DA": "210101",
                "TI": "123456",
            },
            "common": {
                "received_at": "2021-01-01T12:34:56Z",
                "fleet": "fleet",
                "provider": "provider",
                "country": "country",
                "format": "format",
            },
        }
        expected_output = {
            "position": {
                "callsign": "CALLSIGN",
                "course": "123.4",
                "external_id": "EXTERNAL_ID",
                "extra_fields": {},
                "flag": "FLAG",
                "imo": "EXTERNAL_ID",
                "ingested_at": FAKE_TIME,
                "internal_id": "INTERNAL_ID",
                "lat": "12.3456",
                "lon": "65.4321",
                "mmsi": None,
                "received_at": "2021-01-01T12:34:56Z",
                "shipname": "SHIPNAME",
                "source_fleet": "FLEET",
                "source_provider": "PROVIDER",
                "source_tenant": "COUNTRY",
                "source_type": "VMS-format",
                "speed": "10.5",
                "timestamp": datetime(2021, 1, 1, 12, 34, 56),
            },
            "common": {
                "received_at": "2021-01-01T12:34:56Z",
                "fleet": "fleet",
                "provider": "provider",
                "country": "country",
                "format": "format",
            },
        }
        result = map_naf_to_position(input_data)
        self.assertEqual(result, expected_output)

    @patch('common.transforms.map_naf_to_position.now', side_effect=lambda tz: FAKE_TIME)
    def test_map_naf_to_position_missing_fields(self, mock_now):
        input_data = {"message": {"DA": "210101", "TI": "123456"}, "common": {}}
        expected_output = {
            "position": {
                "callsign": None,
                "course": None,
                "external_id": None,
                "extra_fields": {},
                "flag": None,
                "imo": None,
                "ingested_at": FAKE_TIME,
                "internal_id": None,
                "lat": None,
                "lon": None,
                "mmsi": None,
                "received_at": None,
                "shipname": None,
                "source_fleet": None,
                "source_provider": None,
                "source_tenant": None,
                "source_type": "VMS-None",
                "speed": None,
                "timestamp": datetime(2021, 1, 1, 12, 34, 56),
            },
            "common": {},
        }
        result = map_naf_to_position(input_data)
        self.assertEqual(result, expected_output)

    @patch('common.transforms.map_naf_to_position.now', side_effect=lambda tz: FAKE_TIME)
    def test_map_naf_to_position_invalid_datetime(self, mock_now):
        input_data = {"message": {"DA": "invalid_date", "TI": "invalid_time"}, "common": {}}
        with self.assertRaises(ValueError):
            map_naf_to_position(input_data)

    @patch('common.transforms.map_naf_to_position.now', side_effect=lambda tz: FAKE_TIME)
    def test_pipeline(self, mock_now):
        input_data = [
            {
                "message": {
                    "RC": "CALLSIGN",
                    "CO": "123.4",
                    "XR": "EXTERNAL_ID",
                    "FS": "FLAG",
                    "IR": "INTERNAL_ID",
                    "LT": "12.3456",
                    "LG": "65.4321",
                    "NA": "SHIPNAME",
                    "SP": "10.5",
                    "DA": "210101",
                    "TI": "123456",
                },
                "common": {
                    "received_at": "2021-01-01T12:34:56Z",
                    "fleet": "fleet",
                    "provider": "provider",
                    "country": "country",
                    "format": "format",
                },
            }
        ]
        expected_output = [
            {
                "position": {
                    "callsign": "CALLSIGN",
                    "course": "123.4",
                    "external_id": "EXTERNAL_ID",
                    "extra_fields": {},
                    "flag": "FLAG",
                    "imo": "EXTERNAL_ID",
                    "ingested_at": FAKE_TIME,
                    "internal_id": "INTERNAL_ID",
                    "lat": "12.3456",
                    "lon": "65.4321",
                    "mmsi": None,
                    "received_at": "2021-01-01T12:34:56Z",
                    "shipname": "SHIPNAME",
                    "source_fleet": "FLEET",
                    "source_provider": "PROVIDER",
                    "source_tenant": "COUNTRY",
                    "source_type": "VMS-format",
                    "speed": "10.5",
                    "timestamp": datetime(2021, 1, 1, 12, 34, 56),
                },
                "common": {
                    "received_at": "2021-01-01T12:34:56Z",
                    "fleet": "fleet",
                    "provider": "provider",
                    "country": "country",
                    "format": "format",
                },
            }
        ]

        options = PipelineOptions(runner='DirectRunner', temp_location='/tmp/temp1', staging_location='/tmp/staging1')
        with TestPipeline(options=options) as p:
            input = p | beam.Create(input_data)
            output = input | MapNAFToPosition()
            assert_that(output, equal_to(expected_output))


if __name__ == '__main__':
    unittest.main()
