import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from apache_beam import Create
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from common.transforms.map_api_ingest_to_position import MapAPIIngestToPosition, get_datetime_with_millis
from tests.util import pcol_equal_to

FAKE_TIME = datetime(2020, 2, 3, 17, 5, 55)


class TestMapAPIIngestToPosition(unittest.TestCase):

    def test_get_datetime_with_millis(self):
        datetime_value = "2023-10-10T10:10:10Z"
        expected = "2023-10-10T10:10:10.000Z"
        self.assertEqual(get_datetime_with_millis(datetime_value), expected)

    @patch('common.transforms.map_api_ingest_to_position.now', side_effect=lambda tz: FAKE_TIME)
    # @patch('apache_beam.options.pipeline_options.PipelineOptions')
    def test_map_api_ingest_to_position(self, mock_now):
        input_msg = {
            "message": {
                "callsign": "ABC123",
                "course": 90,
                "external_id": "ext123",
                "flag": "US",
                "imo": "1234567",
                "id": "int123",
                "lat": 12.34,
                "lon": 56.78,
                "mmsi": "7654321",
                "receiveDate": "2023-10-10T10:10:10Z",
                "speed": 15,
                "timestamp": "2023-10-10T10:10:10Z",
                "extraInfo": {"name": "Test Ship", "extraField": "extraValue"},
            },
            "common": {"fleet": "Fleet1", "provider": "Provider1", "country": "Country1", "format": "Format1"},
        }

        expected_output = {
            "position": {
                "callsign": "ABC123",
                "course": 90,
                "external_id": "ext123",
                'extra_fields': {},
                "flag": "US",
                "imo": "1234567",
                "ingested_at": FAKE_TIME,
                "internal_id": "int123",
                "lat": 12.34,
                "lon": 56.78,
                "mmsi": "7654321",
                "received_at": datetime(2023, 10, 10, 10, 10, 10, tzinfo=timezone.utc),
                "shipname": "Test Ship",
                "source_fleet": "Fleet1",
                "source_provider": "Provider1",
                "source_tenant": "Country1",
                "source_type": "VMS-Format1",
                "speed": 15,
                "timestamp": datetime(2023, 10, 10, 10, 10, 10, tzinfo=timezone.utc),
            },
            "common": {"fleet": "Fleet1", "provider": "Provider1", "country": "Country1", "format": "Format1"},
        }
        options = PipelineOptions(runner='DirectRunner', temp_location='/tmp/temp1', staging_location='/tmp/staging1')
        with TestPipeline(options=options) as p:
            input_pcoll = p | Create([input_msg])
            output_pcoll = input_pcoll | MapAPIIngestToPosition()

            assert_that(output_pcoll, pcol_equal_to([expected_output]))


if __name__ == '__main__':
    unittest.main()
