import unittest
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization.transforms.blz_map_source_message import BLZMapSourceMessage


class TestBLZMapSourceMessage(unittest.TestCase):

    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            "receiveDate": datetime.fromisoformat("2024-01-01 22:15:19+00:00"),
            "course": None,
            "speed": None,
            "id": "1122",
            "imo": "789123456",
            "callsign": None,
            "name": "ERTYUIO",
            "mmsi": "312312312",
            "lon": 1.840367,
            "timestamp": datetime.fromisoformat("2024-01-01 21:50:21+00:00"),
            "lat": -35.561983,
        }
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": None,
            "course": None,
            "imo": "789123456",
            "flag": "BLZ",
            "lat": -35.561983,
            "lon": 1.840367,
            "mmsi": "312312312",
            "received_at": datetime(2024, 1, 1, 22, 15, 19, tzinfo=timezone.utc),
            "shipname": "ERTYUIO",
            "shiptype": None,
            "speed": None,
            "ssvid": "1122",
            "timestamp": datetime(2024, 1, 1, 21, 50, 21, tzinfo=timezone.utc),
        },
    ]

    # Tests the transform.
    def test_blz_map_source_message(self):
        with TestPipeline() as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestBLZMapSourceMessage.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | BLZMapSourceMessage()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(TestBLZMapSourceMessage.EXPECTED),
                label="CheckOutput",
            )
