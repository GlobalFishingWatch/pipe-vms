import unittest
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization.transforms.png_map_source_message import PNGMapSourceMessage


class TestPNGMapSourceMessage(unittest.TestCase):

    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            "timestamp": datetime.fromisoformat("2019-12-31 23:51:00+00:00"),
            "callsign": "2ABC-9",
            "shipname": "DIANA L.T",
            "registry_number": None,
            "internal_id": None,
            "external_id": None,
            "lat": 6.0341,
            "lon": 125.2536,
            "speed": None,
            "course": None,
            "flag": "PH",
        },
        {
            "timestamp": datetime.fromisoformat("2020-01-01 00:51:00+00:00"),
            "callsign": "2ABC-9",
            "shipname": "DIANA L.T",
            "registry_number": None,
            "internal_id": None,
            "external_id": None,
            "lat": 6.0341,
            "lon": 125.1536,
            "speed": None,
            "course": None,
            "flag": "PH",
        },
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": "2ABC-9",
            "course": None,
            "flag": "PHL",
            "lat": 6.0341,
            "lon": 125.2536,
            "shipname": "DIANA L.T",
            "speed": None,
            "timestamp": datetime(2019, 12, 31, 23, 51, tzinfo=timezone.utc),
        },
        {
            "callsign": "2ABC-9",
            "course": 270.0052560184024,
            "flag": "PHL",
            "lat": 6.0341,
            "lon": 125.1536,
            "shipname": "DIANA L.T",
            "speed": 5.970788583842863,
            "timestamp": datetime(2020, 1, 1, 00, 51, tzinfo=timezone.utc),
        },
    ]

    # Tests the transform.
    def test_png_map_source_message(self):
        with TestPipeline() as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestPNGMapSourceMessage.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | PNGMapSourceMessage()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(TestPNGMapSourceMessage.EXPECTED),
                label="CheckOutput",
            )
