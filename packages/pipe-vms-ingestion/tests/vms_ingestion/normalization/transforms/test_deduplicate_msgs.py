import unittest
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.transforms.deduplicate_msgs import DeduplicateMsgs


class TestDeduplicateMsgs(unittest.TestCase):
    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=bra",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )

    # Tests the pipeline's transforms.
    def test_convert_speed_kph_to_kt(self):
        with TestPipeline(options=TestDeduplicateMsgs.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(
                [
                    {
                        "timestamp": datetime.strptime(
                            "2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
                        ),
                        "lat": 10,
                        "lon": 10,
                        "shipname": "",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime(
                            "2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
                        ),
                        "lat": 10,
                        "lon": 10,
                        "shipname": "SANTA MARIA",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime(
                            "2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
                        ),
                        "lat": 10,
                        "lon": 10,
                        "shipname": "SANTA MARIA",
                        "received_at": datetime.strptime(
                            "2024-01-01T00:00:01", "%Y-%m-%dT%H:%M:%S"
                        ),
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime(
                            "2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
                        ),
                        "lat": 10,
                        "lon": 10,
                        "shipname": "SANTA MARIA",
                        "received_at": datetime.strptime(
                            "2024-01-01T00:10:00", "%Y-%m-%dT%H:%M:%S"
                        ),
                        "ssvid": "1",
                    },
                ]
            )

            # Run the transform to test
            output: pvalue.PCollection = input | DeduplicateMsgs()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(
                    [
                        {
                            "timestamp": datetime.strptime(
                                "2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
                            ),
                            "lat": 10,
                            "lon": 10,
                            "shipname": "SANTA MARIA",
                            "received_at": datetime.strptime(
                                "2024-01-01T00:10:00", "%Y-%m-%dT%H:%M:%S"
                            ),
                            "ssvid": "1",
                        }
                    ]
                ),
                label="CheckOutput",
            )
