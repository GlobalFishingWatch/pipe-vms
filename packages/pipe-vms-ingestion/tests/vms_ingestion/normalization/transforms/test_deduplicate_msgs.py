import unittest
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization.transforms.deduplicate_msgs import DeduplicateMsgs


class TestDeduplicateMsgs(unittest.TestCase):

    # Tests the pipeline's transforms.
    def test_convert_speed_kph_to_kt(self):
        with TestPipeline() as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(
                [
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 10,
                        "lon": 10,
                        "shipname": "",
                        "received_at": None,
                        "source": "country_fleet_1",
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 10,
                        "lon": 10,
                        "shipname": "SANTA MARIA",
                        "received_at": None,
                        "source": "country_fleet_1",
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 10,
                        "lon": 10,
                        "shipname": "SANTA MARIA",
                        "received_at": datetime.strptime("2024-01-01T00:00:01", "%Y-%m-%dT%H:%M:%S"),
                        "source": "country_fleet_1",
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 10,
                        "lon": 10,
                        "shipname": "SANTA MARIA",
                        "received_at": datetime.strptime("2024-01-01T00:10:00", "%Y-%m-%dT%H:%M:%S"),
                        "source": "country_fleet_1",
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 10,
                        "lon": 10,
                        "shipname": "SANTA MARIA",
                        "received_at": datetime.strptime("2024-01-01T00:10:00", "%Y-%m-%dT%H:%M:%S"),
                        "source": "country_fleet_2",
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
                            "lat": 10,
                            "lon": 10,
                            "received_at": datetime(2024, 1, 1, 0, 10),
                            "shipname": "SANTA MARIA",
                            "source": "country_fleet_1",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 0),
                        },
                        {
                            "lat": 10,
                            "lon": 10,
                            "received_at": datetime(2024, 1, 1, 0, 10),
                            "shipname": "SANTA MARIA",
                            "source": "country_fleet_2",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 0),
                        },
                    ]
                ),
                label="CheckOutput",
            )
