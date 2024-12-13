import unittest
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization.transforms.map_latlon import MapLatLon


class TestMapLatLon(unittest.TestCase):

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
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:01:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 10,
                        "lon": 0,
                        "shipname": "",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:01:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 0,
                        "lon": 10,
                        "shipname": "",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:05:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 0,
                        "lon": 0,
                        "shipname": "SANTA MARIA",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:05:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": None,
                        "lon": 10,
                        "shipname": "SANTA MARIA",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:05:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 10,
                        "lon": None,
                        "shipname": "SANTA MARIA",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                        "LATITUDE": 10,
                        "LONGITUDE": 10,
                        "shipname": "",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:05:00", "%Y-%m-%dT%H:%M:%S"),
                        "LATITUDE": None,
                        "LONGITUDE": 10,
                        "shipname": "SANTA MARIA",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:05:00", "%Y-%m-%dT%H:%M:%S"),
                        "LATITUDE": 10,
                        "LONGITUDE": None,
                        "shipname": "SANTA MARIA",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:05:00", "%Y-%m-%dT%H:%M:%S"),
                        "shipname": "SANTA MARIA",
                        "received_at": None,
                        "ssvid": "1",
                    },
                    {
                        "timestamp": datetime.strptime("2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                        "lat": 10,
                        "lon": 10,
                        "LATITUDE": 20,
                        "LONGITUDE": 20,
                        "shipname": "",
                        "received_at": None,
                        "ssvid": "1",
                    },
                ]
            )

            # Run the transform to test
            output: pvalue.PCollection = input | MapLatLon()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(
                    [
                        {
                            "lat": 10,
                            "lon": 10,
                            "received_at": None,
                            "shipname": "",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 0),
                        },
                        {
                            "lat": 10,
                            "lon": 0,
                            "received_at": None,
                            "shipname": "",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 1),
                        },
                        {
                            "lat": 0,
                            "lon": 10,
                            "received_at": None,
                            "shipname": "",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 1),
                        },
                        {
                            "lat": 0,
                            "lon": 0,
                            "received_at": None,
                            "shipname": "SANTA MARIA",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 5),
                        },
                        {
                            "lat": None,
                            "lon": 10,
                            "received_at": None,
                            "shipname": "SANTA MARIA",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 5),
                        },
                        {
                            "lat": 10,
                            "lon": None,
                            "received_at": None,
                            "shipname": "SANTA MARIA",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 5),
                        },
                        {
                            "LATITUDE": 10,
                            "LONGITUDE": 10,
                            "lat": 10,
                            "lon": 10,
                            "received_at": None,
                            "shipname": "",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 0),
                        },
                        {
                            "LATITUDE": None,
                            "LONGITUDE": 10,
                            "lat": None,
                            "lon": 10,
                            "received_at": None,
                            "shipname": "SANTA MARIA",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 5),
                        },
                        {
                            "LATITUDE": 10,
                            "LONGITUDE": None,
                            "lat": 10,
                            "lon": None,
                            "received_at": None,
                            "shipname": "SANTA MARIA",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 5),
                        },
                        {
                            "received_at": None,
                            "shipname": "SANTA MARIA",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 5),
                        },
                        {
                            "LATITUDE": 20,
                            "LONGITUDE": 20,
                            "lat": 10,
                            "lon": 10,
                            "received_at": None,
                            "shipname": "",
                            "ssvid": "1",
                            "timestamp": datetime(2024, 1, 1, 0, 0),
                        },
                    ]
                ),
                label="CheckOutput",
            )
