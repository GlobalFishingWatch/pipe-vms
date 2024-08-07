import os
import unittest
from datetime import date, datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to, read_json
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.pan_normalize import PANNormalize

script_path = os.path.dirname(os.path.abspath(__file__))


class TestPANNormalize(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=pan",
            '--source=""',
            '--destination=""',
            '--start_date=""',
            '--end_date=""',
        ]
    )

    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            **x,
            "timestamp": datetime.fromisoformat(x["timestamp"]),
        }
        for x in read_json(f"{script_path}/data/raw_pan.json")
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": "HP8989",
            "class_b_cs_flag": None,
            "course": 36.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": 5.0,
            "length": None,
            "lon": -135.16,
            "mmsi": "None",
            "msgid": "3a7618de1cb1d52b78ee6acd39975cbd",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "NAUTILUS I",
            "shiptype": "200",
            "source": "PANAMA_VMS",
            "source_fleet": None,
            "source_provider": "ARAP",
            "source_ssvid": None,
            "source_tenant": "PAN",
            "source_type": "VMS",
            "speed": 10.5,
            "ssvid": "PAN|s:NAUTILUS1|c:HP8989",
            "status": None,
            "timestamp": datetime(2024, 7, 3, 0, 0, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 7, 3),
            "type": "VMS",
            "width": None,
        },
        {
            "callsign": "3EDY0",
            "class_b_cs_flag": None,
            "course": 168.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": 37.086,
            "length": None,
            "lon": -9.649,
            "mmsi": "352080000",
            "msgid": "b14ee2831e0a27279162fd13141469e5",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "ZAIRA DEL MAR",
            "shiptype": "887",
            "source": "PANAMA_VMS",
            "source_fleet": None,
            "source_provider": "ARAP",
            "source_ssvid": None,
            "source_tenant": "PAN",
            "source_type": "VMS",
            "speed": 10.5,
            "ssvid": "PAN|s:ZAIRADELMAR|c:3EDY0",
            "status": None,
            "timestamp": datetime(2024, 7, 3, 0, 0, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 7, 3),
            "type": "VMS",
            "width": None,
        },
        {
            "callsign": "4EGV2",
            "class_b_cs_flag": None,
            "course": 194.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": 35.042,
            "length": None,
            "lon": 138.511,
            "mmsi": "None",
            "msgid": "1e6c997f71408a00c60abe2db1f8b133",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "MICHELE",
            "shiptype": "NONE",
            "source": "PANAMA_VMS",
            "source_fleet": None,
            "source_provider": "ARAP",
            "source_ssvid": None,
            "source_tenant": "PAN",
            "source_type": "VMS",
            "speed": 0.0,
            "ssvid": "PAN|s:MICHELE|c:4EGV2",
            "status": None,
            "timestamp": datetime(2024, 7, 3, 0, 0, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 7, 3),
            "type": "VMS",
            "width": None,
        },
        {
            "callsign": "HO9842",
            "class_b_cs_flag": None,
            "course": 185.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": 8.842,
            "length": None,
            "lon": -79.662,
            "mmsi": "371123456",
            "msgid": "8dbe7ba4c0292d7e9def00c1e2229f8f",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "ENRIQUE NAZARENO",
            "shiptype": "458",
            "source": "PANAMA_VMS",
            "source_fleet": None,
            "source_provider": "ARAP",
            "source_ssvid": None,
            "source_tenant": "PAN",
            "source_type": "VMS",
            "speed": 0.0,
            "ssvid": "PAN|s:ENRIQUENAZARENO|c:HO9842",
            "status": None,
            "timestamp": datetime(2024, 7, 3, 0, 0, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 7, 3),
            "type": "VMS",
            "width": None,
        },
    ]

    # Example test that tests the pipeline's transforms.
    def test_normalize(self):
        with TestPipeline(options=TestPANNormalize.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestPANNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | PANNormalize(feed="Pan")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output, pcol_equal_to(TestPANNormalize.EXPECTED), label="CheckOutput"
            )
