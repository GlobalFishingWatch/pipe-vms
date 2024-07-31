import os
import unittest
from datetime import date, datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to, read_json
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.nor_normalize import NORNormalize

script_path = os.path.dirname(os.path.abspath(__file__))


class TestNORNormalize(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=nor",
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
            "timestamp_utc": datetime.fromisoformat(x["timestamp_utc"]),
        }
        for x in read_json(f"{script_path}/data/raw_norway.json")
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": "LLQX",
            "class_b_cs_flag": None,
            "course": 150.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": 68.6139,
            "length": 27.91,
            "lon": 14.253,
            "msgid": "a74fdb1e89888a3a4d5bc769b883914f",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "SPJÆRINGEN",
            "shiptype": "FISHING",
            "source": "NORWAY_VMS",
            "source_fleet": None,
            "source_provider": "FISKERIDIR",
            "source_ssvid": None,
            "source_tenant": "NOR",
            "source_type": "VMS",
            "speed": 1.9,
            "ssvid": "NOR|s:SPJAERINGEN|c:LLQX",
            "status": None,
            "timestamp": datetime(2024, 7, 2, 20, 18, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 7, 2),
            "type": "VMS",
            "width": None,
        },
        {
            "callsign": "3YCU",
            "class_b_cs_flag": None,
            "course": 268.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": 71.1836,
            "length": 36.5,
            "lon": 23.8391,
            "msgid": "6e8b0d5752ea7963c42ea05b67acd643",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "LEANDER",
            "shiptype": "FISHING",
            "source": "NORWAY_VMS",
            "source_fleet": None,
            "source_provider": "FISKERIDIR",
            "source_ssvid": None,
            "source_tenant": "NOR",
            "source_type": "VMS",
            "speed": 7.7,
            "ssvid": "NOR|s:LEANDER|c:3YCU",
            "status": None,
            "timestamp": datetime(2024, 7, 2, 7, 35, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 7, 2),
            "type": "VMS",
            "width": None,
        },
    ]

    # Example test that tests the pipeline's transforms.
    def test_normalize(self):
        with TestPipeline(options=TestNORNormalize.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestNORNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | NORNormalize(feed="nor")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output, pcol_equal_to(TestNORNormalize.EXPECTED), label="CheckOutput"
            )
