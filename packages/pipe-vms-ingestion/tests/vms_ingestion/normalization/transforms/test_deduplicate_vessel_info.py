import datetime as dt
import os
import unittest

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to, read_json
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.transforms.deduplicate_vessel_info import (
    DeduplicateVesselInfo,
)

script_path = os.path.dirname(os.path.abspath(__file__))


class TestDeduplicateVesselInfo(unittest.TestCase):
    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=chl",
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
            "timestamp": dt.datetime.fromisoformat(x["timestamp"]),
            "timestamp_date": dt.datetime.strptime(x["timestamp_date"], "%Y-%m-%d"),
        }
        for x in read_json(f"{script_path}/data/raw_messages.json")
    ]

    EXPECTED = [
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 181.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": -30.849,
            "length": None,
            "lon": -61.485,
            "msgid": "abcdef85e6160dff5818101778686a10218cae0afc8d63049be31fbbacd0f4ea",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "ANASTASIA",
            "shiptype": "fishing",
            "source": "SOME_VMS_SOURCE",
            "source_fleet": "FLEET_1",
            "source_provider": "VMS_PROVIDER",
            "source_ssvid": None,
            "source_tenant": "CHL",
            "source_type": "VMS",
            "speed": 6.5,
            "ssvid": "24ee4c01f2089b3a78ef28d6d46461fcf9981b9ce74c87266cd4683e3503633f",
            "status": None,
            "timestamp": dt.datetime(2019, 12, 31, 23, 20, 18, tzinfo=dt.timezone.utc),
            "timestamp_date": dt.datetime(2019, 12, 31, 0, 0),
            "type": "VMS",
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 161.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": -30.901,
            "length": None,
            "lon": -61.479,
            "msgid": "e3dbb1e8e4c7a35be30c0ca8c266578b4fec780f55d5279744e3aa69b0cd6d8a",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "ANASTASIA",
            "shiptype": "fishing",
            "source": "SOME_VMS_SOURCE",
            "source_fleet": "FLEET_1",
            "source_provider": "VMS_PROVIDER",
            "source_ssvid": None,
            "source_tenant": "CHL",
            "source_type": "VMS",
            "speed": 6.5,
            "ssvid": "24ee4c01f2089b3a78ef28d6d46461fcf9981b9ce74c87266cd4683e3503633f",
            "status": None,
            "timestamp": dt.datetime(2020, 1, 1, 23, 50, 22, tzinfo=dt.timezone.utc),
            "timestamp_date": dt.datetime(2020, 1, 1, 0, 0),
            "type": "VMS",
            "width": None,
        },
    ]

    # Tests the pipeline's transforms.
    def test_deduplicate_vessel_info(self):
        with TestPipeline(options=TestDeduplicateVesselInfo.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestDeduplicateVesselInfo.RECORDS)

            # Run the transform to test
            output: pvalue.PCollection = input | DeduplicateVesselInfo()

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(TestDeduplicateVesselInfo.EXPECTED),
                label="CheckOutput",
            )
