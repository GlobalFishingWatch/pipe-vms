import os
import unittest
from datetime import date, datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to, read_json
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.ecu_normalize import ECUNormalize

script_path = os.path.dirname(os.path.abspath(__file__))


class TestECUNormalize(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=ecu",
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
            "utc_time": datetime.fromisoformat(x["utc_time"]),
        }
        for x in read_json(f"{script_path}/data/raw_ecuador.json")
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": "TN-00-01234",
            "class_b_cs_flag": None,
            "course": 117.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "internal_id": "98765",
            "lat": -2.19445,
            "length": None,
            "lon": -80.0178,
            "msgid": "0ac0577b3ac22f039da88b4707f42f4f",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "NAUTILUS XXX",
            "shiptype": "NATIONAL TRAFFIC",
            "source": "ECUADOR_VMS",
            "source_fleet": None,
            "source_provider": "DIRNEA",
            "source_ssvid": "98765",
            "source_tenant": "ECU",
            "source_type": "VMS",
            "speed": 5.0,
            "ssvid": "ECU|i:98765|s:NAUTILUS30|c:TN0001234",
            "status": None,
            "timestamp": datetime(2024, 4, 20, 8, 14, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 4, 20),
            "type": "VMS",
            "width": None,
        },
        {
            "callsign": "P -00-00123",
            "class_b_cs_flag": None,
            "course": 72.0,
            "destination": None,
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "internal_id": "12345",
            "lat": -7.611016,
            "length": None,
            "lon": -101.358451,
            "msgid": "b72ff16e9bbac5e90ff6d4b7b499b023",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "SANTA MARTA DOS",
            "shiptype": "FISHING",
            "source": "ECUADOR_VMS",
            "source_fleet": None,
            "source_provider": "DIRNEA",
            "source_ssvid": "12345",
            "source_tenant": "ECU",
            "source_type": "VMS",
            "speed": 13.0,
            "ssvid": "ECU|i:12345|s:SANTAMARTA2|c:P0000123",
            "status": None,
            "timestamp": datetime(2024, 4, 20, 8, 0, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 4, 20),
            "type": "VMS",
            "width": None,
        },
    ]

    # Example test that tests the pipeline's transforms.
    def test_normalize(self):
        with TestPipeline(options=TestECUNormalize.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestECUNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | ECUNormalize(feed="ecu")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output, pcol_equal_to(TestECUNormalize.EXPECTED), label="CheckOutput"
            )
