import os
import unittest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import apache_beam as beam
import pytest
import vms_ingestion.normalization.transforms.map_normalized_message
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to, read_json
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.blz_normalize import BLZNormalize

script_path = os.path.dirname(os.path.abspath(__file__))

FAKE_TIME = datetime(2020, 2, 3, 17, 5, 55)


class TestBLZNormalize(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=blz",
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
            "receiveDate": datetime.fromisoformat(x["receiveDate"]),
            "timestamp": datetime.fromisoformat(x["timestamp"]),
        }
        for x in read_json(f"{script_path}/data/raw_blz.json")
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": None,
            "destination": None,
            "heading": None,
            "flag": None,
            "imo": None,
            "ingested_at": None,
            "lat": -35.561983,
            "length": None,
            "lon": 1.840367,
            "msgid": "935f98d55704bc12ef6f645883751dd09a90a25c7eb58fd5babd864c687ea019",
            "received_at": datetime(2024, 1, 1, 22, 15, 19, tzinfo=timezone.utc),
            "receiver": None,
            "receiver_type": None,
            "shipname": "ERTYUIO",
            "shiptype": None,
            "source": "BELIZE_VMS",
            "source_fleet": None,
            "source_provider": "POLESTAR",
            "source_ssvid": None,
            "source_tenant": "BLZ",
            "source_type": "VMS",
            "speed": None,
            "ssvid": "5256f6f8aac982288b4bb28c05b04e86f09d71b408b7fb9114752336b1cad83e",
            "status": None,
            "timestamp": datetime(2024, 1, 1, 21, 50, 21, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 1, 1),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": None,
            "destination": None,
            "heading": None,
            "flag": None,
            "imo": None,
            "ingested_at": None,
            "lat": 6.4383,
            "length": None,
            "lon": -47.32655,
            "msgid": "a74eb2931b88b1d3c074f8c44e2262c9d16761271c7ccaf815e37517ab38f0b3",
            "received_at": datetime(2024, 1, 1, 13, 15, 22, tzinfo=timezone.utc),
            "receiver": None,
            "receiver_type": None,
            "shipname": "ZAIRA",
            "shiptype": None,
            "source": "BELIZE_VMS",
            "source_fleet": None,
            "source_provider": "POLESTAR",
            "source_ssvid": None,
            "source_tenant": "BLZ",
            "source_type": "VMS",
            "speed": None,
            "ssvid": "7e351bda344871efa0464ad9187ab35cc37e3f847a310cac5e9a4c9fc7456b49",
            "status": None,
            "timestamp": datetime(2024, 1, 1, 12, 8, 35, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 1, 1),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 285.39249139403387,
            "destination": None,
            "heading": None,
            "flag": None,
            "imo": None,
            "ingested_at": None,
            "lat": 6.451333,
            "length": None,
            "lon": -47.3742,
            "msgid": "5435473036df2a6e75c43e60e5a27a4cd1f16916f4ca851bb23b409214ff4b94",
            "received_at": datetime(2024, 1, 1, 13, 15, 21, tzinfo=timezone.utc),
            "receiver": None,
            "receiver_type": None,
            "shipname": "ZAIRA",
            "shiptype": None,
            "source": "BELIZE_VMS",
            "source_fleet": None,
            "source_provider": "POLESTAR",
            "source_ssvid": None,
            "source_tenant": "BLZ",
            "source_type": "VMS",
            "speed": 7.892110928616131,
            "ssvid": "7e351bda344871efa0464ad9187ab35cc37e3f847a310cac5e9a4c9fc7456b49",
            "status": None,
            "timestamp": datetime(2024, 1, 1, 12, 31, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 1, 1),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": None,
            "destination": None,
            "heading": None,
            "flag": None,
            "imo": "9876543",
            "ingested_at": None,
            "lat": 13.857133,
            "length": None,
            "lon": -21.080833,
            "msgid": "70a8ff7081d9836488d278058004742b2bba4b6086e0b62a9d64b8fcfa3b8616",
            "received_at": datetime(2024, 1, 1, 18, 15, 22, tzinfo=timezone.utc),
            "receiver": None,
            "receiver_type": None,
            "shipname": "EUGENIA",
            "shiptype": None,
            "source": "BELIZE_VMS",
            "source_fleet": None,
            "source_provider": "POLESTAR",
            "source_ssvid": None,
            "source_tenant": "BLZ",
            "source_type": "VMS",
            "speed": None,
            "ssvid": "a4bffdd073c9a18d9d410a75afc8317b1c03c0dbc1f4b95b22627a02337fa42c",
            "status": None,
            "timestamp": datetime(2024, 1, 1, 17, 45, 54, tzinfo=timezone.utc),
            "timestamp_date": date(2024, 1, 1),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
    ]

    # Example test that tests the pipeline's transforms.
    @patch(
        "vms_ingestion.normalization.transforms.map_normalized_message.now",
        side_effect=lambda tz: FAKE_TIME,
    )
    def test_normalize(self, mock_now):
        with TestPipeline(options=TestBLZNormalize.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestBLZNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | BLZNormalize(feed="blz")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output,
                pcol_equal_to(TestBLZNormalize.EXPECTED),
                label="CheckOutput",
            )
