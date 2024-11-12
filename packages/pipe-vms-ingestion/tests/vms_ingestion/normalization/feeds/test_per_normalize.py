import os
import unittest
from datetime import date, datetime
from unittest.mock import patch

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to, read_json
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.per_normalize import PERNormalize

script_path = os.path.dirname(os.path.abspath(__file__))
FAKE_TIME = datetime(2020, 2, 3, 17, 5, 55)


class TestPERNormalize(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=per",
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
            "DATETRANSMISSION": datetime.fromisoformat(x["DATETRANSMISSION"]),
        }
        for x in read_json(f"{script_path}/data/raw_peru.json")
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 245.0,
            "destination": None,
            "fleet": "artisanal",
            "heading": None,
            "flag": None,
            "imo": None,
            "ingested_at": None,
            "lat": -11.71153,
            "length": None,
            "lon": -78.28806,
            "msgid": "a27fd65ad5d2993887701e80d13dd7aa528f2442105b1e157952b9e30612e312",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "registry_number": "TA-12345-DF",
            "shipname": "SANTA MARIA",
            "shiptype": None,
            "source": "PERU_VMS",
            "source_fleet": "ARTISANAL",
            "source_provider": "DIRNEA",
            "source_ssvid": None,
            "source_tenant": "PER",
            "source_type": "VMS",
            "speed": 0.5,
            "ssvid": "1a3535b86a4727fe6404727eeeb0c136caf043267b535256f3ebd61bf65a57d6",
            "status": None,
            "timestamp": datetime(2024, 7, 31, 5, 0),
            "timestamp_date": date(2024, 7, 31),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 165.0,
            "destination": None,
            "fleet": "small-scale",
            "heading": None,
            "flag": None,
            "imo": None,
            "ingested_at": None,
            "lat": -5.81764,
            "length": None,
            "lon": -81.03528,
            "msgid": "af314b1b519a8f075d09c7dbccba002733701a18348c7373a400611fbba2456b",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "registry_number": "CE-45678-CM",
            "shipname": "SANTA MARTA",
            "shiptype": None,
            "source": "PERU_VMS",
            "source_fleet": "SMALL-SCALE",
            "source_provider": "DIRNEA",
            "source_ssvid": None,
            "source_tenant": "PER",
            "source_type": "VMS",
            "speed": 0.0,
            "ssvid": "9cf0992e714fe3fb557707b218dba5daa3f89e0a97361b5e97424a30f22631c6",
            "status": None,
            "timestamp": datetime(2024, 7, 30, 14, 56, 59),
            "timestamp_date": date(2024, 7, 30),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 23.0,
            "destination": None,
            "fleet": "artisanal",
            "heading": None,
            "flag": None,
            "imo": None,
            "ingested_at": None,
            "lat": -10.12852,
            "length": None,
            "lon": -79.17246,
            "msgid": "bd33418b076e4707082a958d182a1a0d896393b6479be61f025e81283efaca41",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "registry_number": "CE-98765-CM",
            "shipname": "NAUTILUS",
            "shiptype": None,
            "source": "PERU_VMS",
            "source_fleet": "ARTISANAL",
            "source_provider": "DIRNEA",
            "source_ssvid": None,
            "source_tenant": "PER",
            "source_type": "VMS",
            "speed": 0.5,
            "ssvid": "6d342768e8a7123445bbee804c58e00342909fa83edd733690435940688d6f9f",
            "status": None,
            "timestamp": datetime(2024, 7, 30, 21, 21, 37),
            "timestamp_date": date(2024, 7, 30),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 271.0,
            "destination": None,
            "fleet": "artisanal",
            "heading": None,
            "flag": None,
            "imo": None,
            "ingested_at": None,
            "lat": -10.15091,
            "length": None,
            "lon": -79.12178,
            "msgid": "189384014029369382bcf37b6e0468d7c84b024b08ccd201f1bf80d8527d427b",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "registry_number": "CE-98765-CM",
            "shipname": "NAUTILUS",
            "shiptype": None,
            "source": "PERU_VMS",
            "source_fleet": "ARTISANAL",
            "source_provider": "DIRNEA",
            "source_ssvid": None,
            "source_tenant": "PER",
            "source_type": "VMS",
            "speed": 4.3,
            "ssvid": "6d342768e8a7123445bbee804c58e00342909fa83edd733690435940688d6f9f",
            "status": None,
            "timestamp": datetime(2024, 7, 30, 19, 21, 37),
            "timestamp_date": date(2024, 7, 30),
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
        with TestPipeline(options=TestPERNormalize.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestPERNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | PERNormalize(feed="PEr")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output, pcol_equal_to(TestPERNormalize.EXPECTED), label="CheckOutput"
            )
