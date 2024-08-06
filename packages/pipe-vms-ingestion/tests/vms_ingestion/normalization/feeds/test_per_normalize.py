import os
import unittest
from datetime import date, datetime

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to, read_json
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.per_normalize import PERNormalize

script_path = os.path.dirname(os.path.abspath(__file__))


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
            "imo": None,
            "ingested_at": None,
            "lat": -11.71153,
            "length": None,
            "lon": -78.28806,
            "msgid": "1c2c05b34d715df228d11551c858c0f6",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "SANTA MARIA",
            "shiptype": None,
            "source": "PERU_VMS",
            "source_fleet": "ARTISANAL",
            "source_provider": "DIRNEA",
            "source_ssvid": None,
            "source_tenant": "PER",
            "source_type": "VMS",
            "speed": 0.5,
            "ssvid": "PER|s:SANTAMARIA",
            "status": None,
            "timestamp": datetime(2024, 7, 31, 5, 0),
            "timestamp_date": date(2024, 7, 31),
            "type": "VMS",
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 165.0,
            "destination": None,
            "fleet": "small-scale",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": -5.81764,
            "length": None,
            "lon": -81.03528,
            "msgid": "e73770498779c024f6bc8df425d2ceae",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "SANTA MARTA",
            "shiptype": None,
            "source": "PERU_VMS",
            "source_fleet": "SMALL-SCALE",
            "source_provider": "DIRNEA",
            "source_ssvid": None,
            "source_tenant": "PER",
            "source_type": "VMS",
            "speed": 0.0,
            "ssvid": "PER|s:SANTAMARTA",
            "status": None,
            "timestamp": datetime(2024, 7, 30, 14, 56, 59),
            "timestamp_date": date(2024, 7, 30),
            "type": "VMS",
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 23.0,
            "destination": None,
            "fleet": "artisanal",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": -10.12852,
            "length": None,
            "lon": -79.17246,
            "msgid": "8984e6704f2f9e6560b6768e3d95ad16",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "NAUTILUS",
            "shiptype": None,
            "source": "PERU_VMS",
            "source_fleet": "ARTISANAL",
            "source_provider": "DIRNEA",
            "source_ssvid": None,
            "source_tenant": "PER",
            "source_type": "VMS",
            "speed": 0.5,
            "ssvid": "PER|s:NAUTILUS",
            "status": None,
            "timestamp": datetime(2024, 7, 30, 21, 21, 37),
            "timestamp_date": date(2024, 7, 30),
            "type": "VMS",
            "width": None,
        },
        {
            "callsign": None,
            "class_b_cs_flag": None,
            "course": 271.0,
            "destination": None,
            "fleet": "artisanal",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": -10.15091,
            "length": None,
            "lon": -79.12178,
            "msgid": "5764a523ca40f5f4413960f9c9989539",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "NAUTILUS",
            "shiptype": None,
            "source": "PERU_VMS",
            "source_fleet": "ARTISANAL",
            "source_provider": "DIRNEA",
            "source_ssvid": None,
            "source_tenant": "PER",
            "source_type": "VMS",
            "speed": 4.3,
            "ssvid": "PER|s:NAUTILUS",
            "status": None,
            "timestamp": datetime(2024, 7, 30, 19, 21, 37),
            "timestamp_date": date(2024, 7, 30),
            "type": "VMS",
            "width": None,
        },
    ]

    # Example test that tests the pipeline's transforms.
    def test_normalize(self):
        with TestPipeline(options=TestPERNormalize.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestPERNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | PERNormalize(feed="PEr")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output, pcol_equal_to(TestPERNormalize.EXPECTED), label="CheckOutput"
            )
