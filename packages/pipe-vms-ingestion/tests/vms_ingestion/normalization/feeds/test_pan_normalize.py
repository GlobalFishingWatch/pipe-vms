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
            "msgid": "23189111b24c09f93ed697ee840205c97ca253aef0e726e6037b9bbbaf634885",
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
            "ssvid": "866998058dd3d97a5e7a2787a6f924eb1550bd209a9cb9ab122f127fe5821e7a",
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
            "msgid": "5a1c81134d885b5b26e933381cdb938f08aefbfeecb30433f268d5ccaa3a209c",
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
            "ssvid": "c8ab39bb19a4b0e06a878514693dec460619b15ab3bb8c083f7bd9269bacda9d",
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
            "msgid": "d9f8d4229bf70996affd5537b1f1bc7e57475b7dbf747f1c1e6ff931809875af",
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
            "ssvid": "2bcca667a1a4ba7ee97700f169a41dbf58294c640547e175b7cfa42f6123169b",
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
            "msgid": "cc1c28f6b2f8c7a74046f5129bc830345d1b7ef6e2f696da43120477d85d19b2",
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
            "ssvid": "493a1375e0306b69e75c57d41f54bb2129830aa6ebf38c5fddc0432ce0c33cf7",
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
