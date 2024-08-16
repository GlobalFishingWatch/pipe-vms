import datetime
import os
import unittest

import apache_beam as beam
import pytest
from apache_beam import pvalue
from apache_beam.io.fileio import ReadMatches
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.ingestion.excel_to_bq.feed_ingestion_factory import (
    FeedIngestionFactory,
)
from vms_ingestion.ingestion.excel_to_bq.transforms import map_ingested_message
from vms_ingestion.ingestion.excel_to_bq.transforms.read_excel_to_dict import (
    read_excel_to_dict,
)
from vms_ingestion.normalization import build_pipeline_options_with_defaults

script_path = os.path.dirname(os.path.abspath(__file__))


FAKE_TIME = datetime.datetime(2020, 2, 3, 17, 5, 55)


def mocked_now():
    return FAKE_TIME


class TestPANIngestion(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=[
            "--country_code=pan",
            '--source=""',
            '--destination=""',
            '--start_date="2020-07-01"',
            '--end_date="2020-07-02"',
            "--fleet=trawler",
        ]
    )

    # Our input data, which will make up the initial PCollection.
    RECORDS = [f"{script_path}/data/pan_ingestion.xlsx"]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "beacon_number": None,
            "callsign": None,
            "fishing_gear": None,
            "fishing_zone": None,
            "flag": None,
            "heading": 146,
            "imo": None,
            "ingested_at": datetime.datetime(2020, 2, 3, 17, 5, 55),
            "internal_id": None,
            "lat": 7.823811111111111,
            "lon": -78.90566944444446,
            "mmsi": None,
            "msgid": "9704687db8d9d0e0b7b3e53cdb826a1bc9616bb09536c3db059c753dd6283fbe",
            "shipname": "NAUTILUS",
            "source_fleet": "TRAWLER",
            "source_tenant": "PAN",
            "speed": 0.0,
            "ssvid": "a8895004fe71fdfea4cf1dfae96e142096cc15f799be7dd264f2cd6ba963fc2b",
            "timestamp": datetime.datetime(2020, 7, 1, 0, 13),
            "timestamp_date": datetime.date(2020, 7, 1),
        },
        {
            "beacon_number": None,
            "callsign": None,
            "fishing_gear": None,
            "fishing_zone": None,
            "flag": None,
            "heading": None,
            "imo": None,
            "ingested_at": datetime.datetime(2020, 2, 3, 17, 5, 55),
            "internal_id": None,
            "lat": 9.123611111111112,
            "lon": -79.35915833333333,
            "mmsi": None,
            "msgid": "3ed8e3d0533d22c755bc4ebfa042282686b2c05b841b0beebbe149bd63ac045a",
            "shipname": "NAUTILUS",
            "source_fleet": "TRAWLER",
            "source_tenant": "PAN",
            "speed": 3.0,
            "ssvid": "a8895004fe71fdfea4cf1dfae96e142096cc15f799be7dd264f2cd6ba963fc2b",
            "timestamp": datetime.datetime(2020, 7, 9, 7, 1),
            "timestamp_date": datetime.date(2020, 7, 9),
        },
        {
            "beacon_number": None,
            "callsign": None,
            "fishing_gear": None,
            "fishing_zone": None,
            "flag": None,
            "heading": 238,
            "imo": None,
            "ingested_at": datetime.datetime(2020, 2, 3, 17, 5, 55),
            "internal_id": None,
            "lat": 10.123611111111112,
            "lon": -80.27778055555555,
            "mmsi": None,
            "msgid": "2fa4279ccd604b5414e67f771334fd1b40ae41aff36e310300b961e33f2e849f",
            "shipname": "NAUTILUS",
            "source_fleet": "TRAWLER",
            "source_tenant": "PAN",
            "speed": None,
            "ssvid": "a8895004fe71fdfea4cf1dfae96e142096cc15f799be7dd264f2cd6ba963fc2b",
            "timestamp": datetime.datetime(2020, 8, 12, 13, 2),
            "timestamp_date": datetime.date(2020, 8, 12),
        },
        {
            "beacon_number": None,
            "callsign": None,
            "fishing_gear": None,
            "fishing_zone": None,
            "flag": None,
            "heading": 282,
            "imo": None,
            "ingested_at": datetime.datetime(2020, 2, 3, 17, 5, 55),
            "internal_id": None,
            "lat": 11.82246111111111,
            "lon": -79.51938055555556,
            "mmsi": None,
            "msgid": "df04f7dc62f7113a949ecef1b49f175f37351b52afe2d76543fab1b01825c3cf",
            "shipname": "NAUTILUS",
            "source_fleet": "TRAWLER",
            "source_tenant": "PAN",
            "speed": 0.0,
            "ssvid": "a8895004fe71fdfea4cf1dfae96e142096cc15f799be7dd264f2cd6ba963fc2b",
            "timestamp": datetime.datetime(2020, 8, 31, 23, 25),
            "timestamp_date": datetime.date(2020, 8, 31),
        },
    ]

    def setUp(self):
        self.monkeypatch = pytest.MonkeyPatch()

    # Example test that tests the pipeline's transforms.
    def test_excel_to_bq(self):
        with TestPipeline(options=TestPANIngestion.options) as p:
            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestPANIngestion.RECORDS)

            self.monkeypatch.setattr(
                map_ingested_message, "get_ingested_at", mocked_now
            )
            # Run ALL the pipeline's transforms (in this case, the Ingestion transform).
            output: pvalue.PCollection = (
                input
                | "Read Excel Files" >> ReadMatches()
                | "Convert Excel Files to Dict"
                >> beam.FlatMap(lambda x: read_excel_to_dict(x.read()))
                | "Ingest data" >> FeedIngestionFactory.get_ingestion(feed="pan")
                | "Filter messages inside date range"
                >> beam.Filter(
                    lambda x: x["timestamp"] >= datetime.datetime(2020, 7, 1, 0, 0)
                    and x["timestamp"] < datetime.datetime(2020, 9, 1, 0, 0)
                )
                | "Map ingested message"
                >> map_ingested_message.MapIngestedMessage(feed="pan", fleet="trawler")
            )

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(
                output, pcol_equal_to(TestPANIngestion.EXPECTED), label="CheckOutput"
            )
