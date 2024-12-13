import os
import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to, read_json
from vms_ingestion.normalization.feeds.png_normalize import PNGNormalize

script_path = os.path.dirname(os.path.abspath(__file__))
FAKE_TIME = datetime(2020, 2, 3, 17, 5, 55)


class TestPNGNormalize(unittest.TestCase):

    # Our input data, which will make up the initial PCollection.
    RECORDS = [
        {
            **x,
            "timestamp": datetime.fromisoformat(x["timestamp"].replace(".000000 UTC", "+00:00")),
        }
        for x in read_json(f"{script_path}/data/raw_png.json")
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [
        {
            "callsign": "2ABC-9",
            "class_b_cs_flag": None,
            "course": None,
            "destination": None,
            "flag": "PHL",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": 6.0341,
            "length": None,
            "lon": 125.1536,
            "msgid": "d3b1297766e3a4c4c7108a72c472c2b90be7ebfa19d068134e52da360fceaa6b",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "DIANA L.T",
            "shiptype": None,
            "source": "PNG_VMS",
            "source_fleet": None,
            "source_provider": "IFIMS",
            "source_ssvid": None,
            "source_tenant": "PNG",
            "source_type": "VMS",
            "speed": None,
            "ssvid": "72950e8718f89d55b4c92badd581498d598dd687a2e80e327b08cd82eee742b3",
            "status": None,
            "timestamp": datetime(2020, 1, 1, 21, 51, tzinfo=timezone.utc),
            "timestamp_date": date(2020, 1, 1),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": "2ABC-9",
            "class_b_cs_flag": None,
            "course": 144.4438366926064,
            "destination": None,
            "flag": "PHL",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": 6.0309,
            "length": None,
            "lon": 125.1559,
            "msgid": "df989dd2f2bd59d9a9a99bf17fbc9b98b0c41f0b00964063c86dc6351f7f21f6",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "DIANA L.T",
            "shiptype": None,
            "source": "PNG_VMS",
            "source_fleet": None,
            "source_provider": "IFIMS",
            "source_ssvid": None,
            "source_tenant": "PNG",
            "source_type": "VMS",
            "speed": 0.4723259938755648,
            "ssvid": "72950e8718f89d55b4c92badd581498d598dd687a2e80e327b08cd82eee742b3",
            "status": None,
            "timestamp": datetime(2020, 1, 1, 22, 21, tzinfo=timezone.utc),
            "timestamp_date": date(2020, 1, 1),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": "2ABC-9",
            "class_b_cs_flag": None,
            "course": 198.82926367912165,
            "destination": None,
            "flag": "PHL",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": 5.7718,
            "length": None,
            "lon": 125.0671,
            "msgid": "10c8b802ed0db2104cab6af56a30beabad3b480811e55e961524876f13c63100",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "DIANA L.T",
            "shiptype": None,
            "source": "PNG_VMS",
            "source_fleet": None,
            "source_provider": "IFIMS",
            "source_ssvid": None,
            "source_tenant": "PNG",
            "source_type": "VMS",
            "speed": 10.957092649611726,
            "ssvid": "72950e8718f89d55b4c92badd581498d598dd687a2e80e327b08cd82eee742b3",
            "status": None,
            "timestamp": datetime(2020, 1, 1, 23, 51, tzinfo=timezone.utc),
            "timestamp_date": date(2020, 1, 1),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": "P1V1234",
            "class_b_cs_flag": None,
            "course": None,
            "destination": None,
            "flag": "PNG",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": -4.2012,
            "length": None,
            "lon": 152.1693,
            "msgid": "a883412673d3b057614ca7a213f5f804b1d04dfc838f3d39e32e7b1ccee2ebaf",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "TIBURCIA 777",
            "shiptype": None,
            "source": "PNG_VMS",
            "source_fleet": None,
            "source_provider": "IFIMS",
            "source_ssvid": None,
            "source_tenant": "PNG",
            "source_type": "VMS",
            "speed": None,
            "ssvid": "3b0db4962e19f74c64234a1f9da2c58daae54b8da1ed09a2eb115b90d442209d",
            "status": None,
            "timestamp": datetime(2020, 1, 1, 9, 46, tzinfo=timezone.utc),
            "timestamp_date": date(2020, 1, 1),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": "P1V1234",
            "class_b_cs_flag": None,
            "course": 0.0,
            "destination": None,
            "flag": "PNG",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": -4.2012,
            "length": None,
            "lon": 152.1693,
            "msgid": "56c48762bfeb01233482830511cd480a1c2a310bb6b3b1b5755d3de07a77e399",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "TIBURCIA 777",
            "shiptype": None,
            "source": "PNG_VMS",
            "source_fleet": None,
            "source_provider": "IFIMS",
            "source_ssvid": None,
            "source_tenant": "PNG",
            "source_type": "VMS",
            "speed": 0.0,
            "ssvid": "3b0db4962e19f74c64234a1f9da2c58daae54b8da1ed09a2eb115b90d442209d",
            "status": None,
            "timestamp": datetime(2020, 1, 1, 11, 46, tzinfo=timezone.utc),
            "timestamp_date": date(2020, 1, 1),
            "type": "VMS",
            "updated_at": FAKE_TIME,
            "width": None,
        },
        {
            "callsign": "P1V1234",
            "class_b_cs_flag": None,
            "course": 0.0,
            "destination": None,
            "flag": "PNG",
            "heading": None,
            "imo": None,
            "ingested_at": None,
            "lat": -4.2012,
            "length": None,
            "lon": 152.1693,
            "msgid": "768a9080c927fd245b9c881ee6c02765a074c57be97b7b0a8abdb1687cf07403",
            "received_at": None,
            "receiver": None,
            "receiver_type": None,
            "shipname": "TIBURCIA 777",
            "shiptype": None,
            "source": "PNG_VMS",
            "source_fleet": None,
            "source_provider": "IFIMS",
            "source_ssvid": None,
            "source_tenant": "PNG",
            "source_type": "VMS",
            "speed": 0.0,
            "ssvid": "3b0db4962e19f74c64234a1f9da2c58daae54b8da1ed09a2eb115b90d442209d",
            "status": None,
            "timestamp": datetime(2020, 1, 1, 20, 16, tzinfo=timezone.utc),
            "timestamp_date": date(2020, 1, 1),
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
        with TestPipeline() as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestPNGNormalize.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = input | PNGNormalize(feed="png")

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(output, pcol_equal_to(TestPNGNormalize.EXPECTED), label="CheckOutput")
