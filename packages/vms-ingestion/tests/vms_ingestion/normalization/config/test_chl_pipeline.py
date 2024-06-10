import unittest
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.chl_pipeline import CHLFeedPipeline
from vms_ingestion.normalization.transforms.map_normalized_message import \
    MapNormalizedMessage


class FakePTransform(beam.PTransform):

    def __init__(self, **_) -> None:
        return

    def expand(self, pcoll):
        return (pcoll)


class TestCHLFeedPipeline(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=['--country_code=chl',
              '--source=""',
              '--destination=""',
              '--start_date=""',
              '--end_date=""'])

    # Our input data, which will make up the initial PCollection.
    RECORDS = [{
        "shipname": "AUSTRAL TRAVELER",
        "timestamp": datetime.fromisoformat("2020-01-01 20:23:01+00:00"),
        "lat": -52.546,
        "lon": -71.947,
        "speed": 9.0,
        "course": 37.0,
        "callsign": "ABC123",
        "fleet": "some_fleet"},
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [{'msgid': '93a2a54787fd1275f57d30bc22c4900d',
                 'source': 'chile_vms_some_fleet',
                 'source_type': 'VMS',
                 'source_tenant': 'CHL',
                 'source_provider': 'SERNAPESCA',
                 'source_fleet': 'some_fleet',
                 'type': 'VMS',
                 'ssvid': 'CHL|s:AUSTRAL TRAVELER|c:ABC123',
                 'timestamp': datetime.fromisoformat('2020-01-01 20:23:01+00:00'),
                 'lat': -52.546,
                 'lon': -71.947,
                 'speed': 9.0,
                 'course': 37.0,
                 'heading': None,
                 'shipname': 'AUSTRAL TRAVELER',
                 'callsign': 'ABC123',
                 'destination': None,
                 'imo': None,
                 'shiptype': None,
                 'receiver_type': None,
                 'receiver': None,
                 'length': None,
                 'width': None,
                 'status': None,
                 'class_b_cs_flag': None,
                 'received_at': None,
                 'ingested_at': None,
                 'timestamp_date': datetime.date(datetime.fromisoformat('2020-01-01 20:23:01+00:00'))}]

    # Example test that tests the pipeline's transforms.
    def test_normalize(self):
        with TestPipeline(options=TestCHLFeedPipeline.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestCHLFeedPipeline.RECORDS)
            ops = TestCHLFeedPipeline.options.from_dictionary(dict(country_code='chl',
                                                                   source='',
                                                                   destination='',
                                                                   start_date='2021-01-01',
                                                                   end_date='2021-01-01',
                                                                   labels='foo=bar,fobar=foobar'))

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            pipe = CHLFeedPipeline(ops,
                                   read_source=FakePTransform,
                                   write_sink=FakePTransform,
                                   )
            output: pvalue.PCollection = (
                input
                | MapNormalizedMessage(feed=pipe.feed,
                                       source_provider=pipe.source_provider,
                                       source_format=pipe.source_format)
            )

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(output, pcol_equal_to(TestCHLFeedPipeline.EXPECTED), label='CheckOutput')
