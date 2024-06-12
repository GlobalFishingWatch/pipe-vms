import unittest
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.feeds.bra_pipeline import BRAFeedPipeline
from vms_ingestion.normalization.transforms.bra_map_source_message import \
    BRAMapSourceMessage
from vms_ingestion.normalization.transforms.convert_speed import \
    ConvertSpeedKPHToKT
from vms_ingestion.normalization.transforms.map_normalized_message import \
    MapNormalizedMessage
from vms_ingestion.normalization.transforms.pick_output_fields import \
    PickOutputFields


class FakePTransform(beam.PTransform):

    def __init__(self, **_) -> None:
        return

    def expand(self, pcoll):
        return (pcoll)


class TestBRAFeedPipeline(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=['--country_code=bra',
              '--source=""',
              '--destination=""',
              '--start_date=""',
              '--end_date=""'])

    # Our input data, which will make up the initial PCollection.
    RECORDS = [{
        "datahora": datetime.fromisoformat("2024-05-01 05:35:45+00:00"),
        "ID": "4961089",
        "mID": "181473822",
        "codMarinha": "210180889PA",
        "lat": "-1,21861112117767",
        "lon": "-48,4911117553711",
        "curso": "192",
        "nome": "Cibradep X",
        "speed": "17"
    },
    ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [{'msgid': '181473822',
                 'source': 'ONYXSAT_BRAZIL_VMS',
                 'source_type': 'VMS',
                 'source_tenant': 'BRA',
                 'source_provider': 'ONYXSAT',
                 'source_fleet': 'some_fleet',
                 'source_ssvid': '4961089',
                 'type': 'VMS',
                 'ssvid': 'BRA|i:4961089',
                 'timestamp': datetime.fromisoformat('2024-05-01 05:35:45+00:00'),
                 'lat': -1.21861112117767,
                 'lon': -48.4911117553711,
                 'speed': 9.1792656587473,
                 'course': 192.0,
                 'heading': None,
                 'shipname': 'Cibradep X',
                 'callsign': '',
                 'destination': None,
                 'imo': None,
                 'shiptype': 'fishing',
                 'receiver_type': None,
                 'receiver': None,
                 'length': None,
                 'width': None,
                 'status': None,
                 'class_b_cs_flag': None,
                 'received_at': None,
                 'ingested_at': None,
                 'timestamp_date': datetime.date(datetime.fromisoformat('2024-05-01 05:35:45+00:00'))}]

    # Example test that tests the pipeline's transforms.
    def test_normalize(self):
        with TestPipeline(options=TestBRAFeedPipeline.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(TestBRAFeedPipeline.RECORDS)
            ops = TestBRAFeedPipeline.options.from_dictionary(dict(country_code='bra',
                                                                   source='',
                                                                   destination='',
                                                                   start_date='2021-01-01',
                                                                   end_date='2021-01-01',
                                                                   labels='foo=bar,fobar=foobar'))

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            pipe = BRAFeedPipeline(ops,
                                   read_source=FakePTransform,
                                   write_sink=FakePTransform,
                                   )

            output: pvalue.PCollection = (
                input
                | BRAMapSourceMessage()
                | ConvertSpeedKPHToKT()
                | MapNormalizedMessage(feed=pipe.feed,
                                       source_provider=pipe.source_provider,
                                       source_format=pipe.source_format)
                | PickOutputFields(fields=[f'{field}' for field in pipe.output_fields])
            )

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(output, pcol_equal_to(TestBRAFeedPipeline.EXPECTED), label='CheckOutput')
