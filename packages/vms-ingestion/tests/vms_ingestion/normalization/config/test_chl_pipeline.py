import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.config.chl_pipeline import CHLFeedPipeline


class CHLFeedPipelineTest(unittest.TestCase):

    options = build_pipeline_options_with_defaults(
        argv=['--country_code=chl',
              '--source=""',
              '--destination=""',
              '--start_date=""',
              '--end_date=""'])

    # Our input data, which will make up the initial PCollection.
    RECORDS = [{"msgid": "863f7f163ce4fbd5ddfa4c86ec12eb8f",
                "shipname": "AUSTRAL TRAVELER",
                "timestamp": "2020-01-01 20:23:01+00:00",
                "lat": -52.546,
                "lon": -71.947,
                "speed": 9.0,
                "course": 37.0,
                "ssvid": "804dabc7243f9705d1903f8d30838d37",
                "callsign": "ABC123",
                "source": "chile_vms_tanker",
                "fleet": "tanker"}
               ]

    # Our output data, which is the expected data that the final PCollection must match.
    EXPECTED = [{'msgid': '863f7f163ce4fbd5ddfa4c86ec12eb8f',
                 'source': 'chile_vms_tanker',
                 'source_type': 'VMS',
                 'source_tenant': 'CHL',
                 'source_provider': 'SERNAPESCA',
                 'source_ssvid': '804dabc7243f9705d1903f8d30838d37',
                 'source_fleet': 'tanker',
                 'type': 'VMS',
                 'ssvid': 'chl|804dabc7243f9705d1903f8d30838d37',
                 'timestamp': '2020-01-01 20:23:01+00:00',
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
                 'timestamp_date': '2020-01-01'}]

    # Example test that tests the pipeline's transforms.

    def test_normalize(self):
        with TestPipeline(options=CHLFeedPipelineTest.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create(CHLFeedPipelineTest.RECORDS)

            # Run ALL the pipeline's transforms (in this case, the CountWords Normalize transform).
            pipe = CHLFeedPipeline(source='', destination='', start_date='',
                                   end_date='', labels='')
            output = input | pipe.Normalize(pipe=pipe)
            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(output, equal_to(CHLFeedPipelineTest.EXPECTED), label='CheckOutput')

        # The pipeline will run and verify the results.
