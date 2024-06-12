

import unittest

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import pcol_equal_to
from vms_ingestion.normalization import build_pipeline_options_with_defaults
from vms_ingestion.normalization.transforms.convert_speed import \
    ConvertSpeedKPHToKT


class TestConvertSpeedKPHToKT(unittest.TestCase):
    options = build_pipeline_options_with_defaults(
        argv=['--country_code=bra',
              '--source=""',
              '--destination=""',
              '--start_date=""',
              '--end_date=""'])

    # Tests the pipeline's transforms.
    def test_convert_speed_kph_to_kt(self):
        with TestPipeline(options=TestConvertSpeedKPHToKT.options) as p:

            # Create a PCollection from the RECORDS static input data.
            input = p | beam.Create([{'speed_kph': 17.0}])

            # Run ALL the pipeline's transforms (in this case, the Normalize transform).
            output: pvalue.PCollection = (
                input
                | ConvertSpeedKPHToKT()
            )

            # Assert that the output PCollection matches the EXPECTED data.
            assert_that(output, pcol_equal_to([{'speed': 9.1792656587473}]), label='CheckOutput')
