import os
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from tests.util import , pcol_equal_to

script_path = os.path.dirname(os.path.abspath(__file__))


class TestFetchRaw(unittest.TestCase):

    def test_fetch_raw_pipe(self):

        # Create a test pipeline
        options = PipelineOptions(runner='DirectRunner', temp_location='/tmp/temp1', staging_location='/tmp/staging1')
        with TestPipeline(options=options) as p:
            # Create a PCollection with test data
            input_data = [{'name': 'foo', 'type': 'a'}, {'name': 'bar', 'type': 'b'}]
            input_pcoll = p | beam.Create(input_data)

            a = input_pcoll | "Filter A" >> beam.ParDo(FilterByType('a'))
            b = input_pcoll | "Filter B" >> beam.ParDo(FilterByType('b'))

            # result_pcoll = a + b
            # join a and b pcollection into one
            result_pcoll = (a, b) | beam.Flatten()

            # Assert that the output matches the expected output
            assert_that(result_pcoll, pcol_equal_to(input_data))


class FilterByType(beam.DoFn):
    """Filter the message by type"""

    def __init__(self, type):
        self.type = type

    def process(self, message):
        if message['type'] == self.type:
            yield message


if __name__ == '__main__':
    unittest.main()
