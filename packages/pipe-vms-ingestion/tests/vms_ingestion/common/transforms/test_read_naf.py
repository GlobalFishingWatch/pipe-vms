import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from common.transforms.read_naf import ReadNAF
from tests.util import MockTransform, pcol_equal_to

script_path = os.path.dirname(os.path.abspath(__file__))


class ReadNAFTest(unittest.TestCase):

    @patch('apache_beam.io.WriteToPubSub')
    def test_read_naf(self, MockWriteToPubSub):
        # Mock the WriteToPubSub
        MockWriteToPubSub.return_value = MockTransform()

        # Create a test pipeline
        options = PipelineOptions(runner='DirectRunner', temp_location='/tmp/temp1', staging_location='/tmp/staging1')
        with TestPipeline(options=options) as p:
            # Create a PCollection with test data
            input_data = [
                {
                    'bucket': f"{script_path}/data/",
                    'name': 'naf-sample.data',
                    "common": {
                        "notificationConfig": "projects/_/buckets/gcs-bucket/notificationConfigs/68",
                        "fleet": "SOME_FLEET",
                        "objectGeneration": "1733428914890573",
                        "eventType": "OBJECT_FINALIZE",
                        "tf_project": "vms-chl",
                        "bucketId": f"{script_path}/data/",
                        "eventTime": "2024-12-05T20:01:54.893263Z",
                        "country": "XYZ",
                        "format": "NAF",
                        "objectId": f"{script_path}/data/",
                        "provider": "VMS_PROVIDER",
                        "payloadFormat": "JSON_API_V1",
                        "received_at": datetime.fromisoformat("2024-12-05T20:01:54.893263").replace(
                            tzinfo=timezone.utc
                        ),
                    },
                },
            ]
            input_pcoll = p | beam.Create(input_data)

            # Apply the ReadNAF transform
            result_pcoll = input_pcoll | ReadNAF(schema="", error_topic='projects/my_project/topics/test-error-topic')

            # Define the expected output
            expected_output = [
                {
                    "message": {
                        "AD": "XYZ",
                        "FR": "FISHERIES DEPT",
                        "TM": "POS",
                        "NA": "DOÑA MARTA",
                        "IR": "ABC-00083",
                        "RC": "1234567",
                        "XR": "XR-PO-123",
                        "DA": "20240904",
                        "TI": "152300",
                        "LT": "+09.9801",
                        "LG": "-084.8371",
                        "SP": "0",
                        "CO": "0",
                        "FS": "XYZ",
                    },
                    "common": {
                        "notificationConfig": "projects/_/buckets/gcs-bucket/notificationConfigs/68",
                        "fleet": "SOME_FLEET",
                        "objectGeneration": "1733428914890573",
                        "eventType": "OBJECT_FINALIZE",
                        "tf_project": "vms-chl",
                        "bucketId": f"{script_path}/data/",
                        "eventTime": "2024-12-05T20:01:54.893263Z",
                        "country": "XYZ",
                        "format": "NAF",
                        "objectId": f"{script_path}/data/",
                        "provider": "VMS_PROVIDER",
                        "payloadFormat": "JSON_API_V1",
                        "received_at": datetime.fromisoformat("2024-12-05T20:01:54.893263").replace(
                            tzinfo=timezone.utc
                        ),
                    },
                }
            ]
            # Assert that the output matches the expected output
            assert_that(result_pcoll, pcol_equal_to(expected_output))


if __name__ == '__main__':
    unittest.main()
