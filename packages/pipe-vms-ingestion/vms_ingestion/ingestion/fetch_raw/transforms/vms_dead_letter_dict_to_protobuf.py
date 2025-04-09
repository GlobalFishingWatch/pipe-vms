import json
import traceback
from datetime import datetime, timezone

import apache_beam as beam
from common.coders.json_coder import default_serializer
from logger import logger
from vms_ingestion.ingestion.fetch_raw.dtos import vms_dead_letter_pb2

logger.setup_logger(1)
logging = logger.get_logger()


def convert_to_dead_letter_protobuf(element, error_message, pipeline_step):
    from google.protobuf.json_format import ParseDict

    message = vms_dead_letter_pb2.VmsDeadLetter()

    attributes = element["attributes"]
    data = {
        "message_id": element["id"],
        "subscription_name": element["attributes"].get("notificationConfig", "default_subscription"),
        "publish_time": element["attributes"].get("eventTime", datetime.now(timezone.utc).isoformat()),
        "attributes": json.dumps(element["attributes"], default=default_serializer),
        "data": json.dumps(element, default=default_serializer).encode('utf-8'),
        "error_message": str(error_message),
        "stack_trace": "".join(traceback.format_exception(Exception(error_message))),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline_step": pipeline_step,
        "gcs_bucket": element["attributes"].get("bucketId"),
        "gcs_file_path": element["attributes"].get("objectId"),
    }

    # Map dictionary fields to the Protobuf message
    ParseDict(data, message, ignore_unknown_fields=True)

    # Serialize the Protobuf message to binary
    serialized_message = message.SerializeToString()

    # Set attributes for Pub/Sub
    # ensuring that all of thenm are strings
    attributes = {
        "tf_project": str(element["common"]["tf_project"]),
        "fleet": str(element["common"].get("fleet")),
        "provider": str(element["common"]["provider"]),
        "country": str(element["common"]["country"]),
        "format": str(element["common"]["format"]),
        "received_at": str(element["common"]["received_at"].isoformat()),
    }
    return {"data": serialized_message, "attributes": attributes}


class VMSDeadLetterDictToProtobuf(beam.DoFn):

    def __init__(self, error_message, pipeline_step):
        self.error_message = error_message
        self.pipeline_step = pipeline_step

    def process(self, element):
        """
        Converts a dictionary to a Protobuf-encoded dead letter.
        :param element: A dictionary representing the VMS Dead Letter.
        :return: A tuple containing the serialized message and attributes.
        """
        # Create a Protobuf message object
        # try:
        result = convert_to_dead_letter_protobuf(element, self.error_message, self.pipeline_step)
        yield result
        # yield beam.pvalue.TaggedOutput("protobuf", result)
        # except Exception as e:
        #     print(f"Error converting to Protobuf: {e}")

        #     logging.error(f"Error converting to Protobuf: {e}", element, exc_info=True, stack_info=True)
        #     yield beam.pvalue.TaggedOutput("errors", {"error": f"Error converting to protobuf: {e}", "data": element})
