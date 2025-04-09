import apache_beam as beam
from logger import logger
from vms_ingestion.ingestion.fetch_raw.dtos import vms_message_pb2

logger.setup_logger(1)
logging = logger.get_logger()


def convert_to_protobuf(element):
    from google.protobuf.json_format import ParseDict

    message = vms_message_pb2.VmsMessage()
    data = {
        "callsign": element["position"]["callsign"],
        "course": element["position"]["course"],
        "external_id": element["position"]["external_id"],
        "flag": element["position"]["flag"],
        "imo": element["position"]["imo"],
        "ingested_at": element["position"]["ingested_at"].isoformat(),
        "internal_id": element["position"]["internal_id"],
        "lat": element["position"]["lat"],
        "lon": element["position"]["lon"],
        "mmsi": element["position"]["mmsi"],
        "received_at": element["position"]["received_at"].isoformat(),
        "shipname": element["position"]["shipname"],
        "source_fleet": element["position"]["source_fleet"],
        "source_provider": element["position"]["source_provider"],
        "source_tenant": element["position"]["source_tenant"],
        "source_type": element["position"]["source_type"],
        "speed": element["position"]["speed"],
        "timestamp": element["position"]["timestamp"].isoformat(),
        "extra_fields": element["position"]["extra_fields"],
    }

    # Map dictionary fields to the Protobuf message
    ParseDict(data, message)

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


class VMSMessageDictToProtobuf(beam.DoFn):
    def process(self, element):
        """
        Converts a dictionary to a Protobuf-encoded message.
        :param element: A dictionary representing the VMS message.
        :return: A tuple containing the serialized message and attributes.
        """
        # Create a Protobuf message object
        try:

            if not element.get("position"):
                yield beam.pvalue.TaggedOutput(
                    "errors", {"error": f"Invalid element {element}, does not have a position key", "data": element}
                )
                return
            result = convert_to_protobuf(element)

            yield beam.pvalue.TaggedOutput("protobuf", result)
        except Exception as e:
            print(f"Error converting to Protobuf: {e}")

            logging.error(f"Error converting to Protobuf: {e}", element, exc_info=True, stack_info=True)
            yield beam.pvalue.TaggedOutput("errors", {"error": f"Error converting to protobuf: {e}", "data": element})
