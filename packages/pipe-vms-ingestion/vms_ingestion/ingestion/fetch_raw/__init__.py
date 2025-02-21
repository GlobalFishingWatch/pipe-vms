import pprint
from datetime import date, datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import PubsubMessage, ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from common.transforms.map_api_ingest_to_position import MapAPIIngestToPosition
from common.transforms.map_naf_to_position import MapNAFToPosition
from common.transforms.read_json import ReadJson
from common.transforms.read_naf import ReadNAF
from logger import logger
from vms_ingestion.ingestion.fetch_raw.dtos import vms_message_pb2
from vms_ingestion.ingestion.fetch_raw.dtos.dead_letter import DeadLetter
from vms_ingestion.ingestion.fetch_raw.options import IngestionFetchRawOptions
from vms_ingestion.ingestion.fetch_raw.transforms.filter import FilterAndParse, FilterFormat

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


class ConvertToProtobuf(beam.DoFn):
    def process(self, element):
        # from google.protobuf.json_format import ParseDict

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
            logging.info(pprint.pformat(result))

            yield beam.pvalue.TaggedOutput("protobuf", result)
        except Exception as e:
            print(f"Error converting to Protobuf: {e}")

            logging.error(f"Error converting to Protobuf: {e}", element, exc_info=True, stack_info=True)
            yield beam.pvalue.TaggedOutput("errors", {"error": f"Error converting to protobuf: {e}", "data": element})


class CreatePubsubMessage(beam.DoFn):
    def process(self, element):
        yield PubsubMessage(data=element["data"], attributes=element["attributes"])


def default_serializer(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()

    return str(o)


def add_common_output_attributes(message):
    """Add common output attributes to the message."""
    message['common'] = {
        **message['attributes'],
        "received_at": datetime.fromisoformat(message['timeCreated'].replace("T", " ").replace("Z", "")).replace(
            tzinfo=timezone.utc
        ),
    }
    return message


def run(argv):

    logging.info("Running fetch raw ingestion dataflow pipeline with args %s", argv)

    logging.info("Building pipeline options")

    default_options = {"streaming": True, "save_main_session": True}

    options, beam_args = IngestionFetchRawOptions().parse_known_args()

    beam_options = PipelineOptions(beam_args, **default_options)

    logging.info("Launching pipeline")

    with beam.Pipeline(options=beam_options) as pipeline:
        files = (
            pipeline
            | "Read from Pub/Sub" >> ReadFromPubSub(topic=options.input_topic, with_attributes=True)
            | beam.WindowInto(beam.window.FixedWindows(5))
            | 'Parse To JSON' >> beam.ParDo(FilterAndParse())
            | "Add common output attributes" >> beam.Map(add_common_output_attributes)
            # | "Log bucket notification"
            # >> beam.LogElements(label="New File ", prefix="✅", with_timestamp=True, level=beam.logging.INFO)
        )
        naf_positions = (
            files
            | "Filter NAF" >> beam.ParDo(FilterFormat(filter_format_fn=lambda f: f == "NAF"))
            | "Read Lines from NAF File" >> ReadNAF(schema="gs://", error_topic=options.error_topic)
            | "Map NAF message to position" >> MapNAFToPosition()
        )
        api_ingest_positions = (
            files
            | "Filter API_INGEST" >> beam.ParDo(FilterFormat(filter_format_fn=lambda f: f == "API_INGEST"))
            | "Read Lines from JSON File" >> ReadJson(schema="gs://", error_topic=options.error_topic)
            | "Map API_INGEST message to position" >> MapAPIIngestToPosition()
        )
        positions: pvalue.DoOutputsTuple = (
            (naf_positions, api_ingest_positions)
            | "Flatten positions from different formats" >> beam.Flatten()
            | "Convert to Protobuf" >> beam.ParDo(ConvertToProtobuf()).with_outputs("protobuf", "errors")
        )

        # Write positions to PubSub
        (
            positions.protobuf
            # positions
            # | "Log position message dict"
            # >> beam.LogElements(label="Position", prefix="✅ dict ", with_timestamp=True, level=beam.logging.INFO)
            | "Create PubSub Message" >> beam.ParDo(CreatePubsubMessage())
            # | "Log position message"
            # >> beam.LogElements(label="Position", prefix="✅ ", with_timestamp=True, level=beam.logging.INFO)
            | "Write to Pub/Sub" >> WriteToPubSub(topic=options.output_topic, with_attributes=True)
        )

        # Write errors to PubSub
        (
            positions.errors
            #  | "Write conversion errors to Pub/Sub" >> WriteToPubSub(topic=options.error_topic)
            | "Log Errors"
            >> beam.LogElements(label="Errors", prefix="⛔️ Error: ", with_timestamp=True, level=beam.logging.ERROR)
        )

        # unhandled files
        (
            files
            | "Filter Unhandled" >> beam.ParDo(FilterFormat(filter_format_fn=lambda f: f not in ["NAF", "API_INGEST"]))
            | "Map DeadLetter"
            >> beam.Map(
                lambda x: DeadLetter(
                    message_id=x["message_id"],
                    attributes=x["attributes"],
                    data=x["data"],
                    error=Exception("Unhandled format"),
                    pipeline_step="Filter Unhandled",
                    gcs_bucket=None,
                    gcs_file_path=None,
                ).to_dict()
            )
            | "Log unhandled files"
            >> beam.LogElements(label="Errors", prefix="⛔️ Error: ", with_timestamp=True, level=beam.logging.ERROR)
            # | "Write error to Pub/Sub" >> WriteToPubSub(topic=options.error_topic)
        )
