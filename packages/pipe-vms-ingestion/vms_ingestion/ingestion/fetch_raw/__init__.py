from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from common.transforms.map_api_ingest_to_position import MapAPIIngestToPosition
from common.transforms.map_naf_to_position import MapNAFToPosition
from common.transforms.read_json import ReadJson
from common.transforms.read_naf import ReadNAF
from logger import logger
from vms_ingestion.ingestion.fetch_raw.dtos.dead_letter import DeadLetter
from vms_ingestion.ingestion.fetch_raw.options import IngestionFetchRawOptions
from vms_ingestion.ingestion.fetch_raw.transforms.create_pubsub_message import CreatePubsubMessage
from vms_ingestion.ingestion.fetch_raw.transforms.filter import FilterAndParse, FilterFormat
from vms_ingestion.ingestion.fetch_raw.transforms.vms_message_dict_to_protobuf import VMSMessageDictToProtobuf

logger.setup_logger(1)
logging = logger.get_logger()


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
            | "Convert to Protobuf" >> beam.ParDo(VMSMessageDictToProtobuf()).with_outputs("protobuf", "errors")
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
