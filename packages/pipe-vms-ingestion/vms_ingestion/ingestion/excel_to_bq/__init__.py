from apache_beam.options.pipeline_options import PipelineOptions
from common.pipeline import is_blocking_run, success_states
from logger import logger
from vms_ingestion.ingestion.excel_to_bq.options import IngestionExcelToBQOptions
from vms_ingestion.ingestion.excel_to_bq.pipeline import IngestionExcelToBQPipeline

logger.setup_logger(1)
logging = logger.get_logger()


def excel_to_bq(argv):

    logging.info("Running excel to bq ingestion dataflow pipeline with args %s", argv)

    logging.info("Building pipeline options")
    known_args, beam_args = IngestionExcelToBQOptions().parse_known_args()

    options = PipelineOptions(options=beam_args, **vars(known_args))

    logging.info("Launching pipeline")
    pipeline = IngestionExcelToBQPipeline(args=known_args, options=options)
    result = pipeline.run()

    if is_blocking_run(options):
        logging.info("Waiting for job to finish")
        result.wait_until_finish()

    logging.info("Pipeline launch result %s", result.state)

    if result.state in success_states(options):
        logging.info("Terminating process successfully")
        exit(0)
    else:
        logging.info("Terminating process with error")
        exit(1)
