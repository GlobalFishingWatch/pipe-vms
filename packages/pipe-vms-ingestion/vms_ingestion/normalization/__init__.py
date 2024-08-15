from common.pipeline import (
    build_pipeline_options_with_defaults,
    is_blocking_run,
    success_states,
)
from logger import logger
from vms_ingestion.normalization.pipeline import NormalizationPipeline
from vms_ingestion.options import CommonPipelineOptions

logger.setup_logger(1)
logging = logger.get_logger()


def run_normalization(argv):
    logging.info("Running normalization dataflow pipeline with args %s", argv)

    logging.info("Building pipeline options")
    options = build_pipeline_options_with_defaults(argv).view_as(CommonPipelineOptions)

    logging.info("Launching pipeline")
    pipeline = NormalizationPipeline(options=options)
    result = pipeline.run()

    if is_blocking_run(options, CommonPipelineOptions):
        logging.info("Waiting for job to finish")
        result.wait_until_finish()

    logging.info("Pipeline launch result %s", result.state)

    if result.state in success_states(options, CommonPipelineOptions):
        logging.info("Terminating process successfully")
        exit(0)
    else:
        logging.info("Terminating process with error")
        exit(1)
