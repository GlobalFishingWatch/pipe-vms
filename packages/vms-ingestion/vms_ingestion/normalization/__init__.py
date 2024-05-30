import logging
from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from vms_ingestion.normalization.options import NormalizationOptions
from vms_ingestion.normalization.pipeline import NormalizationPipeline


def build_pipeline_options_with_defaults(argv):
    return PipelineOptions(
        flags=argv,
        setup_file="./setup.py",
    )


def is_blocking_run(pipeline_options):
    return (
        pipeline_options.view_as(NormalizationOptions).wait_for_job or
        pipeline_options.view_as(StandardOptions).runner == "DirectRunner"
    )


def success_states(pipeline_options):
    if is_blocking_run(pipeline_options):
        return {
            PipelineState.DONE
        }
    else:
        return {
            PipelineState.DONE,
            PipelineState.RUNNING,
            PipelineState.UNKNOWN,
            PipelineState.PENDING,
        }


def run_normalization(argv):
    logging.info("Running normalization dataflow pipeline with args %s", argv)

    logging.info("Building pipeline options")
    options = build_pipeline_options_with_defaults(argv)

    logging.info("Launching pipeline")
    pipeline = NormalizationPipeline(options)
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
