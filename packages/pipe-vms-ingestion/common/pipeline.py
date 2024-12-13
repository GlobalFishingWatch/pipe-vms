from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners import PipelineState
from vms_ingestion.options import IngestionPipelineOptions


def build_pipeline_options_with_defaults(argv, **kwargs):
    return PipelineOptions(
        flags=argv,
        **kwargs,
    )


def is_blocking_run(pipeline_options, pipeline_options_class=IngestionPipelineOptions):
    return (
        pipeline_options.view_as(pipeline_options_class).wait_for_job
        or pipeline_options.view_as(StandardOptions).runner == "DirectRunner"
    )


def success_states(pipeline_options, pipeline_options_class=IngestionPipelineOptions):
    if is_blocking_run(pipeline_options, pipeline_options_class):
        return {PipelineState.DONE}
    else:
        return {
            PipelineState.DONE,
            PipelineState.RUNNING,
            PipelineState.UNKNOWN,
            PipelineState.PENDING,
        }
