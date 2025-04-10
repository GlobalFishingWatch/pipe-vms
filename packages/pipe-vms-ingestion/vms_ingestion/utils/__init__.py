from utils.cli import execute_subcommand
from vms_ingestion.utils.create_unified_normalized_view import run_create_unified_normalized_view

SUBCOMMANDS = {"create_unified_normalized_view": run_create_unified_normalized_view}


def run_utils(argv):
    execute_subcommand(
        args=argv,
        subcommands=SUBCOMMANDS,
        missing_subcomand_message="No utils subcommand specified. Run pipeline utils [SUBCOMMAND]",
    )
