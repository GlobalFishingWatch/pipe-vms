from logger import logger
from utils.cli import execute_subcommand
from vms_ingestion.ingestion.excel_to_bq import excel_to_bq

logging = logger.get_logger()

SUBCOMMANDS = {
    "excel_to_bq": excel_to_bq,
}


def run_ingestion(argv):
    execute_subcommand(
        args=argv,
        subcommands=SUBCOMMANDS,
        missing_subcomand_message="No ingestion subcommand specified. Run pipeline ingestion [SUBCOMMAND]",
    )
