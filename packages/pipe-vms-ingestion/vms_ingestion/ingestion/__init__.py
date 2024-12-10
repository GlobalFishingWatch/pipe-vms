from logger import logger
from vms_ingestion.ingestion.excel_to_bq import excel_to_bq
from vms_ingestion.ingestion.fetch_raw import fetch_raw

logger.setup_logger(1)
logging = logger.get_logger()

SUBCOMMANDS = {
    "excel_to_bq": excel_to_bq,
    "fetch_raw": fetch_raw,
}


def run_ingestion(argv):

    if len(argv) < 2:
        logging.info(
            "No ingestion subcommand specified. Run pipeline ingestion [SUBCOMMAND], "
            + "where subcommand is one of %s",
            SUBCOMMANDS.keys(),
        )
        exit(1)

    subcommand = argv[0]
    subcommand_args = argv[1:]

    SUBCOMMANDS[subcommand](subcommand_args)
