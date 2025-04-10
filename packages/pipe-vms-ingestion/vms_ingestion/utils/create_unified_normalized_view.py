import argparse

from bigquery import query, view
from logger import logger
from vms_ingestion import __getattr__

logging = logger.get_logger()

NAME = "create_unified_normalized_view"
DESCRIPTION = """
    Ensures the unified view of normalized positions for all the countries is
    created.
"""
EPILOG = (
    "Example: \n"
    "    create_unified_normalized_view \n"
    "        --view_name world-fishing-827,pipe_vms_v3_internal.normalized_positions \n"
    "        --source_table_name vms_country_one.normalized_positions\n"
    "        --source_table_name vms_country_two.normalized_positions\n"
    "        --labels environment=development\n"
    "        --labels version=v3\n"
)

BQ_VIEW_DESCRIPYION = """\
「 ✦ 𝙽𝙾𝚁𝙼𝙰𝙻𝙸𝚉𝙴𝙳 𝙿𝙾𝚂𝙸𝚃𝙸𝙾𝙽𝚂 ✦ 」
𝗣𝗼𝘀𝗶𝘁𝗶𝗼𝗻 𝗿𝗲𝗰𝗼𝗿𝗱𝘀 𝗳𝗿𝗼𝗺 𝗮𝗹𝗹 𝘀𝗼𝘂𝗿𝗰𝗲𝘀 𝗻𝗼𝗿𝗺𝗮𝗹𝗶𝘇𝗲𝗱
⬖ Created by pipe-vms-ingestion: v{version}
⬖ https://github.com/GlobalFishingWatch/pipe-vms/blob/main/packages/pipe-vms-ingestion

𝗦𝘂𝗺𝗺𝗮𝗿𝘆
  ⬖ Normalized positions for all vms providers. Daily produced.
  ⬖ Includes all countries normalized_positions records generated by `pipe-vms-ingestion` normalize step
    in vms_v3 ingestion dags:

{table_names}

𝗦𝗼𝘂𝗿𝗰𝗲
  ⬖ Multiple sources from the different countries feeds.
"""
HELP_VIEW_NAME = "BigQuery view name of the unified normalized positions from all countries."
HELP_SOURCE_TABLE_NAMES = "BigQuery table name of normalized positions source. Multiple values are accepted."
HELP_SKIP_VIEW_CREATION_IF_EXISTS = "Skips overwriting the view if exists."
HELP_LABELS = "Labels to assign to the BigQuery view in the form of KEY=VALUE pairs. Multiple values are accepted."


def formatter():
    """Returns a formatter for argparse help."""

    def formatter(prog):
        return argparse.RawTextHelpFormatter(prog, max_help_position=50)

    return formatter


def run_create_unified_normalized_view(argv):
    p = argparse.ArgumentParser(
        prog=NAME,
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=formatter(),
    )

    add = p.add_argument
    add(
        "-o",
        "--view_name",
        type=str,
        metavar="project_id.dataset_id.table_name",
        required=True,
        help=f"{HELP_VIEW_NAME} (Required)",
    )
    add(
        "-i",
        "--source_table_name",
        type=str,
        metavar="project_id.dataset_id.table_name",
        required=True,
        dest="source_table_names",
        default=[],
        action="append",
        help=f"{HELP_SOURCE_TABLE_NAMES} (Required)",
    )
    add("-s", "--skip_if_exists", default=False, action="store_true", help=HELP_SKIP_VIEW_CREATION_IF_EXISTS)
    add("--labels", type=str, metavar="KEY=VALUE", default=[], action="append", help=HELP_LABELS)

    ns, _ = p.parse_known_args(args=argv or ["--help"])

    try:
        view_query = query.get_sql_from_file(
            filepath='assets/normalized_positions.view.sql.j2', source_table_names=ns.source_table_names
        )
        view_description = BQ_VIEW_DESCRIPYION.format(
            version=__getattr__('version'),
            table_names="\n".join([f"    ⬖ {table_name}" for table_name in ns.source_table_names]),
        )
        view_labels = {k: v for k, v in (item.split("=") for item in ns.labels)}
        delete_if_exists = not ns.skip_if_exists
        view.create(
            view_name=ns.view_name,
            query=view_query,
            description=view_description,
            delete_view_if_exists=delete_if_exists,
            labels=view_labels,
        )
    except Exception as e:
        logging.error(e)
        raise e
