from google.cloud import bigquery
from logger import logger

logging = logger.get_logger()


def create(
    view_name,
    query,
    schema=None,
    delete_view_if_exists=False,
    description=None,
    labels=None,
):
    logging.info(f"Creating view: {view_name}")

    client = bigquery.Client()

    if delete_view_if_exists:
        delete_if_exists(view_name)

    view_id = view_name
    view_ref = bigquery.Table(view_id)

    if description is not None:
        logging.info(f"Setting description to the view: {description}")
        view_ref.description = description

    if labels is not None:
        logging.info(f"Setting labels to the view: {labels}")
        view_ref.labels = labels

    view_ref.view_query = query

    view = client.create_table(view_ref, exists_ok=True)
    logging.info(f"View created: {str(view.reference)}")

    if schema is not None:
        view_ref = bigquery.Table(view_id, schema=schema)
        client.update_table(view_ref, ["schema"])


def delete_if_exists(view_name):
    logging.info(f"Deleting view (if exists) with name: {view_name}")
    client = bigquery.Client()

    client.delete_table(view_name, not_found_ok=True)
    logging.info(f"View deleted: {view_name}")


def get_default_view_description(table_name: str) -> str:
    return f"""
This view points to the latest version of the {table_name} table.
The table should be next to the view in the same dataset. If you want to get more information about the table schema
or sources, please visit the table documentation.
    """
