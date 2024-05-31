from google.cloud import bigquery


def create(
        view_name,
        table_name,
        schema=None,
        delete_view_if_exists=False,
        description=None,
        labels=None,
):
    print(f'Creating view: {view_name}')

    client = bigquery.Client()

    if delete_view_if_exists:
        delete_if_exists(view_name)

    view_id = view_name
    source_id = table_name
    view_ref = bigquery.Table(view_id)

    if description is not None:
        print(f'Setting description to the view: {description}')
        view_ref.description = description

    if labels is not None:
        print(f'Setting labels to the view: {labels}')
        view_ref.labels = labels

    view_ref.view_query = f"SELECT * FROM `{source_id}`"

    view = client.create_table(view_ref, exists_ok=True)
    print(f'View created: {str(view.reference)}')

    if schema is not None:
        view_ref = bigquery.Table(view_id, schema=schema)
        client.update_table(view_ref, ['schema'])


def delete_if_exists(view_name):
    print(f'Deleting view (if exists) with name: {view_name}')
    client = bigquery.Client()

    client.delete_table(view_name, not_found_ok=True)
    print(f'View deleted: {view_name}')


def get_default_view_description(table_name: str) -> str:
    return f'''
This view points to the latest version of the {table_name} table.
The table should be next to the view in the same dataset. If you want to get more information about the table schema
or sources, please visit the table documentation.
    '''
