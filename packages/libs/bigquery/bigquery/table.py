from typing import List
from google.cloud import bigquery

def create(
        query,
        destination_table,
        delete_table_if_exists=False,
        schema=None,
        destination_table_description=None,
        labels=None,
        write_disposition='WRITE_TRUNCATE'
):
    print(f'Creating table: {destination_table}')
    client = bigquery.Client()

    if delete_table_if_exists:
        delete_if_exists(destination_table)

    # BQ Table reference
    table_ref = bigquery.Table(destination_table)

    if destination_table_description is not None:
        print(f'Setting description to the table: {destination_table_description}')
        table_ref.description = destination_table_description

    if labels is not None:
        print(f'Setting labels to the table: {labels}')
        table_ref.labels = labels

    client.create_table(table_ref, exists_ok=True)

    # BigQuery job
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = write_disposition
    job = client.query(query, job_config=job_config)
    if job.error_result:
        msg = job.error_result["message"]
        print(f'Query to produce {destination_table} failed with error: {msg}')
        raise RuntimeError(msg)
    else:
        job.result()

    print(f'Table created: {destination_table}')
    print(f'Schema: \n {schema}')
    if schema is not None:
        table_ref = bigquery.Table(destination_table, schema=schema)
        client.update_table(table_ref, ['schema'])


def copy(table, destination_table, write_disposition='WRITE_TRUNCATE'):
    client = bigquery.Client()
    job_config = bigquery.job.CopyJobConfig()
    job_config.write_disposition = write_disposition
    job = client.copy_table(table, destination_table, job_config=job_config)

    if job.error_result:
        msg = job.error_result["message"]
        print(f'Process to copy {destination_table} failed with error: {msg}')
        raise RuntimeError(msg)
    else:
        job.result()

    print(f'Table copied: {destination_table}')


def delete_if_exists(table_name):
    print(f'Deleting table (if exists) with name: {table_name}')
    client = bigquery.Client()

    client.delete_table(table_name, not_found_ok=True)
    print(f'Table deleted: {table_name}')


def get_default_table_description(
        query_link: str = 'not provided',
        sources: str = 'not provided',
        extra_info: str = 'not provided',
        data_or_analyst_maintainer: str = 'not provided',
        engineer_maintainer: str = 'not provided',
        research_maintainer: str = 'not provided',
) -> str:
    description = f'''

This table was generated using the next query: 
{query_link}      

=============================================
Sources:
=============================================
{sources} 

=============================================
More details or specifications (optional):
=============================================
{extra_info}

=============================================
Maintainers:
=============================================
Data / Analyst: {data_or_analyst_maintainer}
Engineer: {engineer_maintainer}
Research: {research_maintainer}

    '''

    return description

def clear_records(table_id, date_field, date_from, date_to, additional_conditions: List[str] = []):
    print(f'Deleting records at table {table_id} between {date_from:%Y-%m-%d} and {date_to:%Y-%m-%d}')
    [project, dataset, table] = table_id.split('.')
    client = bigquery.Client(project=project)
    conditions = ' AND '.join(['TRUE'] + additional_conditions)
    sql = f"""
            DELETE FROM `{table_id}`
            WHERE {date_field} BETWEEN '{date_from:%Y-%m-%d}' AND '{date_to:%Y-%m-%d}'
            AND {conditions}
    """
    query_job = client.query(sql,
                             bigquery.QueryJobConfig(
                                 use_legacy_sql=False,
                             )
    )
    result = query_job.result()
    print(f'Records at table {table_id} between {date_from:%Y-%m-%d} and {date_to:%Y-%m-%d} deleted')
    return result

def ensure_table_exists(table):
    print(f'Ensuring table {table.table_id} exists')
    client = bigquery.Client()
    result = client.create_table(table=table, exists_ok=True)
    print(f'Table {result.full_table_id} exists')
    return result