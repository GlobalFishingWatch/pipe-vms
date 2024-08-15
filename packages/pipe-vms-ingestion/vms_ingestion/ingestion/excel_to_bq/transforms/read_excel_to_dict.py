import io

import pandas as pd


def read_excel_to_dict(file_content):
    # This function processes each Excel file and yields dictionaries for BigQuery
    df = pd.read_excel(io.BytesIO(file_content))

    # Yielding each row as a dictionary for BigQuery
    for _, row in df.iterrows():
        yield row.to_dict()
