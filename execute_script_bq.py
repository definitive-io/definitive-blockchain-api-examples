import os
import glob
import logging
import requests
import pandas as pd
import time
import json

from typing import List

# Your API Key
api_key = "my_key"

# The headers for your requests
headers = {
    "Authorization": f"{api_key}",
    "Content-Type": "application/json"
}

# Base URL of your API
base_url = ""

sql_path = os.path.join('./', 'sql/daily')


def get_scripts(
        script_path: str
    ) -> List[object]:
    """
    Get SQL scripts from the specified path.

    Args:
    script_path (str): Path to the SQL scripts.

    Returns:
    List[object]: List of objects with dataset name and sql.
    """
    model_scripts = glob.glob(os.path.join(script_path, "**", "*.sql"), recursive=True)

    models = []

    try:
        for script in model_scripts:
            # This is necessary to ignore our schema configs vs dataset configs
            with open(script) as f:
                model_dict = {
                    "name": script.split("/")[-1].split(".")[0],
                    "script": f.read(),
                }
                models.append(model_dict)

    # TODO: Update this to be more specific logging
    except json.JSONDecodeError as error:
        logging.error(f"Error in JSON file {script}: {error}")
        raise error

    return models


def bq_to_pd_dtype(
        bq_type: str
    ) -> str:
    """
    Convert BigQuery data type to pandas data type.
    
    Args:
    bq_type (str): BigQuery data type.
    
    Returns:
    str: Equivalent pandas data type.
    """
    bq_to_pd = {
        'INT64': 'Int64',
        'INTEGER': 'Int64',
        'FLOAT64': 'float64',
        'FLOAT': 'float64',
        'NUMERIC': 'float64',
        'BOOLEAN': 'bool',
        'BOOL': 'bool',
        'STRING': 'object',
        'TEXT': 'object',
        'BYTES': 'object',
        'DATETIME': 'datetime64[ns]',
        'DATE': 'datetime64[ns]',
        'TIMESTAMP': 'datetime64[ns]',
        'TIME': 'object',
        'GEOGRAPHY': 'object',
        'ARRAY': 'object',
        'STRUCT': 'object',
    }
    
    return bq_to_pd.get(bq_type.upper(), 'object')


def normalize_response(
        response: dict
    ) -> pd.DataFrame():
    """
    Normalizes the response from the API into a DataFrame. 
    This is specific to how advisor-api returns the query result.
    Result format:
    {
        "schema": {
            "fields": [
                {
                    "name": "column_1_name",
                    "type": "column_1_type",
                    "mode": "column_1_mode"
                },
                ...
                {
                    "name": "column_x_name",
                    "type": "column_x_type",
                    "mode": "column_x_mode"
                }
            ]
        "rows": [
            [
                "column_1_value",
                ...
                "column_x_value"
            ]
        ]
        }
    }

    Args:
    response (dict): Response from the API.

    Returns:
    DataFrame: Normalized DataFrame.
    """
    # Get the schema
    columns = []
    schema = response['schema']['fields']

    for field in schema:
        columns.append(
            {
                'name': field['name'],
                'type': bq_to_pd_dtype(field['type']),
            }
        )

    # create a dataframe with the column names
    rows = response['rows']
    df = pd.DataFrame(rows, columns=[col['name'] for col in columns])

    # set the data types for each column
    for col in columns:
        df[col['name']] = df[col['name']].astype(col['type'])

    return df


def execute_sql(
        sql: str
    ) -> str:
    """
    Calls advisor-api to execute a SQL query and returns the job ID.

    Args:
    dataset (str): Dataset name.
    sql (str): SQL query.

    Returns:
    str: Job ID.
    """
    # Prepare the payload
    payload = {
        "query": sql
    }

    # Send the request
    response = requests.post(f"{base_url}/execute-sql", headers=headers, json=payload)

    # Make sure the request was successful
    if response.status_code == 200:
        data = response.json()
        return data['jobReference']['jobId']
    else:
        logging.error(f"Request failed with status code {response.status_code}")
        return None


def poll_job(
        job_id: str,
    ) -> dict():
    """
    Polls advisor-api for the result of a job.

    Args:
    job_id (str): Job ID.
    dataset (str): Dataset name.

    Returns:
    dict: Response from the API.
    """
    # Send the request
    response = requests.get(f"{base_url}/execute-sql/{job_id}/poll", headers=headers)

    # Make sure the request was successful
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        logging.error(f"Request failed with status code {response.status_code}")
        return None


def poll_job_until_complete(
        job_id: str, 
        sleep_time: int = 5
    ) -> dict():
    """
    Provides Big Query job id and dataset name to check status of job until complete.

    Args:
    job_id (str): Job ID.
    dataset (str): Dataset name.
    sleep_time (int, optional): Time to sleep between polls. Defaults to 5.

    Returns:
    dict: Response from the API.
    """
    while True:
        result = poll_job(job_id)

        if result is not None:
            return result
        else:
            # Sleep for a bit before polling again
            time.sleep(sleep_time)


def run_query(
        sql: str
    ) -> pd.DataFrame():
    """
    Executes a SQL query and returns the result as a DataFrame.

    Args:
    dataset (str): Dataset name.
    sql (str): SQL query.

    Returns:
    DataFrame: Result of the query.
    """
    job_id = execute_sql(sql)

    if job_id is not None:
        result = poll_job_until_complete(job_id)
        
        if result is not None:
            
            # Transform the result into a DataFrame
            df = normalize_response(result)
            return df
        else:
            logging.error("Job did not complete successfully.")
            return None
    else:
        logging.error("Job execution did not start successfully.")
        return None


# Execution entry point
models = get_scripts(sql_path)

for model in models:
    df = run_query(model['script'])

    print(df)
    logging.info(df)
