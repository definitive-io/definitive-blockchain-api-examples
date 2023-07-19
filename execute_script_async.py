import os
import glob
import logging
import requests
import pandas as pd
import time
import json

from typing import List

# Your API Key
api_key = ""

# The headers for your requests
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

# Base URL of your API
base_url = "https://public.advisor.definitive.io/v0"

sql_path = os.path.join('./', 'sql/ethereum_v1_0_x/daily')


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
                    "dataset": script.split("/")[2],
                    "script": f.read(),
                }
                models.append(model_dict)

    # TODO: Update this to be more specific logging
    except json.JSONDecodeError as error:
        logging.error(f"Error in JSON file {script}: {error}")
        raise error

    return models


def normalize_response(
        response: dict
    ) -> pd.DataFrame():
    """
    Normalizes the response from the API into a DataFrame. 
    This is specific to how advisor-api returns the query result.

    Args:
    response (dict): Response from the API.

    Returns:
    DataFrame.
    """

    df = pd.DataFrame(**response)

    return df


def execute_sql(
        sql: str,
        dataset: str
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
        "query": {
            "type": "sql",
            "sql": sql,
        },
        "dataset": {
            "type": "warehouse",
            "id": dataset,
        },
        "timeout": 0  # Set timeout to get results in one call. If not successful, poll job_id
    }

    # Send the request
    response = requests.post(f"{base_url}/datasets/execute", headers=headers, json=payload)

    # Make sure the request was successful
    if response.status_code == 200 and response.json()['status'].lower() == 'completed':
        logging.info(response.json()['result'])

        return response.json()['result']

    elif response.status_code == 200 and response.json()['status'].lower() != 'completed':
        logging.info(f"Request successful but job is still pending. Job ID: {response.json()['job_id']}")

        return response.json()

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
    response = requests.get(f"{base_url}/datasets/execute/{job_id}", headers=headers)

    # Make sure the request was successful
    if response.status_code == 200 and response.json()['status'].lower() == 'completed':
        data = response.json()['result']
        return data

    else:
        logging.error(f"Request incomplete with status code {response.status_code} and response status {response.json()['status']}.")
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
        sql: str,
        dataset: str
    ) -> pd.DataFrame():
    """
    Executes a SQL query and returns the result as a DataFrame.

    Args:
    dataset (str): Dataset name.
    sql (str): SQL query.

    Returns:
    DataFrame: Result of the query.
    """
    result = execute_sql(sql, dataset)
    df = None

    if result is not None:

        # If response did not contain result on execute, poll results using job_id
        if 'job_id' in result:
            data = poll_job_until_complete(result['job_id'])
        
            if data is not None and data['type'] == 'dataframe':
                # Transform the result into a DataFrame
                df = normalize_response(data['dataframe'])
            else:
                logging.error("Job result is not valid.")

        elif 'type' in result :
            if result['type'] == 'dataframe':
                df = normalize_response(result['dataframe'])

            else:
                logging.error(f"Result type {result['type']} not yet supported.")

        else:
            logging.error("Job did not complete successfully.")

    else:
        logging.error("Job execution did not start successfully.")

    return df



# Execution entry point
models = get_scripts(sql_path)

for model in models:
    df = run_query(model['script'], model['dataset'])

    # Verify dataframe results
    print(df)
    logging.info(df)
