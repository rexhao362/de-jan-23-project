import pandas as pd
import boto3
import json
import logging
from os.path import (join, exists)
from os import makedirs


def write_file_to_local(filepath: str,
                        table: pd.DataFrame, filename: str) -> None:
    """
    Write parquet from pandas dataframe.

    Args:
        filepath: Filepath to the local processing bucket
        table: Pandas dataframe containing the table to be processed
        filename: filename for the output file at <filepath>/<filename>

    Returns: None
    """
    parquet_binary = table.to_parquet()
    if not exists(filepath):
        makedirs(filepath)
    with open(join(filepath, filename), "wb") as outfile:
        outfile.write(parquet_binary)


def load_file_from_local(filepath: str) -> dict[int, dict[list, list]]:
    """
    Load json data representing a table from local .json file.

    Args:
        filepath: Complete filepath to the .json to be read

    Returns:
        Dictionary object containing status and table
            ['status']: status code (int)
            ['table']: table (dict)
                ['headers']: headers (list)
                ['data']: data (list)
    """
    file_wrapper = {
        "status": 404,
        "table": {
                "headers": [],
                "data": []
            }
    }
    json_data = open(filepath)
    data = json.load(json_data)
    json_data.close()
    if(data):
        file_wrapper["table"] = data
        file_wrapper["status"] = 200
    if(len(data) == 0):
        file_wrapper['table'] = []
    return file_wrapper


def load_file_from_s3(bucket: str, key: str) -> dict:
    """
    Loads JSON from S3.

    Args:
        bucket: Bucket name
        key: Object key

    Returns: Response as a dictionary
        ['status']: status code (int)
        ['table']: parsed JSON (dict)

    Raises:
        KeyError: Raises an exception.
    """
    client = boto3.client('s3')
    file_wrapper = {
        "status": 404,
        "table": None,
    }
    try:
        response = client.get_object(
            Bucket=bucket,
            Key=key
        )
        file_wrapper["status"] = 200
        file_wrapper["table"] = json.loads(
            response["Body"].read().decode("utf-8"))
    except Exception:
        logging.error('Could not get file from bucket')
    return file_wrapper


def process(table: dict) -> pd.DataFrame:
    """
    Converts dictionary into a pandas DataFrame.

    Args:
        table: Dictionary representing a table from the ingestion bucket

    Returns: Table as a dataframe

    Raises:
        KeyError: Raises an exception.
    """
    try:
        df = pd.DataFrame(
            table["table"]["data"], columns=table["table"]["headers"])
    except KeyError as e:
        raise(e)
    return df


def print_csv(dataframe: pd.DataFrame, filepath: str) -> None:
    """
    Writes given dataframe to .csv

    Args:
        dataframe: Dataframe to be written
        filepath: Filepath to the output file

    Returns:
        None
    """
    dataframe.to_csv(filepath)


def timestamp_to_date(table: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Converts given column of timecode to datetime object,
    truncating to YYYY/MM/DD

    Args
        table: Dataframe containign a timecode column
        column: Column key for the timecode column

    Returns:
        Dataframe of one column with datetime objects
    """
    table[column] = pd.to_datetime(table[column])
    table[column] = table[column].dt.date
    return table


def write_to_bucket(bucket_name: str, table: pd.DataFrame, key: str) -> dict:
    """
    Converts pandas dataframe to parquet and
    puts the parquet in the specified S3 bucket.

    Args:
        bucket_name: Processing bucket name for the output parquet file
        table: Table to be converted and put into S3
        key: Key for the output table in the S3 bucket

    Returns:
        A dictionary with a status code and response
            ['status']: Status code (int)
            ['response']: Response from AWS (dict)
    }

    Raises:
        Exception: If could not put table into bucket
    """
    response_object = {
        "status": 404,
        "response": None,
    }

    s3 = boto3.client("s3")
    parquet_binary = table.to_parquet(engine='pyarrow')

    try:
        response = s3.put_object(
            Body=parquet_binary, Bucket=bucket_name, Key=f'{key}')
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            response_object["status"] = status
            response_object["response"] = response

    except Exception as e:
        logging.error("Could not put table into bucket")
        raise e

    return response_object


def get_last_updated(bucket_name: str, local: bool = False) -> tuple[str, str]:
    """
    Gets the datetime held within s3://date/last_updated.json
    and returns the date and time

    Args:
        bucket_name: Name of the ingestion bucket
        local: local, optional, defaults to false

    Returns:
        A tuple containing a date and time
            [0]: date (str)
            [1]: time (str)
    """

    try:
        if local:
            res = load_file_from_local(
                join(bucket_name, 'date/last_updated.json'))
        else:
            res = load_file_from_s3(bucket_name, 'date/last_updated.json')
        timestamp = res['table']['last_updated']
        return (timestamp[:10], timestamp[11:19])
    except Exception:
        logging.error('Could not retrieve last updated json')
        return (None, None)


# TODO enter default json structure if file not found,
# so dataframe compehension can proceed
def get_all_jsons(bucket_name: str, date: str,
                  time: str, local: bool = False) -> dict:
    """
    Unfinished or unstable, do not use.

    Gets all the most recent JSONs from the ingestion
    bucket according to the last_updated.json.

    Args:
        bucket_name: name of the bucket
        date: date string
        time: time string
        local (optional): If true, use local buckets,
        if false, use S3. Defaults to False.

    Returns:
        Dictionary of tables
            ['address']: The address table (dict)
            ['counterparty']: The counterparty table (dict)
            ['currency']: The currency table (dict)
            ['department']: The department table (dict)
            ['design']: The design table (dict)
            ['payment']: The payment table (dict)
            ['payment_type']: The payment_type table (dict)
            ['purchase_order']: The purchase_order table (dict)
            ['sales_order']: The sales_order table (dict)
            ['staff']: The staff table (dict)
            ['transaction']: The transaction table (dict)
    """
    files = ['address',
             'counterparty',
             'currency',
             'department',
             'design', 'payment',
             'payment_type',
             'purchase_order',
             'sales_order',
             'staff',
             'transaction']
    date, time = get_last_updated(bucket_name, local=local)
    json_files = {}
    for file in files:
        try:
            if not local:
                json_files[file] = load_file_from_s3(
                    bucket_name, f'{date}/{time}/{file}.json')['table']
            else:
                json_files[file] = load_file_from_local(
                    bucket_name, f'{date}/{time}/{file}.json')['table']

        except Exception:
            json_files[file] = {
                'table_name': file, 'headers': None, 'data': None}

    return json_files


def bucket_cleanup(bucket_name) -> dict[int, str]:
    """
    Removes contents of S3 bucket, for use before and after process execution.

    Args:
        bucket_name: Name of the bucket to clean up

    Returns:
        Dictionary containing a status code and response
            ['status']: status code (int)
            ['response']: AWS response (str)
    """
    response_object = {
        "status": 404,
        "response": None,
    }
    # list all the current objects, and use that to delete
    s3 = boto3.client("s3")
    try:
        list_response = s3.list_objects_v2(Bucket=bucket_name)
    except Exception:
        logging.error('Failed to list bucket contents before deletion.')
    try:
        keys_to_delete = [
            {'Key': item['Key']} for item in list_response['Contents']]
        delete_config = {
            'Objects': keys_to_delete
        }
        delete_response = s3.delete_objects(Bucket=bucket_name,
                                            Delete=delete_config)
        response_object['status'] = 200
        response_object['response'] = delete_response
        return response_object
    except Exception:
        logging.error('Failed to delete items from bucket.')
        return response_object
