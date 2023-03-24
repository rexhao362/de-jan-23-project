import pandas as pd
import boto3
import json
import logging
import re


# import pyarrow

def load_file_from_local(filepath):
    """
    Load JSON from local path.

    Args:
        param1: filepath, string

    Returns: data, dict
    """
    json_data = open(filepath)
    data = json.load(json_data)
    json_data.close()
    return data

def load_file_from_s3(bucket, key):
    """
    Loads JSON from S3.

    Args:
        param1: bucket name, string
        param2: object key, string

    Returns: JSON, dict
    
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
        file_wrapper["table"] = json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        # print(e)
        # print(key)
        logging.error('Could not get file from bucket')
    return file_wrapper
        
def process(table):
    """
    Converts dictionary into a pandas DataFrame.

    Args:
        param1: table, dict

    Returns: table, pd data frame

    Raises: 
        KeyError: Raises an exception.
    """
    try:

        df = pd.DataFrame(table["data"], columns=table["headers"])

    except KeyError as e:
        raise(e)
    return df

def print_csv(dataframe, filepath):
    """
    Writes given dataframe to .csv

    Args:
        param1: table, dataframe
        param2: filepath, string
    """
    dataframe.to_csv(filepath)

def timestamp_to_date(table, column):
    """
    Converts given column of timecode to datetime object, trunkating to YYYY/MM/DD

    Args
        param1: table, dataframe 
        param2: column key, string

    Returns: table, dataframe
    """
    table[column] = pd.to_datetime(table[column])
    table[column] = table[column].dt.date
    return table

def write_to_bucket(bucket_name, table, key):
    """
    Posts given object to given s3 bucket.

    Args:
        param1: bucket, string
        param2: table, dataframe
        param3: key, string

    Returns: {"status" : int, "response" : dict}
    }
    """
    response_object = {
        "status": 404,
        "response": None,
    }

    s3 = boto3.client("s3")
    parquet_binary = table.to_parquet()

    try:
        response = s3.put_object(Body=parquet_binary, Bucket=bucket_name, Key=f'{key}.parquet') 
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            response_object["status"] = status
            response_object["response"] = response

    except Exception as e:
        logging.error("Could not load table into bucket")
    
    return response_object

def get_last_updated(bucket_name):
    """
    Gets and processes the datetime held within s3://date/last_updated.json
    
    Args:
        param1: bucket_name, string

    Returns:
        date, string, [0]
        time, string, [1]
    """
    try:
        res = load_file_from_s3(bucket_name, 'date/last_updated.json')
        timestamp = res['table']['last_updated']
        return (timestamp[:10], timestamp[12:19])
    except:
        logging.error('Could not retrieve last updated json')
        return (None, None)

