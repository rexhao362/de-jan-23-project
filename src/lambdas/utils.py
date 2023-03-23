import pandas as pd
import json
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
        df = pd.DataFrame(table['data'], columns=table['headers'])
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
