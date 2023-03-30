import json
import os
from src.lambdas.ingestion.utils import get_table_data
from src.lambdas.ingestion.utils import select_last_updated
from src.lambdas.ingestion.utils import make_table_dict
from src.lambdas.ingestion.utils import get_table_names
from src.lambdas.ingestion.utils import retrieve_last_updated
from src.lambdas.ingestion.utils import store_last_updated
from src.lambdas.ingestion.utils import upload_to_s3
import logging

from src.utils.environ import is_dev_environ
from src.utils.environ import is_production_environ

# from utils.environ import set_dev_environ


def data_ingestion(path=None):
    """
    Uses the get_table_names() and the get_table_data() functions
    to retrieve data for each table. Formats datetime objects into
    string and Decimal objects into float. Turns each table into a
    dictionary and saves them to json files.

    Args:
        param1: the last update timestamp retrieved using the
        retrieve_last_updated() function to pass to the
        get_table_data()

    Returns:
        no return

    Raises:
        Error: Raises an exception.
    """
    if is_dev_environ():
        os.makedirs(f'{path}/date', exist_ok=True)
    #-------
    timestamp = retrieve_last_updated()
    date_time = select_last_updated(timestamp)
    # if check_s3_data():
    #     return None
    #-------
    if is_dev_environ():
        os.makedirs(f'{path}/{date_time[0]}', exist_ok=True)
    #-------
    for table_name in get_table_names():
        table_data = get_table_data(table_name, timestamp)
        table_dict = make_table_dict(table_name, table_data)

        upload_to_s3(table_dict, date_time[0])
    
    store_last_updated(date_time[1], path)


if __name__ == "__main__":
    test_path = './local/aws/s3/ingested'
    try:
        data_ingestion(test_path)

    except Exception as e:
        logging.error(f"\nError: {e}")

    finally:
        pass
