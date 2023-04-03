import os
from src.lambdas.ingestion.utils.utils import get_table_data
from src.lambdas.ingestion.utils.utils import make_table_dict
from src.lambdas.ingestion.utils.utils import upload_to_s3
from src.lambdas.ingestion.utils.utils import get_table_names
from src.lambdas.ingestion.utils.dates import create_date_string
from src.lambdas.ingestion.utils.dates import create_date_key
from src.lambdas.ingestion.utils.dates import select_last_updated
from src.lambdas.ingestion.utils.dates import retrieve_last_updated
from src.lambdas.ingestion.utils.dates import store_last_updated
from src.utils.environ import is_dev_environ
import logging


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

    timestamp = retrieve_last_updated()
    date_time = select_last_updated(timestamp)
    date_key = create_date_key(create_date_string())

    if is_dev_environ():
        os.makedirs(f'{path}/{date_key}', exist_ok=True)
        os.makedirs(f'{path}/date', exist_ok=True)

    for table_name in get_table_names():
        table_data = get_table_data(table_name, timestamp)
        table_dict = make_table_dict(table_name, table_data)

        upload_to_s3(table_dict, date_key)

    store_last_updated(date_time, date_key, path)


def lambda_handler(context, event):
    data_ingestion()
    return None


if __name__ == "__main__":
    test_path = './local/aws/s3/ingested'
    try:
        data_ingestion(path=test_path)

    except Exception as e:
        logging.error(f"\nError: {e}")

    finally:
        pass
