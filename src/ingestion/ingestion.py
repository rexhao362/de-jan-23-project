import os
from src.ingestion.utils import get_table_data
from src.ingestion.utils import make_table_dict
from src.ingestion.utils import upload_to_s3
from src.ingestion.utils import get_table_names
from src.ingestion.dates import create_date_string
from src.ingestion.dates import create_date_key
from src.ingestion.dates import select_last_updated
from src.ingestion.dates import retrieve_last_updated
from src.ingestion.dates import store_last_updated
from src.utils.environ import is_dev_environ
from src.utils.path import join
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
    path = join(path, 'ingestion')

    timestamp = retrieve_last_updated()
    date_time = select_last_updated(timestamp)
    date_now = create_date_string()
    date_key = create_date_key(date_now)

    if is_dev_environ():
        os.makedirs(f'{path}/{date_key}', exist_ok=True)
        os.makedirs(f'{path}/date', exist_ok=True)

    for table_name in get_table_names():
        table_data = get_table_data(table_name, timestamp)
        table_dict = make_table_dict(table_name, table_data)

        upload_to_s3(table_dict, date_key, path)

    store_last_updated(date_time, date_now, path)


if __name__ == "__main__":
    test_path = './local/aws/s3/'
    try:
        data_ingestion(test_path)

    except Exception as e:
        logging.error(f"\nError: {e}")

    finally:
        pass
