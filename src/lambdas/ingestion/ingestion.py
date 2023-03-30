import json
import os
from src.lambdas.ingestion.utils.utils import get_table_data
from src.lambdas.ingestion.utils.utils import make_table_dict
from src.lambdas.ingestion.utils.utils import get_table_names
from src.lambdas.ingestion.utils.utils import retrieve_last_updated
from src.lambdas.ingestion.utils.utils import store_last_updated
from src.lambdas.ingestion.utils.utils import upload_to_s3
import logging


def data_ingestion():
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
    # path = join(path, "ingested")  # TODO: use global/config variable

    #-------
    os.makedirs('./local/aws/s3/ingested/date', exist_ok=True)
    #-------
    timestamp = None #retrieve_last_updated()
    date_time = store_last_updated(timestamp)
    #-------
    os.makedirs(f'./local/aws/s3/ingested/{date_time}', exist_ok=True)
    #-------
    for table_name in get_table_names():
        table_data = get_table_data(table_name, timestamp)
        table_dict = make_table_dict(table_name, table_data)
        #-------
        with open(f'./local/aws/s3/ingested/{date_time}/{table_name}.json', 'w') as f:
            f.write(json.dumps(table_dict))
        #-------

        # upload_to_s3(table_dict, date_time)


if __name__ == "__main__":
    try:
        data_ingestion()

    except Exception as e:
        logging.error(f"\nError: {e}")

    finally:
        pass
