from os.path import join
from datetime import datetime
from decimal import Decimal
from src.lambdas.ingestion.utils.utils import get_table_data
from src.lambdas.ingestion.utils.utils import get_table_names
from src.lambdas.ingestion.utils.utils import retrieve_last_updated
from src.lambdas.ingestion.utils.utils import store_last_updated
from src.lambdas.ingestion.utils.utils import upload_to_s3
import json


def data_ingestion(path):
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
    path = join(path, "ingestion")  # TODO: use global/config variable
    timestamp = retrieve_last_updated()

    for table_name in get_table_names():
        table_entries = get_table_data(table_name, timestamp)
        for row in table_entries:
            for i in range(len(row)):
                if isinstance(row[i], datetime):
                    row[i] = row[i].strftime('%Y-%m-%dT%H:%M:%S.%f')
                elif isinstance(row[i], Decimal):
                    row[i] = float(row[i])

        data = []
        if len(table_entries) > 1:
            data = table_entries[1:]

        table_data = {
            'table_name': table_name,
            'headers': table_entries[0],
            'data': data
        }

        filepath = f'{path}/table_data/{table_name}.json'
        with open(filepath, 'w') as f:
            f.write(json.dumps(table_data))

    #upload_to_s3(path)

    store_last_updated(timestamp, path)


if __name__ == "__main__":
    test_path = "./local/aws/s3/ingestion"

    try:
        data_ingestion(test_path)

    except Exception as exc:
        msg = f"\nError: {exc}"
        exit(msg)  # replace with log() to CloudWatch

    finally:
        pass
