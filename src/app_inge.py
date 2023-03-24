from datetime import datetime
from decimal import Decimal
from src.utils.utils_inge import get_table_data
from src.utils.utils_inge import get_table_names
from src.utils.utils_inge import retrieve_last_updated
from src.utils.utils_inge import store_last_updated
from src.utils.utils_inge import upload_to_s3
import json


def data_ingestion():
    """
    Uses the get_table_names() and the get_table_data() functions to retrieve data for each table. Formats datetime objects into string and Decimal objects into float. Turns each table into a dictionary and saves them to json files.

    Args:
        param1: the last update timestamp retrieved using the retrieve_last_updated() function to pass to the get_table_data()

    Returns:
        no return

    Raises:
        Error: Raises an exception.
    """

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
        # if there's no new data entries then provide empty list in table_data dict
        if len(table_entries) > 1:
            data = table_entries[1:]

        table_data = {
            'table_name': table_name,
            'headers': table_entries[0],
            'data': data
        }

        with open(f'./ingestion_function/data/{table_name}.json', 'w') as f:
            f.write(json.dumps(table_data))

    upload_to_s3()

    store_last_updated(timestamp)
