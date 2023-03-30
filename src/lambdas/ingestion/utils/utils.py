from datetime import datetime
from datetime import timedelta
from decimal import Decimal
import boto3
import pg8000.native
import json
import logging

from src.environ.totesys_db import totesys_db_user as user
from src.environ.totesys_db import totesys_db_password as passwd
from src.environ.totesys_db import totesys_db_host as host
from src.environ.totesys_db import totesys_db_port as port
from src.environ.totesys_db import totesys_db_database as db
from src.environ.totesys_db import totesys_db_schema as db_schema_name


# DB connection
con = pg8000.native.Connection(
    user=user,
    host=host,
    database=db,
    port=port,
    password=passwd
)


def get_table_names():
    """
    Queries database for table names

    Args:
        No args

    Returns:
        List of table names

    Raises:
        Error: Raises an exception.
    """
    try:
        table_names = con.run(
            """SELECT table_name FROM information_schema.tables
            WHERE table_schema = :schema""",
            schema=db_schema_name
        )
        return [item[0] for item in table_names]

    except Exception as e:
        logging.error(e, 'Table names were not extracted')


def get_headers():
    """
    Extracts column names for current table

    Args:
        No args

    Returns:
        List of column names for the
        last queried table

    Raises:
        Error: Raises an exception.
    """
    try:
        return [c['name'] for c in con.columns]

    except Exception as e:
        logging.error(e, 'Column names were not extracted')


def get_table_data(table_name, timestamp):
    """
    Extracts list of table data

    Args:
        param1: table name
        param2: the last update timestamp retrieved using the
        retrieve_last_updated() function

    Returns:
        list of nested lists with the first row contains
        the column names and each subsequent row is a
        row of data from the table

    Raises:
        Error: Raises an exception.
    """

    try:
        if table_name in [
            'address',
            'counterparty',
            'currency',
            'department',
            'design',
            'payment_type',
            'payment',
            'purchase_order',
            'sales_order',
            'staff',
            'transaction'
        ]:
            if timestamp is None:
                table_data = con.run(f'SELECT * FROM {table_name}')
            else:
                table_data = con.run(f"""
                SELECT * FROM {table_name}
                WHERE last_updated >
                TO_TIMESTAMP(:update_ts, :date_format)
                """,
                                     update_ts=(timestamp + timedelta(seconds=1)).strftime(
                                         '%d-%m-%Y %H:%M:%S.%f'),
                                     date_format="dd-mm-yyyy hh24:mi:ss"
                                     )
                
            # if len(table_data) > 0:
            #     logging.info(f'{table_name} has {len(table_data)} rows')
            table_data.insert(0, get_headers())
            return table_data

    except Exception as e:
        logging.error(e, f'Table {table_name} was not extracted')


def get_ingested_bucket_name():
    """
    Returns name of ingested S3 bucket

    Args:
        no args

    Returns:
        The name of the ingested bucket name

    Raises:
        Error: Raises an exception.
    """
    try:
        s3 = boto3.client('s3')
        list_buckets = s3.list_buckets()
        bucket_prefix = 's3-de-ingestion-query-queens'
        bucket_name = ''
        for bucket in list_buckets['Buckets']:
            if bucket['Name'].startswith(bucket_prefix):
                bucket_name = bucket['Name']
        return bucket_name

    except Exception as e:
        logging.error(e, 'Bucket name was not extracted')


def upload_to_s3(table, date_time):
    """
    Uploads files made by data_ingestion() function to the
    necessary s3 bucket with the current date
    and times as the file path

    Args:
        No args

    Returns:
        No returns

    Raises:
        Error: Raises an exception.
    """
    try:
        s3 = boto3.client('s3')
        key = f'{date_time}/{table["table_name"]}.json'
        table_json = json.dumps(table)
        s3.put_object(
            Body=table_json,
            Bucket=get_ingested_bucket_name(),
            Key=key
        )

    except Exception as e:
        logging.error(e, 'JSON were not uploaded')


def retrieve_last_updated():
    """
    Retrieves the last updated date from the s3 bucket.
    If the file does not exist then uses a default date.

    Args:
        No args

    Returns:
        Returns the last updated date
        as a datetime object

    Raises:
        Error: Raises an exception.
    """
    try:
        s3 = boto3.client('s3')
        bucket_name = get_ingested_bucket_name()
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix='date/last_updated.json'
        )
        if 'Contents' not in response:
            return None
        else:
            res = s3.get_object(
                Bucket=bucket_name,
                Key='date/last_updated.json'
            )
            json_res = json.loads(res['Body'].read())
            timestamp = json_res['last_updated']
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')

    except Exception as e:
        logging.error(e, 'Timestamp was not retrieved')


def store_last_updated(timestamp):
    """
    Finds the last updated date.
    Writes it to a file and then
    sends it to the s3 bucket.

    Args:
        param1: the last update timestamp retrieved using
        the retrieve_last_updated() function to pass
        to the get_table_data()

    Returns:
        String for use in file path

    Raises:
        Error: Raises an exception.
    """

    try:
        date_to_store = timestamp
        for table_name in get_table_names():
            if table_name in [
                'address',
                'counterparty',
                'currency',
                'department',
                'design',
                'payment_type',
                'payment',
                'purchase_order',
                'sales_order',
                'staff',
                'transaction'
            ]:
                most_recent = con.run(
                    f"""SELECT last_updated FROM {table_name}
                    GROUP BY last_updated ORDER BY
                    last_updated DESC LIMIT 1""")[0][0]
                if date_to_store is None or most_recent > date_to_store:
                    date_to_store = most_recent

        s3 = boto3.client('s3')
        bucket_name = get_ingested_bucket_name()
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix='date/'
        )
        if 'Contents' in response:
            s3.copy_object(
                Bucket=bucket_name,
                CopySource=f'{bucket_name}/date/last_updated.json',
                Key='date/date_2.json'
            )

        # writes files to local folder
        date_string = date_to_store.strftime('%Y-%m-%dT%H:%M:%S.%f')
        date_object = {'last_updated': date_string}
        date_json = json.dumps(date_object)
        #------
        with open('./local/aws/s3/ingested/date/last_updated.json', 'w') as f:
            f.write(date_json)
        #-----

        # uploads files to S3 bucket
        s3.put_object(
            Body=date_json,
            Bucket=bucket_name,
            Key='date/last_updated.json'
        )

        return f'{date_string[:10]}/{date_string[11:19]}'

    except Exception as e:
        logging.error(e, 'Timestamp was not stored')


def make_table_dict(table_name, table_data):
    """
    Turns table data into dictionary form

    Args:
        param1: table name
        param2: table data received from get_table_data function

    Returns:
        dictionary form of table data
        including table headers and data

    Raises:
        Error: Raises an exception.
    """
    try:
        for row in table_data:
            for i in range(len(row)):
                if isinstance(row[i], datetime):
                    row[i] = row[i].strftime('%Y-%m-%dT%H:%M:%S.%f')
                elif isinstance(row[i], Decimal):
                    row[i] = float(row[i])

        data = table_data[1:] if len(table_data) > 1 else []

        return {
            'table_name': table_name,
            'headers': table_data[0],
            'data': data
        }

    except Exception as e:
        logging.error(e, 'Table was not made in dict form')
