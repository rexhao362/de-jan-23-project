from os.path import join
import logging
from src.lambdas.process.utils import *
from src.lambdas.process.build import *
from os import environ
from src.utils.environ import *
dev_environ_variable = "DE_Q2_DEV"
dev_environ_variable_value = "local"

input_tables = {
    'currency': {
        'table_name': 'currency',
        'dataframe': None,
        'required': True,
    },
    'design': {
        'table_name': 'design',
        'dataframe': None,
        'required': True
    },
    'staff': {
        'table_name': 'staff',
        'dataframe': None,
        'required': True
    },
    'department': {
        'table_name': 'department',
        'dataframe': None,
        'required': True
    },
    'counterparty': {
        'table_name': 'counterparty',
        'dataframe': None,
        'required': True
    },
    'address': {
        'table_name': 'address',
        'dataframe': None,
        'required': True
    },
    'sales_order': {
        'table_name': 'sales_order',
        'dataframe': None,
        'required': True
    },
    'purchase_order': {
        'table_name': 'purchase_order',
        'dataframe': None,
        'required': False
    },
    'payment_type': {
        'table_name': 'payment_type',
        'dataframe': None,
        'required': False
    },
    'payment': {
        'table_name': 'payment',
        'dataframe': None,
        'required': False
    },
    'transaction': {
        'table_name': 'transaction',
        'dataframe': None,
        'required': False
    },
}

output_tables = [
    {
        'table_name': 'currency',
        'dataframe': None,
        'dependencies': ['currency'],
        'build_function': build_dim_currency,
        'prefix': 'dim_'
    },
    {
        'table_name': 'design',
        'dataframe': None,
        'dependencies': ['design'],
        'build_function': build_dim_design,
        'prefix': 'dim_'
    },
    {
        'table_name': 'staff',
        'dataframe': None,
        'dependencies': ['staff', 'department'],
        'build_function': build_dim_staff,
        'prefix': 'dim_'
    },
    {
        'table_name': 'location',
        'dataframe': None,
        'dependencies': ['address'],
        'build_function': build_dim_location,
        'prefix': 'dim_'
    },
    {
        'table_name': 'date',
        'dataframe': None,
        'dependencies': [],
        'build_function': build_dim_date,
        'prefix': 'dim_'
    },
    {
        'table_name': 'counterparty',
        'dataframe': None,
        'dependencies': ['counterparty', 'address'],
        'build_function': build_dim_counterparty,
        'prefix': 'dim_'
    },
    {
        'table_name': 'sales_order',
        'dataframe': None,
        'dependencies': ['sales_order'],
        'build_function': build_fact_sales_order,
        'prefix': 'fact_'
    },
]


def main(path='', force_local=False, force_s3=False, ingestion_bucket_name="query-queens-ingestion-bucket",
         processing_bucket_name="query-queens-processing-bucket", ingestion_directory_name="ingestion", processing_directory_name="processed"):
    local = is_dev_environ() or force_local
    if force_s3:
        local = False
    INGESTION_BUCKET_NAME = ingestion_bucket_name if not local else join(
        path, ingestion_directory_name)
    PROCESSING_BUCKET_NAME = processing_bucket_name if not local else join(
        path, processing_directory_name)

    #TODO cleanup_bucket before execution, then we can just raise exceptions and halt execution instead of checking with success boolean
    bucket_cleanup(PROCESSING_BUCKET_NAME)

    date, time = get_last_updated(INGESTION_BUCKET_NAME, local=local)

    current_timestamp = f"{date}/{time}"

    for key, table in input_tables.items():
        table['filename'] = join(
            current_timestamp, table['table_name'] + '.json')

# Try loading all the data

    try:
        for key, table in input_tables.items():
            if table['required']:
                table['dataframe'] = load_file_from_local(
                    join(INGESTION_BUCKET_NAME, table['filename'])) if local \
                    else load_file_from_s3(INGESTION_BUCKET_NAME, table['filename'])
    except Exception as e:
        # Do something with the exception, log it to Cloudwatch
        logging.error("Couldn't load tables.")
        raise Exception

    # If all is well, try processing dictionaries

    try:
        for key, table in input_tables.items():
            if table['required']:
                table['dataframe'] = process(table['dataframe'])
    except Exception as e:
        # Do something with the exception, log it to Cloudwatch
        logging.error("Couldn't process tables.")
        raise Exception
    # if all is well, try remodeling dataframes

    try:
        for table in output_tables:
            required_tables = [input_tables[dep]['dataframe']
                    for dep in table['dependencies']]
            if (table['table_name'] == 'date'):
                # start and end dates generated for the dates table
                required_tables = ['2020/01/01', '2050/01/01']
            table['dataframe'] = table['build_function'](*required_tables)
    except Exception as e:
        # Do something with the exception, log it to Cloudwatch
        logging.error("Couldn't remodel dataframes.")
        raise Exception

    # If all is well, try writing remodeled dataframes to bucket, god willing
    try:
        for table in output_tables:
            table_output_path = f"{table['prefix']}{table['table_name']}.parquet"
            write_file_to_local(PROCESSING_BUCKET_NAME, table['dataframe'], table_output_path) if local \
            else write_to_bucket(PROCESSING_BUCKET_NAME, table['dataframe'], table_output_path)
        logging.info("All processed tables are written to the bucket.")

    except Exception as e:
        # Do something with the exception, tell Cloudwatch, and clean up the bucket
        # remove contents of bucket
        logging.error("Couldn't write tables to bucket.")
        raise Exception

if __name__ == "__main__":
    main()


def main_local(**kwargs):
    # compatibility wrapper for tests
    return main(force_local=True, **kwargs)


def main_s3(**kwargs):
    # compatiblity wrapper for tests
    return main(force_s3=True, **kwargs)
