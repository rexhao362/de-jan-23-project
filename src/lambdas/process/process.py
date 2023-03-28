from os.path import join
import logging
from src.lambdas.process.utils import *
from src.lambdas.process.build import *


def main_local(path):
    LOCAL_INGESTION_DIRECTORY = join(path, "ingested")
    LOCAL_PROCESSING_DIRECTORY = join(path, "processed")

    date, time = get_last_updated(LOCAL_INGESTION_DIRECTORY, local=True)
    jsons = get_all_jsons(LOCAL_INGESTION_DIRECTORY, date, time, local=True)

    current_timestamp = f"{date}/{time}"

    if (len(jsons) == 11):
        # create a lookup object
        table_names = {
            'currency': current_timestamp + '/currency.json',
            'design': current_timestamp + '/design.json',
            'staff': current_timestamp + '/staff.json',
            'department': current_timestamp + '/department.json',
            # 'purchase_order': current_timestamp + '/purchase_order.json',
            'counterparty': current_timestamp + '/counterparty.json',
            'address': current_timestamp + '/address.json',
            'sales_order': current_timestamp + '/sales_order.json',
            # 'payment_type': current_timestamp + '/payment_type.json',
            # 'payment': current_timestamp + '/payment.json',
            # 'transaction': current_timestamp + '/transaction.json'
        }

# Try loading all the data
    success = False

    try:
        dim_currency = load_file_from_local(
            join(LOCAL_INGESTION_DIRECTORY, table_names['currency']))
        dim_design = load_file_from_local(
            join(LOCAL_INGESTION_DIRECTORY, table_names['design']))
        dim_staff = load_file_from_local(
            join(LOCAL_INGESTION_DIRECTORY, table_names['staff']))
        dim_counterparty = load_file_from_local(
            join(LOCAL_INGESTION_DIRECTORY, table_names['counterparty']))
        fact_sales_order = load_file_from_local(
            join(LOCAL_INGESTION_DIRECTORY, table_names['sales_order']))
        dim_department = load_file_from_local(
            join(LOCAL_INGESTION_DIRECTORY, table_names['department']))
        dim_address = load_file_from_local(
            join(LOCAL_INGESTION_DIRECTORY, table_names['address']))
        success = True
    except Exception as e:
        # Do something with the exception, log it to Cloudwatch
        logging.error("Couldn't load tables.")

    # If all is well, try processing dictionaries

    if (success):
        success = False
        try:
            dim_currency = process(dim_currency)
            dim_design = process(dim_design)
            dim_staff = process(dim_staff)
            dim_location = process(dim_address)
            dim_counterparty = process(dim_counterparty)
            fact_sales_order = process(fact_sales_order)
            dim_department = process(dim_department)
            dim_address = process(dim_address)
            success = True
        except Exception as e:
            # Do something with the exception, log it to Cloudwatch
            logging.error("Couldn't process tables.")

    # if all is well, try remodeling dataframes

    if (success):
        success = False
        try:
            dim_currency = build_dim_currency(dim_currency)
            dim_design = build_dim_design(dim_design)
            dim_staff = build_dim_staff(dim_staff, dim_department)
            dim_location = build_dim_location(dim_address)
            dim_date = build_dim_date("2020/01/01", "2050/01/01")
            dim_counterparty = build_dim_counterparty(
                dim_counterparty, dim_address)
            fact_sales_order = build_fact_sales_order(fact_sales_order)
            success = True
        except Exception as e:
            # Do something with the exception, log it to Cloudwatch
            logging.error("Couldn't remodel dataframes.")

    # If all is well, try writing remodeled dataframes to bucket
    if (success):
        try:
            write_file_to_local(join(LOCAL_PROCESSING_DIRECTORY, current_timestamp), dim_currency, "currency.parquet")
            write_file_to_local(join(LOCAL_PROCESSING_DIRECTORY, current_timestamp), dim_design,
                                "design.parquet")
            write_file_to_local(join(LOCAL_PROCESSING_DIRECTORY, current_timestamp), dim_staff,
                                "staff.parquet")
            write_file_to_local(join(LOCAL_PROCESSING_DIRECTORY, current_timestamp), dim_location,
                                "location.parquet")
            write_file_to_local(join(LOCAL_PROCESSING_DIRECTORY, current_timestamp), dim_date,
                                "date.parquet")
            write_file_to_local(join(LOCAL_PROCESSING_DIRECTORY, current_timestamp), dim_counterparty,
                                "counter_party.parquet")
            write_file_to_local(join(LOCAL_PROCESSING_DIRECTORY, current_timestamp), fact_sales_order,
                                "sales_order.parquet")

            logging.info("All processed tables are written to the bucket.")

        except Exception as e:
            # Do something with the exception, tell Cloudwatch, and clean up the bucket
            logging.error("Couldn't write tables to bucket.")
            print(e)

            # TO-DO clean up bucket ticket 85


def main_s3():
    INGESTION_BUCKET_NAME = "query_queens_ingestion_bucket"
    PROCESSING_BUCKET_NAME = "query_queens_processing_bucket"

    date, time = get_last_updated(INGESTION_BUCKET_NAME)
    jsons = get_all_jsons(INGESTION_BUCKET_NAME, date, time)

    current_timestamp = f"{date}/{time}"

    if (len(jsons) == 11):
        # create a lookup object
        table_names = {
            'currency': current_timestamp + '/currency.json',
            'design': current_timestamp + '/design.json',
            'staff': current_timestamp + '/staff.json',
            'department': current_timestamp + '/department.json',
            # 'purchase_order': current_timestamp + '/purchase_order.json',
            'counterparty': current_timestamp + '/counterparty.json',
            'address': current_timestamp + '/address.json',
            'sales_order': current_timestamp + '/sales_order.json',
            # 'payment_type': current_timestamp + '/payment_type.json',
            # 'payment': current_timestamp + '/payment.json',
            # 'transaction': current_timestamp + '/transaction.json'
        }

        # Try loading all the data
        success = False

        try:
            dim_currency = load_file_from_s3(
                INGESTION_BUCKET_NAME, table_names['currency'])
            dim_design = load_file_from_s3(
                INGESTION_BUCKET_NAME, table_names['design'])
            dim_staff = load_file_from_s3(
                INGESTION_BUCKET_NAME, table_names['staff'])
            dim_counterparty = load_file_from_s3(
                INGESTION_BUCKET_NAME, table_names['counterparty'])
            fact_sales_order = load_file_from_s3(
                INGESTION_BUCKET_NAME, table_names['sales_order'])
            dim_department = load_file_from_s3(
                INGESTION_BUCKET_NAME, table_names['department'])
            dim_address = load_file_from_s3(
                INGESTION_BUCKET_NAME, table_names['address'])
            success = True
        except Exception as e:
            # Do something with the exception, log it to Cloudwatch
            logging.error("Couldn't load tables.")

        # If all is well, try processing dictionaries

        if (success):
            success = False
            try:
                dim_currency = process(dim_currency)
                dim_design = process(dim_design)
                dim_staff = process(dim_staff)
                dim_location = process(dim_address)
                dim_counterparty = process(dim_counterparty)
                fact_sales_order = process(fact_sales_order)
                dim_department = process(dim_department)
                dim_address = process(dim_address)
                success = True
            except Exception as e:
                # Do something with the exception, log it to Cloudwatch
                print(e)
                logging.error("Couldn't process tables.")

        # if all is well, try remodeling dataframes

        if (success):
            success = False
            try:
                dim_currency = build_dim_currency(dim_currency)
                dim_design = build_dim_design(dim_design)
                dim_staff = build_dim_staff(dim_staff, dim_department)
                dim_location = build_dim_location(dim_address)
                dim_date = build_dim_date("2020/01/01", "2050/01/01")
                dim_counterparty = build_dim_counterparty(
                    dim_counterparty, dim_address)
                fact_sales_order = build_fact_sales_order(fact_sales_order)
                success = True
            except Exception as e:
                # Do something with the exception, log it to Cloudwatch
                print(e)
                logging.error("Couldn't remodel dataframes.")

        # If all is well, try writing remodeled dataframes to bucket
        if (success):
            try:
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_currency,
                                current_timestamp + "/currency")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_design,
                                current_timestamp + "/design")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_staff,
                                current_timestamp + "/staff")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_location,
                                current_timestamp + "/location")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_date,
                                current_timestamp + "/date")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_counterparty,
                                current_timestamp + "/counter_party")
                write_to_bucket(PROCESSING_BUCKET_NAME, fact_sales_order,
                                current_timestamp + "/sales_order")

                logging.info("All processed tables are written to the bucket.")

            except Exception as e:
                # Do something with the exception, tell Cloudwatch, and clean up the bucket
                logging.error("Couldn't write tables to bucket.")

                # TO-DO clean up bucket ticket 85


if __name__ == "__main__":
    # main_s3()
    main_local()
