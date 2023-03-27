import logging
from src.lambdas.process.utils import *
from src.lambdas.process.build import *


def main_local():
    # LOAD CURRENCY DATAFRAME
    curr_path = 'test/json_files/currency_test_2.json'
    curr_data = load_file_from_local(curr_path)
    curr_dataframe = process(curr_data)

    # LOAD STAFF DATAFRAME
    staff_path = 'test/json_files/staff_test_1.json'
    staff_data = load_file_from_local(staff_path)
    staff_dataframe = process(staff_data)

    # LOAD DEPARTMENT DATAFRAME
    dep_path = 'test/json_files/department_test_1.json'
    dep_data = load_file_from_local(dep_path)
    department_dataframe = process(dep_data)

    # LOAD ADDRESS DATAFRAME
    address_path = 'test/json_files/address_test_2.json'
    address_data = load_file_from_local(address_path)
    address_dataframe = process(address_data)

    # LOAD COUNTERPARTY DATAFRAME
    counter_path = 'test/json_files/counterparty_test_2.json'
    counter_data = load_file_from_local(counter_path)
    counter_dataframe = process(counter_data)

    # LOAD DESIGN DATAFRAME
    design_path = 'test/json_files/design_test_2.json'
    design_data = load_file_from_local(design_path)
    design_dataframe = process(design_data)

    # LOAD SALES DATAFRAME
    sales_path = 'test/json_files/sales_order_test_1.json'
    sales_data = load_file_from_local(sales_path)
    sales_dataframe = process(sales_data)


def main_s3():
    INGESTION_BUCKET_NAME = "query_queens_ingestion_bucket"
    PROCESSING_BUCKET_NAME = "query_queens_processing_bucket"

    date, time = get_last_updated(INGESTION_BUCKET_NAME)
    jsons = get_all_jsons(INGESTION_BUCKET_NAME, date, time)

  # TO-DO:
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
            #'payment_type': current_timestamp + '/payment_type.json',
            #'payment': current_timestamp + '/payment.json',
            #'transaction': current_timestamp + '/transaction.json'
        }
        # Try processing all the data
        success = False
        try:
            dim_currency = build_dim_currency(table_names['currency'])
            dim_design = build_dim_design(table_names['design'])
            dim_staff = build_dim_staff(table_names['staff'], table_names['department'])
            dim_location = build_dim_location(table_names['address'])
            dim_date = build_dim_date("2020/01/01", "2050/01/01")
            dim_counterparty = build_dim_counterparty(table_names['counterparty'], table_names['address'])
            fact_sales_order = build_fact_sales_order(table_names['sales_order']) 



            success = True
        except Exception as e:
            # Do something with the exception, log it to Cloudwatch
            logging.error("Couldn't build tables.")
        # If all is well, try putting all the new tables into the bucket
        if (success):
            try:
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_currency,
                                current_timestamp + "/currency.json")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_design,
                                current_timestamp + "/design.json")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_staff,
                                current_timestamp + "/staff.json")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_location,
                                current_timestamp + "/location.json")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_date,
                                current_timestamp + "/date.json")
                write_to_bucket(PROCESSING_BUCKET_NAME, dim_counterparty,
                                current_timestamp + "/counter_party.json")
                write_to_bucket(PROCESSING_BUCKET_NAME, fact_sales_order,
                                current_timestamp + "/sales_order.json")

                logging.info("All processed tables are written to the bucket.")
                
            except Exception as e:
                # Do something with the exception, tell Cloudwatch, and clean up the bucket
                logging.error("Couldn't write tables to bucket.")
                
                # TO-DO clean up bucket ticket 85


if __name__ == "__main__":
    main_s3()
