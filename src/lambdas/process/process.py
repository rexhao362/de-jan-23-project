from src.lambdas.process.utils import *
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
    # TODO - Might have issues with async stuff within a comprehension but would be nice!
    bucket_name='FOOBAR'
    date, time = get_last_updated(bucket_name)
    jsons = get_all_jsons(bucket_name, date, time)
    df_dict = {k:process(v) for (k, v) in jsons.items()}
    print(df_dict.keys())

if __name__ == "__main__":
    # main_s3()

  