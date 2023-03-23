from utils import *
from build import build_dim_currency



def make_curr_df():
    curr_path = 'test/json_files/currency_test_2.json'
    curr_data = load_file_from_local(curr_path)
    return process(curr_data)


def main():
    curr_df = make_curr_df()
    dim_currency = build_dim_currency(curr_df)
    res = write_to_bucket('processed-test-bucket-65', dim_currency, 'dim_currency', '2023-03-22/11-33-01')
    print(res)
    
# main()