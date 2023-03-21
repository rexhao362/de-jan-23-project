import pandas as pd
import json

# injest json
# TODO BUILD NEW DIM/FACT TABLES 
# process into parquet
# upload to s3 / write to local
def load_file_from_s3():
    pass

def load_file_from_local(filepath):
    """
    Input: filepath, string
    Returns: data, dict
    """
    json_data = open(filepath)
    data = json.load(json_data)
    json_data.close()
    return data

def process(table):
    '''
    Input: table, dict
    Returns: table, pd data frame
    '''
    try:
        df = pd.DataFrame(table['Data'], columns=table['Headers'])
    except KeyError as e:
        raise(e)
    return df

def upload_to_s3():
    pass

def save_to_local():
    '''
    Input: table, pd data frame
    Return: some kind of json object with a status code
    '''
    pass

def print_csv(df, filepath):
    df.to_csv(filepath)

def build_dim_design(dataframe):

    """
    BUILD DIM_DESIGN

    - strips some data from original design table

    input columns: 
    [design_id, created_at, last_updated, design_name, file_location, file_name]

    output columns: 
    [design_id, design_name, file_location, file_name]

    """
    df = dataframe.copy()
    df.drop(['created_at', 'last_updated'])
    return df


def build_dim_currency(dataframe):

    """
    BUILD DIM_CURRENCY

    - strips some data from original currency table
    - inserts currency_name value depending on currency_code

    input columns:
    [currency_id, currency_code, created_at, last_updated]

    output columns:
    [currency_id, currency_code, currency_name]

    """

    df = dataframe.copy()
    dim_currency = df.drop(columns=['created_at', 'last_updated'])
    # TODO - FIND OUT WHAT CODES THERE ARE AND BUILD A BETTER NP ARRAY :)
    # TODO - Possibly implement with a function?
    currency_names = ['British Pounds' for item in dim_currency['currency_code']]
    dim_currency['currency_names'] = currency_names
    return dim_currency

def build_fact_sales_order():
    """
    BUILD FACT_SALES_ORDER

    """
def build_dim_staff():
    """
    BUILD DIM_STAFF
    """

def build_dim_location():
    """
    BUILD DIM_LOCATION
    """

def build_dim_dat():
    """
    BUILD DIM_DATE
    """

def build_dim_counterparty():
    """
    BUILD DIM_COUNTERPARTY
    """



def main():
    table1 = 'test/json_files/currency_test_1.json'
    table1_data = load_file_from_local(table1)
    table1_dataframe = process(table1_data)
    dim_currency = build_dim_currency(table1_dataframe)
    print(dim_currency)

if __name__ == "__main__":
    main()


 

