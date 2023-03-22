import pandas as pd
import json
import numpy as np

#! INGESTION LABELS ARE LOWER CASE
#! A LOT OF DATA HAS NULL VALUES SO DONT TEST FOR NONE

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
        df = pd.DataFrame(table['data'], columns=table['headers'])
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
    Input: design, dataframe
    Returns: dim_design, dataframe

    BUILD DIM_DESIGN

    - strips some data from original design table

    input columns: 
    [design_id, created_at, last_updated, design_name, file_location, file_name]

    output columns: 
    [design_id, design_name, file_location, file_name]

    """
    df = dataframe.copy()
    dim_design = df.drop(columns=['created_at', 'last_updated'])
    return dim_design


def build_dim_currency(dataframe):

    """
    Input: currency, dataframe
    Returns: dim_currency, dataframe

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

    # {"GBP" : "Pounds", "USD": "Dollars", "EUR": Euros}

    currency_names = []
    for item in dim_currency['currency_code']:
        if item == "GBP":
            currency_names.append('Pounds')
        elif item == "USD":
            currency_names.append('Dollars')
        elif item == "EUR":
            currency_names.append('Euros')
        else:
            currency_names.append(None)

    dim_currency['currency_name'] = currency_names

    return dim_currency

def build_fact_sales_order():
    """
    BUILD FACT_SALES_ORDER

    """
def build_dim_staff():
    """
    BUILD DIM_STAFF
    """

def build_dim_location(dataframe):
    """
    Input: addresses, dataframe
    Returns: dim_location, dataframe

    BUILD DIM_LOCATION
    input columns:
    [address_id, address_line_1, address_line_2, district, city, postal_code, country, phone, created_at, last_updated]

    output columns:
    [
    location_id, address_line_1, address_line_2, district, city, postal_code, country, phone
    ]
    """
    df = dataframe.copy()
    dim_location = df.drop(columns=['created_at', 'last_updated'])
    dim_location = dim_location.rename(columns={'address_id':'location_id'})
    return dim_location


def build_dim_date():
    """
    BUILD DIM_DATE
    """

def build_dim_counterparty(original_dataframe, address_dataframe):
    """
    BUILD DIM_COUNTERPARTY
    arg1: original counterparty dataframe
    arg2: original address dataframe
    
    input columns = [counterparty_id, counterparty_legal_name, legal_address_id, commercial_contact, delivery_contact, created_at, last_updated]
     1
    output columns = [counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1, counterparty_legal_address_line_2, counterparty_legal_district, counterparty_legal_city, counterparty_legal_postal_code, counterparty_legal_country, counterparty_legal_phone_number]
    """
    df = original_dataframe.copy()
    ids = original_dataframe['legal_address_id']
    df = df.drop(columns=['last_updated', 'created_at', 'delivery_contact', 'commercial_contact', 'legal_address_id'])
    #print(ids)
    length = df.shape[0]
    #init new cols
    l_add_1 = np.empty(length, dtype='U10')
    l_add_2 = np.empty(length, dtype='U10')
    l_district = np.empty(length, dtype='U10')
    l_country = np.empty(length, dtype='U10')
    l_city = np.empty(length, dtype='U10')
    l_postal_code = np.empty(length, dtype='U10')
    l_phone = np.empty(length, dtype='U10')
    
    #for each record in dataframe and corresponding address_id
    for i, id in enumerate(ids):
        #query the address table for the right id
        address = address_dataframe.query(f"address_id == {id}")
        #initialize the columns
        l_add_1[i] = address['address_line_1'].item()
        l_add_2[i] = address['address_line_2'].item()
        l_district[i] = address['district'].item()
        l_city[i] = address['city'].item()
        l_country[i] = address['country'].item()
        l_postal_code[i] = address['postal_code'].item()
        l_phone[i] = address['phone'].item()
        
    df['counterparty_legal_address_line_1'] = l_add_1
    df['counterparty_legal_address_line_2'] = l_add_2
    df['counterparty_legal_district'] = l_district
    df['counterparty_legal_city'] = l_city
    df['counterparty_legal_postal_code'] = l_postal_code
    df['counterparty_legal_country'] = l_country
    df['counterparty_legal_phone_number'] = l_phone
        
    return df
    
    
    # use ids to fetch from address:
    # counterparty_legal_address_line_1
    # counterparty_legal_address_line_2
    # counterparty_legal_district
    # counterparty_legal_city
    # counterparty_legal_postcode
    # counterparty_legal_county


def main():
    counterparty_file = 'test/json_files/counterparty_test_1.json'
    address_file = 'test/json_files/address_test_1.json'
    counteryparty_data = load_file_from_local(counterparty_file)
    address_data = load_file_from_local(address_file)
    counterparty_dataframe = process(counteryparty_data)
    address_dataframe = process(address_data)
    dim_counterparty = build_dim_counterparty(counterparty_dataframe, address_dataframe)
    print(dim_counterparty)

if __name__ == "__main__":
    main()


 

