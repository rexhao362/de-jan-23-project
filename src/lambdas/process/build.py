import pandas as pd
import numpy as np
from src.lambdas.process.utils import (timestamp_to_date)
import logging
"""
Functions used to build dimension & fact table dataframes from given table dataframes.
"""

def build_dim_design(dataframe):
    """
    Builds dim_design table from original design table.

    Args: 
        param1: design, dataframe

    Returns: dim_design, dataframe

    Input columns: 
        [design_id, created_at, last_updated, design_name, file_location, file_name]

    Output columns: 
        [design_id, design_name, file_location, file_name]
    """
    # df = dataframe.copy()
    dim_design = dataframe.copy().drop(columns=['created_at', 'last_updated'])
    return dim_design[['design_id', 'design_name', 'file_location', 'file_name']]

def build_dim_currency(dataframe):
    """
    Builds dim_currency table from original currency table.

    Args: 
        param1: currency, dataframe

    Returns: dim_currency, dataframe

    Input columns:
        [currency_id, currency_code, created_at, last_updated]

    Output columns:
        [currency_id, currency_code, currency_name]

    """
    # df = dataframe.copy()
    dim_currency = dataframe.copy().drop(columns=['created_at', 'last_updated'])

    currency_names = []
    for item in dim_currency['currency_code']:
        if item == "GBP":
            currency_names.append('Pounds')
        elif item == "USD":
            currency_names.append('Dollars')
        elif item == "EUR":
            currency_names.append('Euros')
        else:
            logging.error(f'Unknown Currency - Currency Code : {item}')

    dim_currency['currency_name'] = currency_names
    dim_currency = dim_currency[['currency_id','currency_code', 'currency_name']]
    return dim_currency

def build_fact_sales_order(sales_order_dataframe):
    """
    Builds fact_sales_order table from original sales table.

    Args: 
        param1: sales, dataframe

    Returns: fact_sales_order, dataframe

    Input Columns: 
        ["sales_order_id", "created_at", "last_updated", "design_id", "staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_id"]
    """
    sales_order = sales_order_dataframe.copy()

    # convert timestamps to datetime objects for 'last_updated' and 'created_at'
    sales_order['last_updated'] = pd.to_datetime(sales_order['last_updated'])
    sales_order['created_at'] = pd.to_datetime(sales_order['created_at'])

    # can now generate 'last_updated_time' and 'created_time'
    sales_order['last_updated_time'] = sales_order['last_updated'].dt.time
    sales_order['created_time'] = sales_order['created_at'].dt.time

    # trunkate dates
    sales_order['last_updated'] = sales_order['last_updated'].dt.date
    sales_order['created_at'] = sales_order['created_at'].dt.date

    #generate rest of date columns
    columns = ['agreed_delivery_date', 'agreed_payment_date']
    for col in columns:
        sales_order = timestamp_to_date(sales_order, col)
    
    # rename columns
    sales_order = sales_order.rename(columns={"created_at":"created_date", "last_updated":"last_updated_date", "staff_id":"sales_staff_id"})

    order_list = ['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']

    return sales_order[order_list]

def build_dim_staff(staff_dataframe, department_dataframe):
    """
    Builds dim_staff table from original staff and department tables.

    Args:
        param1: staff, DataFrame
        param2: department, DataFrame

    Returns: dim_staff, DataFrame

    Output Columns:
        ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']
    """
    dim_staff = staff_dataframe.copy()
    department_id = dim_staff['department_id']
    dim_staff = dim_staff.drop(columns=['department_id', 'created_at', 'last_updated'])
    length = dim_staff.shape[0]

    department_name_col = np.empty(length, dtype='U15')
    location_col = np.empty(length, dtype='U10')

    for i, id in enumerate(department_id):
        department = department_dataframe.query(f"department_id == {id}")
        department_name_col[i] = department['department_name'].item()
        location_col[i] = department['location'].item()
    
    dim_staff['department_name'] = department_name_col
    dim_staff['location'] = location_col
    return dim_staff[['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']] 

def build_dim_location(dataframe):
    """
    Builds dim_location table from original location table.

    Args:
        param1: addresses, dataframe

    Returns: dim_location, dataframe

    Input columns:
        [address_id, address_line_1, address_line_2, district, 
        city, postal_code, country, phone, created_at, last_updated]

    Output columns:
        [location_id, address_line_1, address_line_2, district, 
        city, postal_code, country, phone]
    """
    df = dataframe.copy()
    dim_location = df.drop(columns=['created_at', 'last_updated'])
    return dim_location.rename(columns={'address_id':'location_id'})

def build_dim_date(start_date, end_date):
    """
    Generates and populates a dataframe with a range of dates from start_date to end_date.

    Args:
        param1: start date, string, "yyyy/mm/dd", inclusive
        param2: end date, string, "yyyy/mm/dd", inclusive
    Returns: dim_date, dataframe
    
    Output columns:
        [date_id, year, month, day, day_of_week, day_name, month_name, quarter]
    """
    #generate a date time index
    dti = pd.date_range(start=start_date, end=end_date).to_series()
    # df = dti.to_frame(index=False)
    years = dti.dt.year
    months = dti.dt.month
    days = dti.dt.day
    day_of_week = dti.dt.day_of_week
    day_name = dti.dt.day_name()
    month_name=dti.dt.month_name()
    quarter = dti.dt.quarter
    d={
        'date_id': dti,
        'year': years,
        'month': months,
        'day': days,
        'day_of_week': day_of_week,
        'day_name': day_name,
        'month_name': month_name,
        'quarter': quarter
    }
    # df = pd.DataFrame(data=d)
    return pd.DataFrame(data=d)
    
def build_dim_counterparty(original_dataframe, address_dataframe):
    """
    Builds dim_counterparty table from original counterparty and address tables.
    
    Args:
        param1: counterparty, dataframe
        param2: address, dataframe
    
    Returns: dim_counterparty, dataframe
    
    Input columns:
        [counterparty_id, counterparty_legal_name, legal_address_id,
        commercial_contact, delivery_contact, created_at, last_updated]

    Output columns: 
        [counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1, 
        counterparty_legal_address_line_2, counterparty_legal_district, 
        counterparty_legal_city, counterparty_legal_postal_code, counterparty_legal_country, 
        counterparty_legal_phone_number]
    """
    df = original_dataframe.copy()
    ids = original_dataframe['legal_address_id']
    df = df.drop(columns=['last_updated', 'created_at', 'delivery_contact', 'commercial_contact', 'legal_address_id'])
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
        # print(address['address_line_1'].item())
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