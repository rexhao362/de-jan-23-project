from src.lambdas.process.utils import load_file_from_local, process
from src.lambdas.process.build import build_dim_counterparty
from pandas import DataFrame
from numpy import equal

def test_returns_dataframe():
    counterparty_file = 'test/lambdas/process/json_files/counterparty_test_1.json'
    address_file = 'test/lambdas/process/json_files/address_test_1.json'
    counterparty_data = load_file_from_local(counterparty_file)
    address_data = load_file_from_local(address_file)
    counterparty_dataframe = process(counterparty_data)
    address_dataframe = process(address_data)
    
    dim_counterparty = build_dim_counterparty(counterparty_dataframe, address_dataframe)
    
    assert isinstance(dim_counterparty, DataFrame)
    
def test_returned_dataframe_has_expected_columns():
    counterparty_file = 'test/lambdas/process/json_files/counterparty_test_1.json'
    address_file = 'test/lambdas/process/json_files/address_test_1.json'
    expected_cols = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']
    counterparty_data = load_file_from_local(counterparty_file)
    address_data = load_file_from_local(address_file)
    counterparty_dataframe = process(counterparty_data)
    address_dataframe = process(address_data)
    
    dim_counterparty = build_dim_counterparty(counterparty_dataframe, address_dataframe)
    
    
    dim_counterparty_cols = dim_counterparty.columns.values
    assert equal(dim_counterparty_cols, expected_cols).all()

def test_returned_data_columns_have_expected_data_types():
    counterparty_file = 'test/lambdas/process/json_files/counterparty_test_1.json'
    address_file = 'test/lambdas/process/json_files/address_test_1.json'
    #O for string (numpy object), int64 for id
    expected_dtypes = ['int64', 'O', 'O', 'O', 'O', 'O', 'O', 'O', 'O']
    cols = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number']
    counterparty_data = load_file_from_local(counterparty_file)
    address_data = load_file_from_local(address_file)
    counterparty_dataframe = process(counterparty_data)
    address_dataframe = process(address_data)
    
    dim_counterparty = build_dim_counterparty(counterparty_dataframe, address_dataframe)
    
    types = dim_counterparty.dtypes
    for i in range(len(cols)):
        assert types[cols[i]] == expected_dtypes[i]
    
    
def test_returned_data_is_maintained_through_build():
    counterparty_file = 'test/lambdas/process/json_files/counterparty_test_1.json'
    address_file = 'test/lambdas/process/json_files/location_test_2.json'
    counterparty_data = load_file_from_local(counterparty_file)
    address_data = load_file_from_local(address_file)
    counterparty_dataframe = process(counterparty_data)
    address_dataframe = process(address_data)
    expected_first_row = [1, "bar1", "6826 Herzog Via", None, "Avon", "New Patienceburgh", "28441", "Turkey", "1803 637401"]
    
    dim_counterparty = build_dim_counterparty(counterparty_dataframe, address_dataframe)
    
    
    