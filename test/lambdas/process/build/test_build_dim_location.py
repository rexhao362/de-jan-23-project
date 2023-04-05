import sys
sys.path.append('./src/')
from process.utils import load_file_from_local, process
from process.build import build_dim_location
from pandas import DataFrame
from numpy import equal

location_json_path = 'test/lambdas/process/json_files/location_test_1.json'

location_data = load_file_from_local(location_json_path)
location_dataframe = process(location_data)
dim_location = build_dim_location(location_dataframe)

def test_returns_dataframe():
    assert isinstance(dim_location, DataFrame)

def test_returned_dataframe_has_expected_columns():
    dim_location_cols = dim_location.columns.values
    assert equal(dim_location_cols, ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']).all()

def test_returned_data_is_maintained_through_build():
    assert dim_location['location_id'][0] == 1
    assert dim_location['address_line_1'][0] == "line_1_1"
    assert dim_location['address_line_2'][0] == "line_2_1"
    assert dim_location['district'][0] == "district_1"
    assert dim_location['city'][0] == "city_1"
    assert dim_location['postal_code'][0] == "postal_code_1"
    assert dim_location['country'][0] == "country_1"
    assert dim_location['phone'][0] == "phone_1"



    
