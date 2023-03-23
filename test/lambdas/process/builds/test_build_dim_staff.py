from src.lambdas.process.utils import load_file_from_local, process
from src.lambdas.process.build import build_dim_staff
from pandas import DataFrame
from numpy import (equal, int64)

department_file = 'test/json_files/department_test_1.json'
staff_file = 'test/json_files/staff_test_1.json'
department_data = load_file_from_local(department_file)
staff_data = load_file_from_local(staff_file)
department_dataframe = process(department_data)
staff_dataframe = process(staff_data)

dim_staff = build_dim_staff(staff_dataframe, department_dataframe)

def test_returns_dataframe():
    assert isinstance(dim_staff, DataFrame)

def test_returned_dataframe_has_expected_columns():
    dim_staff_cols = dim_staff.columns.values
    expected_cols = ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']
    assert equal(dim_staff_cols, expected_cols).all()

def test_returned_data_is_of_correct_datatype():
    for i in range(dim_staff.shape[0]):
        assert type(dim_staff['staff_id'][i]) == int64
        assert type(dim_staff['first_name'][i]) == str
        assert type(dim_staff['last_name'][i]) == str
        assert type(dim_staff['department_name'][i]) == str
        assert type(dim_staff['location'][i]) == str
        assert type(dim_staff['email_address'][i]) == str

def test_returned_data_is_maintained_through_build():
    for i in range(staff_dataframe.shape[0]):
        dep_id = staff_dataframe['department_id'][i]
        if dep_id == 1:
            assert dim_staff['department_name'][i] == 'Sales'
            assert dim_staff['location'][i] == 'Manchester'
        if dep_id == 2:
            assert dim_staff['department_name'][i] == 'Purchasing'
            assert dim_staff['location'][i] == 'Manchester'
        if dep_id == 3:
            assert dim_staff['department_name'][i] == 'Production'
            assert dim_staff['location'][i] == 'Leeds'
        if dep_id == 4:
            assert dim_staff['department_name'][i] == 'Dispatch'
            assert dim_staff['location'][i] == 'Leds'
        if dep_id == 5:
            assert dim_staff['department_name'][i] == 'Finance'
            assert dim_staff['location'][i] == 'Manchester'
        if dep_id == 6:
            assert dim_staff['department_name'][i] == 'Facilities'
            assert dim_staff['location'][i] == 'Manchester'
        if dep_id == 7:
            assert dim_staff['department_name'][i] == 'Communications'
            assert dim_staff['location'][i] == 'Leeds'
        if dep_id == 8:
            assert dim_staff['department_name'][i] == 'HR'
            assert dim_staff['location'][i] == 'Leeds'