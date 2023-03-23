from src.lambdas.utils import load_file_from_local, process
from src.lambdas.build import build_dim_design
from pandas import DataFrame
from numpy import equal

design_json_path = 'test/lambdas/process/json_files/design_test_1.json'

design_data = load_file_from_local(design_json_path)
design_dataframe = process(design_data)
dim_design = build_dim_design(design_dataframe)

def test_returns_dataframe():
    assert isinstance(dim_design, DataFrame)

def test_returned_dataframe_has_expected_columns():
    dim_design_cols = dim_design.columns.values
    assert equal(dim_design_cols, ['design_id', 'design_name', 'file_location', 'file_name']).all()

def test_returned_data_is_maintained_through_build():
    assert dim_design['design_id'][0] == "des_id_1"
    assert dim_design['design_name'][0] == "name_1"
    assert dim_design['file_location'][0] == "file_loc_1"
    assert dim_design['file_name'][0] == "file_name_1"
