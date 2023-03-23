from src.lambdas.load.utils.test_populate_tables import _test_populate_independent_table

table_name = "dim_design"

def test_loads_one_row_when_passed_list_of_one_record():
    # arrange
    input_data = [
        [1, "fancy dress #1", "/dev/null", "f_dress.png"]
    ]
    
    _test_populate_independent_table(table_name, input_data)

def test_loads_multiple_rows_when_passed_list_of_multiple_records():
    # arrange
    input_data = [
        [1, "fancy dress #1", "/dev/null", "f_dress.png"],
        [2, "corporate", "c:/design/", "corporate.png"],
        [3, "casual", "c:/design/", "casual.jpg"]
    ]

    _test_populate_independent_table(table_name, input_data)