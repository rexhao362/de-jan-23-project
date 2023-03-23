from src.lambdas.load.utils.test_populate_tables import _test_populate_independent_table

table_name = "dim_staff"

def test_loads_one_row_when_passed_list_of_one_record():
    # arrange
    input_data = [
        [1, "Joe", "Boo", "Dep1", "Manchester", "j.b@c.com"]
    ]
    
    _test_populate_independent_table(table_name, input_data)

def test_loads_multiple_rows_when_passed_list_of_multiple_records():
    # arrange
    input_data = [
        [1, "Joe", "Boo", "Dep1", "Manchester", "j.b@c.com"],
        [3, "Bill", "Gates", "0_policy", "Redmond", "bgates@ms.com"],
        [6, "Steve", "J", "mobile", "San Jose", "sjobs@apple.com"]
    ]

    _test_populate_independent_table(table_name, input_data)