from src.lambdas.process import (load_file_from_local)
full_json = 'test/json_files/process_test_1.json'
empty_json = 'test/json_files/process_test_2.json'
no_data_json = 'test/json_files/process_test_3.json'


def test_load_file_from_local_returns_dict():
    result = load_file_from_local(empty_json)
    assert type(result) == dict

def test_load_file_from_local_dict_contains_data():
    result = load_file_from_local(no_data_json)
    assert result['Headers'] == []
    assert result['Data'] == []
    result = load_file_from_local(full_json)
    assert result['Headers'] == ["Name", "Type", "Description"]
    assert result['Data'] == \
    [   
    ["foo1", "bar1","foo bar1"],
    ["foo2", "bar2", "foobar2"],
    ["foo3", "bar3", "foobar3"]
    ]
