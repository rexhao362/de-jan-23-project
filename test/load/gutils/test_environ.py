# to allow running tests without PYTHONPATH
import sys
sys.path.append('./')

from os import environ
from unittest.mock import patch
import pytest
from src.load.gutils.environ import \
    dev_environ_variable, \
    dev_environ_variable_value, \
    set_dev_environ, \
    unset_dev_environ, \
    is_production_environ, \
    is_dev_environ

@pytest.fixture
def mock_empty_environ():
    with patch.dict("os.environ", {}, clear=True) as e:
        yield e

@pytest.fixture
def mock_dev_environ():
    with patch.dict("os.environ", {}, clear=True) as e:
        environ[dev_environ_variable] = dev_environ_variable_value
        yield e

### is_production_environ
def test_is_production_environ(mock_empty_environ):
    assert is_production_environ(), f'should return True if {dev_environ_variable} env variable is not set'
    environ[dev_environ_variable] = "True"
    assert not is_production_environ(), f'should return False if {dev_environ_variable} env variable is set'

### is_dev_environ
def test_is_dev_environ(mock_empty_environ):
    assert not is_dev_environ(), f'should return False if {dev_environ_variable} env variable is not set'
    environ[dev_environ_variable] = "True"
    assert is_dev_environ(), f'should return True if {dev_environ_variable} env variable is set'

### set_dev_environ
@pytest.mark.parametrize('test_value', ["", "abc", '{"my_key": "my_value"}'])
def test_set_def_environ(mock_empty_environ, test_value):
    # act
    set_dev_environ(test_value)
    # assert
    assert dev_environ_variable in environ, f'set_dev_environ() failed to create {dev_environ_variable} env variable'
    actual_value = environ[dev_environ_variable]
    assert actual_value == test_value, f'set_dev_environ() failed to set {dev_environ_variable} env variable to "{test_value}", got "{actual_value}"'

### unset_dev_environ
@pytest.mark.parametrize('mock_environ', [mock_dev_environ, mock_empty_environ])
def test_unset_def_environ(mock_environ):
    # act
    unset_dev_environ()
    # assert
    assert not dev_environ_variable in environ, f'unset_dev_environ() failed to unset {dev_environ_variable} env variable'
