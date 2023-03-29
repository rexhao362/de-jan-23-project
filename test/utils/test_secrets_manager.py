from os import environ
from unittest.mock import patch
import pytest
import boto3
from moto import mock_secretsmanager
from botocore.exceptions import ClientError
from src.utils.environ import dev_environ_variable, dev_environ_variable_value

from src.utils.secrets_manager import secrets_manager

@pytest.fixture
def dev_mock_environ():
    with patch.dict("os.environ", {dev_environ_variable: dev_environ_variable_value}, clear=True) as e:
        yield e

aws_credentials = {
    'AWS_ACCESS_KEY_ID': "test",
    'AWS_SECRET_ACCESS_KEY': "test",
    'AWS_SECURITY_TOKEN': "test",
    'AWS_SESSION_TOKEN': "test",
    'AWS_DEFAULT_REGION': "us-east-1"
}

@pytest.fixture
def production_mock_environ():
    with patch.dict("os.environ", aws_credentials, clear=True) as e:
        yield e

# existing keys
def _test_get_existing_secret_value_in_dev(dev_mock_environ):
    # arrange
    secret_name = "WAREHOUSE_DB_USER"
    secret_value = "user"
    environ[secret_name] = secret_value
    assert environ[secret_name] == secret_value, "mock error"
    # act
    value = secrets_manager.get_secret_value(secret_name)
    # assert
    assert value == secret_value

@mock_secretsmanager
def _test_get_existing_secret_value_in_production(production_mock_environ):
    # arrange
    secret_name = "test_id"
    secret_value = "test_secret"
    secretm = boto3.client('secretsmanager')
    secretm.create_secret(Name=secret_name, SecretString=secret_value)
    # act
    value = secrets_manager.get_secret_value(secret_name)
    # assert
    assert value == secret_value, f'secrets_manager.get_secret_value("{secret_name}") should return "{secret_value}" (got "{value}")'

# non-existing secret name
@pytest.fixture
def non_existing_secret_name():
    return "complete_nonsense_101"

@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def _test_returns_none_when_passed_non_existing_secret_name(mock_environ, non_existing_secret_name):
    assert secrets_manager.get_secret_value(non_existing_secret_name) == None

@pytest.fixture
def default_secret_value():
    return "test_secret"

@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def _test_returns_default_value_when_passed_non_existing_secret_name_and_default_value(mock_environ, non_existing_secret_name, default_secret_value):
    assert secrets_manager.get_secret_value(non_existing_secret_name, default_secret_value) == default_secret_value

## sad path
@pytest.fixture
def non_string_secret_name():
    return 1234

@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def _test_returns_none_when_passed_non_string_argument_as_secret_name(mock_environ, non_string_secret_name, default_secret_value):
    assert secrets_manager.get_secret_value(non_string_secret_name, default_secret_value) == None

@pytest.fixture
def non_string_secret_value():
    return 1

@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def _test_returns_none_when_passed_unknown_secret_name_and_non_string_default_value(mock_environ, non_existing_secret_name, non_string_secret_value):
    assert secrets_manager.get_secret_value(non_existing_secret_name, non_string_secret_value) == None


## get_secret_int_value

# existing key
def test_get_existing_secret_int_value_in_dev(dev_mock_environ):
    # arrange
    secret_name = "PORT"
    secret_int_value = 1234
    secret_value = str(secret_int_value)
    environ[secret_name] = secret_value
    assert environ[secret_name] == secret_value, "mock error"
    # act & assert
    assert secrets_manager.get_secret_int_value(secret_name) == secret_int_value

@mock_secretsmanager
def test_get_existing_secret_int_value_in_production(production_mock_environ):
    # arrange
    secret_name = "PORT"
    secret_int_value = 1234
    secret_value = str(secret_int_value)
    secretm = boto3.client('secretsmanager')
    secretm.create_secret(Name=secret_name, SecretString=secret_value)
    assert secretm.get_secret_value(SecretId=secret_name)['SecretString'] == secret_value, "mock error"
    # act
    value = secrets_manager.get_secret_int_value(secret_name)
    # assert
    assert value == secret_int_value, f'secrets_manager.get_secret_int_value("{secret_name}") should return {secret_int_value} (got {value})'

# non-existing secret name
@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def test_raises_exception_when_passed_non_existing_secret_name(mock_environ, non_existing_secret_name):
    with pytest.raises(BaseException):
        secrets_manager.get_secret_int_value(non_existing_secret_name)

# non-existing secret name and default_value is not int
@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def test_raises_exception_when_passed_non_existing_secret_name_and_non_int_default_value(mock_environ, non_existing_secret_name, non_string_secret_value):
    with pytest.raises(BaseException):
        secrets_manager.get_secret_int_value(non_existing_secret_name, non_string_secret_value)

# non-string secret name
@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def test_raises_exception_when_passed_non_string_argument_as_secret_name(mock_environ, non_string_secret_name, default_secret_value):
    with pytest.raises(BaseException):
        secrets_manager.get_secret_int_value(non_string_secret_name)