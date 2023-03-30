from os import environ
import json
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

# existing secret
@pytest.fixture
def existing_secret_name():
    return "existing_secret_name"

@pytest.fixture
def secret_value():
    return "secret_value"

# existing keys
def test_get_secret_value_returns_existing_secret_value_in_dev_environ(dev_mock_environ, existing_secret_name, secret_value):
    # arrange
    environ[existing_secret_name] = secret_value
    assert environ[existing_secret_name] == secret_value, "mock error"
    # act
    value = secrets_manager.get_secret_value(existing_secret_name)
    # assert
    assert value == secret_value

@pytest.fixture
def mock_aws_secret_manager():
    return boto3.client('secretsmanager')

@mock_secretsmanager
def test_get_secret_value_returns_existing_secret_value_in_production_environ(production_mock_environ, existing_secret_name, secret_value, mock_aws_secret_manager):
    # arrange
    mock_aws_secret_manager.create_secret(Name=existing_secret_name, SecretString=secret_value)
    # act
    value = secrets_manager.get_secret_value(existing_secret_name)
    # assert
    assert value == secret_value, f'secrets_manager.get_secret_value("{existing_secret_name}") should return "{secret_value}" (got "{value}")'

# non-existing secret name
@pytest.fixture
def non_existing_secret_name():
    return "non_existing_secret_name"

@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def test_get_secret_value_returns_none_on_non_existing_secret_name(mock_environ, non_existing_secret_name):
    assert secrets_manager.get_secret_value(non_existing_secret_name) == None

@pytest.fixture
def default_secret_value():
    return "default_secret_value"

@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def test_get_secret_value_returns_default_value_on_non_existing_secret_name_and_default_value(mock_environ, non_existing_secret_name, default_secret_value):
    assert secrets_manager.get_secret_value(non_existing_secret_name, default_secret_value) == default_secret_value

## sad path
@pytest.fixture
def non_string_secret_name():
    return 1234

@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def test_get_secret_value_returns_none_on_non_string_secret_name(mock_environ, non_string_secret_name, default_secret_value):
    assert secrets_manager.get_secret_value(non_string_secret_name, default_secret_value) == None

@pytest.fixture
def non_string_default_value():
    return 1

@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def test_get_secret_value_returns_none_on_non_existing_secret_name_and_non_string_default_value(mock_environ, non_existing_secret_name, non_string_default_value):
    assert secrets_manager.get_secret_value(non_existing_secret_name, non_string_default_value) == None


## get_secret_int_value

# existing key
def test_get_secret_int_value_returns_existing_secret_int_value_in_dev_environ(dev_mock_environ):
    # arrange
    secret_name = "PORT"
    secret_int_value = 1234
    secret_value = str(secret_int_value)
    environ[secret_name] = secret_value
    assert environ[secret_name] == secret_value, "mock error"
    # act & assert
    assert secrets_manager.get_secret_int_value(secret_name) == secret_int_value

@mock_secretsmanager
def test_get_secret_int_value_returns_existing_secret_int_value_in_production_environ(production_mock_environ):
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
def test_get_secret_int_value_raises_exception_on_non_existing_secret_name(mock_environ, non_existing_secret_name):
    with pytest.raises(BaseException):
        secrets_manager.get_secret_int_value(non_existing_secret_name)

@pytest.fixture
def non_int_default_value():
    return "1"

# non-existing secret name and default_value is not int
@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def test_get_secret_int_value_raises_exception_on_non_existing_secret_name_and_non_int_default_value(mock_environ, non_existing_secret_name, non_int_default_value):
    with pytest.raises(BaseException):
        secrets_manager.get_secret_int_value(non_existing_secret_name, non_int_default_value)

# non-string secret name
@pytest.fixture
def default_secret_int_value():
    return 1

@pytest.mark.parametrize("mock_environ", [dev_mock_environ, production_mock_environ])
def test_get_secret_int_value_raises_exception_on_non_string_secret_name(mock_environ, non_string_secret_name, default_secret_value):
    with pytest.raises(BaseException):
        secrets_manager.get_secret_int_value(non_string_secret_name, default_secret_int_value)

## special prefedined secrets for the project

# get_secret_totesys_config
mock_totesys_config_env_variables = {
    "TOTESYS_DB_USER": "user",
    "TOTESYS_DB_PASSWORD": "passwd",
    "TOTESYS_DB_HOST": "host",
    "TOTESYS_DB_PORT": "1234",
    "TOTESYS_DB_DATABASE": "db",
    "TOTESYS_DB_DATABASE_SCHEMA": "schema_1"
}

@pytest.fixture
def mock_environ_with_totesys_config_variables():
    with patch.dict("os.environ", mock_totesys_config_env_variables, clear=True) as e:
        yield e

@pytest.fixture
def dev_mock_environ_with_totesys_config_variables(mock_environ_with_totesys_config_variables):
    with patch.dict("os.environ", mock_environ_with_totesys_config_variables, clear=True) as e:
        # add dev flag
        environ[dev_environ_variable] = dev_environ_variable_value
        yield e

@pytest.fixture
def totesys_config(mock_environ_with_totesys_config_variables):
    return {
        "credentials": {
            "user": environ["TOTESYS_DB_USER"],
            "password": environ["TOTESYS_DB_PASSWORD"],
            "host": environ["TOTESYS_DB_HOST"],
            "port": int( environ["TOTESYS_DB_PORT"] ),
            "database": environ["TOTESYS_DB_DATABASE"],
        },
        "schema": environ["TOTESYS_DB_DATABASE_SCHEMA"]
    }

# existing secret
def test_get_secret_totesys_config_returns_existing_totesys_config_in_dev_environ(dev_mock_environ_with_totesys_config_variables, existing_secret_name, totesys_config):
    # arrange
    secret_name = existing_secret_name
    secret_value = json.dumps(totesys_config)
    environ[secret_name] = secret_value
    assert environ[secret_name] == secret_value, "mock error"

    # act
    restored_config = secrets_manager.get_secret_totesys_config(secret_name)
    # assert
    assert totesys_config == restored_config

# existing secret
@mock_secretsmanager
def test_get_secret_totesys_config_returns_existing_totesys_config_in_production_environ(mock_environ_with_totesys_config_variables, existing_secret_name, totesys_config):
    # arrange
    secret_name = existing_secret_name
    secret_value =json.dumps(totesys_config)

    secretm = boto3.client('secretsmanager')
    response = secretm.create_secret(Name=secret_name, SecretString=secret_value)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    
    # act
    restored_totesys_config = secrets_manager.get_secret_totesys_config(secret_name)
    # assert
    assert restored_totesys_config == totesys_config, f'secrets_manager.get_secret_totesys_config("{secret_name}") should return "{totesys_config}" (got "{restored_totesys_config}")'
