from os import environ
import json
from unittest.mock import patch
import pytest
import boto3
from moto import mock_secretsmanager
from botocore.exceptions import ClientError
from src.utils.environ import \
    set_dev_environ, \
    is_dev_environ, \
    is_production_environ

from src.utils.secrets_manager import secrets_manager

@pytest.fixture
def mock_dev_environ():
    with patch.dict("os.environ", {}, clear=True) as e:
        set_dev_environ()
        yield e

aws_credentials = {
    'AWS_ACCESS_KEY_ID': "test",
    'AWS_SECRET_ACCESS_KEY': "test",
    'AWS_SECURITY_TOKEN': "test",
    'AWS_SESSION_TOKEN': "test",
    'AWS_DEFAULT_REGION': "us-east-1"
}

@pytest.fixture
def mock_production_environ():
    with patch.dict("os.environ", aws_credentials, clear=True) as e:
        yield e

# existing secret
@pytest.fixture
def secret_name():
    return "secret_name"

@pytest.fixture
def secret_value():
    return "secret_value"

# existing keys
def test_get_secret_value_returns_existing_secret_value_in_dev_environ(mock_dev_environ, secret_name, secret_value):
    assert is_dev_environ()
    # arrange
    environ[secret_name] = secret_value
    assert environ[secret_name] == secret_value, "mock error"
    # act
    value = secrets_manager.get_secret_value(secret_name)
    # assert
    assert value == secret_value

@pytest.fixture
def mock_aws_secrets_manager():
    return boto3.client('secretsmanager')

@mock_secretsmanager
def test_get_secret_value_returns_existing_secret_value_in_production_environ(mock_production_environ, secret_name, secret_value, mock_aws_secrets_manager):
    assert is_production_environ()
    # arrange
    mock_aws_secrets_manager.create_secret(Name=secret_name, SecretString=secret_value)
    # act
    value = secrets_manager.get_secret_value(secret_name)
    # assert
    assert value == secret_value, f'secrets_manager.get_secret_value("{secret_name}") should return "{secret_value}" (got "{value}")'

@pytest.mark.parametrize("mock_environ", [mock_dev_environ, mock_production_environ])
def test_get_secret_value_returns_none_on_non_existing_secret_name(mock_environ, secret_name):
    assert secrets_manager.get_secret_value(secret_name) == None

@pytest.fixture
def default_secret_value():
    return "default_secret_value"

@pytest.mark.parametrize("mock_environ", [mock_dev_environ, mock_production_environ])
def test_get_secret_value_returns_default_value_on_non_existing_secret_name_and_default_value(mock_environ, secret_name, default_secret_value):
    assert secrets_manager.get_secret_value(secret_name, default_secret_value) == default_secret_value

## sad path
@pytest.fixture
def non_string_secret_name():
    return 1234

@pytest.mark.parametrize("mock_environ", [mock_dev_environ, mock_production_environ])
def test_get_secret_value_returns_none_on_non_string_secret_name(mock_environ, non_string_secret_name, default_secret_value):
    assert secrets_manager.get_secret_value(non_string_secret_name, default_secret_value) == None

@pytest.fixture
def non_string_default_value():
    return 1

@pytest.mark.parametrize("mock_environ", [mock_dev_environ, mock_production_environ])
def test_get_secret_value_returns_none_on_non_existing_secret_name_and_non_string_default_value(mock_environ, secret_name, non_string_default_value):
    assert secrets_manager.get_secret_value(secret_name, non_string_default_value) == None


## get_secret_int_value

@pytest.fixture
def secret_int_value():
    return 1234

# existing key
def test_get_secret_int_value_returns_existing_secret_int_value_in_dev_environ(mock_dev_environ, secret_name, secret_int_value):
    assert is_dev_environ()
    # arrange
    secret_value = str(secret_int_value)    # as secret_value must be a string
    environ[secret_name] = secret_value
    assert environ[secret_name] == secret_value, "mock error"
    # act
    int_value = secrets_manager.get_secret_int_value(secret_name)
    # assert
    assert int_value == secret_int_value

@mock_secretsmanager
def test_get_secret_int_value_returns_existing_secret_int_value_in_production_environ(mock_production_environ, secret_name, secret_int_value, mock_aws_secrets_manager):
    assert is_production_environ()
    # arrange
    secret_value = str(secret_int_value)
    mock_aws_secrets_manager.create_secret(Name=secret_name, SecretString=secret_value)
    assert mock_aws_secrets_manager.get_secret_value(SecretId=secret_name)['SecretString'] == secret_value, "mock error"
    # act
    int_value = secrets_manager.get_secret_int_value(secret_name)
    # assert
    assert int_value == secret_int_value, f'secrets_manager.get_secret_int_value("{secret_name}") should return {secret_int_value} (got {int_value})'

# non-existing secret name
@pytest.mark.parametrize("mock_environ", [mock_dev_environ, mock_production_environ])
def test_get_secret_int_value_raises_exception_on_non_existing_secret_name(mock_environ, secret_name):
    with pytest.raises(BaseException):
        secrets_manager.get_secret_int_value(secret_name)

@pytest.fixture
def non_int_default_value():
    return "1"

# non-existing secret name and default_value is not int
@pytest.mark.parametrize("mock_environ", [mock_dev_environ, mock_production_environ])
def test_get_secret_int_value_raises_exception_on_non_existing_secret_name_and_non_int_default_value(mock_environ, secret_name, non_int_default_value):
    with pytest.raises(BaseException):
        secrets_manager.get_secret_int_value(secret_name, non_int_default_value)

# non-string secret name
@pytest.mark.parametrize("mock_environ", [mock_dev_environ, mock_production_environ])
def test_get_secret_int_value_raises_exception_on_non_string_secret_name(mock_environ, non_string_secret_name, secret_int_value):
    with pytest.raises(BaseException):
        secrets_manager.get_secret_int_value(non_string_secret_name, secret_int_value)

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
def mock_dev_environ_with_totesys_config_variables():
    with patch.dict("os.environ", mock_totesys_config_env_variables, clear=True) as e:
        set_dev_environ()
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

# existing totesys_config
def test_get_secret_totesys_config_returns_existing_totesys_config_in_dev_environ(totesys_config, mock_dev_environ_with_totesys_config_variables, secret_name): # order of fixtures DOES MATTER here!
    assert is_dev_environ()
    # arrange
    secret_value = json.dumps(totesys_config)
    environ[secret_name] = secret_value
    assert environ[secret_name] == secret_value, "mock error"

    # act
    restored_config = secrets_manager.get_secret_totesys_config(secret_name)
    # assert
    assert totesys_config == restored_config

@mock_secretsmanager
def test_get_secret_totesys_config_returns_existing_totesys_config_in_production_environ(mock_environ_with_totesys_config_variables, secret_name, totesys_config, mock_aws_secrets_manager):
    assert is_production_environ()
    # arrange
    secret_value =json.dumps(totesys_config)
    response = mock_aws_secrets_manager.create_secret(Name=secret_name, SecretString=secret_value)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, "mock error"
    
    # act
    restored_totesys_config = secrets_manager.get_secret_totesys_config(secret_name)
    # assert
    assert restored_totesys_config == totesys_config, f'secrets_manager.get_secret_totesys_config("{secret_name}") should return "{totesys_config}" (got "{restored_totesys_config}")'

# non-existing totesys_config
def test_get_secret_totesys_config_raises_exception_on_non_existing_totesys_config_in_dev(mock_dev_environ, secret_name):
    with pytest.raises(KeyError):
        secrets_manager.get_secret_totesys_config(secret_name)

def test_get_secret_totesys_config_raises_exception_on_non_existing_totesys_config_in_production(mock_production_environ, secret_name):
    with pytest.raises(TypeError):
        secrets_manager.get_secret_totesys_config(secret_name)

# incomplete totesys_config
incomplete_totesys_config_env_variables = {
    "TOTESYS_DB_PASSWORD": "passwd",
    "TOTESYS_DB_HOST": "host",
    "TOTESYS_DB_PORT": "1234",
    "TOTESYS_DB_DATABASE_SCHEMA": "schema_1"
}

@pytest.fixture
def mock_production_environ_with_incomplete_totesys_config_variables():
    with patch.dict("os.environ", incomplete_totesys_config_env_variables, clear=True) as e:
        yield e

@pytest.fixture
def mock_dev_environ_with_incomplete_totesys_config_variables():
    with patch.dict("os.environ", incomplete_totesys_config_env_variables, clear=True) as e:
        set_dev_environ()
        yield e

def test_get_secret_totesys_config_raises_on_incomplete_env_in_dev(mock_dev_environ_with_incomplete_totesys_config_variables, secret_name):
    with pytest.raises(KeyError):
        secrets_manager.get_secret_totesys_config(secret_name)

def test_get_secret_totesys_config_raises_on_incomplete_env_in_production(mock_production_environ_with_incomplete_totesys_config_variables, secret_name):
    with pytest.raises(TypeError):
        secrets_manager.get_secret_totesys_config(secret_name)