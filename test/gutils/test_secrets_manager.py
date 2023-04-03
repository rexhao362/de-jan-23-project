# to allow running tests without PYTHONPATH
import sys
sys.path.append('./src')

from os import environ
import json
from unittest.mock import patch
import pytest
import boto3
from moto import mock_secretsmanager
from botocore.exceptions import ClientError
from gutils.environ import \
    set_dev_environ, \
    is_dev_environ, \
    is_production_environ

from gutils.secrets_manager import secrets_manager, project_secrets

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

## --- get_secret_database_config
# dev

# no database description provided
def test_get_secret_database_config_returns_none_on_none_in_dev(mock_dev_environ):
    assert secrets_manager.get_secret_database_config() is None

mock_database_config_env_variables = {
    "DB_USER": "user",
    "DB_PASSWORD": "passwd",
    "DB_HOST": "host",
    "DB_PORT": "1234",
    "DB_DATABASE": "db",
    "DB_DATABASE_SCHEMA": "schema_1"
}

@pytest.fixture
def mock_dev_environ_with_database_config_env_variables():
    with patch.dict("os.environ", mock_database_config_env_variables, clear=True) as e:
        set_dev_environ()
        yield e

@pytest.fixture
def mock_database_config_description():
    return {
        "name": "DB_CONFIG",
        "variables": {
            "user": "DB_USER",
            "password": "DB_PASSWORD",
            "host": { "name": "DB_HOST", "default": "localhost"},
            "port": { "name": "DB_PORT", "default": 5432},
            "database": "DB_DATABASE",
            "schema": "DB_DATABASE_SCHEMA"
        }
    }

@pytest.fixture
def mock_database_config(mock_dev_environ_with_database_config_env_variables):
    return {
        "credentials": {
            "user": environ["DB_USER"],
            "password": environ["DB_PASSWORD"],
            "host": environ["DB_HOST"],
            "port": int( environ["DB_PORT"] ),
            "database": environ["DB_DATABASE"],
        },
        "schema": environ["DB_DATABASE_SCHEMA"]
    }

# existing database_config
def test_get_secret_database_config_returns_existing_database_config_in_dev_environ(
    mock_dev_environ_with_database_config_env_variables, mock_database_config_description, mock_database_config
    ):
    assert is_dev_environ()
    # arrange
    secret_name = mock_database_config_description["name"]
    secret_value = json.dumps(mock_database_config)
    environ[secret_name] = secret_value
    assert environ[secret_name] == secret_value, "mock error"

    # act
    restored_database_config = secrets_manager.get_secret_database_config(mock_database_config_description)
    # assert
    assert restored_database_config == mock_database_config

# incomplete database_config
incomplete_database_config_env_variables = {
    "DB_PASSWORD": "passwd",
    "DB_HOST": "host",
    "DB_PORT": "1234",
    "DB_DATABASE_SCHEMA": "schema_1"
}

@pytest.fixture
def mock_dev_environ_with_incomplete_database_config_env_variables():
    with patch.dict("os.environ", incomplete_database_config_env_variables, clear=True) as e:
        set_dev_environ()
        yield e

@pytest.fixture
def mock_incomplete_database_config_description():
    return {
        "name": "DB_CONFIG",
        "variables": {
            "password": "DB_PASSWORD",
            "host": { "name": "DB_HOST", "default": "localhost"},
            "port": { "name": "DB_PORT", "default": 5432},
            "database": "DB_DATABASE",
            "schema": "DB_DATABASE_SCHEMA"
        }
    }

@pytest.fixture
def mock_incomplete_database_config(mock_environ_with_database_config_variables):
    return {
        "credentials": {
            "password": environ["DB_PASSWORD"],
            "host": environ["DB_HOST"],
            "port": int( environ["DB_PORT"] ),
            "database": environ["DB_DATABASE"],
        },
        "schema": environ["DB_DATABASE_SCHEMA"]
    }


def _test_get_secret_database_config_returns_none_on_incomplete_config_variables_in_dev(
    mock_dev_environ_with_incomplete_database_config_env_variables
    ):
    assert secrets_manager.get_secret_database_config(mock_dev_environ_with_incomplete_database_config_env_variables) is None


## --- get_secret_database_config 
# production

# no database description provided
def test_get_secret_database_config_returns_none_on_none_in_production(mock_production_environ):
    assert secrets_manager.get_secret_database_config() is None

# existing config name
@mock_secretsmanager
def _test_get_secret_database_config_returns_existing_database_config_in_production_environ(
    mock_production_environ, mock_aws_secrets_manager,
    mock_database_config_description, mock_database_config
    ):
    assert is_production_environ()
    # arrange
    secret_name = mock_database_config_description["name"]
    secret_value =json.dumps(mock_database_config)
    response = mock_aws_secrets_manager.create_secret(Name=secret_name, SecretString=secret_value)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, "mock error"
    
    # act
    restored_database_config = secrets_manager.get_secret_database_config(mock_database_config_description)
    # assert
    assert restored_database_config == mock_database_config
    print( f'mock_database_config={mock_database_config}')
    print( f'restored_database_config={restored_database_config}')

# unknown name
@mock_secretsmanager
def _test_get_secret_database_config_returns_none_on_unknown_database_config_name_in_production(mock_production_environ, mock_database_config_description):
    assert secrets_manager.get_secret_database_config(mock_database_config_description) is None

@mock_secretsmanager
def _test_get_secret_database_config_returns_none_on_incomplete_database_config_in_production(
    mock_production_environ, mock_database_config, mock_incomplete_database_config_description
    ):
    # arrange
    secret_value =json.dumps(mock_database_config)
    secret_name = mock_database_config_description["name"]
    response = mock_aws_secrets_manager.create_secret(Name=secret_name, SecretString=secret_value)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, "mock error"
    
    # act
    restored_database_config = secrets_manager.get_secret_database_config(mock_database_config_description)
    # assert
    assert secrets_manager.get_secret_database_config(mock_production_environ_with_incomplete_database_config_variables) is None


## --- get_secret_totesys_db_config
@pytest.mark.parametrize( "mock_environ", [mock_dev_environ, mock_production_environ] )
def _test_get_secret_totesys_db_config_returns_the_same_as_get_secret_database_config(mock_environ):
    assert secrets_manager.get_secret_totesys_db_config() == secrets_manager.get_secret_database_config( project_secrets["totesys_database_config"] )

## --- get_secret_warehouse_db_config
@pytest.mark.parametrize( "mock_environ", [mock_dev_environ, mock_production_environ] )
def _test_get_secret_totesys_db_config_returns_the_same_as_get_secret_database_config(mock_environ):
    assert secrets_manager.get_secret_warehouse_db_config() == secrets_manager.get_secret_database_config( project_secrets["warehouse_database_config"] )
