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
def test_get_existing_secret_value_from_environ_in_dev(dev_mock_environ):
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
def test_get_existing_secret_value_from_environ_in_production(production_mock_environ):
    # arrange
    secret_name = "test_id"
    secret_value = "test_secret"
    secretm = boto3.client('secretsmanager')
    secretm.create_secret(Name=secret_name, SecretString=secret_value)
    # act
    value = secrets_manager.get_secret_value(secret_name)
    # assert
    assert value == secret_value, f'secrets_manager.get_secret_value("{secret_name}") should return "{secret_value}" (got "{value}")'

# non-existing keys
@pytest.fixture
def non_existing_secret_name():
    return "complete_nonsense_101"
    
def test_returns_none_when_passed_unknown_name_dev_environ(dev_mock_environ, non_existing_secret_name):
    assert secrets_manager.get_secret_value(non_existing_secret_name) == None

@mock_secretsmanager
def test_returns_none_when_passed_unknown_name_production_environ(production_mock_environ, non_existing_secret_name):
    assert secrets_manager.get_secret_value(non_existing_secret_name) == None