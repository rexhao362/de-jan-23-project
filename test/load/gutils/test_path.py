from os import path
from unittest.mock import patch
import pytest
# from moto import mock_secretsmanager
from src.load.gutils.environ import set_dev_environ
from src.load.gutils.path import (join, get_bucket_path)

@pytest.fixture
def mock_dev_environ():
    with patch.dict("os.environ", {}, clear=True) as e:
        set_dev_environ()
        yield e

# join
def test_join_returns_correct_value_in_dev(mock_dev_environ):
    data_path = "./local/aws/s3"
    file_name = "dim_currency.parquet"
    assert join(data_path, file_name) == path.join(data_path, file_name)
    