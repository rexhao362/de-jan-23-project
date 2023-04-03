# to allow running tests without PYTHONPATH
import sys
sys.path.append('./')
# to allow for flattened lambda file structure
sys.path.append('./src/load/')

import pytest
from src.load.processed_data_loader import _ProcessedDataLoader

@pytest.fixture
def test_path():
    return "test/lambdas/load/input_files"

@pytest.fixture()
def database(postgresql):
    with open("test.sql") as f:
        setup_sql = f.read()
    with postgresql.cursor() as cursor:
        cursor.execute(setup_sql)
        postgresql.commit()
    yield postgresql

# def test_reads_data_and_loads_to_db(test_path):
#     dl = DataLoader()
#     dl.run(test_path)
