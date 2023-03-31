from src.lambdas.ingestion.utils.dates import create_date_key
from src.lambdas.ingestion.utils.dates import create_date_string
from freezegun import freeze_time


@freeze_time('20-10-2021 14:10:01')
def test_returns_key_string_format_of_current_time():
    result = create_date_key(create_date_string())
    assert result == '2021-10-20/14:10:01'