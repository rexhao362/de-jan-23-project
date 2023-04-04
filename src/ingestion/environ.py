import json
import pg8000.native
from os import environ
from src.utils.secrets_manager import secrets_manager
from src.utils.environ import is_production_environ


def env_variables():
    if is_production_environ():
        totesys_credentials_json = secrets_manager.get_secret_value(
            'TOTESYS_CREDENTIALS')
        return json.loads(totesys_credentials_json)
    else:
        return {
            'TOTESYS_DB_USER': environ.get('TOTESYS_DB_USER'),
            'TOTESYS_DB_PASSWORD': environ.get('TOTESYS_DB_PASSWORD'),
            'TOTESYS_DB_HOST': environ.get('TOTESYS_DB_HOST', 'localhost'),
            'TOTESYS_DB_PORT': environ.get('TOTESYS_DB_PORT', 5432),
            'TOTESYS_DB_DATABASE': environ.get('TOTESYS_DB_DATABASE'),
            'TOTESYS_DB_SCHEMA': environ.get('TOTESYS_DB_DATABASE_SCHEMA')
        }


totesys_credentials = env_variables()
# DB connection
con = pg8000.native.Connection(
    user=totesys_credentials['TOTESYS_DB_USER'],
    host=totesys_credentials['TOTESYS_DB_HOST'],
    database=totesys_credentials['TOTESYS_DB_DATABASE'],
    port=int(totesys_credentials['TOTESYS_DB_PORT']),
    password=totesys_credentials['TOTESYS_DB_PASSWORD'],
)


schema = totesys_credentials['TOTESYS_DB_SCHEMA']
