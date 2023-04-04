import json
from gutils.environ import set_dev_environ, unset_dev_environ
from gutils.secrets_manager import secrets_manager 

totesys_database_config_name = "TOTESYS_DB_CONFIG"
warehouse_database_config_name = "WAREHOUSE_DB_CONFIG"

# run your export_production_settings.sh first!
set_dev_environ()

totesys_database_config = {
    "credentials": {
        "user": secrets_manager.get_secret_value("TOTESYS_DB_USER"),
        "password": secrets_manager.get_secret_value("TOTESYS_DB_PASSWORD"),
        "host": secrets_manager.get_secret_value("TOTESYS_DB_HOST"),
        "port": secrets_manager.get_secret_int_value("TOTESYS_DB_PORT", 5432),
        "database": secrets_manager.get_secret_value("TOTESYS_DB_DATABASE"),
    },
    "schema": secrets_manager.get_secret_value("TOTESYS_DB_DATABASE_SCHEMA")
}

warehouse_database_config = {
    "credentials": {
        "user": secrets_manager.get_secret_value("WAREHOUSE_DB_USER"),
        "password": secrets_manager.get_secret_value("WAREHOUSE_DB_PASSWORD"),
        "host": secrets_manager.get_secret_value("WAREHOUSE_DB_HOST"),
        "port": secrets_manager.get_secret_int_value("WAREHOUSE_DB_PORT", 5432),
        "database": secrets_manager.get_secret_value("WAREHOUSE_DB_DATABASE"),
    },
    "schema": secrets_manager.get_secret_value("WAREHOUSE_DB_DATABASE_SCHEMA")
}


warehouse_database_config_json = json.dumps(warehouse_database_config)

unset_dev_environ()

# create new secret in AWS SecretsManager
def create_and_test_secret(name, value):
    print( f'creating secret {name}={value} ..' )
    value_json=json.dumps(value)
    response = secretm.create_secret(Name=name, SecretString=value_json)
    print("create_secret responded:\n", response)

    secret_json = secrets_manager.get_secret_value(name)
    secret = json.loads(secret_json)

    print( f'{name}: {secret}' )
    assert value == secret

import boto3

secretm = boto3.client('secretsmanager')
create_and_test_secret(totesys_database_config_name, totesys_database_config)
create_and_test_secret(warehouse_database_config_name, warehouse_database_config)




