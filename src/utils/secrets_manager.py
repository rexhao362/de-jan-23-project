"""
usage:
from src.utils.secrets_manager import secrets_manager

user = secrets_manager.get_secret_value("WAREHOUSE_DB_USER")
port = secrets_manager.get_secret_int_value('WAREHOUSE_DB_PORT', 5432)
if user:
    # do something
else:
    raise Exception("cannot retrieve secret 'user'")
"""

from os import environ
import boto3
from botocore.exceptions import ClientError
import json
from src.utils.environ import is_production_environ

class _SecretsManager:
    """
    Gets secret by name.
    Uses AWS Secrets Manager in Production environment and os.environ in Dev
    """
    project_secrets = {
        "totesys_database_config": "TOTESYS_DB_CONFIG",
        "warehouse_database_config": "WAREHOUSE_DB_CONFIG"
    }

    # def __init__(self):
    #     self.project_secrets = {
    #         "totesys_database_config": "TOTESYS_DB_CONFIG",
    #         "warehouse_database_config": "WAREHOUSE_DB_CONFIG"
    #     }

    @staticmethod
    def get_secret_value(secret_name, default_value=None):
        """
        Retrieves a string secret by its name.

        Args:
            param1: secret_name, string.
            param2: default_value, string.

        Returns:
            Returns a string value associated with the secret name if defined.
            If not defined and no default_value specified, returns None, otherwise default_value.
        """
        _invalid_result = None
        if not isinstance(secret_name, str):
            return _invalid_result

        secret_value = default_value if isinstance(default_value, str) else _invalid_result
        if is_production_environ():
            try:
                secretm = boto3.client('secretsmanager')
                response = secretm.get_secret_value(SecretId=secret_name)
                secret_value = response['SecretString']
            except:
                pass
        elif secret_name in environ:
            secret_value = environ[secret_name]

        return secret_value

    @staticmethod
    def get_secret_int_value(secret_name, default_value=None):
        """
        Retrieves an integer by its name.

        Args:
            param1: secret_name, string.
            param2: default_value, int.

        Returns:
            Returns an integer value associated with the secret name if defined.

        Raises:
            ValueError if:
                - secret not found
                
            TypeError if:
                - name is not a string
                - secret not found and default_value is not an int
        """
        if not isinstance(secret_name, str):
            msg = f'secret_name should be a string (got {secret_name} ({ type(secret_name) })'
            raise TypeError(msg)
        
        secret_value = _SecretsManager.get_secret_value(secret_name)

        if isinstance(secret_value, str):
            try:
                string_value = int(secret_value)
            except:
                msg = f'cannot convert "{secret_value}" to int'
                raise TypeError(msg)
        else:
            if isinstance(default_value, int):
                string_value = int(default_value)
            else:
                raise ValueError( f'unable to retrieve int value associated with secret "{secret_name}"' )

        return string_value

    @staticmethod
    def get_secret_totesys_config(secret_name=None):
        """
        A wrapper to get the whole configuration in one go.

        Args:
            param1: secret_name, string.

        Returns:
            A dictionary of the following structure:
            {
                "credentials": {
                    "user": string,
                    "password": string,
                    "host": string,
                    "port": int,
                    "database": string,
                },
                "schema"; string
            }

        Raises:
            KeyError in dev, TypeError in production:
                - if secret_name does not exist
                - totesys_config is incomplete
        """
        #print( f'get_secret_totesys_config({secret_name}: is_production_environ() = {is_production_environ()}' )
        if is_production_environ():
            config_json = _SecretsManager.get_secret_value(secret_name)
            config = json.loads(config_json)
            #print( f"get_secret_totesys_config(production) = {config}")
            return config
        else:
            key = "TOTESYS_DB_HOST"
            host = environ[key] if key in environ else "localhost"
            key = "TOTESYS_DB_PORT"
            port = environ[key] if key in environ else "5432"
            config =  {
                "credentials": {
                    "user": environ["TOTESYS_DB_USER"],
                    "password": environ["TOTESYS_DB_PASSWORD"],
                    "host": host,
                    "port": int(port),
                    "database": environ["TOTESYS_DB_DATABASE"],
                },
                "schema": environ["TOTESYS_DB_DATABASE_SCHEMA"]
            }
            #print( f"get_secret_totesys_config(dev) = {config}")
            return config

secrets_manager = _SecretsManager()