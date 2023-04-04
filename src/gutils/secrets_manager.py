"""
usage:
from gutils.secrets_manager import secrets_manager

user = secrets_manager.get_secret_value("WAREHOUSE_DB_USER")
if user:
    # do something
else:
    raise Exception("cannot retrieve secret 'user'")

port = secrets_manager.get_secret_int_value('WAREHOUSE_DB_PORT', 5432)

db_config = secrets_manager.get_secret_totesys_db_config()
if db_config:
    with Connection(**db_config["credentials"]) as connection

"""

from os import environ
import boto3
from botocore.exceptions import ClientError
import json
from gutils.environ import is_production_environ

project_secrets = {
        "totesys_database_config": {
            "name": "TOTESYS_DB_CONFIG",
            "variables": {
                "user": "TOTESYS_DB_USER",
                "password": "TOTESYS_DB_PASSWORD",
                "host": { "name": "TOTESYS_DB_HOST", "default": "localhost"},
                "port": { "name": "TOTESYS_DB_PORT", "default": 5432},
                "database": "TOTESYS_DB_DATABASE",
                "schema": "TOTESYS_DB_DATABASE_SCHEMA"
            }
        },
        "warehouse_database_config": {
            "name": "WAREHOUSE_DB_CONFIG",
            "variables": {
                "user": "WAREHOUSE_DB_USER",
                "password": "WAREHOUSE_DB_PASSWORD",
                "host": { "name": "WAREHOUSE_DB_HOST", "default": "localhost"},
                "port": { "name": "WAREHOUSE_DB_PORT", "default": 5432},
                "database": "WAREHOUSE_DB_DATABASE",
                "schema": "WAREHOUSE_DB_DATABASE_SCHEMA"
            }
        }
    }

class _SecretsManager:
    """
    Gets secret by name.
    Uses AWS Secrets Manager in Production environment and os.environ in Dev
    """

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
    def get_secret_database_config(database_config_decription=None):
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

            or None if unseccessful (no description provided, secret_name does not exist, config is incomplete)
        """
        if not database_config_decription:
            return None

        if is_production_environ():
            config_json = _SecretsManager.get_secret_value( database_config_decription["name"] )
            return None if config_json is None else json.loads(config_json)
        else:
            try:
                vars = database_config_decription["variables"]
                config = {
                    "credentials": {
                        "user": environ[ vars["user"] ],
                        "password": environ[ vars["password"] ],
                        "host": environ.get( vars["host"]["name"],  vars["host"]["default"] ),
                        "port": int( environ.get( vars["port"]["name"],  vars["port"]["default"] )),
                        "database": environ[ vars["database"] ],
                    },
                    "schema": environ[ vars["schema"] ]
                }
            except:
                config = None
            return config
    
    @staticmethod
    def get_secret_totesys_db_config():
        return _SecretsManager.get_secret_database_config( project_secrets["totesys_database_config"] )

    @staticmethod
    def get_secret_warehouse_db_config():
        return _SecretsManager.get_secret_database_config( project_secrets["warehouse_database_config"] )


secrets_manager = _SecretsManager()