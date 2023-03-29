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
from src.utils.environ import is_production_environ

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
                - secret not found and default_value is not an int
                - name is not a string
        """
        try:
            secret_value = _SecretsManager.get_secret_value(secret_name)
            string_value = int(secret_value)
        except BaseException:
            if isinstance(default_value, int):
                string_value = int(default_value)
            else:
                raise ValueError( f'unable to retrieve int value associated with secret "{secret_name}"' )

        return string_value

def _create_secrets_manager():
    return _SecretsManager()

secrets_manager = _create_secrets_manager()
del _create_secrets_manager