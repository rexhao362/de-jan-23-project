from os import environ
import boto3
from botocore.exceptions import ClientError
from src.utils.environ import is_production_environ

class _SecretsManager:
    def __init__(self):
        pass

    """
    Gets secret by name.
    Uses AWS Secrets Manager in Production environment and os.environ in Dev

    Returns a string if successfull and None otherwise
    """
    @staticmethod
    def get_secret_value(secret_name):
        secret_value = None
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

def _create_secrets_manager():
    return _SecretsManager()

secrets_manager = _create_secrets_manager()
del _create_secrets_manager