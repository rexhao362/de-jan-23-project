from os import environ

dev_environ_variable = "DE_Q2_DEV"
dev_environ_variable_value = "local"

def is_production_environ():
    """
    Checks if the environment is set for production
    
    Returns True if os.environ[dev_environ_variable] is not set and False otherwise
    """

    return dev_environ_variable not in environ