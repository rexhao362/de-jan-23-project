"""
The module allows to distinguish between production (cloud, “lambda”)
and development (local) modes.
If os.environ[dev_environ_variable] is set,
it is considered as a development environment.
You can use this information in your code to act depending on the mode.

Example:

file = get_file_from_s3() if is_production_environ() else get_local_file()
"""

from os import environ

dev_environ_variable = "DE_Q2_DEV"
dev_environ_variable_value = "local"


def is_production_environ():
    """
    Checks if the environment is set for production

    Returns True if os.environ[dev_environ_variable]
    is not set and False otherwise
    """

    return dev_environ_variable not in environ


def is_dev_environ():
    """
    Checks if the environment is set for development (i.e not production)
    It is a shorthand to "not is_production_environ()"

    Returns True if os.environ[dev_environ_variable] is set and false otherwise
    """

    return not is_production_environ()


def set_dev_environ(value=dev_environ_variable_value):
    """
    Assigns the value to os.environ[dev_environ_variable] key
    """
    environ[dev_environ_variable] = value


def unset_dev_environ():
    """
    Deletes dev_environ_variable from os.environ
    """
    if dev_environ_variable in environ:
        del environ[dev_environ_variable]
