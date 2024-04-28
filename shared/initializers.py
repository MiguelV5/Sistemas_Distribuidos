import logging
import os
from configparser import ConfigParser

def init_log(logging_level: str):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

def init_configs(env_vars_to_collect: list[str]):
    """
    Collects the environment variables and returns them in a dictionary

    :param env_vars_to_collect: list of environment variables to collect
    :return: dictionary with the collected environment variables
    """
    try:
        parser = ConfigParser(os.environ)
        config_params = {}
        for env_var in env_vars_to_collect:
            config_params[env_var] = parser["DEFAULT"][env_var]
    except KeyError as e:
        raise KeyError("Key was not found. Error: {}".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}".format(e))
    return config_params
