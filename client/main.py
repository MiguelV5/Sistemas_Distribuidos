import logging
from configparser import ConfigParser
import os
from client.client import Client
from shared.logger import initialize_log

def initialize_config():
    config = ConfigParser()
    config.read('config.ini')
    config_params = {}
    try:    
        config_params['server_ip'] = os.getenv('SERVER_IP', config["DEFAULT"]["SERVER_IP"])
        config_params['server_port'] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params['logging_level'] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params['reviews_file'] = os.getenv('REVIEWS_FILE', config["DEFAULT"]["REVIEWS_FILE"])
        config_params['books_file'] = os.getenv('BOOKS_FILE', config["DEFAULT"]["BOOKS_FILE"])    
    except KeyError as e:
        raise KeyError("Key was not found. Error: {}. Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():
    logging.info("Starting client")
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    server_ip = config_params["server_ip"]
    server_port = config_params["server_port"]
    reviews_file = config_params["reviews_file"]
    books_file = config_params["books_file"]

    initialize_log(logging_level)
    client = Client(server_ip, server_port, reviews_file, books_file)
    client.start()
    

if __name__ == "__main__":
    main()
