import logging
from configparser import ConfigParser
import os
from shared.logger import initialize_log
from server.server import Server

def initialize_config():
    config = ConfigParser()
    config.read('config.ini')
    config_params = {}
    try:
        config_params['logging_level'] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"]) 
        config_params['server_ip'] = os.getenv('SERVER_IP', config["DEFAULT"]["SERVER_IP"])
        config_params['server_port'] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params['input_exchange'] = os.getenv('INPUT_EXCHANGE', config["DEFAULT"]["INPUT_EXCHANGE"]) 
        config_params['output_exchange_of_reviews'] = os.getenv('OUTPUT_EXCHANGE_OF_REVIEWS', config["DEFAULT"]["OUTPUT_EXCHANGE_OF_REVIEWS"]) 
        config_params['output_exchange_of_books'] = os.getenv('OUTPUT_EXCHANGE_OF_BOOKS', config["DEFAULT"]["OUTPUT_EXCHANGE_OF_BOOKS"]) 
        config_params['output_queue_of_reviews'] = os.getenv('OUTPUT_QUEUE_OF_REVIEWS', config["DEFAULT"]["OUTPUT_QUEUE_OF_REVIEWS"]) 
        config_params['output_queue_of_books'] = os.getenv('OUTPUT_QUEUE_OF_BOOKS', config["DEFAULT"]["OUTPUT_QUEUE_OF_BOOKS"]) 
        config_params['input_queue_of_query_results'] = os.getenv('INPUT_QUEUE_OF_QUERY_RESULTS', config["DEFAULT"]["INPUT_QUEUE_OF_QUERY_RESULTS"]) 
        
    except KeyError as e:
        raise KeyError("Key was not found. Error: {}. Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():
    logging.info("Starting server")
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    server_ip = config_params["server_ip"]
    server_port = config_params["server_port"]
    input_exchange = config_params["input_exchange"]
    output_exchange_of_reviews = config_params["output_exchange_of_reviews"]
    output_exchange_of_books = config_params["output_exchange_of_books"]
    output_queue_of_reviews = config_params["output_queue_of_reviews"]
    output_queue_of_books = config_params["output_queue_of_books"]
    input_queue_of_query_results = config_params["input_queue_of_query_results"]

    initialize_log(logging_level)
    server = Server(server_ip, server_port, input_exchange, output_exchange_of_reviews, output_exchange_of_books, output_queue_of_reviews, output_queue_of_books, input_queue_of_query_results)
    server.run()
    

if __name__ == "__main__":
    main()
