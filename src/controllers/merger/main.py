from shared.initializers import init_log, init_configs
import logging
from merger import Merger

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE_OF_REVIEWS", "INPUT_EXCHANGE_OF_BOOKS", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_REVIEWS", "INPUT_QUEUE_OF_BOOKS","OUTPUT_QUEUE_OF_COMPACT_REVIEWS", "OUTPUT_QUEUE_OF_FULL_REVIEWS", "OUTPUT_QUEUE_OF_BOOKS_CONFIRMS", "CONTROLLER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])
    merger = Merger(config_params["INPUT_EXCHANGE_OF_REVIEWS"], 
                    config_params["INPUT_EXCHANGE_OF_BOOKS"], 
                    config_params["OUTPUT_EXCHANGE"],
                    config_params["INPUT_QUEUE_OF_REVIEWS"], 
                    config_params["INPUT_QUEUE_OF_BOOKS"], 
                    config_params["OUTPUT_QUEUE_OF_COMPACT_REVIEWS"],
                    config_params["OUTPUT_QUEUE_OF_FULL_REVIEWS"],
                    config_params["OUTPUT_QUEUE_OF_BOOKS_CONFIRMS"],
                    config_params["CONTROLLER_NAME"])
    merger.start()


if __name__ == "__main__":
    main()