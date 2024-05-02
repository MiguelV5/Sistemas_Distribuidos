from shared.initializers import init_configs, init_log
import logging
from sorter import Sorter

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "OUTPUT_QUEUE_OF_BOOKS", "TOP_OF_BOOKS"])
    init_log(config_params["LOGGING_LEVEL"])
    logging.info("Sorter of books by review count started")
    sorter = Sorter(input_exchange=config_params["INPUT_EXCHANGE"],
                    output_exchange=config_params["OUTPUT_EXCHANGE"],
                    input_queue=config_params["INPUT_QUEUE_OF_BOOKS"],
                    output_queue=config_params["OUTPUT_QUEUE_OF_BOOKS"],
                    required_top_of_books=config_params["TOP_OF_BOOKS"])
    sorter.start()
    
if __name__ == "__main__":
    main()