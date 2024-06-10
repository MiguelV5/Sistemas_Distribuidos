from shared.initializers import init_configs, init_log
from filter import FilterByTitle
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "OUTPUT_QUEUE_OF_BOOKS", "TITLE_KEYWORD", "WORKER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])
    logging.info("Filter of authors by title started.")
    filter = FilterByTitle(config_params["INPUT_EXCHANGE"], 
                           config_params["OUTPUT_EXCHANGE"], 
                           config_params["INPUT_QUEUE_OF_BOOKS"], 
                           config_params["OUTPUT_QUEUE_OF_BOOKS"], 
                           config_params["TITLE_KEYWORD"],
                           config_params["WORKER_NAME"])
    filter.start()
    
if __name__ == "__main__":
    main()