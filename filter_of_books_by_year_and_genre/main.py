from shared.initializers import init_configs, init_log
from filter import FilterByGenreAndYear
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "OUTPUT_QUEUE_OF_BOOKS", "MIN_YEAR", "MAX_YEAR", "GENRE"])
    init_log(config_params["LOGGING_LEVEL"])
    logging.info("Filter of books by year and genre started.")
    filter = FilterByGenreAndYear(config_params["INPUT_EXCHANGE"], 
                                  config_params["OUTPUT_EXCHANGE"], 
                                  config_params["INPUT_QUEUE_OF_BOOKS"], 
                                  config_params["OUTPUT_QUEUE_OF_BOOKS"], 
                                  config_params["MIN_YEAR"], 
                                  config_params["MAX_YEAR"],
                                  config_params["GENRE"])
    filter.start()
    
if __name__ == "__main__":
    main()