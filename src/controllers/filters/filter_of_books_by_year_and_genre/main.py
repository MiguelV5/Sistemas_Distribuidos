from shared.initializers import init_configs, init_log
from filter import FilterByGenreAndYear
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "OUTPUT_QUEUE_OF_BOOKS", "MIN_YEAR", "MAX_YEAR", "GENRE", "CONTROLLER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])
    filter = FilterByGenreAndYear(config_params["INPUT_EXCHANGE"], 
                                  config_params["OUTPUT_EXCHANGE"], 
                                  config_params["INPUT_QUEUE_OF_BOOKS"], 
                                  config_params["OUTPUT_QUEUE_OF_BOOKS"], 
                                  config_params["MIN_YEAR"], 
                                  config_params["MAX_YEAR"],
                                  config_params["GENRE"],
                                  config_params["CONTROLLER_NAME"])
    filter.start()
    
if __name__ == "__main__":
    main()