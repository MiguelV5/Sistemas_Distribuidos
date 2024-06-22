from shared.initializers import init_configs, init_log
from filter import FilterOfAuthorsByDecade
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_AUTHORS", "OUTPUT_QUEUE_OF_AUTHORS", "MIN_DECADES_TO_FILTER", "CONTROLLER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])
    filter = FilterOfAuthorsByDecade(config_params["INPUT_EXCHANGE"], 
                                     config_params["OUTPUT_EXCHANGE"], 
                                     config_params["INPUT_QUEUE_OF_AUTHORS"], 
                                     config_params["OUTPUT_QUEUE_OF_AUTHORS"], 
                                     int(config_params["MIN_DECADES_TO_FILTER"]),
                                     config_params["CONTROLLER_NAME"])
    logging.info("Filter of authors by decade started.")
    filter.start()
    
if __name__ == "__main__":
    main()