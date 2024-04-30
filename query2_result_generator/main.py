from shared.initializers import init_log, init_configs
import logging
from generator import Generator

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_AUTHORS", "OUTPUT_QUEUE_OF_QUERY", "FILTERS_QUANTITY"])
    init_log(config_params["LOGGING_LEVEL"])
    logging.info("Q2 results generator started.")
    generator = Generator(config_params["INPUT_EXCHANGE"], 
                          config_params["OUTPUT_EXCHANGE"], 
                          config_params["INPUT_QUEUE_OF_AUTHORS"], 
                          config_params["OUTPUT_QUEUE_OF_QUERY"], 
                          config_params["FILTERS_QUANTITY"])
    generator.start()
    
if __name__ == "__main__":
    main()
    