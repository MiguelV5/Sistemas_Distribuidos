from shared.initializers import init_configs, init_log
from filter import FilterReviewByBookGenre
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_REVIEWS", "OUTPUT_QUEUE_OF_REVIEWS", "GENRE", "NUM_OF_INPUT_WORKERS"])
    init_log(config_params["LOGGING_LEVEL"])
    logging.info("Filter of Merged Reviews by Book Genre started.")
    filter = FilterReviewByBookGenre(config_params["INPUT_EXCHANGE"], 
                                     config_params["OUTPUT_EXCHANGE"], 
                                     config_params["INPUT_QUEUE_OF_REVIEWS"], 
                                     config_params["OUTPUT_QUEUE_OF_REVIEWS"], 
                                     config_params["GENRE"],
                                     config_params["NUM_OF_INPUT_WORKERS"])
    filter.start()
    
if __name__ == "__main__":
    main()