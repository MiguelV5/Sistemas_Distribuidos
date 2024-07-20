from shared.initializers import init_configs, init_log
from counter import CounterOfReviewsPerBook
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_REVIEWS", "OUTPUT_QUEUE_OF_REVIEWS", "BATCH_SIZE", "CONTROLLER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])
    counter = CounterOfReviewsPerBook(config_params["INPUT_EXCHANGE"], 
                                      config_params["OUTPUT_EXCHANGE"], 
                                      config_params["INPUT_QUEUE_OF_REVIEWS"], 
                                      config_params["OUTPUT_QUEUE_OF_REVIEWS"],
                                      int(config_params["BATCH_SIZE"]),
                                      config_params["CONTROLLER_NAME"])
    counter.start()
    
if __name__ == "__main__":
    main()