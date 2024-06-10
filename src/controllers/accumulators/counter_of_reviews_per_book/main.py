from shared.initializers import init_configs, init_log
from counter import CounterOfReviewsPerBook
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_REVIEWS", "OUTPUT_QUEUE_OF_REVIEWS", "WORKER_NAME", "WORKER_ID"])
    init_log(config_params["LOGGING_LEVEL"])
    counter = CounterOfReviewsPerBook(config_params["INPUT_EXCHANGE"], 
                                      config_params["OUTPUT_EXCHANGE"], 
                                      config_params["INPUT_QUEUE_OF_REVIEWS"], 
                                      config_params["OUTPUT_QUEUE_OF_REVIEWS"],
                                      config_params["WORKER_NAME"],
                                      int(config_params["WORKER_ID"]))
    counter.start()
    logging.info("Counter started")
    
if __name__ == "__main__":
    main()