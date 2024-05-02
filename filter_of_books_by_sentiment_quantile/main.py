from shared.initializers import init_configs, init_log
from filter import FilterBySentimentQuantile
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "OUTPUT_QUEUE_OF_BOOKS", "QUANTILE"])
    init_log(config_params["LOGGING_LEVEL"])
    logging.info("Filter of Books by Sentiment Quantile started")
    filter = FilterBySentimentQuantile(config_params["INPUT_EXCHANGE"], 
                                       config_params["OUTPUT_EXCHANGE"], 
                                       config_params["INPUT_QUEUE_OF_BOOKS"], 
                                       config_params["OUTPUT_QUEUE_OF_BOOKS"],
                                       float(config_params["QUANTILE"]))
    filter.start()
    
if __name__ == "__main__":
    main()