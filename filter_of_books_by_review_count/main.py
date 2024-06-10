from shared.initializers import init_configs, init_log
import logging
from filter import FilterByReviewsCount

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "OUTPUT_QUEUE_OF_BOOKS_TOWARDS_QUERY3", "OUTPUT_QUEUE_OF_BOOKS_TOWARDS_sorter", "MIN_REVIEWS", "NUM_OF_COUNTERS", "WORKER_NAME"])
    init_log(config_params["LOGGING_LEVEL"])
    filter = FilterByReviewsCount(input_exchange=config_params["INPUT_EXCHANGE"], 
                                  output_exchange=config_params["OUTPUT_EXCHANGE"], 
                                  input_queue=config_params["INPUT_QUEUE_OF_BOOKS"], 
                                  output_queue_towards_query3=config_params["OUTPUT_QUEUE_OF_BOOKS_TOWARDS_QUERY3"], output_queue_towards_sorter=config_params["OUTPUT_QUEUE_OF_BOOKS_TOWARDS_sorter"], min_reviews=config_params["MIN_REVIEWS"],
                                  num_of_counters=config_params["NUM_OF_COUNTERS"],
                                  worker_name=config_params["WORKER_NAME"])
    filter.start()
    logging.info("Filter of books by review count started")

if __name__ == "__main__":
    main()