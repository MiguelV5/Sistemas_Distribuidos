import logging
from review_sanitizer import ReviewSanitizer
from shared.initializers import init_log, init_configs, init_dyn_configs

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_REVIEWS", "NUM_OF_DYN_OUTPUT_QUEUES"])
    prefix = "OUTPUT_QUEUE_OF_REVIEWS"
    dyn_config_params = init_dyn_configs(config_params, config_params["NUM_OF_DYN_OUTPUT_QUEUES"], prefix)

    init_log(config_params["LOGGING_LEVEL"])

    logging.info("Starting review_sanitizer")
    review_sanitizer = ReviewSanitizer(input_exchange=config_params["INPUT_EXCHANGE"], 
                                       input_queue=config_params["INPUT_QUEUE_OF_BOOKS"],
                                       output_exchange=config_params["OUTPUT_EXCHANGE"],
                                       output_queues=[dyn_config_params[f"{prefix}_{i}"] for i in range(1, config_params["NUM_OF_DYN_OUTPUT_QUEUES"] + 1)])
    review_sanitizer.start()


if __name__ == "__main__":
    main()