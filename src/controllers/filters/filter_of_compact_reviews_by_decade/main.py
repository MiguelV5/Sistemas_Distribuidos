from shared.initializers import init_configs, init_log
from filter import FilterOfCompactReviewsByDecade
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE","INPUT_QUEUE_OF_REVIEWS", "NUM_OF_DYN_OUTPUT_QUEUES", "DECADE_TO_FILTER","NUM_OF_INPUT_WORKERS", "CONTROLLER_NAME"])
    output_queues = []
    for i in range(1, int(config_params["NUM_OF_DYN_OUTPUT_QUEUES"]) + 1):
        output_queues.append(f"OUTPUT_QUEUE_OF_REVIEWS_{i}")
    config_params_output_queues = init_configs(output_queues)
    init_log(config_params["LOGGING_LEVEL"])
    filter = FilterOfCompactReviewsByDecade(input_exchange=config_params["INPUT_EXCHANGE"],
                                            output_exchange=config_params["OUTPUT_EXCHANGE"],
                                            input_queue_of_reviews=config_params["INPUT_QUEUE_OF_REVIEWS"],
                                            output_queues=config_params_output_queues,
                                            decade_to_filter=int(config_params["DECADE_TO_FILTER"]),
                                            num_of_input_workers=int(config_params["NUM_OF_INPUT_WORKERS"]),
                                            controller_name=config_params["CONTROLLER_NAME"])
    logging.info("Starting filter")
    filter.start()
    
if __name__ == "__main__":
    main()
    