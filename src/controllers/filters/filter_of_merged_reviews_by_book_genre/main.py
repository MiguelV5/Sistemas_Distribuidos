from shared.initializers import init_configs, init_log
from filter import FilterReviewByBookGenre
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_REVIEWS", "GENRE", "NUM_OF_INPUT_WORKERS", "NUM_OF_DYN_OUTPUT_QUEUES", "CONTROLLER_NAME"])
    output_queues = []
    for i in range(1, int(config_params["NUM_OF_DYN_OUTPUT_QUEUES"]) + 1):
        output_queues.append(f"OUTPUT_QUEUE_OF_REVIEWS_{i}")
    config_params_output_queues = init_configs(output_queues)
    init_log(config_params["LOGGING_LEVEL"])
    logging.info("Filter of Merged Reviews by Book Genre started.")
    filter = FilterReviewByBookGenre(config_params["INPUT_EXCHANGE"], 
                                     config_params["OUTPUT_EXCHANGE"], 
                                     config_params["INPUT_QUEUE_OF_REVIEWS"], 
                                     config_params_output_queues, 
                                     config_params["GENRE"],
                                     int(config_params["NUM_OF_INPUT_WORKERS"]),
                                     config_params["CONTROLLER_NAME"])
    filter.start()
    
if __name__ == "__main__":
    main()