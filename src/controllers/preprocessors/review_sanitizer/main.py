import logging
from review_sanitizer import ReviewSanitizer
from shared.initializers import init_log, init_configs

def main():
    config_params = init_configs(["LOGGING_LEVEL", 
                                  "INPUT_EXCHANGE", 
                                  "OUTPUT_EXCHANGE", 
                                  "INPUT_QUEUE_OF_REVIEWS", 
                                  "NUM_OF_DYN_OUTPUT_QUEUES",
                                  "WORKER_NAME"])
    
    dyn_output_queues_env = []
    for i in range(1, int(config_params["NUM_OF_DYN_OUTPUT_QUEUES"]) + 1):
        dyn_output_queues_env.append(f"OUTPUT_QUEUE_OF_REVIEWS_{i}")
    config_params_dyn_output_queues = init_configs(dyn_output_queues_env)
    dyn_output_queues = [config_params_dyn_output_queues[queue_name_env_key] for queue_name_env_key in dyn_output_queues_env]

    init_log(config_params["LOGGING_LEVEL"])

    logging.info("Starting review_sanitizer")
    review_sanitizer = ReviewSanitizer(input_exchange=config_params["INPUT_EXCHANGE"], 
                                       input_queue=config_params["INPUT_QUEUE_OF_REVIEWS"],
                                       output_exchange=config_params["OUTPUT_EXCHANGE"],
                                       output_queues=dyn_output_queues,
                                       worker_name=config_params["WORKER_NAME"])
    review_sanitizer.start()


if __name__ == "__main__":
    main()