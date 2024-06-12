import logging
from decade_preprocessor import DecadePreprocessor
from shared.initializers import init_log, init_configs

def main():
    config_params = init_configs(["LOGGING_LEVEL", 
                                  "INPUT_EXCHANGE", 
                                  "OUTPUT_EXCHANGE", 
                                  "INPUT_QUEUE_OF_BOOKS", 
                                  "OUTPUT_QUEUE_OF_BOOKS_TOWARDS_EXPANDER", 
                                  "NUM_OF_DYN_OUTPUT_QUEUES",
                                  "CONTROLLER_NAME"])
    dyn_output_queues_env = []
    for i in range(1, int(config_params["NUM_OF_DYN_OUTPUT_QUEUES"]) + 1):
        dyn_output_queues_env.append(f"OUTPUT_QUEUE_OF_BOOKS_{i}")
    config_params_dyn_output_queues = init_configs(dyn_output_queues_env)
    dyn_output_queues = [config_params_dyn_output_queues[queue_name_env_key] for queue_name_env_key in dyn_output_queues_env]

    init_log(config_params["LOGGING_LEVEL"])
    
    logging.info("Starting decade_preprocessor")
    decade_preprocessor = DecadePreprocessor(input_exchange=config_params["INPUT_EXCHANGE"], 
                                             input_queue=config_params["INPUT_QUEUE_OF_BOOKS"],
                                             output_exchange=config_params["OUTPUT_EXCHANGE"],
                                             output_queue_towards_expander=config_params["OUTPUT_QUEUE_OF_BOOKS_TOWARDS_EXPANDER"],
                                             output_queues_towards_mergers=dyn_output_queues,
                                             controller_name=config_params["CONTROLLER_NAME"])
    decade_preprocessor.start()


if __name__ == "__main__":
    main()