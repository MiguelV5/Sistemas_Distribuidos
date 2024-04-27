from shared.initializers import init_log, init_configs
from expander import AuthorExpander
import logging

def main():
    config_params = init_configs(["LOGGING_LEVEL", "INPUT_EXCHANGE", "OUTPUT_EXCHANGE", "INPUT_QUEUE_OF_BOOKS", "NUM_OF_DYN_OUTPUT_QUEUES"])
    output_queues = []
    for i in range(1, int(config_params["NUM_OF_DYN_OUTPUT_QUEUES"]) + 1):
        output_queues.append(f"OUTPUT_QUEUE_OF_AUTHORS_{i}")
    config_params_output_queues = init_configs(output_queues)
    init_log(config_params["LOGGING_LEVEL"])
    
    expander = AuthorExpander(input_exchange=config_params["INPUT_EXCHANGE"],
                                 output_exchange=config_params["OUTPUT_EXCHANGE"],
                                 input_queue_of_books=config_params["INPUT_QUEUE_OF_BOOKS"],
                                 output_queues=config_params_output_queues)
    logging.info("Starting author expander")
    expander.start()
    
if __name__ == "__main__":
    main()
        